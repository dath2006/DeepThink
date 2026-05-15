"""
Persistent Context Engine — top-level class.

Benchmark interface
-------------------
  engine.ingest(events: Iterable[dict]) -> None
  engine.reconstruct_context(signal: dict, mode: str) -> Context
  engine.close() -> None

Architecture (Phase 1)
----------------------
  ┌────────────────────────────────────────────────┐
  │  Engine                                        │
  │                                                │
  │  ingest()  ──►  IngestCoordinator              │
  │                     │                          │
  │                     ├──► EventParser           │
  │                     ├──► DatabaseManager       │
  │                     │       ├── RawEventStore  │
  │                     │       ├── NodeStore  ◄───┼── in-memory name→UUID index
  │                     │       └── EdgeStore      │
  │                     ├──► GraphManager  ◄───────┼── NetworkX DiGraph (UUID keys)
  │                     └──► RecentEventsBuffer    │
  │                                                │
  │  reconstruct_context()  ──►  (Phase 1 stub)    │
  │    reads from buffer + DuckDB                  │
  └────────────────────────────────────────────────┘

Phase 1 delivers the full ingest pipeline (parsing, storage, graph
construction, topology mutation).  Context reconstruction returns a
minimal-but-valid Context using time-windowed event retrieval.
Causal inference, incident matching, and remediation suggestion are
Phase 2+ concerns.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from .config import EngineConfig
from .schema import Context, CausalEdge, IncidentMatch, Remediation, EventKind
from .storage.database import DatabaseManager
from .storage.raw_store import RawEventStore
from .storage.node_store import NodeStore
from .storage.edge_store import EdgeStore
from .graph.manager import GraphManager
from .ingestion.coordinator import IngestCoordinator
from .ingestion.buffer import RecentEventsBuffer
from .ingestion.parser import EventParser

log = logging.getLogger(__name__)


class Engine:
    """
    Persistent Context Engine.

    Instantiate once per benchmark run.  The Engine owns all state:
    DuckDB connection, NetworkX graph, and in-memory ring buffer.
    """

    def __init__(self, config: Optional[EngineConfig] = None) -> None:
        self._cfg = config or EngineConfig()

        # Storage layer — single DuckDB connection
        self._db        = DatabaseManager(self._cfg.db_path)
        self._db.connect()

        self._raw_store  = RawEventStore(self._db)
        self._node_store = NodeStore(self._db)
        self._edge_store = EdgeStore(self._db)

        # In-memory graph
        self._graph  = GraphManager()

        # Hot buffer
        self._buffer = RecentEventsBuffer(self._cfg.buffer_size)

        # Ingest pipeline
        self._coordinator = IngestCoordinator(
            db         = self._db,
            raw_store  = self._raw_store,
            node_store = self._node_store,
            edge_store = self._edge_store,
            graph      = self._graph,
            buffer     = self._buffer,
        )

        # Rebuild NetworkX from DuckDB (crash recovery / warm-start)
        self._rebuild_graph()
        log.info("Engine initialised (db=%s)", self._cfg.db_path)

    # ------------------------------------------------------------------
    # Benchmark interface
    # ------------------------------------------------------------------

    def ingest(self, events: Iterable[Any]) -> None:
        """
        Ingest an iterable of events.

        Each event may be:
          * dict  — already-parsed (typical benchmark path)
          * str   — raw JSONL line
        """
        self._coordinator.ingest_many(events)

    def reconstruct_context(
        self,
        signal: Dict[str, Any],
        mode: str = "fast",
    ) -> Context:
        """
        Reconstruct operational context for an incident signal.

        Phase 1 implementation
        ----------------------
        Returns time-windowed related events from the ring buffer (fast
        path) falling back to DuckDB, with empty causal_chain /
        similar_past_incidents / suggested_remediations.

        Phase 2+ will add:
          * Causal inference from graph traversal
          * Incident matching via behavioural fingerprints
          * Remediation suggestion from historical outcomes
        """
        parser = EventParser()
        try:
            normalised_signal = parser.normalise(signal)
        except Exception:
            normalised_signal = dict(signal)
            normalised_signal.setdefault("ts", datetime.now(timezone.utc))

        incident_ts: datetime = normalised_signal.get("ts", datetime.now(timezone.utc))
        if isinstance(incident_ts, str):
            incident_ts = parser._parse_timestamp(incident_ts)

        window = timedelta(minutes=self._cfg.context_window_minutes)
        start_ts = incident_ts - window
        end_ts   = incident_ts + window   # symmetric: also capture post-incident resolution

        max_events = (
            self._cfg.fast_mode_max_events
            if mode == "fast"
            else self._cfg.deep_mode_max_events
        )

        # --- Identify relevant services and expand via graph ----------
        anchor_services: List[str] = []
        svc = normalised_signal.get("service")
        if svc:
            anchor_services.append(svc)
        else:
            trigger: str = normalised_signal.get("trigger", "")
            anchor_services = self._extract_services_from_trigger(trigger)
        expanded_services = self._expand_services(anchor_services)

        # --- Fast path: ring buffer ----------------------------------
        related: List[Dict[str, Any]] = []
        if expanded_services:
            related = self._buffer.for_services(expanded_services, start_ts, end_ts)
        # Supplement or fall back to full window scan
        if len(related) < max_events:
            window_events = self._buffer.in_window(start_ts, end_ts)
            seen_ids: set = {id(e) for e in related}
            for e in window_events:
                if id(e) not in seen_ids:
                    related.append(e)
                    seen_ids.add(id(e))

        # --- Slow path: DuckDB fallback if buffer window is cold -----
        if not related:
            rows = self._raw_store.get_by_timerange(
                start_ts, end_ts, limit=max_events
            )
            related = [r["raw"] for r in rows]

        # Deduplicate by raw event dict identity (use id if present)
        seen_ids: set = set()
        deduped: List[Dict[str, Any]] = []
        for ev in related:
            eid = ev.get("id") or id(ev)
            if eid not in seen_ids:
                seen_ids.add(eid)
                deduped.append(ev)
            if len(deduped) >= max_events:
                break

        # Sort by timestamp ascending
        def _sort_key(e):
            ts = e.get("ts")
            if isinstance(ts, datetime):
                return ts
            try:
                return EventParser._parse_timestamp(ts)
            except Exception:
                return incident_ts

        deduped.sort(key=_sort_key)

        # Normalise ts back to ISO string in every event so related_events
        # always matches the benchmark's Event TypedDict (ts: str).
        deduped = [self._normalise_event_for_output(e) for e in deduped]

        # Build explain narrative
        explain = self._build_explain(
            normalised_signal, deduped, start_ts, end_ts
        )

        confidence = min(0.3 + 0.05 * len(deduped), 0.5) if deduped else 0.0

        return Context(
            related_events         = deduped,
            causal_chain           = [],         # Phase 2+
            similar_past_incidents = [],          # Phase 2+
            suggested_remediations = [],          # Phase 2+
            confidence             = round(confidence, 3),
            explain                = explain,
        )

    def close(self) -> None:
        """Flush and close the DuckDB connection."""
        self._db.close()
        log.info("Engine closed")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _rebuild_graph(self) -> None:
        """Rebuild NetworkX from DuckDB (idempotent, safe on empty DB)."""
        node_rows = [
            (r["id"], r["canonical_name"], r["aliases"])
            for r in self._node_store.get_all()
        ]
        edge_rows = self._edge_store.get_active_edges_snapshot()
        self._graph.rebuild(node_rows, edge_rows)

    def _expand_services(self, anchor_services: List[str]) -> List[str]:
        """
        Expand a list of anchor service names to include:
          - The anchors themselves
          - All canonical names and aliases of their 2-hop graph neighbors
        This ensures related services (e.g. upstream dependencies) are
        included even if they don't appear in the trigger string.
        """
        if not anchor_services:
            return []

        all_node_ids: set = set()
        for name in anchor_services:
            node_id = self._node_store.resolve(name)
            if node_id:
                all_node_ids.add(node_id)
                all_node_ids.update(self._graph.neighbors_within_hops(node_id, hops=2))

        expanded: List[str] = list(anchor_services)
        for node_id in all_node_ids:
            expanded.extend(self._node_store.all_names_for_id(node_id))

        return list(set(expanded))  # deduplicate

    @staticmethod
    def _normalise_event_for_output(event: Dict[str, Any]) -> Dict[str, Any]:
        """Return a copy of *event* with ``ts`` serialised as ISO string."""
        out = dict(event)
        ts = out.get("ts")
        if isinstance(ts, datetime):
            out["ts"] = ts.isoformat()
        return out

    @staticmethod
    def _extract_services_from_trigger(trigger: str) -> List[str]:
        """
        Heuristically extract service names from a trigger string.

        Example: "alert:checkout-api/error-rate>5%"  → ["checkout-api"]
        """
        if not trigger:
            return []
        # Common pattern: "alert:<service>/<metric>"
        match = re.search(r"alert:([a-zA-Z0-9_-]+)/", trigger)
        if match:
            return [match.group(1)]
        # Fallback: try any word that looks like a service name
        tokens = re.findall(r"[a-zA-Z][a-zA-Z0-9_-]{2,}", trigger)
        return [t for t in tokens if t not in {"alert", "error", "rate", "warn"}]

    @staticmethod
    def _build_explain(
        signal: Dict[str, Any],
        events: List[Dict[str, Any]],
        start_ts: datetime,
        end_ts: datetime,
    ) -> str:
        incident_id = signal.get("incident_id", "unknown")
        trigger     = signal.get("trigger", "")
        n_events    = len(events)
        kinds       = {}
        for e in events:
            k = e.get("kind", "unknown")
            kinds[k] = kinds.get(k, 0) + 1
        kind_summary = ", ".join(f"{v}×{k}" for k, v in sorted(kinds.items()))
        return (
            f"[Phase 1] Incident {incident_id} triggered by '{trigger}'. "
            f"Retrieved {n_events} related events in window "
            f"[{start_ts.isoformat()}, {end_ts.isoformat()}]. "
            f"Event breakdown: {kind_summary or 'none'}. "
            "Causal inference and incident matching require Phase 2."
        )

    # ------------------------------------------------------------------
    # Diagnostics / introspection
    # ------------------------------------------------------------------

    def stats(self) -> Dict[str, Any]:
        return {
            "raw_events":    self._raw_store.count(),
            "nodes":         self._node_store.count(),
            "active_edges":  self._edge_store.count_active(),
            "graph_nodes":   self._graph.node_count(),
            "graph_edges":   self._graph.edge_count(),
            "buffer_size":   len(self._buffer),
            "parser_stats":  self._coordinator.parser_stats(),
        }
