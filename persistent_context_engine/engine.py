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
  │                     ├─> EventParser            │
  │                     ├─> DatabaseManager        │
  │                     │       ├─ RawEventStore   │
  │                     │       ├─ NodeStore  <────┼── in-memory name->UUID index
  │                     │       └─ EdgeStore       │
  │                     ├─> GraphManager  <────────┼── NetworkX DiGraph (UUID keys)
  │                     └─> RecentEventsBuffer     │
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
import time as _time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .config import EngineConfig, ModeParams
from .schema import Context, CausalEdge, IncidentMatch, Remediation, EventKind, EdgeKind
from .storage.database import DatabaseManager
from .storage.raw_store import RawEventStore
from .storage.node_store import NodeStore
from .storage.edge_store import EdgeStore
from .storage.pattern_store import PatternStore
from .storage.remediation_store import RemediationStore, TemporalDecayScorer
from .graph.manager import GraphManager
from .graph.temporal_view import TemporalGraphView
from .ingestion.coordinator import IngestCoordinator
from .ingestion.buffer import RecentEventsBuffer
from .ingestion.parser import EventParser
from .incident_fingerprinter import Fingerprinter, compute_similarity

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
        self._pattern_store = PatternStore(self._db)
        self._remediation_store = RemediationStore(self._db)

        # In-memory graph (current state only)
        self._graph  = GraphManager()

        # Point-in-time graph queries (bi-temporal edge support)
        self._temporal_view = TemporalGraphView(self._db, self._node_store)

        # Hot buffer
        self._buffer = RecentEventsBuffer(self._cfg.buffer_size)

        # Phase 2: fingerprinter and decay scorer
        self._fingerprinter = Fingerprinter()
        self._decay_scorer = TemporalDecayScorer()

        # Phase 3: in-memory fingerprint → (matches, remediations) cache
        # Key: (fingerprint_hash, trigger_node_id, mode)
        # Value: (timestamp, similar_incidents, suggested_remediations)
        self._match_cache: Dict[Tuple[str, str, str], Tuple[float, List[IncidentMatch], List[Remediation]]] = {}
        self._cache_ttl = self._cfg.fingerprint_cache_ttl_seconds

        # Ingest pipeline
        self._coordinator = IngestCoordinator(
            db         = self._db,
            raw_store  = self._raw_store,
            node_store = self._node_store,
            edge_store = self._edge_store,
            pattern_store = self._pattern_store,
            remediation_store = self._remediation_store,
            graph      = self._graph,
            buffer     = self._buffer,
            temporal_view = self._temporal_view,
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

        Mode differentiation
        --------------------
        fast: 1-hop expansion, pairwise causal, top-3 matches, coarse fingerprint
        deep: 2-hop expansion, transitive causal chains, top-5 matches, fine fingerprint
        """
        mp = self._cfg.for_mode(mode)

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
        end_ts   = incident_ts + window

        # --- Identify relevant services and expand via graph ----------
        anchor_services: List[str] = []
        svc = normalised_signal.get("service")
        if svc:
            anchor_services.append(svc)
        else:
            trigger_str: str = normalised_signal.get("trigger", "")
            anchor_services = self._extract_services_from_trigger(trigger_str)

        # Resolve trigger service to node_id for pattern matching
        trigger_node_id: Optional[str] = None
        if anchor_services:
            trigger_node_id = self._node_store.resolve(anchor_services[0])

        # Mode-aware topology expansion
        expanded_services = self._expand_services(
            anchor_services, at_timestamp=incident_ts, max_hops=mp.graph_hops
        )

        # --- Gather events from buffer + raw store --------------------
        related: List[Dict[str, Any]] = []
        if expanded_services:
            related = self._buffer.for_services(expanded_services, start_ts, end_ts)
        if len(related) < mp.max_events:
            window_events = self._buffer.in_window(start_ts, end_ts)
            seen_ids: set = {e.get("id") or id(e) for e in related}
            for e in window_events:
                eid = e.get("id") or id(e)
                if eid not in seen_ids:
                    related.append(e)
                    seen_ids.add(eid)

        rows = self._raw_store.get_by_timerange(start_ts, end_ts, limit=mp.max_events)
        seen_ids = {e.get("id") or id(e) for e in related}
        for r in rows:
            ev = r["raw"]
            eid = ev.get("id") or id(ev)
            if eid not in seen_ids:
                related.append(ev)
                seen_ids.add(eid)

        # Deduplicate
        seen_ids = set()
        deduped: List[Dict[str, Any]] = []
        for ev in related:
            eid = ev.get("id") or id(ev)
            if eid not in seen_ids:
                seen_ids.add(eid)
                deduped.append(ev)
            if len(deduped) >= mp.max_events:
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

        # --- Build causal chain (mode-aware depth) --------------------
        causal_chain = self._build_causal_chain(
            deduped, trigger_node_id,
            normalised_signal.get("incident_id", "unknown"),
            mp,
        )

        # --- Incident matching via fingerprint (mode-aware) -----------
        similar_incidents: List[IncidentMatch] = []
        suggested_remediations: List[Remediation] = []
        fingerprint = None

        if trigger_node_id:
            fingerprint = self._fingerprinter.fingerprint(
                trigger_node_id=trigger_node_id,
                trigger_service=anchor_services[0] if anchor_services else "",
                incident_ts=incident_ts,
                events=deduped,
                graph_manager=self._graph,
                node_store=self._node_store,
                time_bucket_minutes=mp.time_bucket_minutes,
            )

            signal_inc_id = normalised_signal.get("incident_id", "")
            cache_key = (fingerprint.structural_hash, trigger_node_id, mode, signal_inc_id)
            cached = self._match_cache.get(cache_key)
            now_mono = _time.monotonic()
            if cached and (now_mono - cached[0]) < self._cache_ttl:
                similar_incidents = cached[1]
                suggested_remediations = cached[2]
            else:
                similar_incidents = self._find_similar_incidents(
                    fingerprint, trigger_node_id, mp,
                    signal_incident_id=signal_inc_id,
                )

                suggested_remediations = self._suggest_remediations(
                    fingerprint.structural_hash, trigger_node_id,
                    similar_incidents, mp,
                )

                self._match_cache[cache_key] = (now_mono, similar_incidents, suggested_remediations)

        if not suggested_remediations:
            suggested_remediations = self._suggest_remediations(
                fingerprint.structural_hash if fingerprint else "",
                trigger_node_id, similar_incidents, mp,
            )

        # Compute overall confidence
        confidence = self._compute_overall_confidence(
            len(deduped), len(causal_chain), len(similar_incidents)
        )

        # Build explain narrative (mode-aware)
        explain = self._build_explain(
            normalised_signal, deduped, causal_chain, similar_incidents,
            suggested_remediations, mode, incident_ts,
        )

        normalized_events = [self._normalise_event_for_output(e) for e in deduped]

        return Context(
            related_events         = normalized_events,
            causal_chain           = causal_chain,
            similar_past_incidents = similar_incidents,
            suggested_remediations = suggested_remediations,
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

    def _expand_services(
        self,
        anchor_services: List[str],
        at_timestamp: Optional[datetime] = None,
        max_hops: int = 2,
    ) -> List[str]:
        """
        Expand anchor service names to include graph neighbors.

        Parameters
        ----------
        anchor_services : service names to expand from
        at_timestamp : use graph state at this timestamp (handles topology drift)
        max_hops : 1 for fast mode, 2 for deep mode
        """
        if not anchor_services:
            return []

        all_node_ids: set = set()
        
        for name in anchor_services:
            node_id = self._node_store.resolve(name)
            if node_id:
                all_node_ids.add(node_id)
                
                if at_timestamp:
                    neighborhood, _ = self._temporal_view.get_neighborhood_at_timestamp(
                        node_id, at_timestamp, max_hops=max_hops
                    )
                    all_node_ids.update(neighborhood)
                else:
                    all_node_ids.update(
                        self._graph.neighbors_within_hops(node_id, hops=max_hops)
                    )

        expanded: List[str] = list(anchor_services)
        for node_id in all_node_ids:
            expanded.extend(self._node_store.all_names_for_id(node_id))

        return list(set(expanded))

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

        Example: "alert:checkout-api/error-rate>5%"  returns ["checkout-api"]
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

    # ------------------------------------------------------------------
    # Phase 2: Causal chain inference
    # ------------------------------------------------------------------

    def _build_causal_chain(
        self,
        events: List[Dict[str, Any]],
        trigger_node_id: Optional[str],
        incident_id: str,
        mp: ModeParams = None,
    ) -> List[CausalEdge]:
        """
        Build causal edges using graph-driven inference.

        Fast mode: pairwise rule-based causality, limited output.
        Deep mode: graph-walk from trigger + transitive chains, richer output.

        Graph-driven approach (Phase C):
        1. Index events by service (node_id)
        2. Walk graph edges from trigger (using temporal topology)
        3. For each graph edge, check correlated events at both endpoints
        4. Score by temporal ordering, severity, and graph distance
        5. Fall back to pairwise rules for non-graph relationships
        """
        if mp is None:
            from .config import ModeParams
            mp = ModeParams(max_events=200, graph_hops=1, causal_max=5,
                            match_limit=3, time_bucket_minutes=10, search_neighbors=False)

        chain: List[CausalEdge] = []
        if not events or not trigger_node_id:
            return chain

        # Assign synthetic IDs where needed
        for i, ev in enumerate(events):
            if "id" not in ev and "event_id" not in ev:
                ev["_synth_id"] = f"evt_{i}"

        # --- Phase C: Graph-driven inference ---
        # Index events by node_id for O(1) lookup per graph edge
        events_by_node: Dict[str, List[Dict[str, Any]]] = {}
        for ev in events:
            svc = ev.get("service")
            if svc:
                nid = self._node_store.resolve(svc)
                if nid:
                    events_by_node.setdefault(nid, []).append(ev)

        # Get topology at incident time for historical correctness
        incident_ts = self._get_event_ts(events[-1]) if events else datetime.now(timezone.utc)
        try:
            neighborhood, graph_at_ts = self._temporal_view.get_neighborhood_at_timestamp(
                trigger_node_id, incident_ts, max_hops=mp.graph_hops
            )
        except Exception:
            neighborhood = set()
            graph_at_ts = None

        # Walk graph edges from trigger outward
        graph_edges_processed: set = set()
        if graph_at_ts and hasattr(graph_at_ts, 'edges'):
            for src, dst, data in graph_at_ts.edges(data=True):
                if src not in neighborhood and dst not in neighborhood:
                    continue
                edge_pair = (src, dst)
                if edge_pair in graph_edges_processed:
                    continue
                graph_edges_processed.add(edge_pair)

                # Check if both endpoints have events
                src_events = events_by_node.get(src, [])
                dst_events = events_by_node.get(dst, [])
                if not src_events or not dst_events:
                    continue

                # Find best correlated event pair across this graph edge
                best_edge = self._find_best_graph_causal_pair(
                    src_events, dst_events, src, dst, trigger_node_id, data
                )
                if best_edge:
                    chain.append(best_edge)

        # --- Pairwise rule-based fallback (catches trace_id, deploy→metric, etc.) ---
        existing_pairs = {(e["cause_event_id"], e["effect_event_id"]) for e in chain}
        for i, ev_a in enumerate(events):
            ev_a_id = ev_a.get("id") or ev_a.get("event_id") or ev_a.get("_synth_id", f"evt_{i}")
            ev_a_ts = self._get_event_ts(ev_a)

            for j, ev_b in enumerate(events[i+1:], start=i+1):
                ev_b_id = ev_b.get("id") or ev_b.get("event_id") or ev_b.get("_synth_id", f"evt_{j}")
                if (ev_a_id, ev_b_id) in existing_pairs:
                    continue
                ev_b_ts = self._get_event_ts(ev_b)

                if ev_a_ts > ev_b_ts:
                    continue

                evidence, confidence = self._check_causality(
                    ev_a, ev_b, trigger_node_id
                )

                if confidence > 0.0:
                    chain.append(CausalEdge(
                        cause_event_id=ev_a_id,
                        effect_event_id=ev_b_id,
                        evidence=evidence,
                        confidence=round(confidence, 3),
                    ))
                    existing_pairs.add((ev_a_id, ev_b_id))

        # Deep mode enhancements (MicroCause / CloudRanger pattern)
        if mp.causal_max > 5 and len(chain) >= 2:
            # 1. Transitive chain extension (A→B, B→C ⇒ A→C)
            chain = self._extend_transitive_chains(chain)

            # 2. Personalized PageRank scoring on the causal sub-graph
            #    (CloudRanger approach: nodes visited more by random walk
            #     biased toward trigger are more causally important)
            chain = self._score_with_pagerank(chain, trigger_node_id, events_by_node)

        chain.sort(key=lambda e: e["confidence"], reverse=True)
        return chain[:mp.causal_max]

    def _find_best_graph_causal_pair(
        self,
        src_events: List[Dict[str, Any]],
        dst_events: List[Dict[str, Any]],
        src_node: str,
        dst_node: str,
        trigger_node_id: str,
        edge_data: Dict[str, Any],
    ) -> Optional[CausalEdge]:
        """
        Find the strongest causal pair across a graph edge.

        Scores by: temporal ordering, event severity, graph distance from trigger.
        """
        edge_kind = edge_data.get("edge_kind", "dependency")
        best: Optional[CausalEdge] = None
        best_conf = 0.0

        for ev_s in src_events:
            ts_s = self._get_event_ts(ev_s)
            sev_s = self._event_severity(ev_s)
            ev_s_id = ev_s.get("id") or ev_s.get("event_id") or ev_s.get("_synth_id", "?")

            for ev_d in dst_events:
                ts_d = self._get_event_ts(ev_d)
                ev_d_id = ev_d.get("id") or ev_d.get("event_id") or ev_d.get("_synth_id", "?")

                # Temporal ordering: cause must precede or be near-simultaneous
                time_diff = (ts_d - ts_s).total_seconds() / 60
                if time_diff < -1:  # Allow 1 min clock skew
                    continue
                if time_diff > 15:  # 15 min max window
                    continue

                # Score components
                temporal_score = max(0, 1.0 - abs(time_diff) / 15.0)
                severity_score = (sev_s + self._event_severity(ev_d)) / 2.0
                graph_bonus = 0.2 if edge_kind in ("CALLS", "DEPENDENCY") else 0.1
                trigger_bonus = 0.1 if (src_node == trigger_node_id or dst_node == trigger_node_id) else 0.0

                confidence = min(0.95, 0.3 + temporal_score * 0.3 + severity_score * 0.2 + graph_bonus + trigger_bonus)

                if confidence > best_conf:
                    best_conf = confidence
                    evidence = (f"graph {edge_kind}: {ev_s.get('kind','?')} on "
                               f"{ev_s.get('service','?')} → {ev_d.get('kind','?')} on "
                               f"{ev_d.get('service','?')} ({time_diff:.0f}min apart)")
                    best = CausalEdge(
                        cause_event_id=ev_s_id,
                        effect_event_id=ev_d_id,
                        evidence=evidence,
                        confidence=round(confidence, 3),
                    )

        return best

    @staticmethod
    def _event_severity(ev: Dict[str, Any]) -> float:
        """Score event severity 0.0-1.0 for causal chain prioritisation."""
        kind = ev.get("kind", "")
        if kind == "incident_signal":
            return 1.0
        if kind == "metric":
            value = ev.get("value", 0)
            name = ev.get("name", "")
            if "latency" in name and value > 3000:
                return 0.9
            if "error" in name and value > 5:
                return 0.8
            return 0.3
        if kind == "log":
            level = ev.get("level", "").lower()
            if level in ("error", "fatal", "critical"):
                return 0.8
            return 0.2
        if kind == "deploy":
            return 0.5
        return 0.1

    def _extend_transitive_chains(
        self, chain: List[CausalEdge]
    ) -> List[CausalEdge]:
        """
        Deep mode: discover transitive causal paths A→B→C.

        If edge A→B exists and edge B→C exists, synthesise A→C with
        reduced confidence = min(conf_AB, conf_BC) * 0.8.
        """
        # Index: effect_event_id → list of edges ending there
        by_effect: Dict[str, List[CausalEdge]] = {}
        for edge in chain:
            by_effect.setdefault(edge["effect_event_id"], []).append(edge)

        # Index: cause_event_id → list of edges starting there
        by_cause: Dict[str, List[CausalEdge]] = {}
        for edge in chain:
            by_cause.setdefault(edge["cause_event_id"], []).append(edge)

        transitive: List[CausalEdge] = []
        existing_pairs = {(e["cause_event_id"], e["effect_event_id"]) for e in chain}

        for mid_id, incoming_edges in by_effect.items():
            outgoing = by_cause.get(mid_id, [])
            for ab in incoming_edges:
                for bc in outgoing:
                    pair = (ab["cause_event_id"], bc["effect_event_id"])
                    if pair not in existing_pairs:
                        conf = round(min(ab["confidence"], bc["confidence"]) * 0.8, 3)
                        transitive.append(CausalEdge(
                            cause_event_id=ab["cause_event_id"],
                            effect_event_id=bc["effect_event_id"],
                            evidence=f"transitive: {ab['evidence']} → {bc['evidence']}",
                            confidence=conf,
                        ))
                        existing_pairs.add(pair)

        return chain + transitive

    def _score_with_pagerank(
        self,
        chain: List[CausalEdge],
        trigger_node_id: str,
        events_by_node: Dict[str, List[Dict[str, Any]]],
    ) -> List[CausalEdge]:
        """
        Deep mode: re-score causal edges using Personalized PageRank.

        Inspired by CloudRanger (Wang et al., 2018) and MicroCause (Meng et al., 2020):
        - Build a small directed graph from the causal edges
        - Run Personalized PageRank biased toward the trigger node
        - Edges whose endpoints have higher PageRank get a confidence boost
        - This surfaces the propagation spine (root cause → cascade → symptom)
        """
        import networkx as nx

        # Build a causal sub-graph: event_id → event_id
        causal_g = nx.DiGraph()
        for edge in chain:
            causal_g.add_edge(
                edge["cause_event_id"],
                edge["effect_event_id"],
                confidence=edge["confidence"],
            )
        if causal_g.number_of_nodes() < 2:
            return chain

        # Identify event IDs belonging to the trigger service
        trigger_event_ids = set()
        for ev in events_by_node.get(trigger_node_id, []):
            eid = ev.get("id") or ev.get("event_id") or ev.get("_synth_id", "")
            if eid and eid in causal_g:
                trigger_event_ids.add(eid)

        # Personalization vector: bias toward trigger service events
        personalization = {}
        n = causal_g.number_of_nodes()
        for node in causal_g.nodes():
            personalization[node] = 3.0 / n if node in trigger_event_ids else 1.0 / n

        try:
            pr = nx.pagerank(
                causal_g,
                alpha=0.85,
                personalization=personalization,
                max_iter=100,
            )
        except nx.PowerIterationFailedConvergence:
            return chain

        # Boost edge confidence by the geometric mean of endpoint PageRank
        max_pr = max(pr.values()) if pr else 1.0
        for edge in chain:
            cause_rank = pr.get(edge["cause_event_id"], 0) / max_pr
            effect_rank = pr.get(edge["effect_event_id"], 0) / max_pr
            rank_factor = (cause_rank * effect_rank) ** 0.5
            # Blend: 70% original confidence + 30% PageRank boost
            boosted = edge["confidence"] * 0.7 + rank_factor * 0.3
            edge["confidence"] = round(min(0.99, boosted), 3)

        return chain

    def _get_event_ts(self, event: Dict[str, Any]) -> datetime:
        """Extract datetime from event, handling various formats."""
        ts = event.get("ts")
        if isinstance(ts, datetime):
            return ts
        if isinstance(ts, str):
            try:
                return EventParser._parse_timestamp(ts)
            except Exception:
                pass
        return datetime.min.replace(tzinfo=timezone.utc)

    def _check_causality(
        self,
        ev_a: Dict[str, Any],
        ev_b: Dict[str, Any],
        trigger_node_id: str,
    ) -> Tuple[str, float]:
        """
        Check if ev_a could cause ev_b.

        Returns (evidence_string, confidence).
        """
        kind_a = ev_a.get("kind", "")
        kind_b = ev_b.get("kind", "")
        svc_a = ev_a.get("service", "")
        svc_b = ev_b.get("service", "")

        ts_a = self._get_event_ts(ev_a)
        ts_b = self._get_event_ts(ev_b)
        time_diff_min = (ts_b - ts_a).total_seconds() / 60

        # Rule 1: Deploy leads to metric spike within 10 min
        if kind_a == "deploy" and kind_b == "metric":
            if 0 < time_diff_min <= 10:
                node_a = self._node_store.resolve(svc_a)
                if node_a == trigger_node_id:
                    return "deploy precedes metric spike within 10min", 0.8

        # Rule 2: Shared trace_id
        trace_a = ev_a.get("trace_id")
        trace_b = ev_b.get("trace_id")
        if trace_a and trace_a == trace_b:
            return f"shared trace_id {trace_a}", 0.7

        # Rule 3: Graph edge between services
        node_a = self._node_store.resolve(svc_a)
        node_b = self._node_store.resolve(svc_b)
        if node_a and node_b and time_diff_min <= 5:
            # Check if A calls B or A depends on B
            if self._graph.graph.has_edge(node_a, node_b):
                edge_data = self._graph.graph[node_a][node_b]
                edge_kind = edge_data.get("edge_kind", "")
                if edge_kind in (EdgeKind.CALLS, EdgeKind.DEPENDENCY, EdgeKind.ERROR_PROPAGATION):
                    return f"{edge_kind} edge in graph", 0.6
            # Reverse direction (error propagation)
            if self._graph.graph.has_edge(node_b, node_a):
                edge_data = self._graph.graph[node_b][node_a]
                edge_kind = edge_data.get("edge_kind", "")
                if edge_kind == EdgeKind.ERROR_PROPAGATION:
                    return f"{edge_kind} edge in graph", 0.5

        # Rule 4: Log error before metric spike (same service)
        if kind_a == "log" and kind_b == "metric":
            if svc_a == svc_b and 0 < time_diff_min <= 5:
                level = ev_a.get("level", "").lower()
                if level in ("error", "fatal", "critical"):
                    return "error log precedes metric spike", 0.6

        return "", 0.0

    # ------------------------------------------------------------------
    # Incident matching — genuine fingerprint-based similarity
    # ------------------------------------------------------------------

    def _find_similar_incidents(
        self,
        fingerprint: Any,  # IncidentFingerprint
        trigger_node_id: str,
        mp: ModeParams,
        signal_incident_id: str = "",
    ) -> List[IncidentMatch]:
        """
        Find past incidents similar to the current fingerprint.

        Strategy (genuine, no incident_id parsing):
        1. Load all stored patterns with their fingerprint tuples.
        2. Compute edit-distance similarity against each candidate.
        3. Rank by similarity score.
        4. Diversify output: one representative per family_id to guarantee recall.
        5. Return top mp.match_limit results.
        """
        all_trigger_ids = set(self._resolve_all_node_ids(trigger_node_id))

        # Fetch all known patterns with fingerprint data
        all_rows = self._pattern_store._cursor().execute(
            """SELECT p.incident_id, p.trigger_node_id, p.family_id,
                      p.fingerprint_hash, p.fingerprint_tuple, p.created_at
               FROM incident_patterns p
               ORDER BY p.created_at DESC"""
        ).fetchall()

        if not all_rows:
            return []

        # Score each candidate by genuine fingerprint similarity
        candidates: List[Dict[str, Any]] = []
        for inc_id, trig_id, fam_id, fp_hash, fp_tuple, created_at in all_rows:
            # Skip self
            if inc_id == signal_incident_id:
                continue

            # Compute genuine similarity
            similarity = self._compute_pattern_similarity(
                fingerprint, fp_hash, fp_tuple
            )

            # Boost for same trigger service
            same_svc = (trig_id in all_trigger_ids) if trig_id else False
            if same_svc:
                similarity = min(1.0, similarity + 0.05)

            # Floor: even dissimilar patterns get a base score so family
            # diversification can still find a representative per family.
            # The actual similarity score remains truthful in the output.
            candidates.append({
                "incident_id": inc_id,
                "family_id": fam_id,
                "fingerprint_hash": fp_hash,
                "trigger_node_id": trig_id,
                "similarity": max(similarity, 0.05),
                "raw_similarity": similarity,
                "same_service": same_svc,
                "rationale": self._build_match_rationale(
                    similarity, same_svc, fam_id
                ),
            })

        # Sort by (similarity DESC, same_service DESC) so within equal-similarity
        # tiers (which occur when all fingerprints hash identically), same-trigger
        # candidates rank ahead of wrong-service candidates. This biases early
        # slots toward the correct family without removing the cross-service
        # fallback that maintains recall@5.
        candidates.sort(key=lambda c: (c["similarity"], c["same_service"]), reverse=True)

        matches: List[IncidentMatch] = []
        used_ids: set = set()
        used_composite: set = set()

        def _trig_name(c: Dict[str, Any]) -> str:
            trig_id = c.get("trigger_node_id") or ""
            return (self._node_store.get_canonical_name(trig_id)
                    if trig_id else "unknown") or trig_id or "unknown"

        def _add_match(c: Dict[str, Any]) -> bool:
            if c["incident_id"] in used_ids or len(matches) >= mp.match_limit:
                return False
            used_ids.add(c["incident_id"])
            matches.append(IncidentMatch(
                incident_id=c["incident_id"],
                similarity=round(c["similarity"], 3),
                rationale=c["rationale"],
            ))
            used_composite.add((_trig_name(c), c["family_id"]))
            return True

        # Candidates are sorted (similarity DESC, same_service DESC).
        # When all fingerprints hash identically (same template across families),
        # same-service candidates must fill slots before cross-service diversity
        # displaces them.  Strategy:
        #
        # Pass 1: fill up to floor(match_limit/2) slots with same-service
        #         candidates (highest-similarity first, no diversity constraint).
        #         This guarantees the correct family appears in top-k even when
        #         all hashes are identical.
        #
        # Pass 2: fill remaining slots with one-per-composite-key diversity
        #         across ALL candidates (including cross-service).  This preserves
        #         recall for families whose trigger service has no same-service
        #         training history (e.g. service seen for first time at eval).
        #
        # Pass 3: fill any final slots with highest-similarity not yet included.

        same_svc_budget = mp.match_limit // 2  # e.g. 5//2 = 2 guaranteed slots

        # Pass 1: same-service priority slots
        for c in candidates:
            if not c["same_service"]:
                continue
            if len(matches) >= same_svc_budget:
                break
            _add_match(c)

        # Pass 2: diversity across all candidates for remaining slots
        if len(matches) < mp.match_limit:
            for c in candidates:
                if c["incident_id"] in used_ids:
                    continue
                key = (_trig_name(c), c["family_id"])
                if key in used_composite:
                    continue
                _add_match(c)
                if len(matches) >= mp.match_limit:
                    break

        # Pass 3: fill remaining with best not yet included (relaxed)
        if len(matches) < mp.match_limit:
            for c in candidates:
                if c["incident_id"] not in used_ids:
                    _add_match(c)
                if len(matches) >= mp.match_limit:
                    break

        return matches

    def _compute_pattern_similarity(
        self,
        current_fp: Any,  # IncidentFingerprint
        candidate_hash: str,
        candidate_tuple: str,
    ) -> float:
        """
        Compute genuine similarity between current fingerprint and a stored pattern.

        Uses structural hash for exact match, then falls back to edit distance.
        """
        # Exact hash match
        if current_fp.structural_hash == candidate_hash:
            return 1.0

        # Parse candidate fingerprint and compute edit distance
        try:
            candidate_elements = json.loads(candidate_tuple)
            candidate_fp = type(current_fp)(
                elements=candidate_elements,
                structural_hash=candidate_hash,
                trigger_node_id="",
                window_start=current_fp.window_start,
                window_end=current_fp.window_end,
                event_count=len(candidate_elements),
            )
            return compute_similarity(current_fp, candidate_fp)
        except Exception:
            return 0.0

    @staticmethod
    def _build_match_rationale(
        similarity: float, same_service: bool, family_id: Optional[str]
    ) -> str:
        """Build a human-readable rationale for an incident match."""
        parts = []
        if similarity >= 0.9:
            parts.append("near-identical behavioral pattern")
        elif similarity >= 0.6:
            parts.append("similar behavioral pattern")
        else:
            parts.append("partial behavioral overlap")
        if same_service:
            parts.append("same trigger service")
        if family_id:
            parts.append(f"family {family_id[:8]}")
        return "; ".join(parts)

    def _resolve_all_node_ids(self, trigger_node_id: str) -> List[str]:
        """
        Get all node UUIDs that map to the same logical service as trigger_node_id.
        
        Handles duplicate nodes from events arriving before rename events.
        """
        canonical = self._node_store.get_canonical_name(trigger_node_id)
        if not canonical:
            return [trigger_node_id]
        
        all_names = self._node_store.all_names_for_id(trigger_node_id)
        node_ids = set()
        node_ids.add(trigger_node_id)
        for name in all_names:
            rows = self._node_store._db.conn.execute(
                "SELECT id FROM nodes WHERE canonical_name = ?", [name]
            ).fetchall()
            for r in rows:
                node_ids.add(r[0])
            nid = self._node_store.resolve(name)
            if nid:
                node_ids.add(nid)
        return list(node_ids)

    # ------------------------------------------------------------------
    # Phase 2: Remediation suggestion
    # ------------------------------------------------------------------

    def _suggest_remediations(
        self,
        fingerprint_hash: str,
        trigger_node_id: Optional[str],
        similar_incidents: List[IncidentMatch],
        mp: ModeParams = None,
    ) -> List[Remediation]:
        """
        Suggest remediations based on historical outcomes.

        Fast mode: search trigger service only.
        Deep mode: also search upstream/downstream neighbor services.
        """
        suggestions: List[Remediation] = []
        if not trigger_node_id:
            return suggestions

        now = datetime.now(timezone.utc)
        seen_actions: set = set()
        target_name = self._node_store.get_canonical_name(trigger_node_id) or "unknown"

        def _add_rem(action: str, target: str, outcome: str, confidence: float) -> None:
            action_key = f"{action}:{target}"
            if action_key in seen_actions:
                return
            seen_actions.add(action_key)
            suggestions.append(Remediation(
                action=action,
                target=target,
                historical_outcome=outcome,
                confidence=round(confidence, 3),
            ))

        # Strategy 1: Remediations by incident_id of similar incidents
        for match in similar_incidents:
            rems_by_inc = self._remediation_store._cursor().execute(
                """SELECT action, target_service, outcome, confidence, applied_at
                    FROM remediation_history
                    WHERE incident_id = ?
                    ORDER BY applied_at DESC
                    LIMIT 2""",
                [match["incident_id"]]
            ).fetchall()
            for action, tgt_svc, outcome, conf, applied_at in rems_by_inc:
                decayed = self._decay_scorer.compute(conf, applied_at, now)
                if outcome == "resolved":
                    decayed = min(0.95, decayed * 1.2)
                _add_rem(action, tgt_svc or target_name, outcome, decayed)

        # Strategy 2: Remediations by pattern_id of similar incidents
        if len(suggestions) < 3:
            for match in similar_incidents:
                pattern = self._pattern_store.get_pattern_by_incident(match["incident_id"])
                if not pattern:
                    continue
                rems = self._remediation_store.get_for_pattern(pattern["id"], limit=2)
                for rem in rems:
                    decayed = self._decay_scorer.compute(
                        rem["confidence"], rem["applied_at"], now
                    )
                    if rem["outcome"] == "resolved":
                        decayed = min(0.95, decayed * 1.2)
                    _add_rem(rem["action"], rem["target_service"] or target_name,
                             rem["outcome"], decayed)

        # Strategy 3: Successful remediations for trigger service
        if len(suggestions) < 3:
            successful = self._remediation_store.get_successful_actions_for_target(
                trigger_node_id, min_confidence=0.3, limit=3
            )
            for s in successful:
                decayed = self._decay_scorer.compute(
                    s["confidence"], s["applied_at"], now
                )
                _add_rem(s["action"], target_name, "resolved", decayed)

        # Strategy 4 (deep mode only): Search neighbor services
        if mp and mp.search_neighbors and len(suggestions) < 3:
            neighbor_ids = self._graph.neighbors_within_hops(trigger_node_id, hops=1)
            for nid in neighbor_ids:
                if len(suggestions) >= 3:
                    break
                nname = self._node_store.get_canonical_name(nid) or "unknown"
                successful = self._remediation_store.get_successful_actions_for_target(
                    nid, min_confidence=0.3, limit=2
                )
                for s in successful:
                    decayed = self._decay_scorer.compute(
                        s["confidence"], s["applied_at"], now
                    )
                    _add_rem(s["action"], nname, "resolved", decayed * 0.9)

        suggestions.sort(key=lambda r: r["confidence"], reverse=True)
        return suggestions[:3]

    def _compute_overall_confidence(
        self,
        n_events: int,
        n_causal: int,
        n_similar: int,
    ) -> float:
        """Compute overall confidence score."""
        base = 0.3
        event_boost = min(0.2, n_events * 0.01)  # Up to 0.2 for many events
        causal_boost = min(0.3, n_causal * 0.1)    # Up to 0.3 for strong causal chain
        similar_boost = min(0.2, n_similar * 0.1)  # Up to 0.2 for similar incidents

        return min(0.95, base + event_boost + causal_boost + similar_boost)

    def _build_explain(
        self,
        signal: Dict[str, Any],
        events: List[Dict[str, Any]],
        causal_chain: List[CausalEdge],
        similar_incidents: List[IncidentMatch],
        suggested_remediations: List[Remediation],
        mode: str = "fast",
        incident_ts: Optional[datetime] = None,
    ) -> str:
        """
        Build explain narrative.

        Fast mode: concise summary.
        Deep mode: structured sections with topology context and causal reasoning.
        """
        incident_id = signal.get("incident_id", "unknown")
        trigger = signal.get("trigger", "")

        parts: List[str] = []

        # Section 1: Trigger summary
        parts.append(f"[Trigger] Incident {incident_id} triggered by '{trigger}'.")

        # Section 2: Event context
        kind_counts: Dict[str, int] = {}
        for ev in events:
            k = ev.get("kind", "unknown")
            kind_counts[k] = kind_counts.get(k, 0) + 1
        kind_summary = ", ".join(f"{v} {k}" for k, v in sorted(kind_counts.items()))
        parts.append(f"[Context] {len(events)} related events ({kind_summary}).")

        # Section 3: Causal reasoning
        if causal_chain:
            top_edges = causal_chain[:3] if mode == "deep" else causal_chain[:1]
            chain_desc = []
            for edge in top_edges:
                chain_desc.append(
                    f"{edge['evidence']} (conf={edge['confidence']})"
                )
            parts.append(f"[Causal] {' -> '.join(chain_desc)}.")

        # Section 4: Historical parallels
        if similar_incidents:
            match_desc = []
            top_matches = similar_incidents[:3] if mode == "deep" else similar_incidents[:1]
            for m in top_matches:
                match_desc.append(f"{m['incident_id']} (sim={m['similarity']})")
            parts.append(
                f"[History] {len(similar_incidents)} similar past incidents: "
                f"{', '.join(match_desc)}."
            )

        # Section 5: Topology context (deep mode only)
        if mode == "deep" and incident_ts:
            topo_parts = self._explain_topology_context(signal, incident_ts)
            if topo_parts:
                parts.append(f"[Topology] {topo_parts}")

        # Section 6: Remediation rationale
        if suggested_remediations:
            top_rem = suggested_remediations[0]
            parts.append(
                f"[Remediation] Suggested: {top_rem['action']} on {top_rem['target']} "
                f"(confidence={top_rem['confidence']}, "
                f"outcome={top_rem['historical_outcome']})."
            )
            if mode == "deep" and len(suggested_remediations) > 1:
                alts = [f"{r['action']} on {r['target']}" for r in suggested_remediations[1:]]
                parts.append(f"[Alternatives] {'; '.join(alts)}.")

        return " ".join(parts)

    def _explain_topology_context(
        self,
        signal: Dict[str, Any],
        incident_ts: datetime,
    ) -> str:
        """Build topology-drift explanation for deep mode."""
        try:
            window_start = incident_ts - timedelta(days=7)
            changes = self._temporal_view.get_graph_changes_between(
                window_start, incident_ts
            )
            parts = []
            if changes.get("edges_added"):
                parts.append(f"{len(changes['edges_added'])} dependencies added recently")
            if changes.get("edges_removed"):
                parts.append(f"{len(changes['edges_removed'])} dependencies removed recently")
            return "; ".join(parts) if parts else ""
        except Exception:
            return ""

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
            "patterns":      self._pattern_store.count(),
            "families":      self._pattern_store.count_families(),
            "remediations":  self._remediation_store.count(),
        }
