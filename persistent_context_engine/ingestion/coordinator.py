"""
Ingest coordinator - the central pipeline for one event at a time.

Transaction model
-----------------
Every event is processed inside a single DuckDB transaction that covers:
  1. raw_events INSERT  - assign event_id
  2. nodes UPSERT       - assign / update node UUIDs
  3. edges UPSERT       - create or refresh edge records

DuckDB commit happens FIRST.  NetworkX (in-memory derived view) is updated
AFTER the commit.  This ordering means:
  * If the DB transaction fails - NetworkX is unchanged (consistent).
  * If NetworkX update fails after DB commit - the Engine can call
    ``rebuild_graph_from_db()`` on restart to re-sync.

Per-kind routing
----------------
deploy          - upsert node for service
log             - upsert node for service (if present)
metric          - upsert node for service
trace           - upsert nodes for each span.svc, create CALLS edges
topology/rename - NodeStore.handle_rename + GraphManager.handle_rename
topology/dep-sh - TopologyHandler.handle_dependency_shift
incident_signal - raw store only (no graph mutations in Phase 1)
remediation     - upsert node for target service
unknown kinds   - raw store only (forward-compatible with held-out kinds)
"""

from __future__ import annotations

import json as _json_mod
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from datetime import timedelta

from ..storage.raw_store import _json_default

_json_dumps = _json_mod.dumps

from ..schema import EdgeKind, EventKind
from ..storage.database import DatabaseManager
from ..storage.raw_store import RawEventStore
from ..storage.node_store import NodeStore
from ..storage.edge_store import EdgeStore
from ..storage.pattern_store import PatternStore
from ..storage.remediation_store import RemediationStore
from ..graph.manager import GraphManager
from ..graph.topology import TopologyHandler
from ..incident_fingerprinter import Fingerprinter
from .buffer import RecentEventsBuffer
from .parser import EventParser, ParseError

log = logging.getLogger(__name__)


# Pending NetworkX operation types (applied after DB commit)
_NxOp = Tuple  # typed alias; see _apply_nx_ops for shapes


class IngestCoordinator:
    """
    Orchestrates the full ingest pipeline for a single event.

    Constructed once per Engine instance and reused for every ingest call.
    """

    def __init__(
        self,
        db:         DatabaseManager,
        raw_store:  RawEventStore,
        node_store: NodeStore,
        edge_store: EdgeStore,
        pattern_store: PatternStore,
        remediation_store: RemediationStore,
        graph:      GraphManager,
        buffer:     RecentEventsBuffer,
        temporal_view: Any = None,  # Optional TemporalGraphView for point-in-time topology
    ) -> None:
        self._db        = db
        self._raw       = raw_store
        self._nodes     = node_store
        self._edges     = edge_store
        self._patterns  = pattern_store
        self._remeds    = remediation_store
        self._graph     = graph
        self._buffer    = buffer
        self._temporal_view = temporal_view  # Used by fingerprinter for consistent roles
        self._parser    = EventParser()
        self._topology  = TopologyHandler(node_store, edge_store)
        self._fingerprinter = Fingerprinter()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def ingest_one(self, raw_input: Any) -> None:
        """
        Ingest a single event (dict or JSONL string).

        Steps
        -----
        1. Parse / normalise input.
        2. Open DB transaction.
        3. INSERT into raw_events (get event_id).
        4. Route by kind: UPSERT nodes / edges inside the transaction.
        5. Collect pending NetworkX ops (no NetworkX writes yet).
        6. COMMIT transaction.
        7. Apply NetworkX ops.
        8. Push to recent-events buffer.
        """
        try:
            event = self._parser.normalise(raw_input)
        except ParseError as exc:
            log.warning("Skipping unparseable event: %s", exc)
            return

        ts:   datetime = event["ts"]
        kind: str      = event["kind"]

        nx_ops: List[_NxOp] = []

        self._db.begin()
        try:
            # ---- 1. Provenance anchor --------------------------------
            event_id = self._raw.insert(event, ts, kind)

            # ---- 2. Kind-specific routing ---------------------------
            if kind == EventKind.DEPLOY:
                nx_ops += self._handle_deploy(event, ts, event_id)

            elif kind in (EventKind.LOG, EventKind.METRIC):
                nx_ops += self._handle_service_event(event, ts, event_id)

            elif kind == EventKind.TRACE:
                nx_ops += self._handle_trace(event, ts, event_id)

            elif kind == EventKind.TOPOLOGY:
                nx_ops += self._handle_topology(event, ts, event_id)

            elif kind == EventKind.INCIDENT_SIGNAL:
                # Phase 2: Create fingerprint for this incident
                nx_ops += self._handle_incident_signal(event, ts, event_id)

            elif kind == EventKind.REMEDIATION:
                nx_ops += self._handle_remediation(event, ts, event_id)

            else:
                log.debug("Unknown kind '%s' stored raw only", kind)

            # ---- 3. Commit DB ----------------------------------------
            self._db.commit()

        except Exception:
            self._db.rollback()
            log.exception("Transaction rolled back for event kind='%s'", kind)
            return

        # ---- 4. Apply NetworkX ops (after commit) --------------------
        self._apply_nx_ops(nx_ops)

        # ---- 5. Hot buffer -------------------------------------------
        self._buffer.push(event)

    def ingest_many(self, events: Any, batch_size: int = 100) -> None:
        """
        Ingest an iterable of events with batched transactions.

        Groups simple events (deploy, log, metric, trace) into batched
        transactions for throughput.  Complex events (incident_signal,
        topology, remediation) that need cross-table queries or
        fingerprinting run in their own transaction for correctness.
        """
        batch_nx_ops: List[_NxOp] = []
        batch_events: List[Dict[str, Any]] = []
        batch_node_ts: Dict[str, datetime] = {}  # node_id -> max ts seen in batch
        in_batch = False

        def _flush_batch() -> None:
            nonlocal in_batch
            if not in_batch:
                return
            # Bulk-update last_seen_ts for all nodes touched in this batch
            for nid, max_ts in batch_node_ts.items():
                self._db.conn.execute(
                    "UPDATE nodes SET last_seen_ts = ? WHERE id = ? AND last_seen_ts < ?",
                    [max_ts, nid, max_ts],
                )
            self._db.commit()
            self._apply_nx_ops(batch_nx_ops)
            for be in batch_events:
                self._buffer.push(be)
            batch_nx_ops.clear()
            batch_events.clear()
            batch_node_ts.clear()
            in_batch = False

        for raw in events:
            try:
                event = self._parser.normalise(raw)
            except ParseError as exc:
                log.warning("Skipping unparseable event: %s", exc)
                continue

            kind = event["kind"]

            # Complex kinds need their own transaction (fingerprinting, cross-table)
            if kind in (EventKind.INCIDENT_SIGNAL, EventKind.TOPOLOGY, EventKind.REMEDIATION):
                _flush_batch()
                self._ingest_parsed(event)
                continue

            # Simple kind: accumulate in batch
            if not in_batch:
                self._db.begin()
                in_batch = True

            ts = event["ts"]
            # Pre-serialize JSON once for batch throughput
            event["_raw_json"] = _json_dumps(event, default=_json_default)
            event_id = self._raw.insert(event, ts, kind)

            if kind == EventKind.DEPLOY:
                nx_ops = self._handle_deploy_fast(event, ts, event_id, batch_node_ts)
                batch_nx_ops += nx_ops
            elif kind in (EventKind.LOG, EventKind.METRIC):
                nx_ops = self._handle_service_event_fast(event, ts, event_id, batch_node_ts)
                batch_nx_ops += nx_ops
            elif kind == EventKind.TRACE:
                batch_nx_ops += self._handle_trace(event, ts, event_id)
            else:
                pass  # Unknown kind stored raw

            batch_events.append(event)

            # Commit batch when full
            if len(batch_events) >= batch_size:
                _flush_batch()

        # Flush remaining batch
        _flush_batch()

    def _ingest_parsed(self, event: Dict[str, Any]) -> None:
        """Ingest a single pre-parsed event with its own transaction."""
        ts = event["ts"]
        kind = event["kind"]
        nx_ops: List[_NxOp] = []

        self._db.begin()
        try:
            event_id = self._raw.insert(event, ts, kind)

            if kind == EventKind.TOPOLOGY:
                nx_ops += self._handle_topology(event, ts, event_id)
            elif kind == EventKind.INCIDENT_SIGNAL:
                nx_ops += self._handle_incident_signal(event, ts, event_id)
            elif kind == EventKind.REMEDIATION:
                nx_ops += self._handle_remediation(event, ts, event_id)
            else:
                pass

            self._db.commit()
        except Exception:
            self._db.rollback()
            log.exception("Transaction rolled back for event kind='%s'", kind)
            return

        self._apply_nx_ops(nx_ops)
        self._buffer.push(event)

    # ------------------------------------------------------------------
    # Kind-specific handlers — return list of pending NetworkX ops
    # ------------------------------------------------------------------

    def _handle_deploy(
        self, event: Dict[str, Any], ts: datetime, event_id: str
    ) -> List[_NxOp]:
        service = event.get("service")
        if not service:
            return []
        node_id = self._nodes.get_or_create(service, ts)
        return [("node", node_id, service, [])]

    def _handle_deploy_fast(
        self, event: Dict[str, Any], ts: datetime, event_id: str,
        batch_node_ts: Dict[str, datetime],
    ) -> List[_NxOp]:
        """Batch-optimised deploy handler: skips per-event ts update."""
        service = event.get("service")
        if not service:
            return []
        node_id = self._nodes.get_or_create(service, ts, skip_ts_update=True)
        # Track max ts per node for batch-end bulk update
        prev = batch_node_ts.get(node_id)
        if prev is None or ts > prev:
            batch_node_ts[node_id] = ts
        return [("node", node_id, service, [])]

    def _handle_service_event(
        self, event: Dict[str, Any], ts: datetime, event_id: str
    ) -> List[_NxOp]:
        service = event.get("service")
        if not service:
            return []
        node_id = self._nodes.get_or_create(service, ts)
        return [("node", node_id, service, [])]

    def _handle_service_event_fast(
        self, event: Dict[str, Any], ts: datetime, event_id: str,
        batch_node_ts: Dict[str, datetime],
    ) -> List[_NxOp]:
        """Batch-optimised service event handler: skips per-event ts update."""
        service = event.get("service")
        if not service:
            return []
        node_id = self._nodes.get_or_create(service, ts, skip_ts_update=True)
        prev = batch_node_ts.get(node_id)
        if prev is None or ts > prev:
            batch_node_ts[node_id] = ts
        return [("node", node_id, service, [])]

    def _handle_trace(
        self, event: Dict[str, Any], ts: datetime, event_id: str
    ) -> List[_NxOp]:
        """
        For each span, upsert a node.  Create CALLS edges between
        consecutive spans (span[i] to span[i+1]).
        """
        spans: List[Dict[str, Any]] = event.get("spans") or []
        if not spans:
            return []

        nx_ops: List[_NxOp] = []
        span_node_ids: List[str] = []

        for span in spans:
            svc = span.get("svc") or span.get("service")
            if not svc:
                continue
            node_id = self._nodes.get_or_create(svc, ts)
            span_node_ids.append(node_id)
            nx_ops.append(("node", node_id, svc, []))

        # Create CALLS edges between sequential spans
        for i in range(len(span_node_ids) - 1):
            src, dst = span_node_ids[i], span_node_ids[i + 1]
            self._edges.get_or_create(
                src, dst,
                EdgeKind.CALLS,
                ts,
                evidence_event_ids=[event_id],
                confidence=1.0,
            )
            nx_ops.append(("edge", src, dst, EdgeKind.CALLS, 1.0))

        return nx_ops

    def _handle_topology(
        self, event: Dict[str, Any], ts: datetime, event_id: str
    ) -> List[_NxOp]:
        """
        Topology events route to TopologyHandler (DB ops only).
        The returned pending NX ops are applied by the coordinator
        AFTER the DB transaction commits.
        """
        change = event.get("change", "")

        if change == "rename":
            return self._topology.handle_rename(event, ts, event_id)

        elif change in ("dep_add", "dep_remove", "dependency-shift"):
            return self._topology.handle_dependency_shift(event, ts, event_id)

        log.warning("Unknown topology change type '%s'", change)
        return []

    def _handle_remediation(
        self, event: Dict[str, Any], ts: datetime, event_id: str
    ) -> List[_NxOp]:
        target = event.get("target")
        incident_id = event.get("incident_id")
        action = event.get("action", "unknown")
        outcome = event.get("outcome", "unknown")

        if not target:
            return []

        node_id = self._nodes.get_or_create(target, ts)
        target_service = self._nodes.get_canonical_name(node_id) or target

        # Find the pattern for this incident (if any)
        pattern = None
        if incident_id:
            pattern = self._patterns.get_pattern_by_incident(incident_id)

        # Store remediation with event timestamp for correct temporal decay
        self._remeds.insert(
            incident_id=incident_id or "unknown",
            pattern_id=pattern["id"] if pattern else None,
            action=action,
            target_node_id=node_id,
            target_service=target_service,
            outcome=outcome,
            evidence_event_ids=[event_id],
            event_ts=ts,
        )

        # Reinforce family confidence if resolved
        if outcome == "resolved" and pattern and pattern.get("family_id"):
            self._patterns.reinforce_family(pattern["family_id"], increment=0.05)

        return [("node", node_id, target, [])]

    def _handle_incident_signal(
        self, event: Dict[str, Any], ts: datetime, event_id: str
    ) -> List[_NxOp]:
        """
        Process incident signal: extract fingerprint and find/create family.
        """
        incident_id = event.get("incident_id", "unknown")
        trigger = event.get("trigger", "")
        service = event.get("service")

        # Extract trigger service from trigger string if not provided
        if not service and trigger:
            import re
            match = re.search(r"alert:([a-zA-Z0-9_-]+)/", trigger)
            if match:
                service = match.group(1)

        if not service:
            log.warning("Incident signal %s has no service", incident_id)
            return []

        # Resolve to node
        trigger_node_id = self._nodes.resolve(service)
        if not trigger_node_id:
            # Service not seen before, create it
            trigger_node_id = self._nodes.get_or_create(service, ts)

        # Get window of events for fingerprinting
        window_start = ts - timedelta(minutes=30)
        window_end = ts + timedelta(minutes=5)

        # Fetch events in window (from buffer or DB)
        window_events = self._raw.get_by_timerange(
            window_start, window_end, limit=1000
        )

        # Restrict fingerprint events to trigger service + immediate topology neighborhood.
        # This prevents background noise from unrelated services from dominating hashes.
        relevant_services = {service}
        neighbor_ids = self._graph.neighbors_within_hops(trigger_node_id, hops=2)
        for nid in neighbor_ids:
            for name in self._nodes.all_names_for_id(nid):
                relevant_services.add(name)

        # Build event dicts for fingerprinter
        events_for_fp = []
        for row in window_events:
            raw = row.get("raw", {})
            raw_svc = raw.get("service")
            if raw_svc and raw_svc not in relevant_services:
                continue
            # Parse ts back to datetime if needed
            raw_ts = raw.get("ts")
            if isinstance(raw_ts, str):
                try:
                    raw["ts"] = self._parser._parse_timestamp(raw_ts)
                except Exception:
                    pass
            events_for_fp.append(raw)

        # Extract fingerprint using current (static) graph for consistent role computation.
        # Both ingest-time and query-time use the same strategy so hashes align.
        fingerprint = self._fingerprinter.fingerprint(
            trigger_node_id=trigger_node_id,
            trigger_service=service,
            incident_ts=ts,
            events=events_for_fp,
            graph_manager=self._graph,
            node_store=self._nodes,
            temporal_view=self._temporal_view,
        )

        # Store pattern
        pattern_id = self._patterns.insert_pattern(
            incident_id=incident_id,
            fingerprint_hash=fingerprint.structural_hash,
            fingerprint_tuple=fingerprint.to_tuple_string(),
            trigger_node_id=trigger_node_id,
            window_start=fingerprint.window_start,
            window_end=fingerprint.window_end,
            event_count=fingerprint.event_count,
            created_at=ts,
        )

        # Find or create family (scoped to trigger service + fingerprint hash)
        family_id, base_conf = self._patterns.find_or_create_family(
            fingerprint_hash=fingerprint.structural_hash,
            fingerprint_tuple=fingerprint.to_tuple_string(),
            pattern_id=pattern_id,
            created_at=ts,
            trigger_node_id=trigger_node_id,
        )

        log.debug(
            "Incident %s: pattern %s -> family %s (conf %.2f)",
            incident_id, pattern_id, family_id, base_conf
        )

        return []

    # ------------------------------------------------------------------
    # Apply pending NetworkX operations
    # ------------------------------------------------------------------

    def _apply_nx_ops(self, ops: List[_NxOp]) -> None:
        """Apply collected NetworkX operations after DB commit."""
        for op in ops:
            kind = op[0]
            try:
                if kind == "node":
                    _, node_id, canonical, aliases = op
                    self._graph.add_or_update_node(node_id, canonical, aliases)
                elif kind == "edge":
                    _, src, dst, edge_kind, confidence = op
                    self._graph.add_or_update_edge(src, dst, edge_kind, confidence)
                elif kind == "rename_node":
                    _, node_id, new_name, old_name = op
                    self._graph.handle_rename(node_id, new_name, old_name)
                elif kind == "retire_edge":
                    _, src, dst = op
                    self._graph.retire_edge(src, dst)
            except Exception:
                log.exception("Failed to apply NetworkX op %s", op)

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def parser_stats(self) -> Dict[str, int]:
        return self._parser.stats()
