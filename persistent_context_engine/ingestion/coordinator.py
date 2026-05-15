"""
Ingest coordinator — the central pipeline for one event at a time.

Transaction model
-----------------
Every event is processed inside a single DuckDB transaction that covers:
  1. raw_events INSERT  → assign event_id
  2. nodes UPSERT       → assign / update node UUIDs
  3. edges UPSERT       → create or refresh edge records

DuckDB commit happens FIRST.  NetworkX (in-memory derived view) is updated
AFTER the commit.  This ordering means:
  * If the DB transaction fails → NetworkX is unchanged (consistent).
  * If NetworkX update fails after DB commit → the Engine can call
    ``rebuild_graph_from_db()`` on restart to re-sync.

Per-kind routing
----------------
deploy          → upsert node for service
log             → upsert node for service (if present)
metric          → upsert node for service
trace           → upsert nodes for each span.svc, create CALLS edges
topology/rename → NodeStore.handle_rename + GraphManager.handle_rename
topology/dep-sh → TopologyHandler.handle_dependency_shift
incident_signal → raw store only (no graph mutations in Phase 1)
remediation     → upsert node for target service
unknown kinds   → raw store only (forward-compatible with held-out kinds)
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ..schema import EdgeKind, EventKind
from ..storage.database import DatabaseManager
from ..storage.raw_store import RawEventStore
from ..storage.node_store import NodeStore
from ..storage.edge_store import EdgeStore
from ..graph.manager import GraphManager
from ..graph.topology import TopologyHandler
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
        graph:      GraphManager,
        buffer:     RecentEventsBuffer,
    ) -> None:
        self._db        = db
        self._raw       = raw_store
        self._nodes     = node_store
        self._edges     = edge_store
        self._graph     = graph
        self._buffer    = buffer
        self._parser    = EventParser()
        self._topology  = TopologyHandler(node_store, edge_store)

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

            elif kind == EventKind.REMEDIATION:
                nx_ops += self._handle_remediation(event, ts, event_id)

            elif kind == EventKind.INCIDENT_SIGNAL:
                pass  # raw store only in Phase 1

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

    def ingest_many(self, events: Any) -> None:
        """Ingest an iterable of events (benchmark's ingest entry point)."""
        for raw in events:
            self.ingest_one(raw)

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

    def _handle_service_event(
        self, event: Dict[str, Any], ts: datetime, event_id: str
    ) -> List[_NxOp]:
        service = event.get("service")
        if not service:
            return []
        node_id = self._nodes.get_or_create(service, ts)
        return [("node", node_id, service, [])]

    def _handle_trace(
        self, event: Dict[str, Any], ts: datetime, event_id: str
    ) -> List[_NxOp]:
        """
        For each span, upsert a node.  Create CALLS edges between
        consecutive spans (span[i] → span[i+1]).
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
        if not target:
            return []
        node_id = self._nodes.get_or_create(target, ts)
        return [("node", node_id, target, [])]

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
