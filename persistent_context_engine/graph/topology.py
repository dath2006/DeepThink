"""
Topology event handler — DuckDB mutations only.

Handles ``topology/rename`` and ``topology/dependency-shift`` events.

Design contract
---------------
* This handler performs ONLY DuckDB operations (NodeStore / EdgeStore).
* It must be called INSIDE an open DuckDB transaction.
* It returns a list of pending NetworkX ops as plain tuples.
* The caller (IngestCoordinator) applies those ops AFTER db.commit().

This separation guarantees that if the DB transaction rolls back,
the in-memory graph is never mutated — keeping DuckDB and NetworkX
consistent without needing compensating transactions.

dependency-shift format
-----------------------
The problem statement does not fully specify the field layout for
dependency-shift events.  This handler accepts multiple common conventions:
  source service : "source" | "from" | "src"
  target service : "target" | "to"   | "dst"
  operation      : "op"     | "action"| "type"  (add / remove / retire)
  Default operation when none of the above are present: "add"
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ..schema import EdgeKind
from ..storage.node_store import NodeStore
from ..storage.edge_store import EdgeStore

log = logging.getLogger(__name__)

# Pending NetworkX op tuples (same convention as IngestCoordinator._NxOp)
#   ("node",        node_id, canonical_name, aliases)
#   ("edge",        src_id,  dst_id, edge_kind, confidence)
#   ("rename_node", node_id, new_name, old_name)
#   ("retire_edge", src_id,  dst_id)
_NxOp = Tuple


class TopologyHandler:
    """
    Executes DuckDB-side topology mutations and returns pending NX ops.

    Constructed once per Engine, shared with the IngestCoordinator.
    """

    def __init__(
        self,
        node_store: NodeStore,
        edge_store: EdgeStore,
    ) -> None:
        self._nodes = node_store
        self._edges = edge_store

    # ------------------------------------------------------------------
    # topology/rename
    # ------------------------------------------------------------------

    def handle_rename(
        self,
        raw: Dict[str, Any],
        ts: datetime,
        event_id: str,
    ) -> List[_NxOp]:
        """
        Process a rename event (inside an open DB transaction).

        ``raw["from"]`` becomes ``raw["to"]``

        Returns one ``("rename_node", ...)`` op for the coordinator to
        apply to NetworkX after commit.
        """
        from_name: Optional[str] = raw.get("from_") or raw.get("from")
        to_name:   Optional[str] = raw.get("to")

        if not from_name or not to_name:
            log.warning("topology/rename missing 'from_' or 'to': %s", raw)
            return []

        node_id = self._nodes.handle_rename(from_name, to_name, ts)
        log.debug("DB rename %s: '%s' to '%s'", node_id, from_name, to_name)

        return [("rename_node", node_id, to_name, from_name)]

    # ------------------------------------------------------------------
    # topology/dependency-shift
    # ------------------------------------------------------------------

    def handle_dependency_shift(
        self,
        raw: Dict[str, Any],
        ts: datetime,
        event_id: str,
    ) -> List[_NxOp]:
        """
        Process a dependency-shift event (inside an open DB transaction).

        Returns NX ops for the coordinator to apply after commit.
        """
        # Benchmark uses "from_" / "to" for dep_add / dep_remove.
        # Fallback to older defensively-accepted names.
        src_name: Optional[str] = (
            raw.get("from_") or raw.get("source") or raw.get("from") or raw.get("src")
        )
        dst_name: Optional[str] = (
            raw.get("to") or raw.get("target") or raw.get("dst")
        )
        # Benchmark change values: "dep_add", "dep_remove"
        change_val: str = raw.get("change", "")
        if change_val == "dep_remove":
            op = "remove"
        elif change_val == "dep_add":
            op = "add"
        else:
            op = (
                raw.get("op") or raw.get("action") or raw.get("type") or "add"
            ).lower()

        if not src_name or not dst_name:
            log.warning(
                "topology/dependency-shift missing source/target: %s", raw
            )
            return []

        src_id = self._nodes.get_or_create(src_name, ts)
        dst_id = self._nodes.get_or_create(dst_name, ts)

        if op in ("remove", "delete", "retire"):
            self._edges.retire(src_id, dst_id, EdgeKind.DEPENDENCY.value, ts)
            log.debug("DB retired dependency %s to %s", src_name, dst_name)
            return [
                ("node",        src_id, src_name, []),
                ("node",        dst_id, dst_name, []),
                ("retire_edge", src_id, dst_id),
            ]
        else:
            self._edges.get_or_create(
                src_id,
                dst_id,
                EdgeKind.DEPENDENCY.value,
                ts,
                evidence_event_ids=[event_id],
                confidence=1.0,
            )
            log.debug("DB upserted dependency %s to %s", src_name, dst_name)
            return [
                ("node", src_id, src_name, []),
                ("node", dst_id, dst_name, []),
                ("edge", src_id, dst_id, EdgeKind.DEPENDENCY.value, 1.0),
            ]
