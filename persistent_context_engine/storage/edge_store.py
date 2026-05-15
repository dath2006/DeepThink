"""
Edge store — bi-temporal edge table.

Key design decisions
--------------------
* Edges are NEVER deleted.  Retired edges (from topology/dependency-shift)
  receive a ``retired_at`` timestamp instead.
* ``get_or_create`` merges evidence into an existing active edge rather
  than creating a duplicate; confidence takes the higher value.
* All writes must run inside a transaction managed by IngestCoordinator.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ..schema import EdgeRecord
from .database import DatabaseManager


class EdgeStore:
    """CRUD wrapper around the ``edges`` DuckDB table."""

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    # ------------------------------------------------------------------
    # Write  (called inside coordinator transaction)
    # ------------------------------------------------------------------

    def get_or_create(
        self,
        source_node_id: str,
        target_node_id: str,
        edge_kind: str,
        ts: datetime,
        evidence_event_ids: List[str],
        confidence: float = 1.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Return the edge UUID, merging into an existing active edge if one
        already exists for the same (source, target, kind) triple.

        Merging rules:
          - evidence_event_ids are unioned (no duplicates)
          - confidence takes max(existing, new)
          - last_seen_ts bumped to ts
        """
        existing = self._db.conn.execute(
            """
            SELECT id, confidence, evidence_event_ids
            FROM edges
            WHERE source_node_id = ?
              AND target_node_id = ?
              AND edge_kind      = ?
              AND retired_at IS NULL
            LIMIT 1
            """,
            [source_node_id, target_node_id, edge_kind],
        ).fetchone()

        if existing:
            edge_id          = existing[0]
            cur_confidence   = float(existing[1])
            cur_evidence     = json.loads(existing[2])
            merged_evidence  = list({*cur_evidence, *evidence_event_ids})
            new_confidence   = max(cur_confidence, confidence)

            self._db.conn.execute(
                """
                UPDATE edges
                SET last_seen_ts       = ?,
                    confidence         = ?,
                    evidence_event_ids = ?
                WHERE id = ?
                """,
                [ts, new_confidence, json.dumps(merged_evidence), edge_id],
            )
            return edge_id

        edge_id = str(uuid.uuid4())
        self._db.conn.execute(
            """
            INSERT INTO edges
              (id, source_node_id, target_node_id, edge_kind,
               first_seen_ts, last_seen_ts, confidence,
               evidence_event_ids, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                edge_id,
                source_node_id,
                target_node_id,
                edge_kind,
                ts,
                ts,
                confidence,
                json.dumps(evidence_event_ids),
                json.dumps(metadata or {}),
            ],
        )
        return edge_id

    def retire(
        self,
        source_node_id: str,
        target_node_id: str,
        edge_kind: str,
        ts: datetime,
    ) -> int:
        """
        Mark all active edges matching (source, target, kind) as retired.
        Returns number of rows affected.
        """
        result = self._db.conn.execute(
            """
            UPDATE edges
            SET retired_at = ?
            WHERE source_node_id = ?
              AND target_node_id = ?
              AND edge_kind      = ?
              AND retired_at IS NULL
            """,
            [ts, source_node_id, target_node_id, edge_kind],
        )
        return result.rowcount if hasattr(result, "rowcount") else 0

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def get_active_edges_for_node(self, node_id: str) -> List[EdgeRecord]:
        """Return all active edges where node is source or target."""
        rows = self._db.conn.cursor().execute(
            """
            SELECT id, source_node_id, target_node_id, edge_kind,
                   first_seen_ts, last_seen_ts, retired_at,
                   confidence, evidence_event_ids, metadata
            FROM edges
            WHERE (source_node_id = ? OR target_node_id = ?)
              AND retired_at IS NULL
            """,
            [node_id, node_id],
        ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def get_all_edges_for_node(self, node_id: str) -> List[EdgeRecord]:
        """Return all edges (active + retired) for historical analysis."""
        rows = self._db.conn.cursor().execute(
            """
            SELECT id, source_node_id, target_node_id, edge_kind,
                   first_seen_ts, last_seen_ts, retired_at,
                   confidence, evidence_event_ids, metadata
            FROM edges
            WHERE source_node_id = ? OR target_node_id = ?
            ORDER BY first_seen_ts ASC
            """,
            [node_id, node_id],
        ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def get_active_edges_snapshot(self) -> List[Tuple[str, str, str, float]]:
        """
        Lightweight snapshot of all active edges for graph rebuild.
        Returns list of (source_id, target_id, edge_kind, confidence).
        """
        rows = self._db.conn.cursor().execute(
            """
            SELECT source_node_id, target_node_id, edge_kind, confidence
            FROM edges
            WHERE retired_at IS NULL
            """
        ).fetchall()
        return [(r[0], r[1], r[2], float(r[3])) for r in rows]

    def count_active(self) -> int:
        return self._db.conn.cursor().execute(
            "SELECT COUNT(*) FROM edges WHERE retired_at IS NULL"
        ).fetchone()[0]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_record(row) -> EdgeRecord:
        return EdgeRecord(
            id=row[0],
            source_node_id=row[1],
            target_node_id=row[2],
            edge_kind=row[3],
            first_seen_ts=row[4],
            last_seen_ts=row[5],
            retired_at=row[6],
            confidence=float(row[7]),
            evidence_event_ids=json.loads(row[8]),
            metadata=json.loads(row[9]),
        )
