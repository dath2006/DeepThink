"""
Node store — one row per logical service identity.

Key design decisions
--------------------
* Nodes are keyed by a UUID assigned at first observation, NOT by service
  name.  This means a rename is a zero-structural-change operation: we
  update the canonical_name and append the old name to the aliases list.
  Both old and new names continue to resolve to the same UUID.

* An in-memory dict ``_name_to_id`` maps every known name / alias to the
  node UUID.  This gives O(1) topology-independent lookup with no DuckDB
  round-trip on the hot ingest path.

* On startup, ``_load_index()`` rebuilds the dict from the existing nodes
  table, providing crash recovery.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from ..schema import NodeRecord
from .database import DatabaseManager


class NodeStore:
    """CRUD wrapper around the ``nodes`` DuckDB table."""

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db
        self._name_to_id: Dict[str, str] = {}
        self._load_index()

    # ------------------------------------------------------------------
    # Startup: rebuild in-memory index from persisted data
    # ------------------------------------------------------------------

    def _load_index(self) -> None:
        """Populate ``_name_to_id`` from all existing node rows."""
        rows = self._db.conn.execute(
            "SELECT id, canonical_name, aliases FROM nodes"
        ).fetchall()
        for node_id, canonical, aliases_json in rows:
            self._name_to_id[canonical] = node_id
            for alias in json.loads(aliases_json):
                self._name_to_id[alias] = node_id

    # ------------------------------------------------------------------
    # Write  (called inside coordinator transaction)
    # ------------------------------------------------------------------

    def get_or_create(self, service_name: str, ts: datetime) -> str:
        """
        Return the UUID for *service_name*, creating a new node if unseen.

        If the node already exists, ``last_seen_ts`` is bumped to *ts*.
        This call must run inside an open transaction.
        """
        node_id = self._name_to_id.get(service_name)
        if node_id is not None:
            self._db.conn.execute(
                "UPDATE nodes SET last_seen_ts = ? WHERE id = ?",
                [ts, node_id],
            )
            return node_id

        node_id = str(uuid.uuid4())
        self._db.conn.execute(
            """
            INSERT INTO nodes (id, canonical_name, aliases, first_seen_ts, last_seen_ts)
            VALUES (?, ?, ?, ?, ?)
            """,
            [node_id, service_name, "[]", ts, ts],
        )
        self._name_to_id[service_name] = node_id
        return node_id

    def handle_rename(
        self,
        from_name: str,
        to_name: str,
        ts: datetime,
    ) -> str:
        """
        Process a ``topology/rename`` event.

        Rules
        -----
        * Do NOT delete the old node.
        * Add *from_name* to the aliases list.
        * Set *to_name* as the new canonical_name.
        * Both names continue to resolve to the same UUID in ``_name_to_id``.

        Returns the node UUID (unchanged).
        """
        node_id = self._name_to_id.get(from_name)

        if node_id is None:
            # Edge case: the source service was never seen before the rename.
            # Create it with the new name.
            return self.get_or_create(to_name, ts)

        # Load current aliases
        row = self._db.conn.execute(
            "SELECT aliases FROM nodes WHERE id = ?",
            [node_id],
        ).fetchone()
        aliases: List[str] = json.loads(row[0])
        if from_name not in aliases:
            aliases.append(from_name)

        self._db.conn.execute(
            """
            UPDATE nodes
            SET canonical_name = ?,
                aliases        = ?,
                last_seen_ts   = ?
            WHERE id = ?
            """,
            [to_name, json.dumps(aliases), ts, node_id],
        )

        # Both names now resolve to the same UUID
        self._name_to_id[to_name]   = node_id
        self._name_to_id[from_name] = node_id  # keep old mapping
        return node_id

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def resolve(self, service_name: str) -> Optional[str]:
        """Return UUID for *service_name* (or any of its aliases), or None."""
        return self._name_to_id.get(service_name)

    def get_by_id(self, node_id: str) -> Optional[NodeRecord]:
        row = self._db.conn.cursor().execute(
            "SELECT id, canonical_name, aliases, first_seen_ts, last_seen_ts FROM nodes WHERE id = ?",
            [node_id],
        ).fetchone()
        return self._row_to_record(row) if row else None

    def get_all(self) -> List[NodeRecord]:
        rows = self._db.conn.cursor().execute(
            "SELECT id, canonical_name, aliases, first_seen_ts, last_seen_ts FROM nodes"
        ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def all_names_for_id(self, node_id: str) -> List[str]:
        """Return canonical name + all aliases for a node."""
        record = self.get_by_id(node_id)
        if record is None:
            return []
        return [record["canonical_name"]] + record["aliases"]

    def count(self) -> int:
        return self._db.conn.cursor().execute(
            "SELECT COUNT(*) FROM nodes"
        ).fetchone()[0]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_record(row) -> NodeRecord:
        return NodeRecord(
            id=row[0],
            canonical_name=row[1],
            aliases=json.loads(row[2]),
            first_seen_ts=row[3],
            last_seen_ts=row[4],
        )
