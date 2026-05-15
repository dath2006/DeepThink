"""
Raw event store — append-only table, one row per telemetry event.

This is the *provenance anchor*: every later inference (causal edges,
incident matches, remediation suggestions) stores evidence_event_ids that
trace back to rows in this table.

All mutations must run inside a transaction managed by the caller
(IngestCoordinator).  Read helpers open their own cursor so they never
block the write path.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from .database import DatabaseManager


def _json_default(obj: Any) -> Any:
    """Custom JSON encoder: converts datetime to ISO-8601 string."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class RawEventStore:
    """Append-only wrapper around the ``raw_events`` DuckDB table."""

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    # ------------------------------------------------------------------
    # Write  (called inside coordinator transaction)
    # ------------------------------------------------------------------

    def insert(
        self,
        raw: Dict[str, Any],
        ts: datetime,
        kind: str,
    ) -> str:
        """
        Insert one event and return its assigned UUID.

        Parameters
        ----------
        raw:  the original event dict (will be JSON-serialised verbatim)
        ts:   already-parsed datetime (UTC-aware)
        kind: EventKind string
        """
        event_id = str(uuid.uuid4())
        service = raw.get("service")
        # Use pre-serialized JSON if available (set by batch ingest)
        raw_json = raw.get("_raw_json") or json.dumps(raw, default=_json_default)
        self._db.conn.execute(
            """
            INSERT INTO raw_events (id, ts, kind, service, raw_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            [event_id, ts, kind, service, raw_json],
        )
        return event_id

    # ------------------------------------------------------------------
    # Read helpers  (use a cursor so reads don't block writers)
    # ------------------------------------------------------------------

    def _cursor(self):
        return self._db.conn.cursor()

    def get_by_id(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single event by its UUID."""
        row = self._cursor().execute(
            "SELECT id, ts, kind, service, raw_json FROM raw_events WHERE id = ?",
            [event_id],
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def get_by_timerange(
        self,
        start_ts: datetime,
        end_ts: datetime,
        kind: Optional[str] = None,
        service: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Return events within [start_ts, end_ts], ordered by ts ASC."""
        q = "SELECT id, ts, kind, service, raw_json FROM raw_events WHERE ts >= ? AND ts <= ?"
        params: list = [start_ts, end_ts]
        if kind:
            q += " AND kind = ?"
            params.append(kind)
        if service:
            q += " AND service = ?"
            params.append(service)
        q += " ORDER BY ts ASC"
        if limit:
            q += f" LIMIT {int(limit)}"
        rows = self._cursor().execute(q, params).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_by_services(
        self,
        service_names: List[str],
        start_ts: datetime,
        end_ts: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Fetch events for a set of service names in a time window.
        Used by context reconstruction to pull evidence for a node cluster.
        """
        if not service_names:
            return []
        placeholders = ",".join(["?" for _ in service_names])
        q = f"""
            SELECT id, ts, kind, service, raw_json
            FROM raw_events
            WHERE service IN ({placeholders})
              AND ts >= ? AND ts <= ?
            ORDER BY ts ASC
        """
        params = list(service_names) + [start_ts, end_ts]
        rows = self._cursor().execute(q, params).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_by_ids(self, event_ids: List[str]) -> List[Dict[str, Any]]:
        """Batch-fetch events by a list of IDs (for provenance lookup)."""
        if not event_ids:
            return []
        placeholders = ",".join(["?" for _ in event_ids])
        rows = self._cursor().execute(
            f"SELECT id, ts, kind, service, raw_json FROM raw_events WHERE id IN ({placeholders})",
            event_ids,
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def count(self) -> int:
        return self._cursor().execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _row_to_dict(row) -> Dict[str, Any]:
        return {
            "id":      row[0],
            "ts":      row[1],
            "kind":    row[2],
            "service": row[3],
            "raw":     json.loads(row[4]),
        }
