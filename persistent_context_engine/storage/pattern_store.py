"""
Incident pattern and family storage — stores behavioral fingerprints.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ..schema import PATTERN_DDL, PATTERN_INDEX_DDL, IncidentPatternRecord, IncidentFamilyRecord
from .database import DatabaseManager

log = logging.getLogger(__name__)


class PatternStore:
    """
    Stores incident fingerprints and manages incident families.

    Patterns are matched to families based on fingerprint similarity.
    Exact hash matches go to the same family; soft matches (edit distance <= 1)
    also go to the same family with a lower confidence score.
    """

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Create tables if they don't exist."""
        self._db.conn.execute(PATTERN_DDL)
        for ddl in PATTERN_INDEX_DDL:
            self._db.conn.execute(ddl)

    def _cursor(self):
        return self._db.conn.cursor()

    def insert_pattern(
        self,
        incident_id: str,
        fingerprint_hash: str,
        fingerprint_tuple: str,
        trigger_node_id: str,
        window_start: datetime,
        window_end: datetime,
        event_count: int,
    ) -> str:
        """
        Insert a new pattern and return its UUID.

        Called during incident signal ingestion to capture the fingerprint
        of the events leading up to the incident.
        """
        pattern_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        self._db.conn.execute(
            """
            INSERT INTO incident_patterns (
                id, incident_id, fingerprint_hash, fingerprint_tuple,
                trigger_node_id, window_start_ts, window_end_ts,
                family_id, similarity_score, event_count, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                pattern_id, incident_id, fingerprint_hash, fingerprint_tuple,
                trigger_node_id, window_start, window_end,
                None, 0.0, event_count, now
            ],
        )
        return pattern_id

    def find_or_create_family(
        self,
        fingerprint: Any,
        pattern_id: str,
        created_at: datetime,
    ) -> Tuple[str, float]:
        """
        Find an existing family matching this fingerprint, or create a new one.

        This method supports:
        - exact hash match for identical fingerprints
        - soft matching by edit distance <= 1 for near-identical patterns

        Returns (family_id, base_confidence).
        """
        from ..incident_fingerprinter import IncidentFingerprint

        fingerprint_hash = fingerprint.structural_hash
        cursor = self._cursor()
        row = cursor.execute(
            """
            SELECT id, reinforced_confidence FROM incident_families
            WHERE family_hash = ?
            LIMIT 1
            """,
            [fingerprint_hash]
        ).fetchone()

        if row:
            family_id, confidence = row[0], row[1]
            now = datetime.now(timezone.utc)
            self._db.conn.execute(
                """
                UPDATE incident_families
                SET incident_count = incident_count + 1,
                    last_confirmed_ts = ?
                WHERE id = ?
                """,
                [now, family_id]
            )
            self._db.conn.execute(
                """
                UPDATE incident_patterns
                SET family_id = ?, similarity_score = 1.0
                WHERE id = ?
                """,
                [family_id, pattern_id]
            )
            return family_id, confidence

        # Soft match against existing patterns if no exact family exists.
        best_match: Optional[Tuple[str, int]] = None
        all_rows = cursor.execute(
            "SELECT id, fingerprint_tuple, family_id FROM incident_patterns WHERE family_id IS NOT NULL"
        ).fetchall()
        for row in all_rows:
            existing_id, existing_tuple, existing_family_id = row
            if existing_id == pattern_id or existing_family_id is None:
                continue
            try:
                existing_elements = [tuple(item) for item in json.loads(existing_tuple)]
                other_fp = IncidentFingerprint(
                    elements=existing_elements,
                    structural_hash="",
                    trigger_node_id="",
                    window_start=datetime.min.replace(tzinfo=timezone.utc),
                    window_end=datetime.min.replace(tzinfo=timezone.utc),
                    event_count=len(existing_elements),
                )
                dist = fingerprint.edit_distance(other_fp)
                if dist <= 1 and (
                    best_match is None or dist < best_match[1]
                ):
                    best_match = (existing_family_id, dist)
            except Exception:
                continue

        if best_match is not None:
            family_id, dist = best_match
            similarity_score = 1.0 if dist == 0 else 0.6
            now = datetime.now(timezone.utc)
            self._db.conn.execute(
                """
                UPDATE incident_families
                SET incident_count = incident_count + 1,
                    last_confirmed_ts = ?
                WHERE id = ?
                """,
                [now, family_id]
            )
            self._db.conn.execute(
                """
                UPDATE incident_patterns
                SET family_id = ?, similarity_score = ?
                WHERE id = ?
                """,
                [family_id, similarity_score, pattern_id]
            )
            return family_id, similarity_score

        family_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        self._db.conn.execute(
            """
            INSERT INTO incident_families (
                id, family_hash, representative_pattern_id, incident_count,
                first_seen_ts, last_confirmed_ts, reinforced_confidence, decay_rate
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                family_id, fingerprint_hash, pattern_id, 1,
                now, now, 0.5, 0.95
            ],
        )

        self._db.conn.execute(
            """
            UPDATE incident_patterns
            SET family_id = ?, similarity_score = 1.0
            WHERE id = ?
            """,
            [family_id, pattern_id]
        )

        return family_id, 0.5

    def find_similar_patterns(
        self,
        fingerprint_hash: str,
        trigger_node_id: Optional[str] = None,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Find patterns similar to the given hash, prioritizing same-family matches.

        Note: trigger_node_id is optional. When None, searches across all services
        (for rename-proof cross-service matching).
        """
        # First: same hash (exact match)
        cursor = self._cursor()

        if trigger_node_id:
            # Prefer patterns from the same trigger service
            rows = cursor.execute(
                """
                SELECT p.id, p.incident_id, p.fingerprint_hash, p.family_id,
                       p.similarity_score, p.created_at, f.reinforced_confidence
                FROM incident_patterns p
                LEFT JOIN incident_families f ON p.family_id = f.id
                WHERE p.fingerprint_hash = ?
                  AND p.trigger_node_id = ?
                ORDER BY p.created_at DESC
                LIMIT ?
                """,
                [fingerprint_hash, trigger_node_id, limit]
            ).fetchall()

            # If no same-service matches, fall back to any service with same hash
            if not rows:
                rows = cursor.execute(
                    """
                    SELECT p.id, p.incident_id, p.fingerprint_hash, p.family_id,
                           p.similarity_score, p.created_at, f.reinforced_confidence
                    FROM incident_patterns p
                    LEFT JOIN incident_families f ON p.family_id = f.id
                    WHERE p.fingerprint_hash = ?
                    ORDER BY p.created_at DESC
                    LIMIT ?
                    """,
                    [fingerprint_hash, limit]
                ).fetchall()
        else:
            # Search across all services (rename-proof matching)
            rows = cursor.execute(
                """
                SELECT p.id, p.incident_id, p.fingerprint_hash, p.family_id,
                       p.similarity_score, p.created_at, f.reinforced_confidence
                FROM incident_patterns p
                LEFT JOIN incident_families f ON p.family_id = f.id
                WHERE p.fingerprint_hash = ?
                ORDER BY p.created_at DESC
                LIMIT ?
                """,
                [fingerprint_hash, limit]
            ).fetchall()

        return [
            {
                "pattern_id": r[0],
                "incident_id": r[1],
                "fingerprint_hash": r[2],
                "family_id": r[3],
                "similarity_score": r[4],
                "created_at": r[5],
                "family_confidence": r[6] or 0.5,
            }
            for r in rows
        ]

    def get_patterns_by_family(
        self,
        family_id: str,
        exclude_incident_id: Optional[str] = None,
    ) -> List[IncidentPatternRecord]:
        """Get all patterns in a family."""
        cursor = self._cursor()
        if exclude_incident_id:
            rows = cursor.execute(
                """
                SELECT id, incident_id, fingerprint_hash, fingerprint_tuple,
                       trigger_node_id, window_start_ts, window_end_ts,
                       family_id, similarity_score, event_count, created_at
                FROM incident_patterns
                WHERE family_id = ? AND incident_id != ?
                ORDER BY created_at DESC
                """,
                [family_id, exclude_incident_id]
            ).fetchall()
        else:
            rows = cursor.execute(
                """
                SELECT id, incident_id, fingerprint_hash, fingerprint_tuple,
                       trigger_node_id, window_start_ts, window_end_ts,
                       family_id, similarity_score, event_count, created_at
                FROM incident_patterns
                WHERE family_id = ?
                ORDER BY created_at DESC
                """,
                [family_id]
            ).fetchall()

        return [
            IncidentPatternRecord(
                id=r[0],
                incident_id=r[1],
                fingerprint_hash=r[2],
                fingerprint_tuple=r[3],
                trigger_node_id=r[4],
                window_start_ts=r[5],
                window_end_ts=r[6],
                family_id=r[7],
                similarity_score=r[8],
                event_count=r[9],
                created_at=r[10],
            )
            for r in rows
        ]

    def get_pattern_by_incident(self, incident_id: str) -> Optional[IncidentPatternRecord]:
        """Get the pattern for a specific incident."""
        row = self._cursor().execute(
            """
            SELECT id, incident_id, fingerprint_hash, fingerprint_tuple,
                   trigger_node_id, window_start_ts, window_end_ts,
                   family_id, similarity_score, event_count, created_at
            FROM incident_patterns
            WHERE incident_id = ?
            LIMIT 1
            """,
            [incident_id]
        ).fetchone()

        if not row:
            return None

        return IncidentPatternRecord(
            id=row[0],
            incident_id=row[1],
            fingerprint_hash=row[2],
            fingerprint_tuple=row[3],
            trigger_node_id=row[4],
            window_start_ts=row[5],
            window_end_ts=row[6],
            family_id=row[7],
            similarity_score=row[8],
            event_count=row[9],
            created_at=row[10],
        )

    def reinforce_family(
        self,
        family_id: str,
        increment: float = 0.05,
        max_confidence: float = 0.95,
    ) -> None:
        """
        Reinforce a family's confidence after successful remediation.

        This is the memory evolution mechanism — successful patterns
        become more confident over time.
        """
        now = datetime.now(timezone.utc)
        self._db.conn.execute(
            """
            UPDATE incident_families
            SET reinforced_confidence = LEAST(?, reinforced_confidence + ?),
                last_confirmed_ts = ?
            WHERE id = ?
            """,
            [max_confidence, increment, now, family_id]
        )

    def count(self) -> int:
        return self._cursor().execute("SELECT COUNT(*) FROM incident_patterns").fetchone()[0]

    def count_families(self) -> int:
        return self._cursor().execute("SELECT COUNT(*) FROM incident_families").fetchone()[0]

    def get_all_hashes(self) -> List[str]:
        """Get all existing fingerprint hashes for soft matching."""
        rows = self._cursor().execute(
            "SELECT DISTINCT fingerprint_hash FROM incident_patterns"
        ).fetchall()
        return [r[0] for r in rows]
