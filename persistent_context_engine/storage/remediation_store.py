"""
Remediation history storage — stores past remediation actions and outcomes.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..schema import RemediationRecord
from .database import DatabaseManager

log = logging.getLogger(__name__)


class RemediationStore:
    """
    Stores historical remediation actions with outcomes.

    Used to suggest remediations for new incidents based on:
    1. Same-family historical incidents
    2. Temporal decay of old remediations
    3. Success rate per (action, target) pair
    """

    def __init__(self, db: DatabaseManager) -> None:
        self._db = db

    def _cursor(self):
        return self._db.conn.cursor()

    def insert(
        self,
        incident_id: str,
        pattern_id: Optional[str],
        action: str,
        target_node_id: str,
        target_service: str,
        outcome: str,
        evidence_event_ids: List[str],
        event_ts: Optional[datetime] = None,
    ) -> str:
        """
        Record a remediation action.

        Called when processing a 'remediation' event.
        Uses event_ts (the telemetry timestamp) for applied_at so temporal
        decay works correctly with synthetic/historical data.
        """
        rem_id = str(uuid.uuid4())
        applied_at = event_ts or datetime.now(timezone.utc)

        # Compute initial confidence based on outcome
        base_confidence = 0.7 if outcome == "resolved" else 0.3 if outcome == "worsened" else 0.5

        self._db.conn.execute(
            """
            INSERT INTO remediation_history (
                id, incident_id, pattern_id, action, target_node_id,
                target_service, outcome, confidence, applied_at, evidence_event_ids
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                rem_id, incident_id, pattern_id, action, target_node_id,
                target_service, outcome, base_confidence, applied_at,
                json.dumps(evidence_event_ids)
            ],
        )
        return rem_id

    def get_for_family(
        self,
        family_id: str,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Get remediations for patterns in a family, sorted by recency and confidence.
        """
        cursor = self._cursor()
        rows = cursor.execute(
            """
            SELECT r.id, r.action, r.target_node_id, r.target_service,
                   r.outcome, r.confidence, r.applied_at, r.incident_id
            FROM remediation_history r
            JOIN incident_patterns p ON r.pattern_id = p.id
            WHERE p.family_id = ?
            ORDER BY r.applied_at DESC
            LIMIT ?
            """,
            [family_id, limit]
        ).fetchall()

        return [
            {
                "id": r[0],
                "action": r[1],
                "target_node_id": r[2],
                "target_service": r[3],
                "outcome": r[4],
                "base_confidence": r[5],
                "applied_at": r[6],
                "incident_id": r[7],
            }
            for r in rows
        ]

    def get_for_pattern(
        self,
        pattern_id: str,
        limit: int = 3,
    ) -> List[RemediationRecord]:
        """Get remediations linked to a specific pattern."""
        cursor = self._cursor()
        rows = cursor.execute(
            """
            SELECT id, incident_id, pattern_id, action, target_node_id,
                   target_service, outcome, confidence, applied_at, evidence_event_ids
            FROM remediation_history
            WHERE pattern_id = ?
            ORDER BY applied_at DESC
            LIMIT ?
            """,
            [pattern_id, limit]
        ).fetchall()

        return [
            RemediationRecord(
                id=r[0],
                incident_id=r[1],
                pattern_id=r[2],
                action=r[3],
                target_node_id=r[4],
                target_service=r[5],
                outcome=r[6],
                confidence=r[7],
                applied_at=r[8],
                evidence_event_ids=json.loads(r[9]) if r[9] else [],
            )
            for r in rows
        ]

    def get_successful_actions_for_target(
        self,
        target_node_id: str,
        min_confidence: float = 0.0,
        limit: int = 5,
    ) -> List[Dict[str, Any]]:
        """
        Get successful remediations for a specific target service.
        """
        cursor = self._cursor()
        rows = cursor.execute(
            """
            SELECT action, outcome, confidence, applied_at, incident_id
            FROM remediation_history
            WHERE target_node_id = ? AND outcome = 'resolved' AND confidence >= ?
            ORDER BY applied_at DESC
            LIMIT ?
            """,
            [target_node_id, min_confidence, limit]
        ).fetchall()

        return [
            {
                "action": r[0],
                "outcome": r[1],
                "confidence": r[2],
                "applied_at": r[3],
                "incident_id": r[4],
            }
            for r in rows
        ]

    def count(self) -> int:
        return self._cursor().execute("SELECT COUNT(*) FROM remediation_history").fetchone()[0]


class TemporalDecayScorer:
    """
    Applies temporal decay to remediation confidence scores.

    Formula: score × (decay_rate ^ days_since)
    Floor at 0.1 so old remediations never disappear entirely.
    """

    def __init__(self, decay_rate: float = 0.95, floor: float = 0.1) -> None:
        self.decay_rate = decay_rate
        self.floor = floor

    def compute(
        self,
        base_confidence: float,
        applied_at: datetime,
        reference_time: Optional[datetime] = None,
    ) -> float:
        """
        Compute decayed confidence score.

        Parameters
        ----------
        base_confidence: Original confidence (0-1)
        applied_at: When the remediation was applied
        reference_time: Time to compute decay from (default: now)

        Returns
        -------
        Decayed confidence score (0-1, floor at 0.1)
        """
        if reference_time is None:
            reference_time = datetime.now(timezone.utc)

        # Ensure both are timezone-aware
        if applied_at.tzinfo is None:
            applied_at = applied_at.replace(tzinfo=timezone.utc)
        if reference_time.tzinfo is None:
            reference_time = reference_time.replace(tzinfo=timezone.utc)

        days_since = max(0, (reference_time - applied_at).total_seconds() / 86400)
        decayed = base_confidence * (self.decay_rate ** days_since)
        return max(self.floor, decayed)
