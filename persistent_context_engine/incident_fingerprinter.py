"""
Behavioral incident fingerprinter — extracts topology-independent fingerprints.

Core insight: The fingerprint must be based on ROLES (trigger, upstream-1,
downstream-1, etc.) not service NAMES. This is what makes matching rename-proof.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .schema import EventKind


# Fingerprint element: (event_kind, role, direction, relative_minutes)
# Role is topology-based, NOT service name-based
FingerprintElement = Tuple[str, str, str, int]


@dataclass
class IncidentFingerprint:
    """A behavioral fingerprint of an incident window."""
    elements: List[FingerprintElement]
    structural_hash: str
    trigger_node_id: str
    window_start: datetime
    window_end: datetime
    event_count: int

    def to_tuple_string(self) -> str:
        """Serialize to canonical string for storage."""
        return json.dumps(self.elements, separators=(',', ':'))

    def edit_distance(self, other: 'IncidentFingerprint') -> int:
        """Compute Levenshtein distance between two fingerprint sequences."""
        seq1 = self.elements
        seq2 = other.elements

        if not seq1:
            return len(seq2)
        if not seq2:
            return len(seq1)

        # Dynamic programming
        m, n = len(seq1), len(seq2)
        dp = [[0] * (n + 1) for _ in range(m + 1)]

        for i in range(m + 1):
            dp[i][0] = i
        for j in range(n + 1):
            dp[0][j] = j

        for i in range(1, m + 1):
            for j in range(1, n + 1):
                cost = 0 if seq1[i - 1] == seq2[j - 1] else 1
                dp[i][j] = min(
                    dp[i - 1][j] + 1,      # deletion
                    dp[i][j - 1] + 1,      # insertion
                    dp[i - 1][j - 1] + cost  # substitution
                )

        return dp[m][n]


class Fingerprinter:
    """
    Extracts behavioral fingerprints from incident windows.

    The fingerprint uses service ROLES relative to the trigger service:
    - "trigger": the service that triggered the incident
    - "upstream-N": N hops upstream in the graph
    - "downstream-N": N hops downstream in the graph

    This makes fingerprints rename-proof: "payments-svc" and "billing-svc"
    are the same node, so they have the same role.
    """

    def __init__(
        self,
        window_before: timedelta = timedelta(minutes=30),
        window_after: timedelta = timedelta(minutes=5),
    ) -> None:
        self.window_before = window_before
        self.window_after = window_after

    def fingerprint(
        self,
        trigger_node_id: str,
        trigger_service: str,
        incident_ts: datetime,
        events: List[Dict[str, Any]],
        graph_manager: Any,  # GraphManager for topology lookups
        node_store: Any,     # NodeStore for name->UUID resolution
        temporal_view: Any = None,  # Optional TemporalGraphView for point-in-time topology
    ) -> IncidentFingerprint:
        """
        Extract a fingerprint from a window of events.

        Parameters
        ----------
        trigger_node_id: UUID of the triggering service
        trigger_service: Canonical name of trigger service (for resolving names in events)
        incident_ts: When the incident signal fired
        events: Events in the window (with parsed 'ts' as datetime)
        graph_manager: For computing hops from trigger service (current state)
        node_store: For resolving service names to UUIDs
        temporal_view: Optional TemporalGraphView for point-in-time topology.
            If provided, uses topology at incident_ts for consistent role computation.
        """
        window_start = incident_ts - self.window_before
        window_end = incident_ts + self.window_after

        # Compute role mapping: node_id -> role string
        # Use temporal view if available for consistent role computation
        if temporal_view:
            role_map = self._compute_roles_temporal(
                trigger_node_id, temporal_view, incident_ts
            )
        else:
            role_map = self._compute_roles(
                trigger_node_id, graph_manager, node_store
            )

        # Build fingerprint elements
        elements: List[FingerprintElement] = []

        # Signal-dense event kinds for incident patterns
        SIGNAL_KINDS = {EventKind.DEPLOY, EventKind.LOG, EventKind.INCIDENT_SIGNAL, 
                       EventKind.REMEDIATION, EventKind.TRACE}
        
        # Signal-dense metric names (exclude background noise like qps, cpu, etc.)
        SIGNAL_METRICS = {"latency", "error", "memory", "heap", "gc", "timeout", 
                         "failure", "dropped", "rejected", "queue"}

        for ev in events:
            ev_ts = ev.get("ts")
            if not isinstance(ev_ts, datetime):
                continue
            if ev_ts < window_start or ev_ts > window_end:
                continue

            kind = ev.get("kind", "unknown")
            service = ev.get("service")

            # Skip events without a service we can resolve
            if not service:
                continue

            # FILTER: Only include signal-dense event kinds
            if kind == EventKind.METRIC:
                metric_name = ev.get("name", "")
                # Only include metrics that indicate problems
                if not any(sig in metric_name.lower() for sig in SIGNAL_METRICS):
                    continue
            elif kind not in SIGNAL_KINDS:
                continue  # Skip topology, config, etc.

            # Resolve service to node_id, then to role
            node_id = node_store.resolve(service)
            if not node_id:
                continue

            role = role_map.get(node_id, "unknown")

            # Direction relative to incident
            direction = "before" if ev_ts <= incident_ts else "after"

            # Relative time rounded to nearest minute
            rel_minutes = int((ev_ts - incident_ts).total_seconds() / 60)

            # Bucket metric values to avoid over-specificity
            if kind == EventKind.METRIC:
                name = ev.get("name", "unknown")
                value = ev.get("value", 0)
                bucketed = self._bucket_metric(name, value)
                kind_label = f"metric:{name}:{bucketed}"
            elif kind == EventKind.LOG:
                level = ev.get("level", "info")
                kind_label = f"log:{level}"
            elif kind == EventKind.DEPLOY:
                kind_label = "deploy"
            elif kind == EventKind.REMEDIATION:
                action = ev.get("action", "unknown")
                kind_label = f"remediation:{action}"
            elif kind == EventKind.TRACE:
                kind_label = "trace"
            else:
                kind_label = kind

            element = (kind_label, role, direction, rel_minutes)
            elements.append(element)

        # Sort by relative time for canonical ordering
        elements.sort(key=lambda e: (e[3], e[0], e[1]))

        # Compute structural hash
        hash_input = json.dumps(elements, separators=(',', ':'))
        structural_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:32]

        return IncidentFingerprint(
            elements=elements,
            structural_hash=structural_hash,
            trigger_node_id=trigger_node_id,
            window_start=window_start,
            window_end=window_end,
            event_count=len(elements),
        )

    def _compute_roles(
        self,
        trigger_node_id: str,
        graph_manager: Any,
        node_store: Any,
    ) -> Dict[str, str]:
        """
        Compute role mapping: node_id -> role string.

        Roles:
        - "trigger": the trigger service itself
        - "upstream-1", "upstream-2": 1-2 hops upstream
        - "downstream-1", "downstream-2": 1-2 hops downstream
        """
        role_map: Dict[str, str] = {trigger_node_id: "trigger"}

        # Get neighbors at each hop distance
        for hops in range(1, 3):
            neighbors = graph_manager.neighbors_at_hops(trigger_node_id, hops)
            for node_id, direction in neighbors:
                if node_id not in role_map:
                    role = f"{direction}-{hops}"
                    role_map[node_id] = role

        return role_map

    def _compute_roles_temporal(
        self,
        trigger_node_id: str,
        temporal_view: Any,
        incident_ts: datetime,
    ) -> Dict[str, str]:
        """
        Compute role mapping using point-in-time topology.

        This ensures consistent fingerprinting even when topology changes
        over time (dependency additions/removals, renames).
        """
        role_map: Dict[str, str] = {trigger_node_id: "trigger"}

        # Get neighborhood at incident time
        neighborhood, subgraph = temporal_view.get_neighborhood_at_timestamp(
            trigger_node_id, incident_ts, max_hops=2
        )

        # Compute hop distances using BFS on the subgraph
        visited: Dict[str, int] = {trigger_node_id: 0}
        frontier: Dict[str, int] = {trigger_node_id: 0}

        for hop in range(1, 3):
            next_frontier: Dict[str, int] = {}
            for node_id, _ in frontier.items():
                # Check both directions in the subgraph
                if node_id in subgraph.nodes:
                    for succ in subgraph.successors(node_id):
                        if succ not in visited:
                            visited[succ] = hop
                            next_frontier[succ] = hop
                            role_map[succ] = f"downstream-{hop}"
                    for pred in subgraph.predecessors(node_id):
                        if pred not in visited:
                            visited[pred] = hop
                            next_frontier[pred] = hop
                            role_map[pred] = f"upstream-{hop}"
            frontier = next_frontier

        return role_map

    def _bucket_metric(self, name: str, value: float) -> str:
        """Bucket metric values to avoid over-specificity."""
        # Latency bucketing
        if "latency" in name.lower():
            if value < 100:
                return "low"
            elif value < 1000:
                return "medium"
            elif value < 5000:
                return "high"
            else:
                return "critical"

        # Error rate bucketing
        if "error" in name.lower() or "rate" in name.lower():
            if value < 1:
                return "low"
            elif value < 5:
                return "medium"
            elif value < 10:
                return "high"
            else:
                return "critical"

        # Default: relative bucketing
        if value == 0:
            return "zero"
        elif value < 10:
            return "small"
        elif value < 100:
            return "medium"
        elif value < 1000:
            return "large"
        else:
            return "very_large"


def compute_similarity(fp1: IncidentFingerprint, fp2: IncidentFingerprint) -> float:
    """
    Compute similarity score between two fingerprints.

    - Exact match: 1.0
    - Edit distance 1: 0.8
    - Edit distance 2: 0.6
    - Higher distance: drops off
    """
    if fp1.structural_hash == fp2.structural_hash:
        return 1.0

    distance = fp1.edit_distance(fp2)

    if distance == 0:
        return 1.0
    elif distance == 1:
        return 0.8
    elif distance == 2:
        return 0.6
    elif distance <= 4:
        return 0.4
    else:
        max_len = max(len(fp1.elements), len(fp2.elements))
        if max_len == 0:
            return 0.0
        return max(0.0, 1.0 - (distance / max_len))
