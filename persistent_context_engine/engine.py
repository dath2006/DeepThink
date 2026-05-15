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
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from .config import EngineConfig
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

        Phase 2 implementation
        ----------------------
        Returns complete Context with:
          * related_events: time-windowed events from buffer/DB
          * causal_chain: inferred cause-effect edges
          * similar_past_incidents: behavioral pattern matches
          * suggested_remediations: historical remediations with decay
        """
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
        end_ts   = incident_ts + window   # symmetric: also capture post-incident resolution

        max_events = (
            self._cfg.fast_mode_max_events
            if mode == "fast"
            else self._cfg.deep_mode_max_events
        )

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

        # Use temporal topology expansion for robustness to topology drift
        expanded_services = self._expand_services(
            anchor_services, at_timestamp=incident_ts
        )

        # --- Fast path: ring buffer ----------------------------------
        related: List[Dict[str, Any]] = []
        if expanded_services:
            related = self._buffer.for_services(expanded_services, start_ts, end_ts)
        # Supplement from full window scan
        if len(related) < max_events:
            window_events = self._buffer.in_window(start_ts, end_ts)
            seen_ids: set = {id(e) for e in related}
            for e in window_events:
                if id(e) not in seen_ids:
                    related.append(e)
                    seen_ids.add(id(e))

        # --- Always query raw store for complete window (buffer may miss events) -----
        rows = self._raw_store.get_by_timerange(start_ts, end_ts, limit=max_events)
        seen_ids: set = {id(e) for e in related}
        for r in rows:
            ev = r["raw"]
            eid = ev.get("id") or id(ev)
            if eid not in seen_ids:
                related.append(ev)
                seen_ids.add(eid)

        # Deduplicate by raw event dict identity (use id if present)
        seen_ids: set = set()
        deduped: List[Dict[str, Any]] = []
        for ev in related:
            eid = ev.get("id") or id(ev)
            if eid not in seen_ids:
                seen_ids.add(eid)
                deduped.append(ev)
            if len(deduped) >= max_events:
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

        # --- Phase 2: Build causal chain --------------------------------
        causal_chain = self._build_causal_chain(
            deduped, trigger_node_id, normalised_signal.get("incident_id", "unknown")
        )

        # --- Phase 2: Incident matching via fingerprint ------------------
        similar_incidents: List[IncidentMatch] = []
        fingerprint_hash = ""

        if trigger_node_id:
            # Extract fingerprint (BEFORE normalizing timestamps to strings)
            fingerprint = self._fingerprinter.fingerprint(
                trigger_node_id=trigger_node_id,
                trigger_service=anchor_services[0] if anchor_services else "",
                incident_ts=incident_ts,
                events=deduped,
                graph_manager=self._graph,
                node_store=self._node_store,
            )
            fingerprint_hash = fingerprint.structural_hash

            # Find similar past incidents
            similar_incidents = self._find_similar_incidents(
                fingerprint, trigger_node_id, mode
            )
            
            # Fallback 1: match by trigger service history
            if not similar_incidents and len(fingerprint.elements) == 0 and trigger_node_id:
                similar_incidents = self._find_similar_by_trigger_service(
                    trigger_node_id, limit=5
                )
            
            # Fallback 2: if still no matches, match by alert pattern (family-based)
            if not similar_incidents and normalised_signal.get("trigger"):
                similar_incidents = self._find_similar_by_alert_pattern(
                    normalised_signal["trigger"], limit=5
                )

        # --- Phase 2: Suggest remediations -------------------------------
        suggested_remediations = self._suggest_remediations(
            fingerprint_hash, trigger_node_id, similar_incidents
        )

        # Compute overall confidence
        confidence = self._compute_overall_confidence(
            len(deduped), len(causal_chain), len(similar_incidents)
        )

        # Build explain narrative
        explain = self._build_explain_phase2(
            normalised_signal, deduped, causal_chain, similar_incidents,
            suggested_remediations
        )

        # Normalise ts back to ISO string in every event so related_events
        # always matches the benchmark's Event TypedDict (ts: str).
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
    ) -> List[str]:
        """
        Expand a list of anchor service names to include:
          - The anchors themselves
          - All canonical names and aliases of their 2-hop graph neighbors
        This ensures related services (e.g. upstream dependencies) are
        included even if they don't appear in the trigger string.
        
        Parameters
        ----------
        anchor_services : List[str]
            Service names to expand from
        at_timestamp : Optional[datetime]
            If provided, use graph state at this timestamp (handles topology drift).
            If None, uses current graph state.
        """
        if not anchor_services:
            return []

        all_node_ids: set = set()
        
        for name in anchor_services:
            node_id = self._node_store.resolve(name)
            if node_id:
                all_node_ids.add(node_id)
                
                # Use temporal view if timestamp provided, else current graph
                if at_timestamp:
                    neighborhood, _ = self._temporal_view.get_neighborhood_at_timestamp(
                        node_id, at_timestamp, max_hops=2
                    )
                    all_node_ids.update(neighborhood)
                else:
                    all_node_ids.update(self._graph.neighbors_within_hops(node_id, hops=2))

        expanded: List[str] = list(anchor_services)
        for node_id in all_node_ids:
            expanded.extend(self._node_store.all_names_for_id(node_id))

        return list(set(expanded))  # deduplicate

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
    ) -> List[CausalEdge]:
        """
        Build causal edges from event sequence.

        Rules:
        1. Deploy leads to subsequent metric spike within 10 min
        2. Shared trace_id implies causality
        3. Graph edge (CALLS, DEPENDENCY) implies influence
        """
        chain: List[CausalEdge] = []
        if not events or not trigger_node_id:
            return chain

        # Index events by ID and position
        event_index: Dict[str, Dict[str, Any]] = {}
        for i, ev in enumerate(events):
            eid = ev.get("id") or ev.get("event_id") or f"evt_{i}"
            event_index[eid] = ev
            ev["_idx"] = i

        # Build edges based on rules
        for i, ev_a in enumerate(events):
            ev_a_id = ev_a.get("id") or ev_a.get("event_id") or f"evt_{i}"
            ev_a_ts = self._get_event_ts(ev_a)

            for j, ev_b in enumerate(events[i+1:], start=i+1):
                ev_b_id = ev_b.get("id") or ev_b.get("event_id") or f"evt_{j}"
                ev_b_ts = self._get_event_ts(ev_b)

                if ev_a_ts > ev_b_ts:
                    continue  # Must be before

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

        # Sort by confidence descending, limit
        chain.sort(key=lambda e: e["confidence"], reverse=True)
        return chain[:10]

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
    # Phase 2: Incident matching
    # ------------------------------------------------------------------

    def _find_similar_incidents(
        self,
        fingerprint: Any,  # IncidentFingerprint
        trigger_node_id: str,
        mode: str,
    ) -> List[IncidentMatch]:
        """
        Find past incidents similar to the current fingerprint.

        Uses exact hash match + family-based soft matching + edit distance.
        """
        matches: List[IncidentMatch] = []
        seen_incidents: set = set()

        # 1. Try exact hash match first (across all services for rename-proof matching)
        similar = self._pattern_store.find_similar_patterns(
            fingerprint.structural_hash,
            trigger_node_id=None,
            limit=5,
        )

        for s in similar:
            inc_id = s["incident_id"]
            if inc_id in seen_incidents:
                continue
            seen_incidents.add(inc_id)

            matches.append(IncidentMatch(
                incident_id=inc_id,
                similarity=round(s.get("similarity_score", 0.8), 3),
                rationale=f"Exact fingerprint match in family {s.get('family_id', 'unknown')[:8]}",
            ))

        # 2. If no exact matches, try soft matching by edit distance
        if len(matches) == 0:
            # Get all patterns and compute edit distances
            all_patterns = self._pattern_store._cursor().execute(
                """SELECT incident_id, fingerprint_tuple, fingerprint_hash, trigger_node_id, family_id
                    FROM incident_patterns"""
            ).fetchall()

            pattern_distances = []
            for p in all_patterns:
                inc_id, fp_tuple, fp_hash, trig_id, family_id = p
                if fp_hash == fingerprint.structural_hash:
                    continue  # Already handled above
                if inc_id in seen_incidents:
                    continue
                try:
                    other_elements = json.loads(fp_tuple)
                    from .incident_fingerprinter import IncidentFingerprint
                    other_fp = IncidentFingerprint(
                        elements=other_elements,
                        structural_hash=fp_hash,
                        trigger_node_id=trig_id or "",
                        window_start=datetime.min,
                        window_end=datetime.min,
                        event_count=len(other_elements),
                    )
                    dist = fingerprint.edit_distance(other_fp)
                    if dist <= 2:  # Soft match threshold
                        pattern_distances.append((inc_id, dist, family_id))
                except Exception:
                    continue

            # Sort by distance and take best matches
            pattern_distances.sort(key=lambda x: x[1])
            for inc_id, dist, family_id in pattern_distances[:5]:
                if inc_id in seen_incidents:
                    continue
                similarity = 1.0 - (dist / 5.0)  # Normalize: dist 0 -> 1.0, dist 2 -> 0.6
                matches.append(IncidentMatch(
                    incident_id=inc_id,
                    similarity=round(similarity, 3),
                    rationale=f"Soft match (edit distance {dist}) in family {family_id[:8] if family_id else 'unknown'}",
                ))
                seen_incidents.add(inc_id)

        # 3. In deep mode, try more aggressive soft matching if still few matches
        if len(matches) < 3 and mode == "deep":
            all_hashes = self._pattern_store.get_all_hashes()
            for h in all_hashes:
                if h == fingerprint.structural_hash:
                    continue
                patterns = self._pattern_store._cursor().execute(
                    """SELECT incident_id, fingerprint_tuple, trigger_node_id, family_id
                        FROM incident_patterns WHERE fingerprint_hash = ?""",
                    [h]
                ).fetchall()

                for p in patterns:
                    inc_id, fp_tuple, trig_id, family_id = p
                    if inc_id in seen_incidents:
                        continue
                    try:
                        other_elements = json.loads(fp_tuple)
                        from .incident_fingerprinter import IncidentFingerprint
                        other_fp = IncidentFingerprint(
                            elements=other_elements,
                            structural_hash=h,
                            trigger_node_id=trig_id or "",
                            window_start=datetime.min,
                            window_end=datetime.min,
                            event_count=len(other_elements),
                        )
                        dist = fingerprint.edit_distance(other_fp)
                        if dist <= 1:
                            similarity = 0.8 if dist == 0 else 0.6
                            matches.append(IncidentMatch(
                                incident_id=inc_id,
                                similarity=round(similarity, 3),
                                rationale=f"Deep mode soft match (edit distance {dist})",
                            ))
                            seen_incidents.add(inc_id)
                            if len(matches) >= 5:
                                break
                    except Exception:
                        continue
                if len(matches) >= 5:
                    break

        return matches[:5]

    def _find_similar_by_trigger_service(
        self,
        trigger_node_id: str,
        limit: int = 5,
    ) -> List[IncidentMatch]:
        """
        Find past incidents by the same trigger service (fallback when window is empty).
        
        Used for eval incidents where pre-signal context is held out.
        """
        matches: List[IncidentMatch] = []
        
        # Query patterns for this trigger node, ordered by recency
        rows = self._pattern_store._cursor().execute(
            """SELECT incident_id, family_id, similarity_score, created_at
                FROM incident_patterns
                WHERE trigger_node_id = ?
                ORDER BY created_at DESC
                LIMIT ?""",
            [trigger_node_id, limit]
        ).fetchall()
        
        for incident_id, family_id, sim_score, created_at in rows:
            matches.append(IncidentMatch(
                incident_id=incident_id,
                similarity=round(sim_score or 0.7, 3),
                rationale=f"Same trigger service (family {family_id[:8] if family_id else 'unknown'})",
            ))
        
        return matches

    def _find_similar_by_alert_pattern(
        self,
        trigger: str,
        limit: int = 5,
    ) -> List[IncidentMatch]:
        """
        Find past incidents by alert pattern (e.g., 'latency>4s', 'error-rate>5%').
        
        Parses the trigger string to extract metric type and matches to families
        that typically have that alert pattern.
        """
        matches: List[IncidentMatch] = []
        
        # Parse trigger string for alert type
        # Examples: "alert:svc-a/latency_p99_ms>3000", "alert:svc-b/error-rate>5%"
        alert_type = ""
        if "latency" in trigger.lower():
            alert_type = "latency"
        elif "error" in trigger.lower():
            alert_type = "error"
        elif "memory" in trigger.lower():
            alert_type = "memory"
        elif "rate" in trigger.lower():
            alert_type = "rate"
        
        if not alert_type:
            return matches
        
        # Query patterns whose fingerprint contains this alert type
        # Look for patterns with metric events matching the alert type
        rows = self._pattern_store._cursor().execute(
            """SELECT DISTINCT p.incident_id, p.family_id, p.similarity_score
                FROM incident_patterns p
                JOIN incident_families f ON p.family_id = f.id
                WHERE p.fingerprint_tuple LIKE ?
                ORDER BY f.incident_count DESC, p.created_at DESC
                LIMIT ?""",
            [f"%{alert_type}%", limit]
        ).fetchall()
        
        seen: set = set()
        for incident_id, family_id, sim_score in rows:
            if incident_id in seen:
                continue
            seen.add(incident_id)
            matches.append(IncidentMatch(
                incident_id=incident_id,
                similarity=round(sim_score or 0.6, 3),
                rationale=f"Alert pattern match: {alert_type} (family {family_id[:8] if family_id else 'unknown'})",
            ))
        
        return matches

    # ------------------------------------------------------------------
    # Phase 2: Remediation suggestion
    # ------------------------------------------------------------------

    def _suggest_remediations(
        self,
        fingerprint_hash: str,
        trigger_node_id: Optional[str],
        similar_incidents: List[IncidentMatch],
    ) -> List[Remediation]:
        """Suggest remediations based on historical outcomes."""
        suggestions: List[Remediation] = []
        if not trigger_node_id:
            return suggestions

        now = datetime.now(timezone.utc)

        # Strategy 1: Look at remediations for similar incidents
        seen_actions: set = set()

        for match in similar_incidents:
            pattern = self._pattern_store.get_pattern_by_incident(match["incident_id"])
            if not pattern:
                continue

            rems = self._remediation_store.get_for_pattern(pattern["id"], limit=2)
            for rem in rems:
                action_key = f"{rem['action']}:{rem['target_node_id']}"
                if action_key in seen_actions:
                    continue
                seen_actions.add(action_key)

                # Apply temporal decay
                decayed_conf = self._decay_scorer.compute(
                    rem["confidence"], rem["applied_at"], now
                )

                # Boost if outcome was resolved
                if rem["outcome"] == "resolved":
                    decayed_conf = min(0.95, decayed_conf * 1.2)

                suggestions.append(Remediation(
                    action=rem["action"],
                    target=rem["target_service"],  # Use canonical name, not UUID
                    historical_outcome=rem["outcome"],
                    confidence=round(decayed_conf, 3),
                ))

        # Strategy 2: Successful remediations for this target
        if len(suggestions) < 3:
            successful = self._remediation_store.get_successful_actions_for_target(
                trigger_node_id, min_confidence=0.5, limit=3
            )
            for s in successful:
                action_key = f"{s['action']}:{trigger_node_id}"
                if action_key in seen_actions:
                    continue
                seen_actions.add(action_key)

                decayed_conf = self._decay_scorer.compute(
                    s["confidence"], s["applied_at"], now
                )

                target_name = self._node_store.get_canonical_name(trigger_node_id) or "unknown"
                suggestions.append(Remediation(
                    action=s["action"],
                    target=target_name,
                    historical_outcome="resolved",
                    confidence=round(decayed_conf, 3),
                ))

        # Sort by confidence descending
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

    @staticmethod
    def _build_explain_phase2(
        signal: Dict[str, Any],
        events: List[Dict[str, Any]],
        causal_chain: List[CausalEdge],
        similar_incidents: List[IncidentMatch],
        suggested_remediations: List[Remediation],
    ) -> str:
        """Build Phase 2 explain narrative."""
        incident_id = signal.get("incident_id", "unknown")
        trigger = signal.get("trigger", "")

        parts: List[str] = []
        parts.append(f"Incident {incident_id} triggered by '{trigger}'.")
        parts.append(f"Found {len(events)} related events in temporal window.")

        if causal_chain:
            top_edge = causal_chain[0]
            parts.append(
                f"Causal chain: {top_edge['evidence']} "
                f"(confidence {top_edge['confidence']})."
            )

        if similar_incidents:
            parts.append(
                f"{len(similar_incidents)} similar past incidents found, "
                f"most similar: {similar_incidents[0]['incident_id']} "
                f"(score {similar_incidents[0]['similarity']})."
            )

        if suggested_remediations:
            top_rem = suggested_remediations[0]
            parts.append(
                f"Suggested action: {top_rem['action']} on {top_rem['target']} "
                f"(confidence {top_rem['confidence']}, "
                f"historical outcome: {top_rem['historical_outcome']})."
            )

        return " ".join(parts)

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
