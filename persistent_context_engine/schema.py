"""
Schema definitions for the Persistent Context Engine.

Two layers:
  1. Benchmark-compatible TypedDicts: Event, IncidentSignal, Context and
     their nested types (CausalEdge, IncidentMatch, Remediation).
  2. Internal storage TypedDicts: NodeRecord, EdgeRecord.
  3. DuckDB DDL constants — VARCHAR for IDs/JSON, TIMESTAMPTZ for all
     timestamps, no FK constraints (DuckDB parses but does not enforce them).
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, TypedDict


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class EventKind(str, Enum):
    """All guaranteed event kinds.  str mixin so ``kind == EventKind.DEPLOY``
    works without explicit .value access."""
    DEPLOY          = "deploy"
    LOG             = "log"
    METRIC          = "metric"
    TRACE           = "trace"
    TOPOLOGY        = "topology"
    INCIDENT_SIGNAL = "incident_signal"
    REMEDIATION     = "remediation"


class EdgeKind(str, Enum):
    """Graph edge types produced by the ingest pipeline."""
    CALLS               = "calls"           # service A called service B (from trace spans)
    DEPENDENCY          = "dependency"      # explicit declared dependency
    DEPLOYMENT          = "deployment"      # service deployed a new version
    ERROR_PROPAGATION   = "error_propagation"
    CAUSAL              = "causal"          # inferred causal link (Phase 2+)
    REMEDIATION_LINK    = "remediation_link"


# ---------------------------------------------------------------------------
# Benchmark-compatible TypedDicts
# ---------------------------------------------------------------------------

class Event(TypedDict, total=False):
    """
    Telemetry event as received from the benchmark harness.
    All fields are optional because different event kinds carry different keys.
    The only *required* fields at runtime are ``ts`` and ``kind``.
    """
    ts:          str            # ISO-8601 timestamp, e.g. "2026-05-10T14:21:30Z"
    kind:        str            # EventKind value
    # deploy
    service:     str
    version:     str
    actor:       str
    # log
    level:       str
    msg:         str
    trace_id:    str
    # metric
    name:        str
    value:       float
    # trace  (spans use "svc", not "service")
    spans:       List[Dict[str, Any]]
    # topology  (benchmark uses "from_" because "from" is a Python keyword)
    change:      str            # "rename" | "dep_add" | "dep_remove"
    from_:       str
    to:          str
    # incident_signal
    incident_id: str
    trigger:     str
    # remediation
    action:      str
    target:      str
    outcome:     str
    # extension / held-out kinds
    attrs:       Dict[str, Any]


class IncidentSignal(TypedDict, total=False):
    """Incident signal passed to reconstruct_context."""
    ts:          str
    kind:        str
    incident_id: str
    trigger:     str
    service:     str


class CausalEdge(TypedDict):
    cause_event_id:  str
    effect_event_id: str
    evidence:        str
    confidence:      float


class IncidentMatch(TypedDict):
    incident_id: str
    similarity:  float
    rationale:   str


class Remediation(TypedDict):
    action:            str
    target:            str
    historical_outcome: str
    confidence:        float


class Context(TypedDict):
    """Output of reconstruct_context — structured, not free text."""
    related_events:         List[Event]
    causal_chain:           List[CausalEdge]
    similar_past_incidents: List[IncidentMatch]
    suggested_remediations: List[Remediation]
    confidence:             float
    explain:                str


# ---------------------------------------------------------------------------
# Internal storage TypedDicts  (used by storage layer only)
# ---------------------------------------------------------------------------

class NodeRecord(TypedDict):
    id:             str                     # UUID string
    canonical_name: str
    aliases:        List[str]               # historical names
    first_seen_ts:  datetime
    last_seen_ts:   datetime


class EdgeRecord(TypedDict):
    id:                 str
    source_node_id:     str
    target_node_id:     str
    edge_kind:          str                 # EdgeKind value
    first_seen_ts:      datetime
    last_seen_ts:       datetime
    retired_at:         Optional[datetime]  # None = active
    confidence:         float
    evidence_event_ids: List[str]           # raw_event IDs
    metadata:           Dict[str, Any]


class IncidentPatternRecord(TypedDict):
    """Internal storage for incident behavioral fingerprints."""
    id:                 str           # UUID
    incident_id:        str           # from incident_signal
    fingerprint_hash:   str           # SHA-256 of canonical fingerprint
    fingerprint_tuple:  str           # JSON array of fingerprint elements
    trigger_node_id:    str           # UUID of triggering service
    window_start_ts:    datetime
    window_end_ts:      datetime
    family_id:          Optional[str] # NULL until matched
    similarity_score:   float         # 0..1, similarity to family centroid
    event_count:        int
    created_at:         datetime


class IncidentFamilyRecord(TypedDict):
    """A group of incidents sharing similar behavioral fingerprints."""
    id:                    str       # UUID
    family_hash:           str       # Representative fingerprint hash
    representative_pattern_id: str     # Pattern that started this family
    incident_count:        int       # How many incidents in this family
    first_seen_ts:         datetime
    last_confirmed_ts:     datetime  # Updated when new incident joins
    reinforced_confidence: float     # Base confidence + reinforcement
    decay_rate:            float     # 0.95 default


class RemediationRecord(TypedDict):
    """Historical remediation action with outcome."""
    id:                 str       # UUID
    incident_id:        str       # from incident_signal
    pattern_id:         str       # UUID of matched pattern
    action:             str       # rollback, restart, config_change, etc.
    target_node_id:     str       # UUID (not service name!)
    target_service:     str       # Canonical name at time of remediation
    outcome:            str       # resolved, worsened, unknown
    confidence:         float     # Historical success rate
    applied_at:         datetime
    evidence_event_ids: List[str] # Links to raw_events


# ---------------------------------------------------------------------------
# DuckDB DDL
# ---------------------------------------------------------------------------

SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS raw_events (
    id          VARCHAR PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL,
    kind        VARCHAR     NOT NULL,
    service     VARCHAR,
    raw_json    VARCHAR     NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
    id             VARCHAR PRIMARY KEY,
    canonical_name VARCHAR NOT NULL,
    aliases        VARCHAR NOT NULL DEFAULT '[]',
    first_seen_ts  TIMESTAMPTZ NOT NULL,
    last_seen_ts   TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS edges (
    id                 VARCHAR PRIMARY KEY,
    source_node_id     VARCHAR NOT NULL,
    target_node_id     VARCHAR NOT NULL,
    edge_kind          VARCHAR NOT NULL,
    first_seen_ts      TIMESTAMPTZ NOT NULL,
    last_seen_ts       TIMESTAMPTZ NOT NULL,
    retired_at         TIMESTAMPTZ,
    confidence         DOUBLE  NOT NULL DEFAULT 0.0,
    evidence_event_ids VARCHAR NOT NULL DEFAULT '[]',
    metadata           VARCHAR NOT NULL DEFAULT '{}'
);
"""

INDEX_DDL: List[str] = [
    "CREATE INDEX IF NOT EXISTS idx_re_ts      ON raw_events(ts)",
    "CREATE INDEX IF NOT EXISTS idx_re_kind    ON raw_events(kind)",
    "CREATE INDEX IF NOT EXISTS idx_re_service ON raw_events(service)",
    "CREATE INDEX IF NOT EXISTS idx_n_name     ON nodes(canonical_name)",
    "CREATE INDEX IF NOT EXISTS idx_e_src      ON edges(source_node_id)",
    "CREATE INDEX IF NOT EXISTS idx_e_dst      ON edges(target_node_id)",
    "CREATE INDEX IF NOT EXISTS idx_e_kind     ON edges(edge_kind)",
]

# Phase 2: Incident patterns and families DDL
PATTERN_DDL = """
CREATE TABLE IF NOT EXISTS incident_patterns (
    id                  VARCHAR PRIMARY KEY,
    incident_id         VARCHAR NOT NULL,
    fingerprint_hash    VARCHAR NOT NULL,
    fingerprint_tuple   VARCHAR NOT NULL,
    trigger_node_id     VARCHAR NOT NULL,
    window_start_ts     TIMESTAMPTZ NOT NULL,
    window_end_ts       TIMESTAMPTZ NOT NULL,
    family_id           VARCHAR,
    similarity_score    DOUBLE NOT NULL DEFAULT 0.0,
    event_count         INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS incident_families (
    id                      VARCHAR PRIMARY KEY,
    family_hash             VARCHAR NOT NULL,
    trigger_node_id         VARCHAR NOT NULL DEFAULT '',
    representative_pattern_id VARCHAR NOT NULL,
    incident_count          INTEGER NOT NULL DEFAULT 0,
    first_seen_ts           TIMESTAMPTZ NOT NULL,
    last_confirmed_ts       TIMESTAMPTZ NOT NULL,
    reinforced_confidence   DOUBLE NOT NULL DEFAULT 0.5,
    decay_rate              DOUBLE NOT NULL DEFAULT 0.95
);

CREATE TABLE IF NOT EXISTS remediation_history (
    id                  VARCHAR PRIMARY KEY,
    incident_id         VARCHAR NOT NULL,
    pattern_id          VARCHAR,
    action              VARCHAR NOT NULL,
    target_node_id      VARCHAR NOT NULL,
    target_service      VARCHAR NOT NULL,
    outcome             VARCHAR NOT NULL,
    confidence          DOUBLE NOT NULL DEFAULT 0.0,
    applied_at          TIMESTAMPTZ NOT NULL,
    evidence_event_ids  VARCHAR NOT NULL DEFAULT '[]'
);
"""

PATTERN_INDEX_DDL: List[str] = [
    "CREATE INDEX IF NOT EXISTS idx_ip_hash    ON incident_patterns(fingerprint_hash)",
    "CREATE INDEX IF NOT EXISTS idx_ip_family  ON incident_patterns(family_id)",
    "CREATE INDEX IF NOT EXISTS idx_ip_inc   ON incident_patterns(incident_id)",
    "CREATE INDEX IF NOT EXISTS idx_if_hash    ON incident_families(family_hash, trigger_node_id)",
    "CREATE INDEX IF NOT EXISTS idx_rh_inc     ON remediation_history(incident_id)",
    "CREATE INDEX IF NOT EXISTS idx_rh_pattern ON remediation_history(pattern_id)",
    "CREATE INDEX IF NOT EXISTS idx_rh_target  ON remediation_history(target_node_id)",
]
