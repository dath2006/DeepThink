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
