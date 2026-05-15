"""
Persistent Context Engine — package root.

Quick-start
-----------
    from persistent_context_engine import Engine, EngineConfig

    engine = Engine(EngineConfig(db_path="engine.duckdb"))
    engine.ingest(stream_of_events)
    ctx = engine.reconstruct_context(incident_signal, mode="fast")
    engine.close()
"""

from .engine import Engine
from .config import EngineConfig
from .schema import (
    Event,
    IncidentSignal,
    Context,
    CausalEdge,
    IncidentMatch,
    Remediation,
    EventKind,
    EdgeKind,
)

__all__ = [
    "Engine",
    "EngineConfig",
    "Event",
    "IncidentSignal",
    "Context",
    "CausalEdge",
    "IncidentMatch",
    "Remediation",
    "EventKind",
    "EdgeKind",
]
