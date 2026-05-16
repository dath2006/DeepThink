"""
Adapter wrapping the Persistent Context Engine for the Anvil P-02 L3 benchmark.

Usage:
    python run.py --adapter adapters.myteam:Engine --mode fast --out l3_report.json
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable, Literal

# Add the project root to path so persistent_context_engine is importable
# __file__ = .../Anvil-P-E/bench-p02-context/adapters/myteam.py
# 4 parents up → D:\Agents\DeepThink (repo root)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from adapter import Adapter
from persistent_context_engine import Engine as PCEngine, EngineConfig
from schema import Context, Event, IncidentSignal


class PersistentContextAdapter(Adapter):  # also exported as Engine below
    """Benchmark adapter for the Persistent Context Engine."""

    def __init__(self) -> None:
        # Use in-memory DB per seed (harness constructs a fresh adapter per seed)
        # L3 scale: 30 services × 21 days → larger buffer
        # Fix E: Larger buffer (L3 has 75k background events, 2000 evicts too fast)
        # and larger ingest_batch_size to reduce transaction round-trips.
        self._engine = PCEngine(EngineConfig(
            db_path=":memory:",
            buffer_size=5000,
            ingest_batch_size=1000,
        ))

    def ingest(self, events: Iterable[Event]) -> None:
        self._engine.ingest(events)

    def reconstruct_context(
        self,
        signal: IncidentSignal,
        mode: Literal["fast", "deep"] = "fast",
    ) -> Context:
        return self._engine.reconstruct_context(signal, mode=mode)

    def close(self) -> None:
        self._engine.close()


# Alias expected by: python run.py --adapter adapters.myteam:Engine
Engine = PersistentContextAdapter
