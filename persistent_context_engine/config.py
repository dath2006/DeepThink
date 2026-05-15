"""
Engine configuration.  All tuneable parameters live here so downstream
modules never carry magic numbers.
"""

from dataclasses import dataclass, field


@dataclass
class EngineConfig:
    # -----------------------------------------------------------------------
    # Storage
    # -----------------------------------------------------------------------
    db_path: str = ":memory:"
    """DuckDB database path.  Use `:memory:` for tests / benchmarks that
    do not need persistence across process restarts."""

    # -----------------------------------------------------------------------
    # Ingestion
    # -----------------------------------------------------------------------
    buffer_size: int = 500
    """Number of most-recent events held in the in-memory ring buffer.
    Avoids hitting DuckDB for the hot path during context reconstruction."""

    ingest_batch_size: int = 256
    """Number of events processed per explicit DuckDB transaction when
    bulk-ingesting.  Larger batches are faster; smaller batches reduce
    memory pressure during streaming ingestion."""

    # -----------------------------------------------------------------------
    # Context reconstruction (Phase 2+)
    # -----------------------------------------------------------------------
    context_window_minutes: int = 60
    """How many minutes before/after an incident signal to search for
    related events during context reconstruction."""

    fast_mode_max_events: int = 200
    """Maximum related events returned in fast reconstruction mode."""

    deep_mode_max_events: int = 1000
    """Maximum related events returned in deep reconstruction mode."""

    # -----------------------------------------------------------------------
    # Caching (Phase 3)
    # -----------------------------------------------------------------------
    fingerprint_cache_ttl_seconds: float = 30.0
    """TTL for in-memory fingerprint→similar-incidents cache.  Avoids
    redundant DB scans when multiple signals share the same fingerprint
    within a short window."""
