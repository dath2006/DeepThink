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
    # Context reconstruction
    # -----------------------------------------------------------------------
    context_window_minutes: int = 60
    """How many minutes before/after an incident signal to search for
    related events during context reconstruction."""

    fast_mode_max_events: int = 200
    """Maximum related events returned in fast reconstruction mode."""

    deep_mode_max_events: int = 1000
    """Maximum related events returned in deep reconstruction mode."""

    # -----------------------------------------------------------------------
    # Mode-differentiated parameters
    # -----------------------------------------------------------------------
    fast_graph_hops: int = 1
    """Number of graph hops for service expansion in fast mode."""

    deep_graph_hops: int = 2
    """Number of graph hops for service expansion in deep mode."""

    fast_causal_max_pairs: int = 5
    """Max causal edges returned in fast mode (pairwise only)."""

    deep_causal_max_pairs: int = 15
    """Max causal edges returned in deep mode (includes transitive chains)."""

    fast_match_limit: int = 5
    """Max similar incidents returned in fast mode.
    Must be >=5 because benchmark measures recall@5."""

    deep_match_limit: int = 5
    """Max similar incidents returned in deep mode."""

    fast_time_bucket_minutes: int = 10
    """Fingerprint time bucket granularity in fast mode (coarser)."""

    deep_time_bucket_minutes: int = 2
    """Fingerprint time bucket granularity in deep mode (finer)."""

    fast_remediation_search_neighbors: bool = False
    """In fast mode, only search remediations for the trigger service."""

    deep_remediation_search_neighbors: bool = True
    """In deep mode, also search remediations for upstream/downstream services."""

    # -----------------------------------------------------------------------
    # Caching
    # -----------------------------------------------------------------------
    fingerprint_cache_ttl_seconds: float = 30.0
    """TTL for in-memory fingerprint→similar-incidents cache.  Avoids
    redundant DB scans when multiple signals share the same fingerprint
    within a short window."""

    def for_mode(self, mode: str) -> "ModeParams":
        """Return a frozen bag of mode-specific parameters."""
        return ModeParams(
            max_events=self.fast_mode_max_events if mode == "fast" else self.deep_mode_max_events,
            graph_hops=self.fast_graph_hops if mode == "fast" else self.deep_graph_hops,
            causal_max=self.fast_causal_max_pairs if mode == "fast" else self.deep_causal_max_pairs,
            match_limit=self.fast_match_limit if mode == "fast" else self.deep_match_limit,
            time_bucket_minutes=self.fast_time_bucket_minutes if mode == "fast" else self.deep_time_bucket_minutes,
            search_neighbors=self.fast_remediation_search_neighbors if mode == "fast" else self.deep_remediation_search_neighbors,
        )


@dataclass(frozen=True)
class ModeParams:
    """Immutable bag of mode-specific parameters for a single reconstruct_context call."""
    max_events: int
    graph_hops: int
    causal_max: int
    match_limit: int
    time_bucket_minutes: int
    search_neighbors: bool
