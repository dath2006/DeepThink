"""
Recent-events ring buffer.

Holds the last N ingested events in memory using a ``deque`` so that
context reconstruction can query the hot window without hitting DuckDB.
The buffer is completely ephemeral — it is rebuilt from DuckDB on engine
restart if needed.

Thread safety: ``deque`` operations are GIL-protected in CPython but the
class does not add extra locking.  If the engine is ever used in a
multi-threaded write scenario, wrap accesses with a lock.
"""

from __future__ import annotations

from collections import deque
from datetime import datetime
from typing import Any, Callable, Dict, Iterator, List, Optional


class RecentEventsBuffer:
    """
    Fixed-capacity ring buffer of the most recently ingested events.

    The buffer stores the fully-normalised event dict (same structure
    that comes out of EventParser.normalise).  ``deque(maxlen=N)``
    auto-evicts the oldest entry when full — O(1) append and eviction.
    """

    def __init__(self, maxsize: int = 500) -> None:
        self._buf: deque = deque(maxlen=maxsize)
        self.maxsize = maxsize

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def push(self, event: Dict[str, Any]) -> None:
        """Add an event to the front (most-recent end) of the buffer."""
        self._buf.appendleft(event)

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def latest(self, n: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return the most recent *n* events (newest first)."""
        items = list(self._buf)
        return items if n is None else items[:n]

    def in_window(
        self,
        start_ts: datetime,
        end_ts: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Return events whose ``ts`` falls within [start_ts, end_ts].
        Results are ordered newest-first (buffer insertion order).
        """
        return [
            e for e in self._buf
            if start_ts <= e["ts"] <= end_ts
        ]

    def for_service(
        self,
        service_name: str,
        n: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Return recent events for a specific service (newest first)."""
        results = [e for e in self._buf if e.get("service") == service_name]
        return results if n is None else results[:n]

    def for_services(
        self,
        service_names: List[str],
        start_ts: Optional[datetime] = None,
        end_ts:   Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Return events for a set of service names, optionally filtered by time."""
        name_set = set(service_names)
        results  = []
        for e in self._buf:
            if e.get("service") not in name_set:
                continue
            ts: datetime = e["ts"]
            if start_ts and ts < start_ts:
                continue
            if end_ts and ts > end_ts:
                continue
            results.append(e)
        return results

    def search(
        self,
        predicate: Callable[[Dict[str, Any]], bool],
    ) -> List[Dict[str, Any]]:
        """Return events matching an arbitrary predicate."""
        return [e for e in self._buf if predicate(e)]

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self._buf)

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return iter(self._buf)

    def is_full(self) -> bool:
        return len(self._buf) == self.maxsize
