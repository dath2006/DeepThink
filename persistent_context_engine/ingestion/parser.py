"""
Event parser — normalises raw input into a stable internal representation.

Input flexibility
-----------------
The benchmark harness passes already-parsed Python dicts (not raw JSONL
strings).  The parser therefore accepts BOTH:
  * ``dict``  — benchmark path (primary)
  * ``str``   — JSONL file ingestion path

Normalisation performed
-----------------------
* ``ts`` is parsed from ISO-8601 string to a UTC-aware ``datetime``.
* ``kind`` is validated against EventKind enum (unknown kinds are accepted
  with a warning so held-out event types don't crash the engine).
* All original keys are preserved verbatim in the returned dict.

The parser does NOT validate per-kind required fields — that would couple
it to a fixed schema and break on held-out event kinds.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional, Union

from ..schema import EventKind

log = logging.getLogger(__name__)


class ParseError(Exception):
    """Raised only for malformed input (bad JSON, missing ts/kind)."""


class EventParser:
    """Stateful parser that tracks parse/error counts for observability."""

    def __init__(self) -> None:
        self.parsed_count = 0
        self.error_count  = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def normalise(self, raw: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Normalise one event.

        Returns a dict that always has:
          ``ts``   : UTC-aware datetime
          ``kind`` : str  (EventKind value or unknown string)
          All original keys from the source dict.

        Raises
        ------
        ParseError  if the input is not valid JSON (string input) or if
                    the ``ts`` / ``kind`` fields are absent or unparseable.
        """
        if isinstance(raw, str):
            try:
                raw = json.loads(raw.strip())
            except json.JSONDecodeError as exc:
                raise ParseError(f"Invalid JSON: {raw[:120]}") from exc

        if not isinstance(raw, dict):
            raise ParseError(f"Expected dict, got {type(raw).__name__}")

        if "ts" not in raw:
            raise ParseError("Event missing required field 'ts'")
        if "kind" not in raw:
            raise ParseError("Event missing required field 'kind'")

        ts   = self._parse_timestamp(raw["ts"])
        kind = str(raw["kind"])

        if kind not in EventKind._value2member_map_:
            log.warning("Unknown event kind '%s' — will be stored as-is", kind)

        event = dict(raw)
        event["ts"]   = ts
        event["kind"] = kind
        self.parsed_count += 1
        return event

    def normalise_many(
        self,
        items: Iterator[Union[str, Dict[str, Any]]],
    ) -> Iterator[Dict[str, Any]]:
        """Yield normalised events, skipping (and counting) parse errors."""
        for item in items:
            try:
                yield self.normalise(item)
            except ParseError as exc:
                self.error_count += 1
                log.debug("Parse error (skipped): %s", exc)

    # ------------------------------------------------------------------
    # Timestamp parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime:
        """
        Convert various timestamp representations to a UTC-aware datetime.

        Handles:
          * ISO-8601 string with "Z" suffix  (most common from benchmark)
          * ISO-8601 string with "+HH:MM" offset
          * Naive ISO-8601 string (assumed UTC)
          * datetime object (passed through, timezone attached if naive)
        """
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value

        if not isinstance(value, str):
            raise ParseError(f"Cannot parse timestamp: {value!r}")

        s = value.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"

        try:
            dt = datetime.fromisoformat(s)
        except ValueError as exc:
            raise ParseError(f"Invalid timestamp format: {value!r}") from exc

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def stats(self) -> Dict[str, int]:
        return {"parsed": self.parsed_count, "errors": self.error_count}

    def reset_stats(self) -> None:
        self.parsed_count = 0
        self.error_count  = 0
