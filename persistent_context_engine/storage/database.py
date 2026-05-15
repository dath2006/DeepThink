"""
DuckDB connection manager.

Design rules (from DuckDB docs):
  - One primary read-write connection per engine instance.
  - Use conn.cursor() for concurrent read-only queries if needed later.
  - Explicit transactions via begin() / commit() / rollback().
  - DuckDB appends never conflict even on the same table, so the common
    case (ingest) is safe to pipeline without row-level locking.
"""

from __future__ import annotations

import logging
from typing import Optional

import duckdb

from ..schema import SCHEMA_DDL, INDEX_DDL

log = logging.getLogger(__name__)


class DatabaseManager:
    """
    Owns the single DuckDB connection for the engine lifetime.

    The connection is opened lazily on first access so that the object can
    be constructed cheaply (useful in tests).
    """

    def __init__(self, db_path: str = ":memory:") -> None:
        self.db_path = db_path
        self._conn: Optional[duckdb.DuckDBPyConnection] = None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open the DuckDB connection and initialise the schema."""
        if self._conn is not None:
            return
        self._conn = duckdb.connect(database=self.db_path)
        self._init_schema()
        log.debug("DuckDB connected: %s", self.db_path)

    def _init_schema(self) -> None:
        """Create tables and indexes if they do not already exist."""
        for statement in SCHEMA_DDL.strip().split(";"):
            stmt = statement.strip()
            if stmt:
                self._conn.execute(stmt)
        for idx_sql in INDEX_DDL:
            self._conn.execute(idx_sql)

    def close(self) -> None:
        """Flush and close the connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            log.debug("DuckDB connection closed")

    # ------------------------------------------------------------------
    # Property accessor — raises if not yet connected
    # ------------------------------------------------------------------

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            self.connect()
        return self._conn

    # ------------------------------------------------------------------
    # Transaction helpers
    # ------------------------------------------------------------------

    def begin(self) -> None:
        self.conn.begin()

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()
