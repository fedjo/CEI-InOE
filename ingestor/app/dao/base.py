"""
Base DAO class with connection management and common utilities.
"""

import logging
from typing import Any, List, Optional, Tuple
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class BaseDAO:
    """Base Data Access Object with connection management."""

    def __init__(self, connection):
        """
        Initialize DAO with database connection.

        Args:
            connection: psycopg2 database connection
        """
        self._connection = connection

    @property
    def connection(self):
        """Get the database connection."""
        return self._connection

    @contextmanager
    def cursor(self):
        """Context manager for cursor with automatic cleanup."""
        cur = self._connection.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def execute(self, sql: str, params: tuple = None) -> int:
        """Execute SQL and return row count."""
        with self.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount

    def execute_returning(self, sql: str, params: tuple = None) -> Optional[Any]:
        """Execute SQL with RETURNING clause."""
        with self.cursor() as cur:
            cur.execute(sql, params)
            result = cur.fetchone()
            return result[0] if result else None

    def fetch_one(self, sql: str, params: tuple = None) -> Optional[Tuple]:
        """Fetch single row."""
        with self.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()

    def fetch_all(self, sql: str, params: tuple = None) -> List[Tuple]:
        """Fetch all rows."""
        with self.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def commit(self):
        """Commit transaction."""
        self._connection.commit()

    def rollback(self):
        """Rollback transaction."""
        self._connection.rollback()
