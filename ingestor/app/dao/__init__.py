"""
DAO Layer using SQLAlchemy Core.

Centralizes all database access for the ingestor application.
"""

import logging
from typing import Any, Sequence
from contextlib import contextmanager

from sqlalchemy import Connection, text
from sqlalchemy.engine import Result

logger = logging.getLogger(__name__)


class BaseCoreDAO:
    """Base Data Access Object using SQLAlchemy Core."""

    def __init__(self, connection: Connection):
        """
        Initialize DAO with SQLAlchemy connection.

        Args:
            connection: SQLAlchemy Core connection
        """
        self._connection = connection

    @property
    def connection(self) -> Connection:
        """Get the database connection."""
        return self._connection

    def execute(self, statement, parameters=None) -> Result:
        """Execute a SQLAlchemy statement."""
        return self._connection.execute(statement, parameters)

    def execute_sql(self, sql: str, params: dict = None) -> Result:
        """Execute raw SQL with named parameters."""
        return self._connection.execute(text(sql), params or {})

    def fetch_one(self, statement) -> Any | None:
        """Execute and fetch single row."""
        result = self.execute(statement)
        return result.fetchone()

    def fetch_all(self, statement) -> Sequence:
        """Execute and fetch all rows."""
        result = self.execute(statement)
        return result.fetchall()

    def scalar(self, statement) -> Any | None:
        """Execute and return scalar value (first column of first row)."""
        result = self.execute(statement)
        row = result.fetchone()
        return row[0] if row else None


# Import DAOs
from .datasource_dao import DatasourceDAO
from .batch_dao import IngestBatchDAO
from .staging_dao import StagingDAO, sanitize_for_json
from .data_dao import DataDAO
from .pipeline_dao import PipelineDAO
from .cursor_dao import CursorDAO
from .factory import DAOFactory

__all__ = [
    'BaseCoreDAO',
    'DatasourceDAO',
    'IngestBatchDAO',
    'StagingDAO',
    'DataDAO',
    'PipelineDAO',
    'CursorDAO',
    'DAOFactory',
    'sanitize_for_json',
]

    def scalar(self, statement) -> Any | None:
        """Execute and return scalar value."""
        result = self.execute(statement)
        return result.scalar()

    def commit(self):
        """Commit transaction."""
        self._connection.commit()

    def rollback(self):
        """Rollback transaction."""
        self._connection.rollback()
