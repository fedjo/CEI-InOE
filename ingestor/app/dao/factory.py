"""
DAO Factory for SQLAlchemy Core DAOs.

Provides centralized creation and caching of DAO instances.
"""

from typing import Any, Optional

from sqlalchemy import Connection

from .datasource_dao import DatasourceDAO
from .batch_dao import IngestBatchDAO
from .staging_dao import StagingDAO
from .data_dao import DataDAO
from .pipeline_dao import PipelineDAO
from .cursor_dao import CursorDAO


class DAOFactory:
    """Factory for creating DAO instances with shared connection."""

    def __init__(self, connection: Connection):
        """
        Initialize factory with SQLAlchemy Core connection.

        Args:
            connection: SQLAlchemy Core connection
        """
        self._connection = connection
        self._cache: dict[str, Any] = {}

    @property
    def connection(self) -> Connection:
        """Get the database connection."""
        return self._connection

    @property
    def datasource(self) -> DatasourceDAO:
        """Get DatasourceDAO instance."""
        if 'datasource' not in self._cache:
            self._cache['datasource'] = DatasourceDAO(self._connection)
        return self._cache['datasource']

    @property
    def ingest_batch(self) -> IngestBatchDAO:
        """Get IngestBatchDAO instance."""
        if 'ingest_batch' not in self._cache:
            self._cache['ingest_batch'] = IngestBatchDAO(self._connection)
        return self._cache['ingest_batch']

    @property
    def pipeline(self) -> PipelineDAO:
        """Get PipelineDAO instance."""
        if 'pipeline' not in self._cache:
            self._cache['pipeline'] = PipelineDAO(self._connection)
        return self._cache['pipeline']

    @property
    def cursor(self) -> CursorDAO:
        """Get CursorDAO instance."""
        if 'cursor' not in self._cache:
            self._cache['cursor'] = CursorDAO(self._connection)
        return self._cache['cursor']

    def staging(self, dataset: str) -> StagingDAO:
        """Get StagingDAO instance for dataset."""
        key = f'staging_{dataset}'
        if key not in self._cache:
            self._cache[key] = StagingDAO(self._connection, dataset)
        return self._cache[key]

    def data(self, conflict_config: Optional[dict] = None) -> DataDAO:
        """Get DataDAO instance with conflict config."""
        # Don't cache since conflict_config may vary
        return DataDAO(self._connection, conflict_config)

    def commit(self):
        """Commit transaction."""
        self._connection.commit()

    def rollback(self):
        """Rollback transaction."""
        self._connection.rollback()


# Re-export for convenience
__all__ = [
    'DAOFactory',
    'DatasourceDAO',
    'IngestBatchDAO',
    'StagingDAO',
    'DataDAO',
    'PipelineDAO',
    'CursorDAO',
]
