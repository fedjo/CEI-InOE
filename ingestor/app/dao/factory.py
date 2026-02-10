"""
DAO Factory for centralized DAO creation.
"""

from typing import Any, Dict, Optional

from .device_dao import DeviceDAO
from .ingest_file_dao import IngestFileDAO
from .staging_dao import StagingDAO
from .pipeline_dao import PipelineDAO
from .cursor_dao import CursorDAO, EnvironmentalMetricsDAO
from .data_dao import DataDAO


class DAOFactory:
    """Factory for creating DAO instances with shared connection."""

    def __init__(self, connection):
        self._connection = connection
        self._cache: Dict[str, Any] = {}

    @property
    def connection(self):
        """Get the database connection."""
        return self._connection

    @property
    def device(self) -> DeviceDAO:
        """Get DeviceDAO instance."""
        if 'device' not in self._cache:
            self._cache['device'] = DeviceDAO(self._connection)
        return self._cache['device']

    @property
    def ingest_file(self) -> IngestFileDAO:
        """Get IngestFileDAO instance."""
        if 'ingest_file' not in self._cache:
            self._cache['ingest_file'] = IngestFileDAO(self._connection)
        return self._cache['ingest_file']

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

    @property
    def environmental_metrics(self) -> EnvironmentalMetricsDAO:
        """Get EnvironmentalMetricsDAO instance."""
        if 'environmental_metrics' not in self._cache:
            self._cache['environmental_metrics'] = EnvironmentalMetricsDAO(self._connection)
        return self._cache['environmental_metrics']

    def staging(self, dataset: str) -> StagingDAO:
        """Get StagingDAO instance for dataset."""
        key = f'staging_{dataset}'
        if key not in self._cache:
            self._cache[key] = StagingDAO(self._connection, dataset)
        return self._cache[key]

    def data(self, conflict_config: Optional[Dict] = None) -> DataDAO:
        """Get DataDAO instance with conflict config."""
        # Don't cache since conflict_config may vary
        return DataDAO(self._connection, conflict_config)

    def commit(self):
        """Commit transaction."""
        self._connection.commit()

    def rollback(self):
        """Rollback transaction."""
        self._connection.rollback()
