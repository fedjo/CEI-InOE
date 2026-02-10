"""
Staging Module
Legacy wrapper around DAO layer for backward compatibility.

NOTE: This module is deprecated. Use dao.StagingDAO and dao.DataDAO directly.
"""

import logging
from typing import Dict, List, Any, Optional
from uuid import UUID

# Import from DAO layer
from dao import StagingDAO as _StagingDAO, DataDAO as _DataDAO, sanitize_for_json

logger = logging.getLogger(__name__)

# Re-export sanitize_for_json for backward compatibility
__all__ = ['StagingManager', 'ConflictResolver', 'sanitize_for_json']


class StagingManager:
    """
    Legacy wrapper around StagingDAO for backward compatibility.
    
    DEPRECATED: Use dao.StagingDAO directly in new code.
    """

    # Expose class attribute for backward compatibility
    STAGING_TABLES = _StagingDAO.STAGING_TABLES

    def __init__(self, connection, dataset: str):
        """
        Initialize staging manager for a dataset.

        Args:
            connection: Database connection
            dataset: Dataset name (e.g., 'environmental_metrics')
        """
        self._dao = _StagingDAO(connection, dataset)
        self.connection = connection
        self.dataset = dataset
        self.staging_table = self._dao.staging_table

    def insert_raw(self, file_id: UUID, row_number: int,
                   raw_data: Dict[str, Any]) -> int:
        """Insert raw record into staging table."""
        return self._dao.insert_raw(file_id, row_number, raw_data)

    def update_validation(self, staging_id: int, 
                         validation_result: Any,
                         transformed_data: Optional[Dict[str, Any]] = None):
        """Update staging record with validation results and transformed data."""
        self._dao.update_validation(
            staging_id=staging_id,
            is_valid=validation_result.is_valid if validation_result else False,
            validation_errors=validation_result.to_dict() if validation_result else None,
            transformed_data=transformed_data
        )

    def get_valid_records(self, file_id: Optional[UUID] = None) -> List[Dict[str, Any]]:
        """Retrieve valid records ready for loading."""
        return self._dao.get_valid_records(file_id)

    def get_invalid_records(self, file_id: Optional[UUID] = None) -> List[Dict[str, Any]]:
        """Retrieve invalid records with validation errors."""
        return self._dao.get_invalid_records(file_id)

    def mark_loaded(self, staging_ids: List[int]):
        """Mark records as successfully loaded to final table."""
        self._dao.mark_loaded(staging_ids)

    def get_statistics(self, file_id: Optional[UUID] = None) -> Dict[str, int]:
        """Get staging statistics for reporting."""
        return self._dao.get_statistics(file_id)

    def cleanup_loaded(self, retention_days: int = 7):
        """Clean up old staging records that have been successfully loaded."""
        return self._dao.cleanup_loaded(retention_days)


class ConflictResolver:
    """
    Legacy wrapper around DataDAO for backward compatibility.
    
    DEPRECATED: Use dao.DataDAO directly in new code.
    """

    def __init__(self, strategy_config: Dict[str, Any]):
        """
        Initialize with conflict resolution configuration from YAML.
        
        strategy_config format:
        {
            'strategy': 'update',  # or 'ignore', 'fail', 'append'
            'on_columns': ['timestamp', 'device_id'],
            'update_columns': ['temperature', 'humidity']  # for 'update' strategy
        }
        """
        self._config = strategy_config
        self.strategy = strategy_config.get('strategy', 'update')
        self.on_columns = strategy_config.get('on_columns', [])
        self.update_columns = strategy_config.get('update_columns', [])

    def build_insert_sql(self, table: str, columns: List[str]) -> str:
        """Build INSERT SQL with conflict resolution."""
        # Create temporary DAO just for SQL building
        temp_dao = _DataDAO(None, self._config)
        return temp_dao.build_insert_sql(table, columns)

    def execute_insert(self, cursor, table: str, record: Dict[str, Any]) -> bool:
        """Execute insert with conflict resolution."""
        # Use DataDAO with cursor's connection
        dao = _DataDAO(cursor.connection, self._config)
        return dao.insert_record(table, record)
        
        # Check if row was inserted/updated
        return cursor.rowcount > 0
