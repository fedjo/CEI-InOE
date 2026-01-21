"""
Staging Module
Handles staging table operations before loading to final warehouse tables.
"""

from typing import Dict, List, Any, Optional
import json
import logging
from datetime import datetime
from uuid import UUID

logger = logging.getLogger(__name__)


class StagingManager:
    """Manages staging table operations."""
    
    # Mapping of datasets to staging tables
    STAGING_TABLES = {
        'environmental_metrics': 'staging_environmental_metrics',
        'energy_hourly': 'staging_energy_hourly',
        'energy_daily': 'staging_energy_daily',
        'dairy_production': 'staging_dairy_production'
    }
    
    def __init__(self, connection, dataset: str):
        """
        Initialize staging manager for a dataset.
        
        Args:
            connection: Database connection
            dataset: Dataset name (e.g., 'environmental_metrics')
        """
        self.connection = connection
        self.dataset = dataset
        self.staging_table = self.STAGING_TABLES.get(dataset)
        
        if not self.staging_table:
            raise ValueError(f"No staging table configured for dataset: {dataset}")
    
    def insert_raw(self, file_id: UUID, row_number: int, 
                   raw_data: Dict[str, Any]) -> int:
        """
        Insert raw data into staging table.
        
        Args:
            file_id: Source file UUID
            row_number: Row number in source file
            raw_data: Original CSV/API data
        
        Returns:
            staging_id of inserted record
        """
        cursor = self.connection.cursor()
        
        sql = f"""
            INSERT INTO {self.staging_table} 
                (file_id, row_number, raw_data, created_at)
            VALUES (%s, %s, %s, NOW())
            RETURNING staging_id
        """
        
        cursor.execute(sql, (
            str(file_id),
            row_number,
            json.dumps(raw_data)
        ))
        
        staging_id = cursor.fetchone()[0]
        return staging_id
    
    def update_validation(self, staging_id: int, 
                         validation_result: Any,
                         transformed_data: Optional[Dict[str, Any]] = None):
        """
        Update staging record with validation results and transformed data.
        
        Args:
            staging_id: Staging record ID
            validation_result: ValidationResult object
            transformed_data: Transformed data (if validation passed)
        """
        cursor = self.connection.cursor()
        
        sql = f"""
            UPDATE {self.staging_table}
            SET 
                validation_errors = %s,
                is_valid = %s,
                transformed_data = %s
            WHERE staging_id = %s
        """
        
        cursor.execute(sql, (
            json.dumps(validation_result.to_dict()) if validation_result else None,
            validation_result.is_valid if validation_result else False,
            json.dumps(transformed_data, default=str) if transformed_data else None,
            staging_id
        ))
    
    def get_valid_records(self, file_id: Optional[UUID] = None) -> List[Dict[str, Any]]:
        """
        Retrieve valid records ready for loading.
        
        Args:
            file_id: Optional file filter
        
        Returns:
            List of valid transformed records
        """
        cursor = self.connection.cursor()
        
        sql = f"""
            SELECT staging_id, transformed_data
            FROM {self.staging_table}
            WHERE is_valid = TRUE 
              AND loaded_to_final = FALSE
        """
        
        params = []
        if file_id:
            sql += " AND file_id = %s"
            params.append(str(file_id))
        
        sql += " ORDER BY row_number"
        
        cursor.execute(sql, params)
        
        records = []
        for row in cursor.fetchall():
            staging_id, transformed_data = row
            record = json.loads(transformed_data) if transformed_data else {}
            record['_staging_id'] = staging_id
            records.append(record)
        
        return records
    
    def get_invalid_records(self, file_id: Optional[UUID] = None) -> List[Dict[str, Any]]:
        """
        Retrieve invalid records with validation errors.
        
        Returns:
            List of invalid records with errors
        """
        cursor = self.connection.cursor()
        
        sql = f"""
            SELECT staging_id, row_number, raw_data, validation_errors
            FROM {self.staging_table}
            WHERE is_valid = FALSE
        """
        
        params = []
        if file_id:
            sql += " AND file_id = %s"
            params.append(str(file_id))
        
        sql += " ORDER BY row_number"
        
        cursor.execute(sql, params)
        
        records = []
        for row in cursor.fetchall():
            staging_id, row_number, raw_data, validation_errors = row
            records.append({
                'staging_id': staging_id,
                'row_number': row_number,
                'raw_data': json.loads(raw_data) if raw_data else {},
                'validation_errors': json.loads(validation_errors) if validation_errors else {}
            })
        
        return records
    
    def mark_loaded(self, staging_ids: List[int]):
        """
        Mark records as successfully loaded to final table.
        
        Args:
            staging_ids: List of staging record IDs
        """
        if not staging_ids:
            return
        
        cursor = self.connection.cursor()
        
        sql = f"""
            UPDATE {self.staging_table}
            SET loaded_to_final = TRUE
            WHERE staging_id = ANY(%s)
        """
        
        cursor.execute(sql, (staging_ids,))
        logger.info(f"Marked {len(staging_ids)} records as loaded in {self.staging_table}")
    
    def get_statistics(self, file_id: Optional[UUID] = None) -> Dict[str, int]:
        """
        Get staging statistics for reporting.
        
        Returns:
            Dictionary with counts
        """
        cursor = self.connection.cursor()
        
        sql = f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as valid,
                SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END) as invalid,
                SUM(CASE WHEN loaded_to_final THEN 1 ELSE 0 END) as loaded
            FROM {self.staging_table}
        """
        
        params = []
        if file_id:
            sql += " WHERE file_id = %s"
            params.append(str(file_id))
        
        cursor.execute(sql, params)
        row = cursor.fetchone()
        
        return {
            'total': row[0] or 0,
            'valid': row[1] or 0,
            'invalid': row[2] or 0,
            'loaded': row[3] or 0,
            'pending': (row[1] or 0) - (row[3] or 0)
        }
    
    def cleanup_loaded(self, retention_days: int = 7):
        """
        Clean up old staging records that have been successfully loaded.
        
        Args:
            retention_days: Keep records for this many days
        """
        cursor = self.connection.cursor()
        
        sql = f"""
            DELETE FROM {self.staging_table}
            WHERE loaded_to_final = TRUE
              AND created_at < NOW() - INTERVAL '{retention_days} days'
        """
        
        cursor.execute(sql)
        deleted = cursor.rowcount
        
        logger.info(f"Cleaned up {deleted} old records from {self.staging_table}")
        return deleted


class ConflictResolver:
    """Handles conflict resolution strategies when loading to final tables."""
    
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
        self.strategy = strategy_config.get('strategy', 'update')
        self.on_columns = strategy_config.get('on_columns', [])
        self.update_columns = strategy_config.get('update_columns', [])
    
    def build_insert_sql(self, table: str, columns: List[str]) -> str:
        """
        Build INSERT SQL with conflict resolution.
        
        Args:
            table: Target table name
            columns: List of column names to insert
        
        Returns:
            SQL statement with ON CONFLICT clause
        """
        placeholders = ', '.join(['%s'] * len(columns))
        column_list = ', '.join(columns)
        
        sql = f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"
        
        if not self.on_columns:
            # No conflict handling
            return sql
        
        conflict_cols = ', '.join(self.on_columns)
        
        if self.strategy == 'ignore':
            sql += f" ON CONFLICT ({conflict_cols}) DO NOTHING"
        
        elif self.strategy == 'update':
            if self.update_columns:
                # Update specified columns
                updates = ', '.join([
                    f"{col} = EXCLUDED.{col}" 
                    for col in self.update_columns
                ])
            else:
                # Update all columns except conflict columns
                updates = ', '.join([
                    f"{col} = EXCLUDED.{col}" 
                    for col in columns 
                    if col not in self.on_columns
                ])
            
            sql += f" ON CONFLICT ({conflict_cols}) DO UPDATE SET {updates}"
        
        elif self.strategy == 'fail':
            # Don't add ON CONFLICT - let database raise error
            pass
        
        elif self.strategy == 'append':
            # For append, we might need to modify conflict columns
            # This is dataset-specific, so just do nothing for now
            sql += f" ON CONFLICT ({conflict_cols}) DO NOTHING"
        
        return sql
    
    def execute_insert(self, cursor, table: str, record: Dict[str, Any]) -> bool:
        """
        Execute insert with conflict resolution.
        
        Returns:
            True if inserted/updated, False if skipped
        """
        # Remove internal fields
        record = {k: v for k, v in record.items() if not k.startswith('_')}
        
        columns = list(record.keys())
        values = [record[col] for col in columns]
        
        sql = self.build_insert_sql(table, columns)
        
        cursor.execute(sql, values)
        
        # Check if row was inserted/updated
        return cursor.rowcount > 0
