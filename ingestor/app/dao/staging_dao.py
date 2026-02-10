"""
Staging table data access operations.
"""

import json
import logging
import math
from typing import Any, Dict, List, Optional
from uuid import UUID

from .base import BaseDAO

logger = logging.getLogger(__name__)


def sanitize_for_json(obj):
    """
    Recursively sanitize an object for JSON serialization.
    Converts NaN, Inf, -Inf to None.
    """
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif obj is None:
        return None
    else:
        return obj


class StagingDAO(BaseDAO):
    """Data access for staging tables."""

    STAGING_TABLES = {
        'environmental_metrics': 'staging_environmental_metrics',
        'energy_hourly': 'staging_energy_hourly',
        'energy_daily': 'staging_energy_daily',
        'dairy_production': 'staging_dairy_production'
    }

    def __init__(self, connection, dataset: str):
        super().__init__(connection)
        if dataset not in self.STAGING_TABLES:
            raise ValueError(f"No staging table configured for dataset: {dataset}")
        self.dataset = dataset
        self.staging_table = self.STAGING_TABLES[dataset]

    def insert_raw(self, file_id: UUID, row_number: int, raw_data: Dict[str, Any]) -> int:
        """Insert raw record into staging table."""
        sanitized_data = sanitize_for_json(raw_data)

        return self.execute_returning(
            f"""
            INSERT INTO {self.staging_table}
                (file_id, row_number, raw_data, created_at)
            VALUES (%s, %s, %s, NOW())
            RETURNING staging_id
            """,
            (str(file_id), row_number, json.dumps(sanitized_data))
        )

    def update_validation(
        self,
        staging_id: int,
        is_valid: bool,
        validation_errors: Optional[Dict] = None,
        transformed_data: Optional[Dict[str, Any]] = None
    ):
        """Update staging record with validation results."""
        self.execute(
            f"""
            UPDATE {self.staging_table}
            SET 
                validation_errors = %s,
                is_valid = %s,
                transformed_data = %s
            WHERE staging_id = %s
            """,
            (
                json.dumps(validation_errors) if validation_errors else None,
                is_valid,
                json.dumps(transformed_data, default=str) if transformed_data else None,
                staging_id
            )
        )

    def get_valid_records(self, file_id: Optional[UUID] = None) -> List[Dict[str, Any]]:
        """Retrieve valid records ready for loading."""
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

        rows = self.fetch_all(sql, tuple(params) if params else None)

        records = []
        for staging_id, transformed_data in rows:
            record = transformed_data if transformed_data else {}
            record['_staging_id'] = staging_id
            records.append(record)

        return records

    def get_invalid_records(self, file_id: Optional[UUID] = None) -> List[Dict[str, Any]]:
        """Retrieve invalid records with validation errors."""
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

        rows = self.fetch_all(sql, tuple(params) if params else None)

        return [
            {
                'staging_id': row[0],
                'row_number': row[1],
                'raw_data': row[2] if row[2] else {},
                'validation_errors': row[3] if row[3] else {}
            }
            for row in rows
        ]

    def mark_loaded(self, staging_ids: List[int]):
        """Mark records as successfully loaded to final table."""
        if not staging_ids:
            return

        count = self.execute(
            f"""
            UPDATE {self.staging_table}
            SET loaded_to_final = TRUE
            WHERE staging_id = ANY(%s)
            """,
            (staging_ids,)
        )
        logger.info(f"Marked {count} records as loaded in {self.staging_table}")

    def get_statistics(self, file_id: Optional[UUID] = None) -> Dict[str, int]:
        """Get staging statistics for reporting."""
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

        row = self.fetch_one(sql, tuple(params) if params else None)

        return {
            'total': row[0] or 0,
            'valid': row[1] or 0,
            'invalid': row[2] or 0,
            'loaded': row[3] or 0,
            'pending': (row[1] or 0) - (row[3] or 0)
        }

    def cleanup_loaded(self, retention_days: int = 7) -> int:
        """Clean up old staging records that have been successfully loaded."""
        deleted = self.execute(
            f"""
            DELETE FROM {self.staging_table}
            WHERE loaded_to_final = TRUE
              AND created_at < NOW() - INTERVAL '{retention_days} days'
            """
        )
        logger.info(f"Cleaned up {deleted} old records from {self.staging_table}")
        return deleted
