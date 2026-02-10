"""
Final data table access with conflict resolution.
"""

import logging
from typing import Any, Dict, List

from .base import BaseDAO

logger = logging.getLogger(__name__)


class DataDAO(BaseDAO):
    """Data access for final data tables with conflict resolution."""

    def __init__(self, connection, conflict_config: Dict[str, Any] = None):
        super().__init__(connection)
        config = conflict_config or {}
        self.strategy = config.get('strategy', 'update')
        self.on_columns = config.get('on_columns', [])
        self.update_columns = config.get('update_columns', [])

    def build_insert_sql(self, table: str, columns: List[str]) -> str:
        """Build INSERT SQL with conflict resolution."""
        placeholders = ', '.join(['%s'] * len(columns))
        column_list = ', '.join(columns)

        sql = f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"

        if not self.on_columns:
            return sql

        conflict_cols = ', '.join(self.on_columns)

        if self.strategy == 'ignore':
            sql += f" ON CONFLICT ({conflict_cols}) DO NOTHING"

        elif self.strategy == 'update':
            if self.update_columns:
                updates = ', '.join([
                    f"{col} = EXCLUDED.{col}"
                    for col in self.update_columns
                ])
            else:
                updates = ', '.join([
                    f"{col} = EXCLUDED.{col}"
                    for col in columns
                    if col not in self.on_columns
                ])
            sql += f" ON CONFLICT ({conflict_cols}) DO UPDATE SET {updates}"

        elif self.strategy == 'fail':
            # No ON CONFLICT clause - let database raise error
            pass

        elif self.strategy == 'append':
            sql += f" ON CONFLICT ({conflict_cols}) DO NOTHING"

        return sql

    def insert_record(self, table: str, record: Dict[str, Any]) -> bool:
        """
        Insert record with conflict resolution.

        Returns:
            True if inserted/updated, False if skipped
        """
        # Remove internal fields
        record = {k: v for k, v in record.items() if not k.startswith('_')}

        columns = list(record.keys())
        values = [record[col] for col in columns]

        sql = self.build_insert_sql(table, columns)

        count = self.execute(sql, tuple(values))
        return count > 0

    def insert_batch(self, table: str, records: List[Dict[str, Any]]) -> int:
        """Insert multiple records, returns count of successful inserts."""
        success_count = 0
        for record in records:
            if self.insert_record(table, record):
                success_count += 1
        return success_count
