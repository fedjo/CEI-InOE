"""
Data DAO using SQLAlchemy Core.

Handles final data table inserts with conflict resolution.
"""

import logging
from typing import Any

from sqlalchemy import insert, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

from shared import (
    FactEnergyHourly,
    FactEnergyDaily,
    EnvironmentalMetrics,
    DairyProduction,
    FactSolarHourly,
    FactSolarDaily,
    FactSolarMonthly,
)
from . import BaseCoreDAO

logger = logging.getLogger(__name__)


class DataDAO(BaseCoreDAO):
    """Data access for final data tables with conflict resolution using SQLAlchemy Core."""

    # Map dataset names to model classes
    FINAL_MODELS = {
        'environmental_metrics': EnvironmentalMetrics,
        'fact_energy_hourly': FactEnergyHourly,
        'fact_energy_daily': FactEnergyDaily,
        'dairy_production': DairyProduction,
        'fact_solar_hourly': FactSolarHourly,
        'fact_solar_daily': FactSolarDaily,
        'fact_solar_monthly': FactSolarMonthly,
    }

    def __init__(self, connection, conflict_config: dict = None):
        super().__init__(connection)
        config = conflict_config or {}
        self.strategy = config.get('strategy', 'update')
        self.on_columns = config.get('on_columns', [])
        self.update_columns = config.get('update_columns', [])

    def get_table(self, dataset: str) -> Table:
        """Get SQLAlchemy Table for dataset."""
        if dataset not in self.FINAL_MODELS:
            raise ValueError(f"No final table configured for dataset: {dataset}")
        return self.FINAL_MODELS[dataset].__table__

    def insert_record(self, dataset: str, record: dict) -> bool:
        """
        Insert record with conflict resolution using PostgreSQL upsert.

        Returns:
            True if inserted/updated, False if skipped
        """
        table = self.get_table(dataset)

        # Remove internal fields
        record = {k: v for k, v in record.items() if not k.startswith('_')}
        
        # Build upsert statement
        stmt = pg_insert(table).values(**record)
        
        if self.on_columns:
            if self.strategy == 'ignore':
                stmt = stmt.on_conflict_do_nothing(index_elements=self.on_columns)
            
            elif self.strategy == 'update':
                # Determine columns to update
                if self.update_columns:
                    update_dict = {col: stmt.excluded[col] for col in self.update_columns}
                else:
                    # Update all columns except conflict columns
                    update_dict = {
                        col.name: stmt.excluded[col.name]
                        for col in table.columns
                        if col.name not in self.on_columns
                    }
                stmt = stmt.on_conflict_do_update(
                    index_elements=self.on_columns,
                    set_=update_dict
                )
            
            elif self.strategy == 'fail':
                # No conflict handling - let database raise error
                pass
        
        result = self.execute(stmt)
        return result.rowcount > 0

    def insert_batch(self, dataset: str, records: list[dict]) -> int:
        """
        Insert multiple records using bulk operations.
        
        For best performance with large batches.
        
        Returns count of successful inserts.
        """
        if not records:
            return 0
        
        table = self.get_table(dataset)
        
        # Clean records
        clean_records = [
            {k: v for k, v in rec.items() if not k.startswith('_')}
            for rec in records
        ]
        
        if not self.on_columns:
            # Simple bulk insert
            stmt = insert(table).values(clean_records)
            result = self.execute(stmt)
            return result.rowcount
        
        # Batch upsert
        success_count = 0
        for record in clean_records:
            if self.insert_record(dataset, record):
                success_count += 1
        
        return success_count

    def bulk_insert_raw(self, dataset: str, records: list[dict]) -> int:
        """
        High-performance bulk insert without conflict handling.
        
        Use when you're sure there are no conflicts.
        """
        if not records:
            return 0
        
        table = self.get_table(dataset)
        
        clean_records = [
            {k: v for k, v in rec.items() if not k.startswith('_')}
            for rec in records
        ]
        
        stmt = insert(table).values(clean_records)
        result = self.execute(stmt)
        return result.rowcount
