"""
Staging DAO using SQLAlchemy Core.

Handles staging table operations for data validation pipeline.
"""

import json
import logging
import math
from datetime import datetime, date, time
from typing import Any, Optional
from uuid import UUID

from sqlalchemy import select, insert, update, func

from shared import (
    StagingEnvironmentalMetrics,
    StagingEnergyHourly,
    StagingEnergyDaily,
    StagingDairyProduction,
)
from . import BaseCoreDAO

logger = logging.getLogger(__name__)


def sanitize_for_json(obj: Any) -> Any:
    """
    Recursively sanitize an object for JSON serialization.
    Converts NaN, Inf, -Inf to None.
    Converts datetime/date/time objects to ISO format strings.
    """
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, date):
        return obj.isoformat()
    elif isinstance(obj, time):
        return obj.isoformat()
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif obj is None:
        return None
    else:
        return obj


class StagingDAO(BaseCoreDAO):
    """Data access for staging tables using SQLAlchemy Core."""

    # Map dataset names to model classes
    STAGING_MODELS = {
        'environmental_metrics': StagingEnvironmentalMetrics,
        'energy_hourly': StagingEnergyHourly,
        'energy_daily': StagingEnergyDaily,
        'dairy_production': StagingDairyProduction,
    }

    def __init__(self, connection, dataset: str):
        super().__init__(connection)
        if dataset not in self.STAGING_MODELS:
            raise ValueError(f"No staging table configured for dataset: {dataset}")
        
        self.dataset = dataset
        self.model = self.STAGING_MODELS[dataset]
        self.table = self.model.__table__

    def insert_raw(self, batch_id: UUID, row_number: int, raw_data: dict) -> int:
        """Insert raw record into staging table."""
        sanitized_data = sanitize_for_json(raw_data)

        stmt = insert(self.table).values(
            batch_id=batch_id,
            row_number=row_number,
            raw_data=sanitized_data,
            is_valid=False,
            loaded_to_final=False,
        ).returning(self.table.c.staging_id)

        return self.scalar(stmt)

    def update_validation(
        self,
        staging_id: int,
        is_valid: bool,
        validation_errors: Optional[dict] = None,
        transformed_data: Optional[dict] = None
    ):
        """Update staging record with validation results."""
        sanitized_errors = sanitize_for_json(validation_errors) if validation_errors else None
        sanitized_data = sanitize_for_json(transformed_data) if transformed_data else None

        stmt = update(self.table).where(
            self.table.c.staging_id == staging_id
        ).values(
            is_valid=is_valid,
            validation_errors=sanitized_errors,
            transformed_data=sanitized_data
        )
        self.execute(stmt)

    def get_valid_records(self, batch_id: Optional[UUID] = None) -> list[dict]:
        """Retrieve valid records ready for loading."""
        stmt = select(
            self.table.c.staging_id,
            self.table.c.transformed_data
        ).where(
            self.table.c.is_valid == True,
            self.table.c.loaded_to_final == False
        )

        if batch_id:
            stmt = stmt.where(self.table.c.batch_id == batch_id)

        stmt = stmt.order_by(self.table.c.row_number)

        rows = self.fetch_all(stmt)
        return [dict(row._mapping) for row in rows]

    def get_invalid_records(self, batch_id: Optional[UUID] = None) -> list[dict]:
        """Retrieve invalid records with validation errors."""
        stmt = select(self.table).where(self.table.c.is_valid == False)
        
        if batch_id:
            stmt = stmt.where(self.table.c.batch_id == batch_id)
        
        stmt = stmt.order_by(self.table.c.row_number)
        
        rows = self.fetch_all(stmt)
        return [dict(row._mapping) for row in rows]

    def mark_loaded(self, staging_ids: list[int]):
        """Mark records as successfully loaded to final table."""
        if not staging_ids:
            return
        
        stmt = update(self.table).where(
            self.table.c.staging_id.in_(staging_ids)
        ).values(loaded_to_final=True)
        
        self.execute(stmt)

    def get_statistics(self, batch_id: Optional[UUID] = None) -> dict:
        """Get staging statistics for reporting."""
        base_filter = (self.table.c.batch_id == batch_id) if batch_id else True
        
        # Total count
        total_stmt = select(func.count()).select_from(self.table).where(base_filter)
        total = self.scalar(total_stmt) or 0
        
        # Valid count
        valid_stmt = select(func.count()).select_from(self.table).where(
            base_filter,
            self.table.c.is_valid == True
        )
        valid = self.scalar(valid_stmt) or 0
        
        # Loaded count
        loaded_stmt = select(func.count()).select_from(self.table).where(
            base_filter,
            self.table.c.loaded_to_final == True
        )
        loaded = self.scalar(loaded_stmt) or 0
        
        return {
            'total': total,
            'valid': valid,
            'invalid': total - valid,
            'loaded': loaded,
            'pending': valid - loaded
        }

    def cleanup_loaded(self, retention_days: int = 7) -> int:
        """
        Clean up old staging records that have been successfully loaded.
        
        Returns count of deleted records.
        """
        from sqlalchemy import delete
        from datetime import datetime, timedelta
        
        cutoff = datetime.now() - timedelta(days=retention_days)
        
        stmt = delete(self.table).where(
            self.table.c.loaded_to_final == True,
            self.table.c.created_at < cutoff
        )
        
        result = self.execute(stmt)
        return result.rowcount
