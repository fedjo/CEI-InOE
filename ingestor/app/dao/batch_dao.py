"""
Ingest Batch DAO using SQLAlchemy Core.

Replaces the old IngestFileDAO.
"""

import logging
import uuid
from datetime import date
from typing import Optional

from sqlalchemy import select, insert, update, func

from shared import IngestBatch
from . import BaseCoreDAO

logger = logging.getLogger(__name__)


class IngestBatchDAO(BaseCoreDAO):
    """Data access for ingest_batch table using SQLAlchemy Core."""

    def __init__(self, connection):
        super().__init__(connection)
        self.table = IngestBatch.__table__

    def exists_by_sha256(self, sha256: str) -> bool:
        """
        Check if batch with SHA256 hash already exists.

        Args:
            sha256: File hash

        Returns:
            True if duplicate
        """
        stmt = select(self.table.c.batch_id).where(
            self.table.c.file_sha256 == sha256
        )
        return self.scalar(stmt) is not None

    def register(
        self,
        source_type: str,
        source_name: str,
        datasource_id: Optional[int] = None,
        granularity: Optional[str] = None,
        date_range_start: Optional[date] = None,
        date_range_end: Optional[date] = None,
        file_sha256: Optional[str] = None,
        pipeline_version: str = '2.0'
    ) -> uuid.UUID:
        """
        Register new ingest batch.

        Returns:
            Generated batch UUID
        """
        batch_id = uuid.uuid4()

        stmt = insert(self.table).values(
            batch_id=batch_id,
            source_type=source_type,
            source_name=source_name,
            datasource_id=datasource_id,
            granularity=granularity,
            date_range_start=date_range_start,
            date_range_end=date_range_end,
            file_sha256=file_sha256,
            pipeline_version=pipeline_version,
            status='pending',
            records_loaded=0,
            records_failed=0,
        )
        self.execute(stmt)

        return batch_id

    def update_status(
        self,
        batch_id: uuid.UUID,
        status: str,
        records_loaded: int = 0,
        records_failed: int = 0
    ):
        """Update batch processing status."""
        stmt = update(self.table).where(
            self.table.c.batch_id == batch_id
        ).values(
            status=status,
            records_loaded=records_loaded,
            records_failed=records_failed,
            completed_at=func.now()
        )
        self.execute(stmt)

    def update_metrics(
        self,
        batch_id: uuid.UUID,
        execution_time_ms: int,
        validation_status: str,
        quality_score: float
    ):
        """Update batch with execution metrics."""
        stmt = update(self.table).where(
            self.table.c.batch_id == batch_id
        ).values(
            execution_time_ms=execution_time_ms,
            validation_status=validation_status,
            quality_score=quality_score,
            completed_at=func.now()
        )
        self.execute(stmt)

    def get_by_id(self, batch_id: uuid.UUID) -> dict | None:
        """Get batch by ID."""
        stmt = select(self.table).where(self.table.c.batch_id == batch_id)
        row = self.fetch_one(stmt)
        return dict(row._mapping) if row else None

    def get_recent(self, limit: int = 10) -> list[dict]:
        """Get recent batches."""
        stmt = select(self.table).order_by(
            self.table.c.started_at.desc()
        ).limit(limit)
        rows = self.fetch_all(stmt)
        return [dict(row._mapping) for row in rows]
