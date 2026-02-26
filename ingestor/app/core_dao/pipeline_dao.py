"""
Pipeline DAO using SQLAlchemy Core.

Tracks pipeline execution stages for observability.
"""

import logging
from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from sqlalchemy import select, insert, update, func

from shared import PipelineExecution, DataQualityCheck
from . import BaseCoreDAO

logger = logging.getLogger(__name__)


class PipelineDAO(BaseCoreDAO):
    """Data access for pipeline execution tracking using SQLAlchemy Core."""

    def __init__(self, connection):
        super().__init__(connection)
        self.execution_table = PipelineExecution.__table__
        self.quality_table = DataQualityCheck.__table__

    def start_stage(
        self,
        batch_id: UUID,
        pipeline_name: str,
        stage: str,
        records_in: int = 0
    ) -> int:
        """Record start of pipeline stage."""
        stmt = insert(self.execution_table).values(
            batch_id=batch_id,
            pipeline_name=pipeline_name,
            stage=stage,
            status='running',
            records_in=records_in,
            records_out=0,
        ).returning(self.execution_table.c.id)

        return self.scalar(stmt)

    def complete_stage(
        self,
        execution_id: int,
        status: str,
        records_out: int = 0,
        error_message: Optional[str] = None,
        metadata: Optional[dict] = None
    ):
        """Record completion of pipeline stage."""
        stmt = update(self.execution_table).where(
            self.execution_table.c.id == execution_id
        ).values(
            status=status,
            records_out=records_out,
            error_message=error_message,
            execution_metadata=metadata,
            completed_at=func.now()
        )
        self.execute(stmt)

    def fail_stage(
        self,
        execution_id: int,
        error_message: str,
        records_out: int = 0
    ):
        """Record stage failure."""
        self.complete_stage(
            execution_id=execution_id,
            status='failed',
            records_out=records_out,
            error_message=error_message
        )

    def record_quality_check(
        self,
        batch_id: UUID,
        dataset: str,
        check_type: str,
        check_name: str,
        passed: bool,
        failed_count: int = 0,
        total_count: int = 0,
        sample_failures: Optional[list] = None
    ) -> int:
        """Record data quality check result."""
        failure_rate = (failed_count / total_count * 100) if total_count > 0 else 0

        stmt = insert(self.quality_table).values(
            batch_id=batch_id,
            dataset=dataset,
            check_type=check_type,
            check_name=check_name,
            passed=passed,
            failed_count=failed_count,
            total_count=total_count,
            failure_rate=failure_rate,
            sample_failures=sample_failures
        ).returning(self.quality_table.c.id)

        return self.scalar(stmt)

    def get_execution_history(
        self,
        batch_id: Optional[UUID] = None,
        pipeline_name: Optional[str] = None,
        limit: int = 100
    ) -> list[dict]:
        """Get pipeline execution history."""
        stmt = select(self.execution_table)
        
        if batch_id:
            stmt = stmt.where(self.execution_table.c.batch_id == batch_id)
        
        if pipeline_name:
            stmt = stmt.where(self.execution_table.c.pipeline_name == pipeline_name)
        
        stmt = stmt.order_by(self.execution_table.c.started_at.desc()).limit(limit)
        
        rows = self.fetch_all(stmt)
        return [dict(row._mapping) for row in rows]

    def get_quality_checks(
        self,
        batch_id: Optional[UUID] = None,
        dataset: Optional[str] = None,
        passed_only: bool = False
    ) -> list[dict]:
        """Get data quality check results."""
        stmt = select(self.quality_table)
        
        if batch_id:
            stmt = stmt.where(self.quality_table.c.batch_id == batch_id)
        
        if dataset:
            stmt = stmt.where(self.quality_table.c.dataset == dataset)
        
        if passed_only:
            stmt = stmt.where(self.quality_table.c.passed == True)
        
        stmt = stmt.order_by(self.quality_table.c.checked_at.desc())
        
        rows = self.fetch_all(stmt)
        return [dict(row._mapping) for row in rows]

    def get_stage_summary(self, batch_id: UUID) -> dict:
        """Get summary of all stages for a batch."""
        stmt = select(
            self.execution_table.c.stage,
            self.execution_table.c.status,
            self.execution_table.c.records_in,
            self.execution_table.c.records_out,
            self.execution_table.c.started_at,
            self.execution_table.c.completed_at
        ).where(
            self.execution_table.c.batch_id == batch_id
        ).order_by(self.execution_table.c.started_at)
        
        rows = self.fetch_all(stmt)
        return {row.stage: dict(row._mapping) for row in rows}
