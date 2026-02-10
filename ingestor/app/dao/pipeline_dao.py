"""
Pipeline execution and quality metrics data access.
"""

import json
import logging
from typing import Any, Dict, List, Optional
from uuid import UUID

from .base import BaseDAO

logger = logging.getLogger(__name__)


class PipelineDAO(BaseDAO):
    """Data access for pipeline_execution and data_quality_checks tables."""

    def log_stage_start(
        self,
        file_id: Optional[UUID],
        pipeline_name: str,
        stage: str,
        source_type: Optional[str] = None,
        dataset: Optional[str] = None
    ) -> Optional[int]:
        """Log pipeline stage start, returns execution_id."""
        metadata = {}
        if dataset:
            metadata['dataset'] = dataset
        if source_type:
            metadata['source_type'] = source_type

        self.execute(
            """
            INSERT INTO pipeline_execution
                (file_id, pipeline_name, stage, started_at, status, execution_metadata)
            VALUES (%s, %s, %s, NOW(), 'running', %s)
            """,
            (
                str(file_id) if file_id else None,
                pipeline_name,
                stage,
                json.dumps(metadata) if metadata else None
            )
        )
        self.commit()
        return None  # We don't need execution_id for current implementation

    def log_stage_end(
        self,
        pipeline_name: str,
        stage: str,
        records_in: int,
        records_out: int,
        status: str = 'success',
        error_message: Optional[str] = None
    ):
        """Log pipeline stage completion."""
        self.execute(
            """
            UPDATE pipeline_execution
            SET 
                completed_at = NOW(),
                status = %s,
                records_in = %s,
                records_out = %s,
                error_message = %s
            WHERE pipeline_name = %s 
              AND stage = %s 
              AND status = 'running'
              AND started_at >= NOW() - INTERVAL '1 hour'
            """,
            (status, records_in, records_out, error_message, pipeline_name, stage)
        )
        self.commit()

    def log_quality_check(
        self,
        file_id: Optional[UUID],
        dataset: str,
        check_type: str,
        check_name: str,
        passed: bool,
        failed_count: int,
        total_count: int,
        failure_rate: float,
        sample_failures: Optional[Any] = None
    ):
        """Log data quality check results."""
        self.execute(
            """
            INSERT INTO data_quality_checks
                (file_id, dataset, check_type, check_name, passed, 
                 failed_count, total_count, failure_rate, sample_failures)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(file_id) if file_id else None,
                dataset,
                check_type,
                check_name,
                passed,
                failed_count,
                total_count,
                failure_rate,
                json.dumps(sample_failures) if sample_failures else None
            )
        )

    def log_quality_checks_batch(
        self,
        file_id: Optional[UUID],
        dataset: str,
        error_types: Dict[str, List[Dict]],
        total_count: int
    ):
        """Log multiple quality check types at once."""
        for check_type, failures in error_types.items():
            failed_count = len(failures)
            failure_rate = (failed_count / total_count * 100) if total_count > 0 else 0

            self.log_quality_check(
                file_id=file_id,
                dataset=dataset,
                check_type=check_type,
                check_name=f"{check_type}_validation",
                passed=failed_count == 0,
                failed_count=failed_count,
                total_count=total_count,
                failure_rate=round(failure_rate, 2),
                sample_failures=failures[:10]  # Sample first 10
            )
