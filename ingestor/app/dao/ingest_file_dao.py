"""
Ingest file registration and duplicate detection.
"""

import logging
import uuid
from datetime import date
from typing import Optional

from .base import BaseDAO

logger = logging.getLogger(__name__)


class IngestFileDAO(BaseDAO):
    """Data access for ingest_file table."""

    def exists_by_sha256(self, sha256: str) -> bool:
        """
        Check if file was already processed.

        Args:
            sha256: File hash

        Returns:
            True if duplicate
        """
        result = self.fetch_one(
            "SELECT 1 FROM ingest_file WHERE sha256 = %s",
            (sha256,)
        )
        return result is not None

    def register(
        self,
        file_name: str,
        device_id: Optional[int],
        granularity: Optional[str],
        start_date: Optional[date],
        end_date: Optional[date],
        sha256: Optional[str],
        pipeline_version: str = '3.0'
    ) -> uuid.UUID:
        """
        Register new input file.

        Returns:
            Generated file UUID
        """
        file_id = uuid.uuid4()

        self.execute(
            """
            INSERT INTO ingest_file 
                (file_id, file_name, device_id, granularity, start_date, end_date, sha256, pipeline_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(file_id),
                file_name,
                str(device_id) if device_id else None,
                granularity,
                start_date,
                end_date,
                sha256,
                pipeline_version,
            )
        )

        return file_id

    def update_status(
        self,
        file_id: uuid.UUID,
        status: str,
        records_loaded: int = 0,
        records_failed: int = 0
    ):
        """Update file processing status."""
        self.execute(
            """
            UPDATE ingest_file
            SET status = %s,
                records_loaded = %s,
                records_failed = %s,
                completed_at = NOW()
            WHERE file_id = %s
            """,
            (status, records_loaded, records_failed, str(file_id))
        )

    def update_metrics(
        self,
        file_id: uuid.UUID,
        execution_time_ms: int,
        validation_status: str,
        quality_score: float
    ):
        """Update file with execution metrics."""
        self.execute(
            """
            UPDATE ingest_file
            SET execution_time_ms = %s, 
                validation_status = %s, 
                quality_score = %s
            WHERE file_id = %s
            """,
            (execution_time_ms, validation_status, quality_score, str(file_id))
        )
