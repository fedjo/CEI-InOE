"""
API fetch cursor data access.
"""

import logging
from datetime import datetime
from typing import Optional

from .base import BaseDAO

logger = logging.getLogger(__name__)


class CursorDAO(BaseDAO):
    """Data access for api_fetch_cursor table."""

    def upsert_cursor(
        self,
        connector_id: str,
        endpoint_id: str,
        device_id: str,
        last_fetch_timestamp: datetime
    ):
        """Insert or update API fetch cursor."""
        self.execute(
            """
            INSERT INTO api_fetch_cursor 
                (connector_id, endpoint_id, device_id, last_fetch_timestamp, fetch_count)
            VALUES (%s, %s, %s, %s, 1)
            ON CONFLICT (connector_id, endpoint_id, device_id)
            DO UPDATE SET
                last_fetch_timestamp = EXCLUDED.last_fetch_timestamp,
                last_fetch_success = NOW(),
                fetch_count = api_fetch_cursor.fetch_count + 1,
                updated_at = NOW()
            """,
            (connector_id, endpoint_id, device_id, last_fetch_timestamp)
        )
        self.commit()

    def get_cursor(
        self,
        connector_id: str,
        endpoint_id: str,
        device_id: str
    ) -> Optional[datetime]:
        """Get last fetch timestamp for a device/endpoint."""
        result = self.fetch_one(
            """
            SELECT last_fetch_timestamp
            FROM api_fetch_cursor
            WHERE connector_id = %s
              AND endpoint_id = %s
              AND device_id = %s
            """,
            (connector_id, endpoint_id, device_id)
        )
        return result[0] if result else None


class EnvironmentalMetricsDAO(BaseDAO):
    """Data access for environmental_metrics table (read operations)."""

    def get_max_timestamp(self, device_id: str) -> Optional[datetime]:
        """Get max timestamp for a device from environmental_metrics."""
        result = self.fetch_one(
            """
            SELECT MAX(timestamp)
            FROM environmental_metrics
            WHERE source_device_id = %s
            """,
            (device_id,)
        )
        return result[0] if result and result[0] else None
