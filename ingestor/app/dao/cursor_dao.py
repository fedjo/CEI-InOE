"""
Cursor DAO using SQLAlchemy Core.

Tracks API fetch cursors for incremental data loading.
"""

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from shared import ApiFetchCursor
from . import BaseCoreDAO

logger = logging.getLogger(__name__)


class CursorDAO(BaseCoreDAO):
    """Data access for API fetch cursor tracking using SQLAlchemy Core."""

    def __init__(self, connection):
        super().__init__(connection)
        self.table = ApiFetchCursor.__table__

    def get_cursor(
        self,
        connector_id: str,
        endpoint_id: str,
        device_id: str
    ) -> datetime | None:
        """
        Get last fetch timestamp for a device/endpoint combination.

        Returns:
            Last fetch timestamp or None if no cursor exists
        """
        stmt = select(self.table.c.last_fetch_timestamp).where(
            self.table.c.connector_id == connector_id,
            self.table.c.endpoint_id == endpoint_id,
            self.table.c.device_id == device_id
        )
        return self.scalar(stmt)

    def update_cursor(
        self,
        connector_id: str,
        endpoint_id: str,
        device_id: str,
        timestamp: datetime
    ):
        """
        Update or create fetch cursor.

        Uses PostgreSQL upsert for atomic operation.
        """
        stmt = pg_insert(self.table).values(
            connector_id=connector_id,
            endpoint_id=endpoint_id,
            device_id=device_id,
            last_fetch_timestamp=timestamp,
            last_fetch_success=func.now(),
            fetch_count=1
        )

        # On conflict, update timestamp and increment count
        stmt = stmt.on_conflict_do_update(
            constraint='uq_api_cursor_connector_endpoint_device',
            set_={
                'last_fetch_timestamp': timestamp,
                'last_fetch_success': func.now(),
                'fetch_count': self.table.c.fetch_count + 1,
                'updated_at': func.now()
            }
        )

        self.execute(stmt)

    def get_all_cursors(
        self,
        connector_id: Optional[str] = None
    ) -> list[dict]:
        """Get all cursors, optionally filtered by connector."""
        stmt = select(self.table)
        
        if connector_id:
            stmt = stmt.where(self.table.c.connector_id == connector_id)
        
        stmt = stmt.order_by(
            self.table.c.connector_id,
            self.table.c.endpoint_id,
            self.table.c.device_id
        )
        
        rows = self.fetch_all(stmt)
        return [dict(row._mapping) for row in rows]

    def delete_cursor(
        self,
        connector_id: str,
        endpoint_id: str,
        device_id: str
    ) -> bool:
        """Delete a specific cursor. Returns True if deleted."""
        from sqlalchemy import delete
        
        stmt = delete(self.table).where(
            self.table.c.connector_id == connector_id,
            self.table.c.endpoint_id == endpoint_id,
            self.table.c.device_id == device_id
        )
        
        result = self.execute(stmt)
        return result.rowcount > 0

    def reset_all_cursors(self, connector_id: str) -> int:
        """Reset all cursors for a connector. Returns count of deleted cursors."""
        from sqlalchemy import delete
        
        stmt = delete(self.table).where(
            self.table.c.connector_id == connector_id
        )
        
        result = self.execute(stmt)
        return result.rowcount
