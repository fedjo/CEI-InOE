"""
Device data access operations.
"""

import logging
from typing import Any, Dict, List, Optional

from .base import BaseDAO

logger = logging.getLogger(__name__)


class DeviceDAO(BaseDAO):
    """Data access for generic_device table."""

    def get_by_device_id(self, device_id: str) -> Optional[int]:
        """
        Resolve device string ID to database ID.

        Args:
            device_id: External device identifier

        Returns:
            Internal database ID or None
        """
        result = self.fetch_one(
            "SELECT id FROM generic_device WHERE device_id = %s",
            (device_id,)
        )
        return result[0] if result else None

    def get_devices_by_type(self, device_type: str, status: str = 'online') -> List[Dict[str, Any]]:
        """
        Get all devices of a specific type.

        Args:
            device_type: Type of device (e.g., 'weather', 'energy')
            status: Device status filter

        Returns:
            List of device dictionaries
        """
        rows = self.fetch_all(
            """
            SELECT 
                device_id,
                alias,
                client,
                metadata->>'external_id' as external_id,
                metadata
            FROM generic_device
            WHERE device_type = %s
            AND status = %s
            """,
            (device_type, status)
        )

        return [
            {
                'device_id': row[0],
                'alias': row[1],
                'client': row[2],
                'external_id': row[3],
                'metadata': row[4],
            }
            for row in rows
        ]

    def get_weather_devices(self) -> List[Dict[str, Any]]:
        """Get all online weather devices."""
        return self.get_devices_by_type('weather', 'online')

    def get_energy_devices(self) -> List[Dict[str, Any]]:
        """Get all online energy devices."""
        return self.get_devices_by_type('energy', 'online')
