"""Device management utilities for multi-type device support."""

import json
from typing import Dict, Any, Optional
from enum import Enum


class DeviceType(str, Enum):
    ENERGY = "energy"
    WEATHER = "weather"
    PV = "pv"
    DAIRY = "dairy"
    UNKNOWN = "unknown"


def upsert_device(
    cursor,
    device_uid: str,
    device_type: DeviceType,
    display_name: str,
    client: str,
    metadata: Dict[str, Any],
    description: str = None,
    status: str = "unknown",
    timezone: str = "UTC"
) -> int:
    """
    Insert or update a device in device_v2 table.
    Returns the device id.
    """
    cursor.execute("""
        INSERT INTO device_v2 
            (device_uid, device_type, display_name, client, description, status, timezone, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (device_uid) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            client = EXCLUDED.client,
            description = EXCLUDED.description,
            status = EXCLUDED.status,
            timezone = EXCLUDED.timezone,
            metadata = device_v2.metadata || EXCLUDED.metadata,
            updated_at = NOW()
        RETURNING id
    """, (
        device_uid,
        device_type.value,
        display_name,
        client,
        description,
        status,
        timezone,
        json.dumps(metadata)
    ))
    return cursor.fetchone()[0]


def get_device_by_uid(cursor, device_uid: str) -> Optional[Dict[str, Any]]:
    """Get device by external UID."""
    cursor.execute("""
        SELECT id, device_uid, device_type, display_name, client, 
               description, status, timezone, metadata
        FROM device_v2
        WHERE device_uid = %s
    """, (device_uid,))
    row = cursor.fetchone()
    if not row:
        return None
    return {
        'id': row[0],
        'device_uid': row[1],
        'device_type': row[2],
        'display_name': row[3],
        'client': row[4],
        'description': row[5],
        'status': row[6],
        'timezone': row[7],
        'metadata': row[8]
    }


def get_devices_by_type(cursor, device_type: DeviceType) -> list:
    """Get all devices of a specific type."""
    cursor.execute("""
        SELECT id, device_uid, display_name, client, status, metadata
        FROM device_v2
        WHERE device_type = %s
        ORDER BY client, display_name
    """, (device_type.value,))
    return cursor.fetchall()