"""Device queries."""

from typing import Any

from app.db.connection import execute_query, execute_one, execute_count


def get_devices(
    device_type: str | None = None,
    status: str | None = None,
    page: int = 1,
    page_size: int = 100
) -> tuple[list[dict[str, Any]], int]:
    """
    Get devices from generic_device table.
    
    Returns:
        Tuple of (records, total_count)
    """
    conditions = ["1=1"]
    params: dict[str, Any] = {
        "limit": page_size,
        "offset": (page - 1) * page_size
    }
    
    if device_type:
        conditions.append("device_type = :device_type")
        params["device_type"] = device_type
    
    if status:
        conditions.append("status = :status")
        params["status"] = status
    
    where_clause = " AND ".join(conditions)
    
    # Get total count
    count_query = f"""
        SELECT COUNT(*) as count 
        FROM generic_device 
        WHERE {where_clause}
    """
    total = execute_count(count_query, params)
    
    # Get data
    data_query = f"""
        SELECT 
            id,
            device_id,
            device_type,
            alias,
            client,
            description,
            status,
            timezone,
            metadata,
            created_at,
            updated_at
        FROM generic_device
        WHERE {where_clause}
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
    """
    rows = execute_query(data_query, params)
    
    return rows, total


def get_device_by_id(device_id: int) -> dict[str, Any] | None:
    """Get device by internal ID."""
    query = """
        SELECT 
            id,
            device_id,
            device_type,
            alias,
            client,
            description,
            status,
            timezone,
            metadata,
            created_at,
            updated_at
        FROM generic_device
        WHERE id = :id
    """
    return execute_one(query, {"id": device_id})


def get_device_types() -> list[dict[str, Any]]:
    """Get distinct device types with counts."""
    query = """
        SELECT 
            device_type,
            COUNT(*) as count
        FROM generic_device
        GROUP BY device_type
        ORDER BY device_type
    """
    return execute_query(query)
