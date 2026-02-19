"""Energy data queries."""

from datetime import date
from typing import Any

from app.db.connection import execute_query, execute_one, execute_count


def get_hourly(
    start_date: date,
    end_date: date,
    device_id: int | None = None,
    page: int = 1,
    page_size: int = 100
) -> tuple[list[dict[str, Any]], int]:
    """
    Get hourly energy records from fact_energy_hourly.
    
    Returns:
        Tuple of (records, total_count)
    """
    conditions = ["ts >= %(start_date)s", "ts < %(end_date)s + INTERVAL '1 day'"]
    params: dict[str, Any] = {
        "start_date": start_date,
        "end_date": end_date,
        "limit": page_size,
        "offset": (page - 1) * page_size
    }
    
    if device_id is not None:
        conditions.append("device_id = %(device_id)s")
        params["device_id"] = device_id
    
    where_clause = " AND ".join(conditions)
    
    # Get total count
    count_query = f"""
        SELECT COUNT(*) as count 
        FROM fact_energy_hourly 
        WHERE {where_clause}
    """
    total = execute_count(count_query, params)
    
    # Get data
    data_query = f"""
        SELECT 
            energy_id as id,
            device_id,
            ts,
            energy_kwh as kwh,
            source_type,
            source_file,
            created_at
        FROM fact_energy_hourly
        WHERE {where_clause}
        ORDER BY ts DESC
        LIMIT %(limit)s OFFSET %(offset)s
    """
    rows = execute_query(data_query, params)
    
    return rows, total


def get_daily(
    start_date: date,
    end_date: date,
    device_id: int | None = None,
    page: int = 1,
    page_size: int = 100
) -> tuple[list[dict[str, Any]], int]:
    """
    Get daily energy records from fact_energy_daily.
    
    Returns:
        Tuple of (records, total_count)
    """
    conditions = ["ts >= %(start_date)s", "ts <= %(end_date)s"]
    params: dict[str, Any] = {
        "start_date": start_date,
        "end_date": end_date,
        "limit": page_size,
        "offset": (page - 1) * page_size
    }
    
    if device_id is not None:
        conditions.append("device_id = %(device_id)s")
        params["device_id"] = device_id
    
    where_clause = " AND ".join(conditions)
    
    # Get total count
    count_query = f"""
        SELECT COUNT(*) as count 
        FROM fact_energy_daily 
        WHERE {where_clause}
    """
    total = execute_count(count_query, params)
    
    # Get data
    data_query = f"""
        SELECT 
            energy_id as id,
            device_id,
            ts as day,
            energy_kwh as kwh,
            source_type,
            source_file,
            created_at
        FROM fact_energy_daily
        WHERE {where_clause}
        ORDER BY ts DESC
        LIMIT %(limit)s OFFSET %(offset)s
    """
    rows = execute_query(data_query, params)
    
    return rows, total


def get_latest_hourly(device_id: int | None = None) -> dict[str, Any] | None:
    """Get most recent hourly energy reading."""
    if device_id:
        query = """
            SELECT 
                energy_id as id,
                device_id,
                ts,
                energy_kwh as kwh
            FROM fact_energy_hourly
            WHERE device_id = %(device_id)s
            ORDER BY ts DESC
            LIMIT 1
        """
        return execute_one(query, {"device_id": device_id})
    else:
        query = """
            SELECT 
                energy_id as id,
                device_id,
                ts,
                energy_kwh as kwh
            FROM fact_energy_hourly
            ORDER BY ts DESC
            LIMIT 1
        """
        return execute_one(query)


def get_stats() -> dict[str, Any]:
    """Get energy data statistics."""
    query = """
        SELECT 
            (SELECT COUNT(*) FROM fact_energy_hourly) as hourly_count,
            (SELECT COUNT(*) FROM fact_energy_daily) as daily_count,
            (SELECT MIN(ts) FROM fact_energy_hourly) as hourly_first,
            (SELECT MAX(ts) FROM fact_energy_hourly) as hourly_last,
            (SELECT MIN(ts) FROM fact_energy_daily) as daily_first,
            (SELECT MAX(ts) FROM fact_energy_daily) as daily_last,
            (SELECT COUNT(DISTINCT device_id) FROM fact_energy_hourly) as hourly_devices,
            (SELECT COUNT(DISTINCT device_id) FROM fact_energy_daily) as daily_devices
    """
    return execute_one(query) or {}
