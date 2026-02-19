"""Environmental metrics queries."""

from datetime import date
from typing import Any

from app.db.connection import execute_query, execute_one, execute_count


def get_metrics(
    start_date: date,
    end_date: date,
    page: int = 1,
    page_size: int = 100
) -> tuple[list[dict[str, Any]], int]:
    """
    Get environmental metrics records from environmental_metrics table.
    
    Returns:
        Tuple of (records, total_count)
    """
    params: dict[str, Any] = {
        "start_date": start_date,
        "end_date": end_date,
        "limit": page_size,
        "offset": (page - 1) * page_size
    }
    
    where_clause = "timestamp >= %(start_date)s AND timestamp < %(end_date)s + INTERVAL '1 day'"
    
    # Get total count
    count_query = f"""
        SELECT COUNT(*) as count 
        FROM environmental_metrics 
        WHERE {where_clause}
    """
    total = execute_count(count_query, params)
    
    # Get data
    data_query = f"""
        SELECT 
            id,
            timestamp,
            temperature,
            humidity,
            atm_pressure,
            pm10,
            pm2p5,
            noise_level_db,
            wind_speed,
            wind_angle,
            wind_direction_sectors,
            source_type,
            source_file,
            created_at
        FROM environmental_metrics
        WHERE {where_clause}
        ORDER BY timestamp DESC
        LIMIT %(limit)s OFFSET %(offset)s
    """
    rows = execute_query(data_query, params)
    
    return rows, total


def get_latest() -> dict[str, Any] | None:
    """Get most recent environmental reading."""
    query = """
        SELECT 
            id,
            timestamp,
            temperature,
            humidity,
            atm_pressure,
            pm10,
            pm2p5,
            noise_level_db,
            wind_speed,
            wind_angle,
            wind_direction_sectors
        FROM environmental_metrics
        ORDER BY timestamp DESC
        LIMIT 1
    """
    return execute_one(query)


def get_stats() -> dict[str, Any]:
    """Get environmental data statistics."""
    query = """
        SELECT 
            COUNT(*) as total_count,
            MIN(timestamp) as first_record,
            MAX(timestamp) as last_record,
            ROUND(AVG(temperature)::numeric, 2) as avg_temperature,
            ROUND(AVG(humidity)::numeric, 2) as avg_humidity,
            ROUND(AVG(pm10)::numeric, 2) as avg_pm10,
            ROUND(AVG(pm2p5)::numeric, 2) as avg_pm2p5,
            COUNT(DISTINCT DATE(timestamp)) as days_with_data
        FROM environmental_metrics
    """
    return execute_one(query) or {}
