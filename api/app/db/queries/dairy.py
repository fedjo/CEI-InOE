"""Dairy production queries."""

from datetime import date
from typing import Any

from app.db.connection import execute_query, execute_one, execute_count


def get_production(
    start_date: date | None = None,
    end_date: date | None = None,
    page: int = 1,
    page_size: int = 100
) -> tuple[list[dict[str, Any]], int]:
    """
    Get dairy production records from dairy_production table.
    
    Returns:
        Tuple of (records, total_count)
    """
    conditions = ["1=1"]
    params: dict[str, Any] = {
        "limit": page_size,
        "offset": (page - 1) * page_size
    }
    
    if start_date:
        conditions.append("production_date >= %(start_date)s")
        params["start_date"] = start_date
    
    if end_date:
        conditions.append("production_date <= %(end_date)s")
        params["end_date"] = end_date
    
    where_clause = " AND ".join(conditions)
    
    # Get total count
    count_query = f"""
        SELECT COUNT(*) as count 
        FROM dairy_production 
        WHERE {where_clause}
    """
    total = execute_count(count_query, params)
    
    # Get data
    data_query = f"""
        SELECT 
            id,
            production_date,
            day_production_per_cow_kg,
            number_of_animals,
            average_lactation_days,
            fed_per_cow_total_kg,
            fed_per_cow_water_kg,
            feed_efficiency,
            rumination_minutes,
            source_type,
            source_file,
            ingested_at
        FROM dairy_production
        WHERE {where_clause}
        ORDER BY production_date DESC
        LIMIT %(limit)s OFFSET %(offset)s
    """
    rows = execute_query(data_query, params)
    
    return rows, total


def get_latest() -> dict[str, Any] | None:
    """Get most recent dairy production record."""
    query = """
        SELECT 
            id,
            production_date,
            day_production_per_cow_kg,
            number_of_animals,
            average_lactation_days,
            fed_per_cow_total_kg,
            fed_per_cow_water_kg,
            feed_efficiency,
            rumination_minutes
        FROM dairy_production
        ORDER BY production_date DESC
        LIMIT 1
    """
    return execute_one(query)


def get_stats() -> dict[str, Any]:
    """Get dairy data statistics."""
    query = """
        SELECT 
            COUNT(*) as total_count,
            MIN(production_date) as first_record,
            MAX(production_date) as last_record,
            ROUND(AVG(day_production_per_cow_kg)::numeric, 2) as avg_production_per_cow,
            ROUND(AVG(number_of_animals)::numeric, 0) as avg_animals,
            ROUND(AVG(feed_efficiency)::numeric, 4) as avg_feed_efficiency,
            ROUND(AVG(rumination_minutes)::numeric, 0) as avg_rumination_minutes
        FROM dairy_production
    """
    return execute_one(query) or {}
