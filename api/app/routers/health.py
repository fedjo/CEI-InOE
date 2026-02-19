"""Health check and statistics endpoints."""

from fastapi import APIRouter, HTTPException
from datetime import datetime

from app.db.connection import check_connection, execute_one
from app.schemas.common import HealthResponse, StatsResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint for container orchestration.
    
    Returns the health status of the API and database connection.
    """
    db_healthy = check_connection()
    
    return HealthResponse(
        status="healthy" if db_healthy else "degraded",
        database="connected" if db_healthy else "disconnected",
        timestamp=datetime.utcnow()
    )


@router.get("/stats", response_model=StatsResponse)
async def get_stats():
    """
    Get overall data statistics.
    
    Returns counts and latest timestamps for all data types.
    """
    result = execute_one("""
        SELECT 
            (SELECT COUNT(*) FROM environmental_metrics) as environmental_count,
            (SELECT COUNT(*) FROM fact_energy_hourly) as energy_hourly_count,
            (SELECT COUNT(*) FROM fact_energy_daily) as energy_daily_count,
            (SELECT COUNT(*) FROM dairy_production) as dairy_count,
            (SELECT COUNT(*) FROM generic_device) as devices_count,
            (SELECT MAX(timestamp) FROM environmental_metrics) as latest_environmental,
            (SELECT MAX(ts) FROM fact_energy_hourly) as latest_energy,
            (SELECT MAX(production_date) FROM dairy_production) as latest_dairy
    """)
    
    if not result:
        raise HTTPException(status_code=500, detail="Failed to retrieve stats")
    
    return StatsResponse(**result)
