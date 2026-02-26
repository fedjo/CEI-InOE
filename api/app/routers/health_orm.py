"""Health check endpoint."""

from fastapi import APIRouter, Depends
from datetime import datetime
from sqlalchemy.orm import Session

from app.db.session import get_db, check_connection
from shared import HealthResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint.
    
    Returns the health status of the API and database connection.
    """
    db_healthy = check_connection()
    
    return HealthResponse(
        status="healthy" if db_healthy else "degraded",
        database=db_healthy,
        timestamp=datetime.utcnow()
    )


@router.get("/ready")
async def readiness_check():
    """
    Readiness check for container orchestration.
    
    Returns 200 if the service is ready to accept traffic.
    """
    db_healthy = check_connection()
    
    if not db_healthy:
        return {"status": "not ready", "reason": "database connection failed"}
    
    return {"status": "ready"}
