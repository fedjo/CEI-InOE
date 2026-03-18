"""CEI-InOE Data API - Main Application."""

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from app.config import settings, get_cors_origins
from app.auth import verify_api_key
from shared.database import get_engine, close_engine
from app.routers import health, energy, environmental, dairy, datasources, batches

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    logger.info("Starting CEI-InOE Data API...")
    try:
        get_engine()  # Initialize connection pool
        logger.info("Database connection pool initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {e}")
    
    yield
    
    # Shutdown
    logger.info("Shutting down CEI-InOE Data API...")
    close_engine()
    logger.info("Database connection pool closed")


# Create FastAPI application
app = FastAPI(
    title=settings.api_title,
    description="""
CEI-InOE Data API provides RESTful access to environmental, energy, 
and dairy production data collected by the CEI-InOE platform.

## Data Types

- **Environmental**: Temperature, humidity, air quality, noise levels
- **Energy**: Hourly and daily energy consumption per datasource
- **Dairy**: Daily milk production and related metrics
- **Datasources**: Data source metadata (devices, files, APIs)
- **Batches**: Ingestion batch tracking

## Common Patterns

All list endpoints support pagination with `page` and `page_size` parameters.
Date filters use `start_date` and `end_date` parameters.
    """,
    version=settings.api_version,
    lifespan=lifespan,
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
# Health router is public (no auth required)
app.include_router(health.router, tags=["Health"])

# All data routers require API key
app.include_router(
    energy.router, 
    prefix="/api/v1/energy", 
    tags=["Energy"],
    dependencies=[Depends(verify_api_key)],
)

app.include_router(
    environmental.router, 
    prefix="/api/v1/environmental", 
    tags=["Environmental"],
    dependencies=[Depends(verify_api_key)],
)

app.include_router(
    dairy.router, 
    prefix="/api/v1/dairy", 
    tags=["Dairy"],
    dependencies=[Depends(verify_api_key)],
)

app.include_router(
    datasources.router, 
    prefix="/api/v1/datasources", 
    tags=["Datasources"],
    dependencies=[Depends(verify_api_key)],
)

app.include_router(
    batches.router, 
    prefix="/api/v1/batches", 
    tags=["Batches"],
    dependencies=[Depends(verify_api_key)],
)


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": settings.api_title,
        "version": settings.api_version,
        "docs": "/docs",
        "health": "/health",
    }
