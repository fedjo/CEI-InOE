"""Database connection and query utilities using SQLAlchemy Core."""

from typing import Any
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.config import settings

logger = logging.getLogger(__name__)

# SQLAlchemy engine with built-in connection pool
_engine: Engine | None = None


def get_engine() -> Engine:
    """Get or create SQLAlchemy engine."""
    global _engine
    if _engine is None:
        _engine = create_engine(
            settings.database_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )
        logger.info("SQLAlchemy engine created")
    return _engine


def close_engine():
    """Dispose SQLAlchemy engine and connection pool."""
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None
        logger.info("SQLAlchemy engine disposed")


def execute_query(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Execute a query and return all results as list of dicts."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        return [dict(row._mapping) for row in result.fetchall()]


def execute_one(query: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """Execute a query and return single result as dict."""
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        row = result.fetchone()
        return dict(row._mapping) if row else None


def execute_count(query: str, params: dict[str, Any] | None = None) -> int:
    """Execute a count query and return the count."""
    result = execute_one(query, params)
    if result and 'count' in result:
        return result['count']
    return 0


def check_connection() -> bool:
    """Check if database connection is healthy."""
    try:
        result = execute_one("SELECT 1 as ok")
        return result is not None and result.get('ok') == 1
    except Exception:
        logger.exception("Database health check failed")
        return False
