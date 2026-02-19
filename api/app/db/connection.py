"""Database connection and query utilities."""

from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
from typing import Generator, Any
import logging

from app.config import settings

logger = logging.getLogger(__name__)

# Connection pool (lazy initialized)
_pool: ThreadedConnectionPool | None = None


def get_pool() -> ThreadedConnectionPool:
    """Get or create connection pool."""
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            dsn=settings.database_url
        )
        logger.info("Database connection pool created")
    return _pool


def close_pool():
    """Close the connection pool."""
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None
        logger.info("Database connection pool closed")


@contextmanager
def get_connection():
    """Get a connection from the pool."""
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


@contextmanager
def get_cursor() -> Generator[RealDictCursor, None, None]:
    """Get a cursor that returns dictionaries."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur


def execute_query(query: str, params: dict | tuple | None = None) -> list[dict[str, Any]]:
    """Execute a query and return all results as list of dicts."""
    with get_cursor() as cur:
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]


def execute_one(query: str, params: dict | tuple | None = None) -> dict[str, Any] | None:
    """Execute a query and return single result as dict."""
    with get_cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        return dict(row) if row else None


def execute_count(query: str, params: dict | tuple | None = None) -> int:
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
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False
