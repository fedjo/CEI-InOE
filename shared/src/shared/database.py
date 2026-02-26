"""
Database connection utilities for CEI-InOE.

Provides both SQLAlchemy ORM Session (for API) and Core Engine (for Ingestor).
"""

import os
import logging
from typing import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

from shared.models import Base

logger = logging.getLogger(__name__)

# Global engine instance
_engine: Engine | None = None


def get_database_url() -> str:
    """Get database URL from environment."""
    # Try different env var names for compatibility
    url = os.getenv("DATABASE_URL") or os.getenv("DB_DSN")
    if not url:
        raise ValueError("DATABASE_URL or DB_DSN environment variable must be set")
    
    # Handle postgres:// vs postgresql:// (SQLAlchemy 2.0 requires postgresql://)
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    
    return url


def get_engine(pool_size: int = 5, max_overflow: int = 10) -> Engine:
    """
    Get or create SQLAlchemy engine.
    
    Args:
        pool_size: Number of connections to keep in pool
        max_overflow: Max connections beyond pool_size
    
    Returns:
        SQLAlchemy Engine instance
    """
    global _engine
    if _engine is None:
        database_url = get_database_url()
        _engine = create_engine(
            database_url,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,  # Verify connections before use
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


def create_all_tables(engine: Engine | None = None):
    """Create all tables defined in models."""
    engine = engine or get_engine()
    Base.metadata.create_all(engine)
    logger.info("All tables created")


def drop_all_tables(engine: Engine | None = None):
    """Drop all tables (use with caution!)."""
    engine = engine or get_engine()
    Base.metadata.drop_all(engine)
    logger.warning("All tables dropped")


# =============================================================================
# ORM Session Management (for API)
# =============================================================================

def get_session_factory(engine: Engine | None = None) -> sessionmaker:
    """Get a session factory for ORM operations."""
    engine = engine or get_engine()
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_session() -> Generator[Session, None, None]:
    """
    FastAPI dependency for getting a database session.
    
    Usage:
        @app.get("/items")
        def get_items(db: Session = Depends(get_session)):
            return db.query(Item).all()
    """
    SessionLocal = get_session_factory()
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """
    Context manager for session lifecycle.
    
    Usage:
        with session_scope() as session:
            session.query(Item).all()
    """
    SessionLocal = get_session_factory()
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# =============================================================================
# Core Connection Management (for Ingestor)
# =============================================================================

@contextmanager
def get_connection():
    """
    Context manager for raw connection (Core operations).
    
    Usage:
        with get_connection() as conn:
            conn.execute(text("SELECT 1"))
    """
    engine = get_engine()
    conn = engine.connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def check_connection() -> bool:
    """Check if database connection is healthy."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            return result.scalar() == 1
    except Exception as e:
        logger.exception(f"Database health check failed: {e}")
        return False
