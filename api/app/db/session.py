"""
Database session dependency for FastAPI.

Uses shared database utilities for ORM sessions.
"""

from typing import Generator
from sqlalchemy.orm import Session

from shared.database import get_session_factory, get_engine, check_connection

# Re-export for backward compatibility
__all__ = ["get_db", "check_connection", "get_engine"]


def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency for database sessions.
    
    Usage:
        @app.get("/items")
        def get_items(db: Session = Depends(get_db)):
            return db.query(Item).all()
    """
    SessionLocal = get_session_factory()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
