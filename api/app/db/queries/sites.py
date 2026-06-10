"""Site queries using SQLAlchemy ORM."""

from sqlalchemy.orm import Session

from shared import Site


def get_sites(db: Session) -> list[Site]:
    """Get all sites ordered by name."""
    return db.query(Site).order_by(Site.name.asc()).all()


def get_site_by_id(db: Session, site_id: int) -> Site | None:
    """Get a site by internal ID."""
    return db.query(Site).filter(Site.id == site_id).first()