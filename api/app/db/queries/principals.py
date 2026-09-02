"""Queries for API principals and their datasource access grants."""

from sqlalchemy.orm import Session

from shared import ApiPrincipal, ApiPrincipalDatasourceAccess


def get_principal_by_api_key_hash(db: Session, api_key_hash: str) -> ApiPrincipal | None:
    """Look up an active principal by its hashed API key."""
    return db.query(ApiPrincipal).filter(ApiPrincipal.api_key_hash == api_key_hash).first()


def get_principal_by_id(db: Session, principal_id: int) -> ApiPrincipal | None:
    """Get a principal by internal ID."""
    return db.query(ApiPrincipal).filter(ApiPrincipal.id == principal_id).first()


def get_datasource_ids_for_principal(db: Session, principal_id: int) -> list[int]:
    """Get all datasource IDs a principal has been granted access to."""
    rows = db.query(ApiPrincipalDatasourceAccess.datasource_id).filter(
        ApiPrincipalDatasourceAccess.principal_id == principal_id
    ).all()
    return [row.datasource_id for row in rows]


def list_principals(
    db: Session,
    page: int = 1,
    page_size: int = 100,
) -> tuple[list[ApiPrincipal], int]:
    """List principals with pagination."""
    query = db.query(ApiPrincipal)
    total = query.count()
    principals = query.order_by(ApiPrincipal.created_at.desc()) \
        .offset((page - 1) * page_size) \
        .limit(page_size) \
        .all()
    return principals, total


def create_principal(db: Session, principal: ApiPrincipal) -> ApiPrincipal:
    """Create a new principal."""
    db.add(principal)
    db.commit()
    db.refresh(principal)
    return principal


def update_principal(db: Session, principal_id: int, **kwargs) -> ApiPrincipal | None:
    """Update mutable principal fields."""
    principal = get_principal_by_id(db, principal_id)
    if not principal:
        return None

    for key, value in kwargs.items():
        if hasattr(principal, key) and value is not None:
            setattr(principal, key, value)

    db.commit()
    db.refresh(principal)
    return principal


def grant_datasource_access(
    db: Session,
    principal_id: int,
    datasource_id: int,
    granted_by: int | None = None,
) -> ApiPrincipalDatasourceAccess:
    """Grant a principal access to a datasource (idempotent)."""
    existing = db.query(ApiPrincipalDatasourceAccess).filter(
        ApiPrincipalDatasourceAccess.principal_id == principal_id,
        ApiPrincipalDatasourceAccess.datasource_id == datasource_id,
    ).first()
    if existing:
        return existing

    grant = ApiPrincipalDatasourceAccess(
        principal_id=principal_id,
        datasource_id=datasource_id,
        granted_by=granted_by,
    )
    db.add(grant)
    db.commit()
    db.refresh(grant)
    return grant


def revoke_datasource_access(db: Session, principal_id: int, datasource_id: int) -> bool:
    """Revoke a principal's access to a datasource. Returns True if a grant was removed."""
    deleted = db.query(ApiPrincipalDatasourceAccess).filter(
        ApiPrincipalDatasourceAccess.principal_id == principal_id,
        ApiPrincipalDatasourceAccess.datasource_id == datasource_id,
    ).delete()
    db.commit()
    return deleted > 0
