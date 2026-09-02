"""API key authentication backed by DB principals with datasource-level access control."""

import hashlib
import secrets
from dataclasses import dataclass, field

from fastapi import Security, HTTPException, Depends, status
from fastapi.security import APIKeyHeader
from sqlalchemy.orm import Session

from app.config import settings
from app.db.session import get_db
from app.db.queries import principals as principal_queries

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)


def hash_api_key(api_key: str) -> str:
    """Hash an API key for storage/lookup.

    API keys are high-entropy random tokens (not user-chosen passwords), so a
    fast deterministic hash is sufficient and allows indexed exact-match
    lookups (the same approach GitHub/Stripe use for personal access tokens).
    """
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def generate_api_key() -> str:
    """Generate a new random API key."""
    return secrets.token_urlsafe(32)


@dataclass
class AuthenticatedPrincipal:
    """The authenticated caller of the API."""
    id: int | None
    name: str
    is_superuser: bool
    allowed_datasource_ids: frozenset[int] = field(default_factory=frozenset)

    def can_access_datasource(self, datasource_id: int) -> bool:
        return self.is_superuser or datasource_id in self.allowed_datasource_ids


async def verify_api_key(
    api_key: str = Security(api_key_header),
    db: Session = Depends(get_db),
) -> AuthenticatedPrincipal:
    """Verify the API key from request header and resolve it to a principal."""
    # Legacy fallback: the env-configured API_KEY always acts as a superuser,
    # so existing deployments keep working without a DB-backed principal.
    if settings.api_key and secrets.compare_digest(api_key, settings.api_key):
        return AuthenticatedPrincipal(id=None, name="legacy-superuser", is_superuser=True)

    principal = principal_queries.get_principal_by_api_key_hash(db, hash_api_key(api_key))
    if principal is None or principal.status != "active":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key"
        )

    allowed_ids = frozenset(principal_queries.get_datasource_ids_for_principal(db, principal.id))
    return AuthenticatedPrincipal(
        id=principal.id,
        name=principal.name,
        is_superuser=principal.is_superuser,
        allowed_datasource_ids=allowed_ids,
    )


def ensure_datasource_access(principal: AuthenticatedPrincipal, datasource_id: int) -> None:
    """Raise 404 if the principal is not allowed to access this datasource.

    404 (rather than 403) avoids confirming the datasource's existence to
    callers who are not authorized to see it.
    """
    if not principal.can_access_datasource(datasource_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Datasource not found")


async def require_superuser(
    principal: AuthenticatedPrincipal = Depends(verify_api_key),
) -> AuthenticatedPrincipal:
    """Dependency restricting access to superuser principals (admin endpoints)."""
    if not principal.is_superuser:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Superuser access required")
    return principal
