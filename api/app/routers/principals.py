"""Admin endpoints for managing API principals and their datasource access grants.

All endpoints require a superuser principal (enforced at router inclusion in main.py).
"""

from fastapi import APIRouter, Query, HTTPException, Depends
from fastapi import status as http_status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.db.queries import datasources as ds_queries
from app.db.queries import principals as principal_queries
from app.config import settings
from app.auth import AuthenticatedPrincipal, generate_api_key, hash_api_key, require_superuser

from shared import (
    ApiPrincipal,
    ApiPrincipalCreate,
    ApiPrincipalCreateResponse,
    ApiPrincipalRead,
    ApiPrincipalUpdate,
    DatasourceAccessGrant,
    DatasourceAccessRead,
    PaginatedResponse,
)

router = APIRouter()


@router.get("", response_model=PaginatedResponse)
async def list_principals(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(
        settings.default_page_size,
        ge=1,
        le=settings.max_page_size,
        description="Records per page"
    ),
    db: Session = Depends(get_db),
    _: AuthenticatedPrincipal = Depends(require_superuser),
):
    """List all API principals."""
    principals, total = principal_queries.list_principals(db, page=page, page_size=page_size)

    return PaginatedResponse(
        data=[ApiPrincipalRead.model_validate(p) for p in principals],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size
    )


@router.post("", response_model=ApiPrincipalCreateResponse, status_code=http_status.HTTP_201_CREATED)
async def create_principal(
    payload: ApiPrincipalCreate,
    db: Session = Depends(get_db),
    _: AuthenticatedPrincipal = Depends(require_superuser),
):
    """
    Register a new API principal and issue its API key.

    The plaintext API key is only ever returned in this response - it is not
    recoverable afterward. Store it securely and issue a new principal if it's lost.
    """
    api_key = generate_api_key()
    principal = ApiPrincipal(
        api_key_hash=hash_api_key(api_key),
        name=payload.name,
        description=payload.description,
        status=payload.status,
        is_superuser=payload.is_superuser,
    )
    created = principal_queries.create_principal(db, principal)

    return ApiPrincipalCreateResponse(
        **ApiPrincipalRead.model_validate(created).model_dump(),
        api_key=api_key,
    )


@router.get("/{principal_id}", response_model=ApiPrincipalRead)
async def get_principal(
    principal_id: int,
    db: Session = Depends(get_db),
    _: AuthenticatedPrincipal = Depends(require_superuser),
):
    """Get a specific API principal by ID."""
    principal = principal_queries.get_principal_by_id(db, principal_id)
    if not principal:
        raise HTTPException(status_code=404, detail="Principal not found")
    return ApiPrincipalRead.model_validate(principal)


@router.patch("/{principal_id}", response_model=ApiPrincipalRead)
async def update_principal(
    principal_id: int,
    payload: ApiPrincipalUpdate,
    db: Session = Depends(get_db),
    _: AuthenticatedPrincipal = Depends(require_superuser),
):
    """Update mutable fields of a principal (name, description, status)."""
    updated = principal_queries.update_principal(db, principal_id, **payload.model_dump(exclude_none=True))
    if not updated:
        raise HTTPException(status_code=404, detail="Principal not found")
    return ApiPrincipalRead.model_validate(updated)


@router.get("/{principal_id}/access", response_model=list[DatasourceAccessRead])
async def list_principal_access(
    principal_id: int,
    db: Session = Depends(get_db),
    _: AuthenticatedPrincipal = Depends(require_superuser),
):
    """List all datasource access grants for a principal."""
    if not principal_queries.get_principal_by_id(db, principal_id):
        raise HTTPException(status_code=404, detail="Principal not found")

    return [
        DatasourceAccessRead(datasource_id=ds_id)
        for ds_id in principal_queries.get_datasource_ids_for_principal(db, principal_id)
    ]


@router.post("/{principal_id}/access", response_model=DatasourceAccessRead, status_code=http_status.HTTP_201_CREATED)
async def grant_principal_access(
    principal_id: int,
    payload: DatasourceAccessGrant,
    db: Session = Depends(get_db),
    granting_principal: AuthenticatedPrincipal = Depends(require_superuser),
):
    """Grant a principal access to a datasource."""
    if not principal_queries.get_principal_by_id(db, principal_id):
        raise HTTPException(status_code=404, detail="Principal not found")
    if not ds_queries.get_datasource_by_id(db, payload.datasource_id):
        raise HTTPException(status_code=404, detail="Datasource not found")

    grant = principal_queries.grant_datasource_access(
        db, principal_id, payload.datasource_id, granted_by=granting_principal.id
    )
    return DatasourceAccessRead.model_validate(grant)


@router.delete("/{principal_id}/access/{datasource_id}", status_code=http_status.HTTP_204_NO_CONTENT)
async def revoke_principal_access(
    principal_id: int,
    datasource_id: int,
    db: Session = Depends(get_db),
    _: AuthenticatedPrincipal = Depends(require_superuser),
):
    """Revoke a principal's access to a datasource."""
    if not principal_queries.revoke_datasource_access(db, principal_id, datasource_id):
        raise HTTPException(status_code=404, detail="Access grant not found")
