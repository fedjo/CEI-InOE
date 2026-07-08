"""Test fixtures for the API package."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterator

import pytest
from fastapi.testclient import TestClient


REPO_ROOT = Path(__file__).resolve().parents[3]
API_ROOT = REPO_ROOT / "api"
SHARED_SRC = REPO_ROOT / "shared" / "src"

for path in (API_ROOT, SHARED_SRC):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from app.config import settings  # noqa: E402
from app.db.session import get_db  # noqa: E402
from app.main import app  # noqa: E402


class DummyDB:
    """Sentinel object used to prove route handlers receive the dependency."""


@pytest.fixture
def db() -> DummyDB:
    return DummyDB()


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, db: DummyDB) -> Iterator[TestClient]:
    monkeypatch.setattr(settings, "api_key", "test-api-key")

    def override_get_db() -> Iterator[DummyDB]:
        yield db

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


@pytest.fixture
def auth_headers() -> dict[str, str]:
    return {"X-API-Key": "test-api-key"}
