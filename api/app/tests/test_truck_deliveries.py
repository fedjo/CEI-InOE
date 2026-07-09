"""Tests for truck milk delivery router."""

from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.routers import truck_deliveries


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

NOW = datetime(2026, 7, 8, 10, 0, 0, tzinfo=timezone.utc)


def delivery_record(**overrides):
    values = {
        "id": 1,
        "reception_date": date(2026, 7, 8),
        "truck_id": "TRK-001",
        "receipt_number": "RCP-2026-00001",
        "farm_of_origin": "Papageorgiou Farm",
        "cow_milk_delivered_kg": 500.0,
        "total_cow_milk_in_truck_kg": 1200.0,
        "total_milk_in_truck_kg": 1200.0,
        "silo_number": 1,
        "production_batch_numbers": None,
        "batch_produced_date": None,
        "created_at": NOW,
        "updated_at": NOW,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


# ---------------------------------------------------------------------------
# Schema / validator tests (no DB, no HTTP)
# ---------------------------------------------------------------------------

class TestTruckMilkDeliverySchema:
    """Unit tests for Pydantic schema validation."""

    def _valid_payload(self, **overrides):
        from shared.schemas import TruckMilkDeliveryCreate
        data = {
            "reception_date": date(2026, 7, 8),
            "truck_id": "TRK-001",
            "receipt_number": "RCP-001",
            "farm_of_origin": "Test Farm",
            "cow_milk_delivered_kg": 500.0,
            "total_cow_milk_in_truck_kg": 1200.0,
            "total_milk_in_truck_kg": 1200.0,
            "silo_number": 1,
        }
        data.update(overrides)
        return TruckMilkDeliveryCreate(**data)

    def test_valid_payload_accepted(self):
        schema = self._valid_payload()
        assert schema.truck_id == "TRK-001"
        assert schema.silo_number == 1

    def test_silo_must_be_1_or_2(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            self._valid_payload(silo_number=0)
        with pytest.raises(ValidationError):
            self._valid_payload(silo_number=3)

    def test_cow_milk_delivered_must_be_positive(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            self._valid_payload(cow_milk_delivered_kg=0)
        with pytest.raises(ValidationError):
            self._valid_payload(cow_milk_delivered_kg=-10.0)

    def test_total_cow_milk_must_be_positive(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            self._valid_payload(total_cow_milk_in_truck_kg=0)

    def test_total_milk_must_be_positive(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            self._valid_payload(total_milk_in_truck_kg=0)

    def test_truck_id_max_length(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            self._valid_payload(truck_id="X" * 65)

    def test_receipt_number_max_length(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            self._valid_payload(receipt_number="R" * 65)

    def test_truck_id_min_length(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            self._valid_payload(truck_id="")

    def test_optional_batch_fields_default_none(self):
        schema = self._valid_payload()
        assert schema.production_batch_numbers is None
        assert schema.batch_produced_date is None

    def test_optional_batch_fields_accepted(self):
        schema = self._valid_payload(
            production_batch_numbers="B2026-001, B2026-002",
            batch_produced_date=date(2026, 7, 9),
        )
        assert schema.production_batch_numbers == "B2026-001, B2026-002"
        assert schema.batch_produced_date == date(2026, 7, 9)

    def test_update_schema_all_optional(self):
        from shared.schemas import TruckMilkDeliveryUpdate
        schema = TruckMilkDeliveryUpdate()
        assert schema.truck_id is None
        assert schema.silo_number is None

    def test_update_schema_silo_validated(self):
        from pydantic import ValidationError
        from shared.schemas import TruckMilkDeliveryUpdate
        with pytest.raises(ValidationError):
            TruckMilkDeliveryUpdate(silo_number=5)

    def test_read_schema_from_orm(self):
        from shared.schemas import TruckMilkDeliveryRead
        rec = delivery_record()
        schema = TruckMilkDeliveryRead.model_validate(rec)
        assert schema.id == 1
        assert schema.truck_id == "TRK-001"


# ---------------------------------------------------------------------------
# HTTP-level route tests (via TestClient)
# ---------------------------------------------------------------------------

class TestTruckDeliveryRoutes:
    """Integration-style route tests using a mocked DB session."""

    def _mock_db(self):
        return MagicMock()

    # ── POST /api/v1/truck-deliveries/ ────────────────────────────────────

    def test_create_delivery_success(self, client, auth_headers):
        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = None  # no duplicate
        added = []
        db.add.side_effect = added.append
        db.refresh.side_effect = lambda obj: setattr(obj, "__dict__", delivery_record().__dict__)

        from app.db.session import get_db
        from app.main import app

        def override():
            yield db

        app.dependency_overrides[get_db] = override
        try:
            payload = {
                "reception_date": "2026-07-08",
                "truck_id": "TRK-001",
                "receipt_number": "RCP-001",
                "farm_of_origin": "Test Farm",
                "cow_milk_delivered_kg": 500.0,
                "total_cow_milk_in_truck_kg": 1200.0,
                "total_milk_in_truck_kg": 1200.0,
                "silo_number": 1,
            }
            resp = client.post("/api/v1/truck-deliveries/", json=payload, headers=auth_headers)
            assert resp.status_code == 201
        finally:
            from app.db.session import get_db as _get_db
            app.dependency_overrides.pop(get_db, None)

    def test_create_delivery_duplicate_receipt(self, client, auth_headers):
        from app.db.session import get_db
        from app.main import app

        db = MagicMock()
        db.query.return_value.filter.return_value.first.return_value = delivery_record()

        def override():
            yield db

        app.dependency_overrides[get_db] = override
        try:
            payload = {
                "reception_date": "2026-07-08",
                "truck_id": "TRK-001",
                "receipt_number": "RCP-001",
                "farm_of_origin": "Test Farm",
                "cow_milk_delivered_kg": 500.0,
                "total_cow_milk_in_truck_kg": 1200.0,
                "total_milk_in_truck_kg": 1200.0,
                "silo_number": 1,
            }
            resp = client.post("/api/v1/truck-deliveries/", json=payload, headers=auth_headers)
            assert resp.status_code == 409
        finally:
            app.dependency_overrides.pop(get_db, None)

    def test_create_delivery_invalid_silo(self, client, auth_headers):
        payload = {
            "reception_date": "2026-07-08",
            "truck_id": "TRK-001",
            "receipt_number": "RCP-999",
            "farm_of_origin": "Test Farm",
            "cow_milk_delivered_kg": 500.0,
            "total_cow_milk_in_truck_kg": 1200.0,
            "total_milk_in_truck_kg": 1200.0,
            "silo_number": 5,  # invalid
        }
        resp = client.post("/api/v1/truck-deliveries/", json=payload, headers=auth_headers)
        assert resp.status_code == 422

    def test_create_delivery_negative_milk(self, client, auth_headers):
        payload = {
            "reception_date": "2026-07-08",
            "truck_id": "TRK-001",
            "receipt_number": "RCP-999",
            "farm_of_origin": "Test Farm",
            "cow_milk_delivered_kg": -10.0,  # invalid
            "total_cow_milk_in_truck_kg": 1200.0,
            "total_milk_in_truck_kg": 1200.0,
            "silo_number": 1,
        }
        resp = client.post("/api/v1/truck-deliveries/", json=payload, headers=auth_headers)
        assert resp.status_code == 422

    def test_create_requires_auth(self, client):
        payload = {
            "reception_date": "2026-07-08",
            "truck_id": "TRK-001",
            "receipt_number": "RCP-001",
            "farm_of_origin": "Test Farm",
            "cow_milk_delivered_kg": 500.0,
            "total_cow_milk_in_truck_kg": 1200.0,
            "total_milk_in_truck_kg": 1200.0,
            "silo_number": 1,
        }
        resp = client.post("/api/v1/truck-deliveries/", json=payload)
        assert resp.status_code in (401, 403, 422)

    # ── GET /api/v1/truck-deliveries/ ─────────────────────────────────────

    def test_list_deliveries_returns_paginated(self, client, auth_headers):
        from app.db.session import get_db
        from app.main import app

        records = [delivery_record(id=i, receipt_number=f"R-{i}") for i in range(1, 4)]
        db = MagicMock()
        q = db.query.return_value
        q.filter.return_value = q
        q.count.return_value = 3
        q.order_by.return_value.offset.return_value.limit.return_value.all.return_value = records

        def override():
            yield db

        app.dependency_overrides[get_db] = override
        try:
            resp = client.get("/api/v1/truck-deliveries/", headers=auth_headers)
            assert resp.status_code == 200
            data = resp.json()
            assert data["total"] == 3
            assert len(data["data"]) == 3
        finally:
            app.dependency_overrides.pop(get_db, None)

    def test_list_deliveries_invalid_date_range(self, client, auth_headers):
        from app.db.session import get_db
        from app.main import app

        db = MagicMock()

        def override():
            yield db

        app.dependency_overrides[get_db] = override
        try:
            resp = client.get(
                "/api/v1/truck-deliveries/?start_date=2026-07-10&end_date=2026-07-01",
                headers=auth_headers,
            )
            assert resp.status_code == 400
        finally:
            app.dependency_overrides.pop(get_db, None)

    # ── GET /api/v1/truck-deliveries/{id} ────────────────────────────────

    def test_get_delivery_found(self, client, auth_headers):
        from app.db.session import get_db
        from app.main import app

        db = MagicMock()
        db.get.return_value = delivery_record()

        def override():
            yield db

        app.dependency_overrides[get_db] = override
        try:
            resp = client.get("/api/v1/truck-deliveries/1", headers=auth_headers)
            assert resp.status_code == 200
            assert resp.json()["truck_id"] == "TRK-001"
        finally:
            app.dependency_overrides.pop(get_db, None)

    def test_get_delivery_not_found(self, client, auth_headers):
        from app.db.session import get_db
        from app.main import app

        db = MagicMock()
        db.get.return_value = None

        def override():
            yield db

        app.dependency_overrides[get_db] = override
        try:
            resp = client.get("/api/v1/truck-deliveries/999", headers=auth_headers)
            assert resp.status_code == 404
        finally:
            app.dependency_overrides.pop(get_db, None)

    # ── PATCH /api/v1/truck-deliveries/{id} ──────────────────────────────

    def test_patch_delivery_batch_info(self, client, auth_headers):
        from app.db.session import get_db
        from app.main import app

        rec = delivery_record()
        db = MagicMock()
        db.get.return_value = rec
        db.query.return_value.filter.return_value.first.return_value = None

        def override():
            yield db

        app.dependency_overrides[get_db] = override
        try:
            resp = client.patch(
                "/api/v1/truck-deliveries/1",
                json={"production_batch_numbers": "B2026-001", "batch_produced_date": "2026-07-09"},
                headers=auth_headers,
            )
            assert resp.status_code == 200
        finally:
            app.dependency_overrides.pop(get_db, None)

    def test_patch_delivery_not_found(self, client, auth_headers):
        from app.db.session import get_db
        from app.main import app

        db = MagicMock()
        db.get.return_value = None

        def override():
            yield db

        app.dependency_overrides[get_db] = override
        try:
            resp = client.patch(
                "/api/v1/truck-deliveries/999",
                json={"production_batch_numbers": "B2026-001"},
                headers=auth_headers,
            )
            assert resp.status_code == 404
        finally:
            app.dependency_overrides.pop(get_db, None)

    # ── DELETE /api/v1/truck-deliveries/{id} ─────────────────────────────

    def test_delete_delivery_success(self, client, auth_headers):
        from app.db.session import get_db
        from app.main import app

        db = MagicMock()
        db.get.return_value = delivery_record()

        def override():
            yield db

        app.dependency_overrides[get_db] = override
        try:
            resp = client.delete("/api/v1/truck-deliveries/1", headers=auth_headers)
            assert resp.status_code == 204
        finally:
            app.dependency_overrides.pop(get_db, None)

    def test_delete_delivery_not_found(self, client, auth_headers):
        from app.db.session import get_db
        from app.main import app

        db = MagicMock()
        db.get.return_value = None

        def override():
            yield db

        app.dependency_overrides[get_db] = override
        try:
            resp = client.delete("/api/v1/truck-deliveries/999", headers=auth_headers)
            assert resp.status_code == 404
        finally:
            app.dependency_overrides.pop(get_db, None)
