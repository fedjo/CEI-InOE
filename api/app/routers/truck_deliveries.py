"""Truck milk delivery endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import date
from typing import Optional

from app.db.session import get_db
from shared.models import TruckMilkDelivery
from shared.schemas import (
    TruckMilkDeliveryCreate,
    TruckMilkDeliveryRead,
    TruckMilkDeliveryUpdate,
    PaginatedResponse,
)

router = APIRouter()


@router.post("/", response_model=TruckMilkDeliveryRead, status_code=201)
def create_delivery(
    payload: TruckMilkDeliveryCreate,
    db: Session = Depends(get_db),
):
    """Record a new truck milk delivery."""
    existing = db.query(TruckMilkDelivery).filter(
        TruckMilkDelivery.receipt_number == payload.receipt_number
    ).first()
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"Delivery with receipt number '{payload.receipt_number}' already exists.",
        )

    delivery = TruckMilkDelivery(**payload.model_dump())
    db.add(delivery)
    db.commit()
    db.refresh(delivery)
    return TruckMilkDeliveryRead.model_validate(delivery)


@router.get("/", response_model=PaginatedResponse)
def list_deliveries(
    start_date: Optional[date] = Query(None, description="Filter by reception date (from)"),
    end_date: Optional[date] = Query(None, description="Filter by reception date (to)"),
    truck_id: Optional[str] = Query(None, description="Filter by truck identifier"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """List truck milk deliveries with optional filters."""
    if start_date and end_date and start_date > end_date:
        raise HTTPException(
            status_code=400, detail="start_date must be before or equal to end_date"
        )

    q = db.query(TruckMilkDelivery)
    if start_date:
        q = q.filter(TruckMilkDelivery.reception_date >= start_date)
    if end_date:
        q = q.filter(TruckMilkDelivery.reception_date <= end_date)
    if truck_id:
        q = q.filter(TruckMilkDelivery.truck_id == truck_id)

    total = q.count()
    records = (
        q.order_by(TruckMilkDelivery.reception_date.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    return PaginatedResponse(
        data=[TruckMilkDeliveryRead.model_validate(r) for r in records],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=(total + page_size - 1) // page_size,
    )


@router.get("/{delivery_id}", response_model=TruckMilkDeliveryRead)
def get_delivery(delivery_id: int, db: Session = Depends(get_db)):
    """Get a single truck milk delivery by ID."""
    delivery = db.get(TruckMilkDelivery, delivery_id)
    if not delivery:
        raise HTTPException(status_code=404, detail="Delivery not found")
    return TruckMilkDeliveryRead.model_validate(delivery)


@router.patch("/{delivery_id}", response_model=TruckMilkDeliveryRead)
def update_delivery(
    delivery_id: int,
    payload: TruckMilkDeliveryUpdate,
    db: Session = Depends(get_db),
):
    """Partially update a truck milk delivery (e.g. fill in batch info)."""
    delivery = db.get(TruckMilkDelivery, delivery_id)
    if not delivery:
        raise HTTPException(status_code=404, detail="Delivery not found")

    updates = payload.model_dump(exclude_unset=True)
    if "receipt_number" in updates and updates["receipt_number"] != delivery.receipt_number:
        conflict = db.query(TruckMilkDelivery).filter(
            TruckMilkDelivery.receipt_number == updates["receipt_number"]
        ).first()
        if conflict:
            raise HTTPException(
                status_code=409,
                detail=f"Receipt number '{updates['receipt_number']}' already used by delivery {conflict.id}.",
            )

    for field, value in updates.items():
        setattr(delivery, field, value)

    db.commit()
    db.refresh(delivery)
    return TruckMilkDeliveryRead.model_validate(delivery)


@router.delete("/{delivery_id}", status_code=204)
def delete_delivery(delivery_id: int, db: Session = Depends(get_db)):
    """Delete a truck milk delivery record."""
    delivery = db.get(TruckMilkDelivery, delivery_id)
    if not delivery:
        raise HTTPException(status_code=404, detail="Delivery not found")
    db.delete(delivery)
    db.commit()
