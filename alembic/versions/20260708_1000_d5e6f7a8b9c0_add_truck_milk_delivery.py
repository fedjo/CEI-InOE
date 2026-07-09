"""Add truck_milk_delivery table

Revision ID: d5e6f7a8b9c0
Revises: c4d5e6f7a8b9
Create Date: 2026-07-08 10:00:00.000000+00:00
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'd5e6f7a8b9c0'
down_revision: Union[str, None] = 'c1d2e3f4a5b6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'truck_milk_delivery',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('reception_date', sa.Date(), nullable=False),
        sa.Column('truck_id', sa.String(64), nullable=False),
        sa.Column('receipt_number', sa.String(64), nullable=False),
        sa.Column('farm_of_origin', sa.String(255), nullable=False),
        sa.Column('cow_milk_delivered_kg', sa.Numeric(10, 2), nullable=False),
        sa.Column('total_cow_milk_in_truck_kg', sa.Numeric(10, 2), nullable=False),
        sa.Column('total_milk_in_truck_kg', sa.Numeric(10, 2), nullable=False),
        sa.Column('silo_number', sa.Integer(), nullable=False),
        sa.Column('production_batch_numbers', sa.Text(), nullable=True),
        sa.Column('batch_produced_date', sa.Date(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    )

    op.create_index('idx_truck_delivery_date', 'truck_milk_delivery', ['reception_date'])
    op.create_index('idx_truck_delivery_truck', 'truck_milk_delivery', ['truck_id'])
    op.create_index('idx_truck_delivery_receipt', 'truck_milk_delivery', ['receipt_number'])
    op.create_unique_constraint('uq_truck_delivery_receipt', 'truck_milk_delivery', ['receipt_number'])


def downgrade() -> None:
    op.drop_table('truck_milk_delivery')
