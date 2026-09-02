"""Add api_principal and api_principal_datasource_access tables

Revision ID: e6f7a8b9c0d1
Revises: d5e6f7a8b9c0
Create Date: 2026-09-02 10:00:00.000000+00:00
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'e6f7a8b9c0d1'
down_revision: Union[str, None] = 'd5e6f7a8b9c0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'api_principal',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('api_key_hash', sa.String(64), nullable=False),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('status', sa.String(32), nullable=False, server_default='active'),
        sa.Column('is_superuser', sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    )
    op.create_unique_constraint('uq_api_principal_api_key_hash', 'api_principal', ['api_key_hash'])
    op.create_index('idx_api_principal_status', 'api_principal', ['status'])

    op.create_table(
        'api_principal_datasource_access',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('principal_id', sa.Integer(), sa.ForeignKey('api_principal.id', ondelete='CASCADE'), nullable=False),
        sa.Column('datasource_id', sa.Integer(), sa.ForeignKey('datasource.id', ondelete='CASCADE'), nullable=False),
        sa.Column('granted_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('granted_by', sa.Integer(), sa.ForeignKey('api_principal.id'), nullable=True),
    )
    op.create_unique_constraint(
        'uq_principal_datasource_access', 'api_principal_datasource_access',
        ['principal_id', 'datasource_id'],
    )
    op.create_index(
        'idx_principal_datasource_access_principal', 'api_principal_datasource_access', ['principal_id'],
    )
    op.create_index(
        'idx_principal_datasource_access_datasource', 'api_principal_datasource_access', ['datasource_id'],
    )


def downgrade() -> None:
    op.drop_table('api_principal_datasource_access')
    op.drop_table('api_principal')
