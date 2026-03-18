"""Add site table and datasource.site_id FK

Revision ID: a1b2c3d4e5f6
Revises: 8f2c4d5e6a7b
Create Date: 2026-03-18 14:00:00.000000+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = '8f2c4d5e6a7b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'site',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('location', postgresql.JSONB(), nullable=False,
                  comment='GeoJSON coordinates'),
        sa.Column('site_type', sa.String(32), nullable=False),
        sa.Column('owner', postgresql.JSONB(), nullable=False,
                  comment='Owner details (person or organisation)'),
        sa.Column('administrator_email', sa.String(255), nullable=False,
                  comment='Email of the site administrator'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index('idx_site_name', 'site', ['name'])

    # Add site_id FK to datasource
    op.add_column('datasource', sa.Column('site_id', sa.Integer(), nullable=True,
                                          comment='Site this datasource belongs to'))
    op.create_foreign_key('fk_datasource_site_id', 'datasource', 'site',
                          ['site_id'], ['id'])
    op.create_index('idx_datasource_site_id', 'datasource', ['site_id'])


def downgrade() -> None:
    op.drop_index('idx_datasource_site_id', table_name='datasource')
    op.drop_constraint('fk_datasource_site_id', 'datasource', type_='foreignkey')
    op.drop_column('datasource', 'site_id')
    op.drop_index('idx_site_name', table_name='site')
    op.drop_table('site')
