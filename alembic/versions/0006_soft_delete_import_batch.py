"""Add deleted_at to import_batch for soft-delete

Revision ID: 0006
Revises: 0005
Create Date: 2026-03-15 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = '0006'
down_revision = '0005'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('import_batch', sa.Column('deleted_at', sa.DateTime(), nullable=True))


def downgrade():
    op.drop_column('import_batch', 'deleted_at')
