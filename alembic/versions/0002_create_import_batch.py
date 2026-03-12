"""Create import_batch audit table

Revision ID: 0002
Revises: 0001
Create Date: 2026-01-01 00:01:00.000000

"""
from alembic import op

revision = '0002'
down_revision = '0001'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE import_batch (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            filename TEXT NOT NULL,
            started_at TIMESTAMP NOT NULL DEFAULT NOW(),
            finished_at TIMESTAMP,
            row_count INTEGER NOT NULL DEFAULT 0,
            skipped_count INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending',
            error_msg TEXT
        )
    """)


def downgrade():
    op.execute("DROP TABLE IF EXISTS import_batch")
