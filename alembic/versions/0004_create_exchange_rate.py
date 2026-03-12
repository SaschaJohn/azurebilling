"""Create exchange_rate table

Revision ID: 0004
Revises: 0003
Create Date: 2026-01-01 00:03:00.000000

"""
from alembic import op

revision = '0004'
down_revision = '0003'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE exchange_rate (
            id              SERIAL PRIMARY KEY,
            billing_month   DATE NOT NULL,
            from_currency   TEXT NOT NULL,
            to_currency     TEXT NOT NULL,
            rate            NUMERIC(18,6) NOT NULL,
            UNIQUE (billing_month, from_currency, to_currency)
        )
    """)


def downgrade():
    op.execute("DROP TABLE IF EXISTS exchange_rate")
