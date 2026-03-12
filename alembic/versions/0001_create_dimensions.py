"""Create dimension tables

Revision ID: 0001
Revises:
Create Date: 2026-01-01 00:00:00.000000

"""
from alembic import op

revision = '0001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE dim_billing_account (
            id SERIAL PRIMARY KEY,
            billing_account_id TEXT UNIQUE NOT NULL,
            billing_account_name TEXT
        )
    """)

    op.execute("""
        CREATE TABLE dim_billing_profile (
            id SERIAL PRIMARY KEY,
            billing_profile_id TEXT UNIQUE NOT NULL,
            billing_profile_name TEXT,
            billing_account_fk INTEGER REFERENCES dim_billing_account(id)
        )
    """)

    op.execute("""
        CREATE TABLE dim_invoice_section (
            id SERIAL PRIMARY KEY,
            invoice_section_id TEXT UNIQUE NOT NULL,
            invoice_section_name TEXT,
            billing_profile_fk INTEGER REFERENCES dim_billing_profile(id)
        )
    """)

    op.execute("""
        CREATE TABLE dim_subscription (
            id SERIAL PRIMARY KEY,
            subscription_id TEXT UNIQUE NOT NULL,
            subscription_name TEXT
        )
    """)

    op.execute("""
        CREATE TABLE dim_reseller (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            mpn_id TEXT NOT NULL DEFAULT '',
            UNIQUE(name, mpn_id)
        )
    """)

    op.execute("""
        CREATE TABLE dim_publisher (
            id SERIAL PRIMARY KEY,
            publisher_type TEXT NOT NULL DEFAULT '',
            publisher_id TEXT NOT NULL DEFAULT '',
            publisher_name TEXT,
            UNIQUE(publisher_type, publisher_id)
        )
    """)

    op.execute("""
        CREATE TABLE dim_product (
            id SERIAL PRIMARY KEY,
            product_id TEXT NOT NULL DEFAULT '',
            product_order_id TEXT NOT NULL DEFAULT '',
            product_name TEXT,
            product_order_name TEXT,
            UNIQUE(product_id, product_order_id)
        )
    """)

    op.execute("""
        CREATE TABLE dim_meter (
            id SERIAL PRIMARY KEY,
            meter_id TEXT UNIQUE NOT NULL,
            meter_name TEXT,
            meter_category TEXT,
            meter_sub_category TEXT,
            meter_region TEXT
        )
    """)

    op.execute("""
        CREATE TABLE dim_service (
            id SERIAL PRIMARY KEY,
            service_family TEXT NOT NULL DEFAULT '',
            consumed_service TEXT NOT NULL DEFAULT '',
            UNIQUE(service_family, consumed_service)
        )
    """)

    op.execute("""
        CREATE TABLE dim_resource_group (
            id SERIAL PRIMARY KEY,
            resource_id TEXT UNIQUE NOT NULL,
            resource_group_name TEXT,
            subscription_fk INTEGER REFERENCES dim_subscription(id)
        )
    """)

    op.execute("""
        CREATE TABLE dim_invoice (
            id SERIAL PRIMARY KEY,
            invoice_id TEXT UNIQUE NOT NULL,
            previous_invoice_id TEXT
        )
    """)

    op.execute("""
        CREATE TABLE dim_benefit (
            id SERIAL PRIMARY KEY,
            benefit_id TEXT NOT NULL DEFAULT '',
            reservation_id TEXT NOT NULL DEFAULT '',
            benefit_name TEXT,
            reservation_name TEXT,
            UNIQUE(benefit_id, reservation_id)
        )
    """)


def downgrade():
    op.execute("DROP TABLE IF EXISTS dim_benefit")
    op.execute("DROP TABLE IF EXISTS dim_invoice")
    op.execute("DROP TABLE IF EXISTS dim_resource_group")
    op.execute("DROP TABLE IF EXISTS dim_service")
    op.execute("DROP TABLE IF EXISTS dim_meter")
    op.execute("DROP TABLE IF EXISTS dim_product")
    op.execute("DROP TABLE IF EXISTS dim_publisher")
    op.execute("DROP TABLE IF EXISTS dim_reseller")
    op.execute("DROP TABLE IF EXISTS dim_subscription")
    op.execute("DROP TABLE IF EXISTS dim_invoice_section")
    op.execute("DROP TABLE IF EXISTS dim_billing_profile")
    op.execute("DROP TABLE IF EXISTS dim_billing_account")
