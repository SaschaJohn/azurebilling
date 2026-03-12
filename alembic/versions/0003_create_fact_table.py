"""Create fact_billing_line table

Revision ID: 0003
Revises: 0002
Create Date: 2026-01-01 00:02:00.000000

"""
from alembic import op

revision = '0003'
down_revision = '0002'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE fact_billing_line (
            id BIGSERIAL PRIMARY KEY,

            -- Dimension FKs
            billing_account_fk  INTEGER NOT NULL REFERENCES dim_billing_account(id),
            billing_profile_fk  INTEGER NOT NULL REFERENCES dim_billing_profile(id),
            invoice_section_fk  INTEGER NOT NULL REFERENCES dim_invoice_section(id),
            subscription_fk     INTEGER NOT NULL REFERENCES dim_subscription(id),
            reseller_fk         INTEGER REFERENCES dim_reseller(id),
            publisher_fk        INTEGER NOT NULL REFERENCES dim_publisher(id),
            product_fk          INTEGER NOT NULL REFERENCES dim_product(id),
            meter_fk            INTEGER REFERENCES dim_meter(id),
            service_fk          INTEGER NOT NULL REFERENCES dim_service(id),
            resource_group_fk   INTEGER REFERENCES dim_resource_group(id),
            invoice_fk          INTEGER REFERENCES dim_invoice(id),
            benefit_fk          INTEGER REFERENCES dim_benefit(id),
            import_batch_id     UUID NOT NULL REFERENCES import_batch(id),

            -- Date columns
            billing_period_start_date   DATE,
            billing_period_end_date     DATE,
            service_period_start_date   DATE,
            service_period_end_date     DATE,
            charge_date                 DATE NOT NULL,
            exchange_rate_date          DATE,

            -- Numeric columns
            effective_price                     NUMERIC(28,10),
            quantity                            NUMERIC(28,10),
            cost_in_billing_currency            NUMERIC(28,10),
            cost_in_pricing_currency            NUMERIC(28,10),
            cost_in_usd                         NUMERIC(28,10),
            payg_cost_in_billing_currency       NUMERIC(28,10),
            payg_cost_in_usd                    NUMERIC(28,10),
            exchange_rate_pricing_to_billing    NUMERIC(28,10),
            pay_g_price                         NUMERIC(28,10),
            unit_price                          NUMERIC(28,10),

            -- Text columns
            unit_of_measure             TEXT,
            charge_type                 TEXT,
            billing_currency            TEXT,
            pricing_currency            TEXT,
            is_azure_credit_eligible    BOOLEAN,
            service_info1               TEXT,
            service_info2               TEXT,
            frequency                   TEXT,
            term                        TEXT,
            pricing_model               TEXT,
            cost_allocation_rule_name   TEXT,
            provider                    TEXT,
            cost_center                 TEXT,
            resource_location           TEXT,
            location                    TEXT,

            -- JSONB columns
            additional_info JSONB,
            tags            JSONB,

            -- Dedup + audit
            row_hash    CHAR(64) UNIQUE NOT NULL,
            created_at  TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("CREATE INDEX idx_fact_charge_date ON fact_billing_line(charge_date)")
    op.execute("CREATE INDEX idx_fact_billing_period ON fact_billing_line(billing_period_start_date, billing_period_end_date)")
    op.execute("CREATE INDEX idx_fact_subscription_fk ON fact_billing_line(subscription_fk)")
    op.execute("CREATE INDEX idx_fact_import_batch_id ON fact_billing_line(import_batch_id)")
    op.execute("CREATE INDEX idx_fact_tags_gin ON fact_billing_line USING GIN(tags)")
    op.execute("CREATE INDEX idx_fact_additional_info_gin ON fact_billing_line USING GIN(additional_info)")


def downgrade():
    op.execute("DROP TABLE IF EXISTS fact_billing_line")
