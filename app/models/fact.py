from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB, UUID
from app.extensions import db


class FactBillingLine(db.Model):
    __tablename__ = 'fact_billing_line'

    id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)

    # Dimension FKs
    billing_account_fk = db.Column(db.Integer, db.ForeignKey('dim_billing_account.id'), nullable=False)
    billing_profile_fk = db.Column(db.Integer, db.ForeignKey('dim_billing_profile.id'), nullable=False)
    invoice_section_fk = db.Column(db.Integer, db.ForeignKey('dim_invoice_section.id'), nullable=False)
    subscription_fk    = db.Column(db.Integer, db.ForeignKey('dim_subscription.id'), nullable=False)
    reseller_fk        = db.Column(db.Integer, db.ForeignKey('dim_reseller.id'))
    publisher_fk       = db.Column(db.Integer, db.ForeignKey('dim_publisher.id'), nullable=False)
    product_fk         = db.Column(db.Integer, db.ForeignKey('dim_product.id'), nullable=False)
    meter_fk           = db.Column(db.Integer, db.ForeignKey('dim_meter.id'))
    service_fk         = db.Column(db.Integer, db.ForeignKey('dim_service.id'), nullable=False)
    resource_group_fk  = db.Column(db.Integer, db.ForeignKey('dim_resource_group.id'))
    invoice_fk         = db.Column(db.Integer, db.ForeignKey('dim_invoice.id'))
    benefit_fk         = db.Column(db.Integer, db.ForeignKey('dim_benefit.id'))
    import_batch_id    = db.Column(UUID(as_uuid=True), db.ForeignKey('import_batch.id'), nullable=False)

    # Date columns
    billing_period_start_date  = db.Column(db.Date)
    billing_period_end_date    = db.Column(db.Date)
    service_period_start_date  = db.Column(db.Date)
    service_period_end_date    = db.Column(db.Date)
    charge_date                = db.Column(db.Date, nullable=False)
    exchange_rate_date         = db.Column(db.Date)

    # Numeric columns
    effective_price                  = db.Column(db.Numeric(28, 10))
    quantity                         = db.Column(db.Numeric(28, 10))
    cost_in_billing_currency         = db.Column(db.Numeric(28, 10))
    cost_in_pricing_currency         = db.Column(db.Numeric(28, 10))
    cost_in_usd                      = db.Column(db.Numeric(28, 10))
    payg_cost_in_billing_currency    = db.Column(db.Numeric(28, 10))
    payg_cost_in_usd                 = db.Column(db.Numeric(28, 10))
    exchange_rate_pricing_to_billing = db.Column(db.Numeric(28, 10))
    pay_g_price                      = db.Column(db.Numeric(28, 10))
    unit_price                       = db.Column(db.Numeric(28, 10))

    # Text columns
    unit_of_measure           = db.Column(db.Text)
    charge_type               = db.Column(db.Text)
    billing_currency          = db.Column(db.Text)
    pricing_currency          = db.Column(db.Text)
    is_azure_credit_eligible  = db.Column(db.Boolean)
    service_info1             = db.Column(db.Text)
    service_info2             = db.Column(db.Text)
    frequency                 = db.Column(db.Text)
    term                      = db.Column(db.Text)
    pricing_model             = db.Column(db.Text)
    cost_allocation_rule_name = db.Column(db.Text)
    provider                  = db.Column(db.Text)
    cost_center               = db.Column(db.Text)
    resource_location         = db.Column(db.Text)
    location                  = db.Column(db.Text)

    # JSONB columns
    additional_info = db.Column(JSONB)
    tags            = db.Column(JSONB)

    # Dedup + audit
    row_hash   = db.Column(db.String(64), unique=True, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    # Relationships for easy access in views
    billing_account  = db.relationship('DimBillingAccount', foreign_keys=[billing_account_fk])
    billing_profile  = db.relationship('DimBillingProfile', foreign_keys=[billing_profile_fk])
    invoice_section  = db.relationship('DimInvoiceSection', foreign_keys=[invoice_section_fk])
    subscription     = db.relationship('DimSubscription', foreign_keys=[subscription_fk])
    publisher        = db.relationship('DimPublisher', foreign_keys=[publisher_fk])
    product          = db.relationship('DimProduct', foreign_keys=[product_fk])
    meter            = db.relationship('DimMeter', foreign_keys=[meter_fk])
    service          = db.relationship('DimService', foreign_keys=[service_fk])
    resource_group   = db.relationship('DimResourceGroup', foreign_keys=[resource_group_fk])
    invoice          = db.relationship('DimInvoice', foreign_keys=[invoice_fk])
    import_batch     = db.relationship('ImportBatch', foreign_keys=[import_batch_id])
