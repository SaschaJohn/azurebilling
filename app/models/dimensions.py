from app.extensions import db


class DimBillingAccount(db.Model):
    __tablename__ = 'dim_billing_account'

    id = db.Column(db.Integer, primary_key=True)
    billing_account_id = db.Column(db.Text, unique=True, nullable=False)
    billing_account_name = db.Column(db.Text)


class DimBillingProfile(db.Model):
    __tablename__ = 'dim_billing_profile'

    id = db.Column(db.Integer, primary_key=True)
    billing_profile_id = db.Column(db.Text, unique=True, nullable=False)
    billing_profile_name = db.Column(db.Text)
    billing_account_fk = db.Column(db.Integer, db.ForeignKey('dim_billing_account.id'))


class DimInvoiceSection(db.Model):
    __tablename__ = 'dim_invoice_section'

    id = db.Column(db.Integer, primary_key=True)
    invoice_section_id = db.Column(db.Text, unique=True, nullable=False)
    invoice_section_name = db.Column(db.Text)
    billing_profile_fk = db.Column(db.Integer, db.ForeignKey('dim_billing_profile.id'))


class DimSubscription(db.Model):
    __tablename__ = 'dim_subscription'

    id = db.Column(db.Integer, primary_key=True)
    subscription_id = db.Column(db.Text, unique=True, nullable=False)
    subscription_name = db.Column(db.Text)


class DimReseller(db.Model):
    __tablename__ = 'dim_reseller'
    __table_args__ = (db.UniqueConstraint('name', 'mpn_id'),)

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text, nullable=False)
    mpn_id = db.Column(db.Text, nullable=False, default='')


class DimPublisher(db.Model):
    __tablename__ = 'dim_publisher'
    __table_args__ = (db.UniqueConstraint('publisher_type', 'publisher_id'),)

    id = db.Column(db.Integer, primary_key=True)
    publisher_type = db.Column(db.Text, nullable=False, default='')
    publisher_id = db.Column(db.Text, nullable=False, default='')
    publisher_name = db.Column(db.Text)


class DimProduct(db.Model):
    __tablename__ = 'dim_product'
    __table_args__ = (db.UniqueConstraint('product_id', 'product_order_id'),)

    id = db.Column(db.Integer, primary_key=True)
    product_id = db.Column(db.Text, nullable=False, default='')
    product_order_id = db.Column(db.Text, nullable=False, default='')
    product_name = db.Column(db.Text)
    product_order_name = db.Column(db.Text)


class DimMeter(db.Model):
    __tablename__ = 'dim_meter'

    id = db.Column(db.Integer, primary_key=True)
    meter_id = db.Column(db.Text, unique=True, nullable=False)
    meter_name = db.Column(db.Text)
    meter_category = db.Column(db.Text)
    meter_sub_category = db.Column(db.Text)
    meter_region = db.Column(db.Text)


class DimService(db.Model):
    __tablename__ = 'dim_service'
    __table_args__ = (db.UniqueConstraint('service_family', 'consumed_service'),)

    id = db.Column(db.Integer, primary_key=True)
    service_family = db.Column(db.Text, nullable=False, default='')
    consumed_service = db.Column(db.Text, nullable=False, default='')


class DimResourceGroup(db.Model):
    __tablename__ = 'dim_resource_group'

    id = db.Column(db.Integer, primary_key=True)
    resource_id = db.Column(db.Text, unique=True, nullable=False)
    resource_group_name = db.Column(db.Text)
    subscription_fk = db.Column(db.Integer, db.ForeignKey('dim_subscription.id'))


class DimInvoice(db.Model):
    __tablename__ = 'dim_invoice'

    id = db.Column(db.Integer, primary_key=True)
    invoice_id = db.Column(db.Text, unique=True, nullable=False)
    previous_invoice_id = db.Column(db.Text)


class DimBenefit(db.Model):
    __tablename__ = 'dim_benefit'
    __table_args__ = (db.UniqueConstraint('benefit_id', 'reservation_id'),)

    id = db.Column(db.Integer, primary_key=True)
    benefit_id = db.Column(db.Text, nullable=False, default='')
    reservation_id = db.Column(db.Text, nullable=False, default='')
    benefit_name = db.Column(db.Text)
    reservation_name = db.Column(db.Text)
