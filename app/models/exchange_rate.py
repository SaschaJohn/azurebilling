from app.extensions import db


class ExchangeRate(db.Model):
    __tablename__ = 'exchange_rate'
    __table_args__ = (
        db.UniqueConstraint('billing_month', 'from_currency', 'to_currency'),
    )

    id            = db.Column(db.Integer, primary_key=True)
    billing_month = db.Column(db.Date, nullable=False)   # stored as first day of month
    from_currency = db.Column(db.Text, nullable=False)
    to_currency   = db.Column(db.Text, nullable=False)
    rate          = db.Column(db.Numeric(18, 6), nullable=False)
