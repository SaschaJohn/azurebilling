from datetime import date
from flask import Flask, g, redirect, request, session
from .config import Config
from .extensions import db, migrate

_CURRENCY_SYMBOLS = {
    'DKK': 'kr.',
    'USD': '$',
    'EUR': '€',
    'GBP': '£',
    'SEK': 'kr.',
    'NOK': 'kr.',
}


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    db.init_app(app)
    migrate.init_app(app, db, directory='alembic')

    # Import models so SQLAlchemy metadata is populated before migrations
    from . import models  # noqa: F401

    from .controllers.dashboard import bp as dashboard_bp
    from .controllers.imports import bp as imports_bp
    from .controllers.subscriptions import bp as subscriptions_bp
    from .controllers.invoices import bp as invoices_bp
    from .controllers.resources import bp as resources_bp
    from .controllers.meters import bp as meters_bp
    from .controllers.storage import bp as storage_bp
    from .controllers.exchange_rates import bp as exchange_rates_bp
    from .controllers.chat import bp as chat_bp

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(imports_bp)
    app.register_blueprint(subscriptions_bp)
    app.register_blueprint(invoices_bp)
    app.register_blueprint(resources_bp)
    app.register_blueprint(meters_bp)
    app.register_blueprint(storage_bp)
    app.register_blueprint(exchange_rates_bp)
    app.register_blueprint(chat_bp)

    from .cli import import_file_cmd
    app.cli.add_command(import_file_cmd)

    # --- Currency conversion ---

    @app.before_request
    def _set_display_currency():
        from .models.exchange_rate import ExchangeRate
        from sqlalchemy import distinct

        g.display_currency = session.get('display_currency', 'DKK')

        if g.display_currency == 'DKK':
            g.display_rate = 1.0
        else:
            row = (
                ExchangeRate.query
                .filter_by(from_currency='DKK', to_currency=g.display_currency)
                .order_by(ExchangeRate.billing_month.desc())
                .first()
            )
            g.display_rate = float(row.rate) if row else 1.0

        # All to_currencies with a DKK→X rate defined
        to_currencies = [
            r[0] for r in
            db.session.query(distinct(ExchangeRate.to_currency))
            .filter_by(from_currency='DKK')
            .order_by(ExchangeRate.to_currency)
            .all()
        ]
        g.available_currencies = ['DKK'] + to_currencies

    @app.route('/set-currency')
    def set_currency():
        to = request.args.get('to', 'DKK')
        session['display_currency'] = to
        return redirect(request.args.get('next') or request.referrer or '/')

    # --- Month filter ---

    @app.before_request
    def _set_active_month():
        from .models.fact import FactBillingLine
        from sqlalchemy import func

        month_rows = (
            db.session.query(
                func.date_trunc('month', FactBillingLine.charge_date).label('m')
            )
            .distinct()
            .order_by(func.date_trunc('month', FactBillingLine.charge_date).desc())
            .all()
        )
        month_dates = [r.m.date() for r in month_rows if r.m]
        g.available_months = month_dates  # list of date objects, newest first

        stored = session.get('billing_month', '__unset__')
        if stored == '__unset__' and month_dates:
            g.active_month = month_dates[0]
        elif stored == 'all' or not month_dates:
            g.active_month = None
        else:
            try:
                y, m = map(int, stored.split('-'))
                g.active_month = date(y, m, 1)
            except (ValueError, AttributeError):
                g.active_month = month_dates[0] if month_dates else None

        if g.active_month:
            d = g.active_month
            g.next_month = date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)
        else:
            g.next_month = None

    @app.route('/set-month')
    def set_month():
        m = request.args.get('m', 'all')
        session['billing_month'] = m
        return redirect(request.args.get('next') or request.referrer or '/')

    def _fmt_cost(value, decimals=2):
        currency = getattr(g, 'display_currency', 'DKK')
        rate     = getattr(g, 'display_rate', 1.0)
        symbol   = _CURRENCY_SYMBOLS.get(currency, currency)
        converted = float(value or 0) * rate
        return f'{symbol} {converted:,.{decimals}f}'

    app.jinja_env.filters['fmt_cost'] = _fmt_cost

    from urllib.parse import urlencode as _urlencode

    @app.template_global()
    def url_with_params(**overrides):
        args = request.args.to_dict()
        args.update(overrides)
        return '?' + _urlencode(args)

    return app
