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

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(imports_bp)
    app.register_blueprint(subscriptions_bp)
    app.register_blueprint(invoices_bp)
    app.register_blueprint(resources_bp)
    app.register_blueprint(meters_bp)
    app.register_blueprint(storage_bp)
    app.register_blueprint(exchange_rates_bp)

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
