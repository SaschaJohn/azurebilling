from flask import Flask
from .config import Config
from .extensions import db, migrate


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

    def _fmt_cost(value, decimals=2):
        if value is None:
            return f'kr. 0.{"0" * decimals}'
        return f'kr. {float(value):,.{decimals}f}'

    app.jinja_env.filters['fmt_cost'] = _fmt_cost

    from urllib.parse import urlencode as _urlencode

    @app.template_global()
    def url_with_params(**overrides):
        from flask import request
        args = request.args.to_dict()
        args.update(overrides)
        return '?' + _urlencode(args)

    return app
