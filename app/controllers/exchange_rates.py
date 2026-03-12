from datetime import date

from flask import Blueprint, flash, redirect, render_template, request, url_for

from app.extensions import db
from app.models.exchange_rate import ExchangeRate

bp = Blueprint('exchange_rates', __name__, url_prefix='/exchange-rates')


def _first_of_month(year: int, month: int) -> date:
    return date(year, month, 1)


def _form_to_model(form, obj: ExchangeRate) -> str | None:
    """Populate obj from POST form. Returns an error string or None on success."""
    try:
        year  = int(form['year'])
        month = int(form['month'])
        if not (1 <= month <= 12):
            return 'Month must be between 1 and 12.'
        obj.billing_month = _first_of_month(year, month)
    except (KeyError, ValueError):
        return 'Invalid year or month.'

    from_cur = form.get('from_currency', '').strip().upper()
    to_cur   = form.get('to_currency', '').strip().upper()
    if not from_cur or not to_cur:
        return 'Both currency codes are required.'
    obj.from_currency = from_cur
    obj.to_currency   = to_cur

    try:
        obj.rate = float(form['rate'])
        if obj.rate <= 0:
            return 'Rate must be a positive number.'
    except (KeyError, ValueError):
        return 'Invalid rate.'

    return None


@bp.route('/')
def index():
    rates = (
        ExchangeRate.query
        .order_by(ExchangeRate.billing_month.desc(), ExchangeRate.from_currency, ExchangeRate.to_currency)
        .all()
    )
    return render_template('exchange_rates/index.html', rates=rates)


@bp.route('/new', methods=['GET', 'POST'])
def create():
    if request.method == 'POST':
        obj = ExchangeRate()
        err = _form_to_model(request.form, obj)
        if err:
            flash(err, 'danger')
            return render_template('exchange_rates/form.html', action='Create', obj=request.form)

        db.session.add(obj)
        try:
            db.session.commit()
            flash('Exchange rate created.', 'success')
            return redirect(url_for('exchange_rates.index'))
        except Exception:
            db.session.rollback()
            flash('A rate for that month and currency pair already exists.', 'danger')
            return render_template('exchange_rates/form.html', action='Create', obj=request.form)

    return render_template('exchange_rates/form.html', action='Create', obj=None)


@bp.route('/<int:pk>/edit', methods=['GET', 'POST'])
def edit(pk):
    obj = db.get_or_404(ExchangeRate, pk)

    if request.method == 'POST':
        err = _form_to_model(request.form, obj)
        if err:
            flash(err, 'danger')
            return render_template('exchange_rates/form.html', action='Save', obj=request.form, pk=pk)

        try:
            db.session.commit()
            flash('Exchange rate updated.', 'success')
            return redirect(url_for('exchange_rates.index'))
        except Exception:
            db.session.rollback()
            flash('A rate for that month and currency pair already exists.', 'danger')
            return render_template('exchange_rates/form.html', action='Save', obj=request.form, pk=pk)

    return render_template('exchange_rates/form.html', action='Save', obj=obj, pk=pk)


@bp.route('/<int:pk>/delete', methods=['POST'])
def delete(pk):
    obj = db.get_or_404(ExchangeRate, pk)
    db.session.delete(obj)
    db.session.commit()
    flash('Exchange rate deleted.', 'success')
    return redirect(url_for('exchange_rates.index'))
