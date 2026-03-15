from flask import Blueprint, g, render_template, request
from sqlalchemy import func, select

from app.extensions import db
from app.models.dimensions import DimInvoice
from app.models.fact import FactBillingLine

bp = Blueprint('invoices', __name__, url_prefix='/invoices')

_SORT_MAP = {
    'invoice_id':          DimInvoice.invoice_id,
    'previous_invoice_id': DimInvoice.previous_invoice_id,
    'line_count':          func.count(FactBillingLine.id),
    'total_cost':          func.sum(FactBillingLine.cost_in_billing_currency),
}


@bp.route('/')
def index():
    page             = request.args.get('page', 1, type=int)
    sort             = request.args.get('sort', 'total_cost')
    direction        = request.args.get('dir', 'desc')
    invoice_id       = request.args.get('invoice_id', '').strip()
    prev_invoice_id  = request.args.get('prev_invoice_id', '').strip()
    min_lines        = request.args.get('min_lines', type=int)
    max_lines        = request.args.get('max_lines', type=int)
    min_cost         = request.args.get('min_cost', type=float)
    max_cost         = request.args.get('max_cost', type=float)

    query = (
        db.session.query(
            DimInvoice.id,
            DimInvoice.invoice_id,
            DimInvoice.previous_invoice_id,
            func.sum(FactBillingLine.cost_in_billing_currency).label('total_cost'),
            func.count(FactBillingLine.id).label('line_count'),
        )
        .join(FactBillingLine, FactBillingLine.invoice_fk == DimInvoice.id)
        .group_by(DimInvoice.id)
    )

    if g.active_month:
        query = query.filter(
            FactBillingLine.charge_date >= g.active_month,
            FactBillingLine.charge_date < g.next_month,
        )
    if g.active_subscription_ids:
        query = query.filter(FactBillingLine.subscription_fk.in_(g.active_subscription_ids))

    if invoice_id:      query = query.filter(DimInvoice.invoice_id.ilike(f'%{invoice_id}%'))
    if prev_invoice_id: query = query.filter(DimInvoice.previous_invoice_id.ilike(f'%{prev_invoice_id}%'))
    if min_lines:       query = query.having(func.count(FactBillingLine.id) >= min_lines)
    if max_lines:       query = query.having(func.count(FactBillingLine.id) <= max_lines)
    if min_cost:        query = query.having(func.sum(FactBillingLine.cost_in_billing_currency) >= min_cost)
    if max_cost:        query = query.having(func.sum(FactBillingLine.cost_in_billing_currency) <= max_cost)

    col = _SORT_MAP.get(sort, _SORT_MAP['total_cost'])
    query = query.order_by(col.desc() if direction == 'desc' else col.asc())

    pagination = query.paginate(page=page, per_page=20, error_out=False)
    filters = dict(invoice_id=invoice_id, prev_invoice_id=prev_invoice_id,
                   min_lines=min_lines, max_lines=max_lines,
                   min_cost=min_cost, max_cost=max_cost)
    return render_template('invoices/index.html',
                           pagination=pagination, sort=sort, dir=direction, filters=filters)


@bp.route('/<int:pk>')
def detail(pk):
    invoice = db.get_or_404(DimInvoice, pk)
    page = request.args.get('page', 1, type=int)

    month_filters = []
    if g.active_month:
        month_filters = [
            FactBillingLine.charge_date >= g.active_month,
            FactBillingLine.charge_date < g.next_month,
        ]
    if g.active_subscription_ids:
        month_filters.append(FactBillingLine.subscription_fk.in_(g.active_subscription_ids))

    lines_stmt = (
        select(FactBillingLine)
        .where(FactBillingLine.invoice_fk == pk, *month_filters)
        .order_by(FactBillingLine.charge_date.desc())
    )
    pagination = db.paginate(lines_stmt, page=page, per_page=50, error_out=False)

    total_cost = db.session.execute(
        select(func.sum(FactBillingLine.cost_in_billing_currency))
        .where(FactBillingLine.invoice_fk == pk, *month_filters)
    ).scalar() or 0

    return render_template(
        'invoices/detail.html',
        invoice=invoice,
        total_cost=total_cost,
        pagination=pagination,
    )
