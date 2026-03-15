from flask import Blueprint, g, render_template, request
from sqlalchemy import func, select

from app.extensions import db
from app.models.dimensions import DimMeter
from app.models.fact import FactBillingLine

bp = Blueprint('meters', __name__, url_prefix='/meters')

_SORT_MAP = {
    'name':        DimMeter.meter_name,
    'category':    DimMeter.meter_category,
    'region':      DimMeter.meter_region,
    'usage_count': func.count(FactBillingLine.id),
    'total_cost':  func.sum(FactBillingLine.cost_in_billing_currency),
}


@bp.route('/')
def index():
    page       = request.args.get('page', 1, type=int)
    sort       = request.args.get('sort', 'total_cost')
    direction  = request.args.get('dir', 'desc')
    name       = request.args.get('name', '').strip()
    category   = request.args.get('category', '').strip()
    region     = request.args.get('region', '').strip()
    min_usages = request.args.get('min_usages', type=int)
    max_usages = request.args.get('max_usages', type=int)
    min_cost   = request.args.get('min_cost', type=float)
    max_cost   = request.args.get('max_cost', type=float)

    query = (
        db.session.query(
            DimMeter.id,
            DimMeter.meter_id,
            DimMeter.meter_name,
            DimMeter.meter_category,
            DimMeter.meter_region,
            func.sum(FactBillingLine.cost_in_billing_currency).label('total_cost'),
            func.count(FactBillingLine.id).label('usage_count'),
        )
        .join(FactBillingLine, FactBillingLine.meter_fk == DimMeter.id)
        .group_by(DimMeter.id)
    )

    if g.active_month:
        query = query.filter(
            FactBillingLine.charge_date >= g.active_month,
            FactBillingLine.charge_date < g.next_month,
        )
    if g.active_subscription_ids:
        query = query.filter(FactBillingLine.subscription_fk.in_(g.active_subscription_ids))

    if name:       query = query.filter(DimMeter.meter_name.ilike(f'%{name}%'))
    if category:   query = query.filter(DimMeter.meter_category.ilike(f'%{category}%'))
    if region:     query = query.filter(DimMeter.meter_region.ilike(f'%{region}%'))
    if min_usages: query = query.having(func.count(FactBillingLine.id) >= min_usages)
    if max_usages: query = query.having(func.count(FactBillingLine.id) <= max_usages)
    if min_cost:   query = query.having(func.sum(FactBillingLine.cost_in_billing_currency) >= min_cost)
    if max_cost:   query = query.having(func.sum(FactBillingLine.cost_in_billing_currency) <= max_cost)

    col = _SORT_MAP.get(sort, _SORT_MAP['total_cost'])
    query = query.order_by(col.desc() if direction == 'desc' else col.asc())

    pagination = query.paginate(page=page, per_page=20, error_out=False)
    filters = dict(name=name, category=category, region=region,
                   min_usages=min_usages, max_usages=max_usages,
                   min_cost=min_cost, max_cost=max_cost)
    return render_template('meters/index.html',
                           pagination=pagination, sort=sort, dir=direction, filters=filters)


@bp.route('/<int:pk>')
def detail(pk):
    meter = db.get_or_404(DimMeter, pk)
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
        .where(FactBillingLine.meter_fk == pk, *month_filters)
        .order_by(FactBillingLine.charge_date.desc())
    )
    pagination = db.paginate(lines_stmt, page=page, per_page=50, error_out=False)

    total_cost = db.session.execute(
        select(func.sum(FactBillingLine.cost_in_billing_currency))
        .where(FactBillingLine.meter_fk == pk, *month_filters)
    ).scalar() or 0

    usage_count = db.session.execute(
        select(func.count(FactBillingLine.id))
        .where(FactBillingLine.meter_fk == pk, *month_filters)
    ).scalar() or 0

    return render_template(
        'meters/detail.html',
        meter=meter,
        total_cost=total_cost,
        usage_count=usage_count,
        pagination=pagination,
    )
