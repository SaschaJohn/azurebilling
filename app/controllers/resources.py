from flask import Blueprint, g, render_template, request
from sqlalchemy import func, select

from app.extensions import db
from app.models.dimensions import DimResourceGroup
from app.models.fact import FactBillingLine

bp = Blueprint('resources', __name__, url_prefix='/resources')

_SORT_MAP = {
    'name':       DimResourceGroup.resource_group_name,
    'resource_id': DimResourceGroup.resource_id,
    'line_count': func.count(FactBillingLine.id),
    'total_cost': func.sum(FactBillingLine.cost_in_billing_currency),
}


@bp.route('/')
def index():
    page        = request.args.get('page', 1, type=int)
    sort        = request.args.get('sort', 'total_cost')
    direction   = request.args.get('dir', 'desc')
    name        = request.args.get('name', '').strip()
    resource_id = request.args.get('resource_id', '').strip()
    min_lines   = request.args.get('min_lines', type=int)
    max_lines   = request.args.get('max_lines', type=int)
    min_cost    = request.args.get('min_cost', type=float)
    max_cost    = request.args.get('max_cost', type=float)

    query = (
        db.session.query(
            DimResourceGroup.id,
            DimResourceGroup.resource_id,
            DimResourceGroup.resource_group_name,
            func.sum(FactBillingLine.cost_in_billing_currency).label('total_cost'),
            func.count(FactBillingLine.id).label('line_count'),
        )
        .join(FactBillingLine, FactBillingLine.resource_group_fk == DimResourceGroup.id)
        .group_by(DimResourceGroup.id)
    )

    if g.active_month:
        query = query.filter(
            FactBillingLine.charge_date >= g.active_month,
            FactBillingLine.charge_date < g.next_month,
        )

    if name:        query = query.filter(DimResourceGroup.resource_group_name.ilike(f'%{name}%'))
    if resource_id: query = query.filter(DimResourceGroup.resource_id.ilike(f'%{resource_id}%'))
    if min_lines:   query = query.having(func.count(FactBillingLine.id) >= min_lines)
    if max_lines:   query = query.having(func.count(FactBillingLine.id) <= max_lines)
    if min_cost:    query = query.having(func.sum(FactBillingLine.cost_in_billing_currency) >= min_cost)
    if max_cost:    query = query.having(func.sum(FactBillingLine.cost_in_billing_currency) <= max_cost)

    col = _SORT_MAP.get(sort, _SORT_MAP['total_cost'])
    query = query.order_by(col.desc() if direction == 'desc' else col.asc())

    pagination = query.paginate(page=page, per_page=20, error_out=False)
    filters = dict(name=name, resource_id=resource_id,
                   min_lines=min_lines, max_lines=max_lines,
                   min_cost=min_cost, max_cost=max_cost)
    return render_template('resources/index.html',
                           pagination=pagination, sort=sort, dir=direction, filters=filters)


@bp.route('/<int:pk>')
def detail(pk):
    rg = db.get_or_404(DimResourceGroup, pk)
    page = request.args.get('page', 1, type=int)

    month_filters = []
    if g.active_month:
        month_filters = [
            FactBillingLine.charge_date >= g.active_month,
            FactBillingLine.charge_date < g.next_month,
        ]

    lines_stmt = (
        select(FactBillingLine)
        .where(FactBillingLine.resource_group_fk == pk, *month_filters)
        .order_by(FactBillingLine.charge_date.desc())
    )
    pagination = db.paginate(lines_stmt, page=page, per_page=50, error_out=False)

    total_cost = db.session.execute(
        select(func.sum(FactBillingLine.cost_in_billing_currency))
        .where(FactBillingLine.resource_group_fk == pk, *month_filters)
    ).scalar() or 0

    return render_template(
        'resources/detail.html',
        rg=rg,
        total_cost=total_cost,
        pagination=pagination,
    )
