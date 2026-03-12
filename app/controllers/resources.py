from flask import Blueprint, render_template, request
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
    page      = request.args.get('page', 1, type=int)
    sort      = request.args.get('sort', 'total_cost')
    direction = request.args.get('dir', 'desc')
    q         = request.args.get('q', '').strip()

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

    if q:
        like = f'%{q}%'
        query = query.filter(
            DimResourceGroup.resource_group_name.ilike(like) |
            DimResourceGroup.resource_id.ilike(like)
        )

    col = _SORT_MAP.get(sort, _SORT_MAP['total_cost'])
    query = query.order_by(col.desc() if direction == 'desc' else col.asc())

    pagination = query.paginate(page=page, per_page=20, error_out=False)
    return render_template('resources/index.html',
                           pagination=pagination, sort=sort, dir=direction, q=q)


@bp.route('/<int:pk>')
def detail(pk):
    rg = db.get_or_404(DimResourceGroup, pk)
    page = request.args.get('page', 1, type=int)

    lines_stmt = (
        select(FactBillingLine)
        .where(FactBillingLine.resource_group_fk == pk)
        .order_by(FactBillingLine.charge_date.desc())
    )
    pagination = db.paginate(lines_stmt, page=page, per_page=50, error_out=False)

    total_cost = db.session.execute(
        select(func.sum(FactBillingLine.cost_in_billing_currency))
        .where(FactBillingLine.resource_group_fk == pk)
    ).scalar() or 0

    return render_template(
        'resources/detail.html',
        rg=rg,
        total_cost=total_cost,
        pagination=pagination,
    )
