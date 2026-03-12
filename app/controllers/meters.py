from flask import Blueprint, render_template, request
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
    page      = request.args.get('page', 1, type=int)
    sort      = request.args.get('sort', 'total_cost')
    direction = request.args.get('dir', 'desc')
    q         = request.args.get('q', '').strip()

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

    if q:
        like = f'%{q}%'
        query = query.filter(
            DimMeter.meter_name.ilike(like) |
            DimMeter.meter_category.ilike(like) |
            DimMeter.meter_region.ilike(like)
        )

    col = _SORT_MAP.get(sort, _SORT_MAP['total_cost'])
    query = query.order_by(col.desc() if direction == 'desc' else col.asc())

    pagination = query.paginate(page=page, per_page=20, error_out=False)
    return render_template('meters/index.html',
                           pagination=pagination, sort=sort, dir=direction, q=q)


@bp.route('/<int:pk>')
def detail(pk):
    meter = db.get_or_404(DimMeter, pk)
    page = request.args.get('page', 1, type=int)

    lines_stmt = (
        select(FactBillingLine)
        .where(FactBillingLine.meter_fk == pk)
        .order_by(FactBillingLine.charge_date.desc())
    )
    pagination = db.paginate(lines_stmt, page=page, per_page=50, error_out=False)

    total_cost = db.session.execute(
        select(func.sum(FactBillingLine.cost_in_billing_currency))
        .where(FactBillingLine.meter_fk == pk)
    ).scalar() or 0

    usage_count = db.session.execute(
        select(func.count(FactBillingLine.id))
        .where(FactBillingLine.meter_fk == pk)
    ).scalar() or 0

    return render_template(
        'meters/detail.html',
        meter=meter,
        total_cost=total_cost,
        usage_count=usage_count,
        pagination=pagination,
    )
