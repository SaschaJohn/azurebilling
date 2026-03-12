from flask import Blueprint, render_template, request
from sqlalchemy import func, select

from app.extensions import db
from app.models.dimensions import DimSubscription, DimService
from app.models.fact import FactBillingLine

bp = Blueprint('subscriptions', __name__, url_prefix='/subscriptions')

_SORT_MAP = {
    'name':       DimSubscription.subscription_name,
    'id':         DimSubscription.subscription_id,
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
            DimSubscription.id,
            DimSubscription.subscription_id,
            DimSubscription.subscription_name,
            func.sum(FactBillingLine.cost_in_billing_currency).label('total_cost'),
            func.count(FactBillingLine.id).label('line_count'),
        )
        .join(FactBillingLine, FactBillingLine.subscription_fk == DimSubscription.id)
        .group_by(DimSubscription.id)
    )

    if q:
        like = f'%{q}%'
        query = query.filter(
            DimSubscription.subscription_name.ilike(like) |
            DimSubscription.subscription_id.ilike(like)
        )

    col = _SORT_MAP.get(sort, _SORT_MAP['total_cost'])
    query = query.order_by(col.desc() if direction == 'desc' else col.asc())

    pagination = query.paginate(page=page, per_page=20, error_out=False)
    return render_template('subscriptions/index.html',
                           pagination=pagination, sort=sort, dir=direction, q=q)


@bp.route('/<int:pk>')
def detail(pk):
    subscription = db.get_or_404(DimSubscription, pk)
    page = request.args.get('page', 1, type=int)

    service_costs = db.session.execute(
        select(
            DimService.service_family,
            func.sum(FactBillingLine.cost_in_billing_currency).label('total_cost'),
        )
        .join(FactBillingLine, FactBillingLine.service_fk == DimService.id)
        .where(FactBillingLine.subscription_fk == pk)
        .group_by(DimService.service_family)
        .order_by(func.sum(FactBillingLine.cost_in_billing_currency).desc())
    ).all()

    lines_stmt = (
        select(FactBillingLine)
        .where(FactBillingLine.subscription_fk == pk)
        .order_by(FactBillingLine.charge_date.desc())
    )
    pagination = db.paginate(lines_stmt, page=page, per_page=50, error_out=False)

    total_cost = db.session.execute(
        select(func.sum(FactBillingLine.cost_in_billing_currency))
        .where(FactBillingLine.subscription_fk == pk)
    ).scalar() or 0

    return render_template(
        'subscriptions/detail.html',
        subscription=subscription,
        service_costs=service_costs,
        total_cost=total_cost,
        pagination=pagination,
    )
