from flask import Blueprint, g, render_template, request
from sqlalchemy import func, select

from app.extensions import db
from app.models.dimensions import DimProduct, DimService, DimSubscription
from app.models.fact import FactBillingLine
from app.models.import_batch import ImportBatch

bp = Blueprint('dashboard', __name__)


@bp.route('/')
def index():
    month_filters = []
    if g.active_month:
        month_filters = [
            FactBillingLine.charge_date >= g.active_month,
            FactBillingLine.charge_date < g.next_month,
        ]

    # Cost by service family
    service_costs = db.session.execute(
        select(
            DimService.service_family,
            func.sum(FactBillingLine.cost_in_billing_currency).label('total_cost'),
            func.count(FactBillingLine.id).label('line_count'),
        )
        .join(FactBillingLine, FactBillingLine.service_fk == DimService.id)
        .where(*month_filters)
        .group_by(DimService.service_family)
        .order_by(func.sum(FactBillingLine.cost_in_billing_currency).desc())
    ).all()

    # Top 10 subscriptions by cost
    top_subs = db.session.execute(
        select(
            DimSubscription.id,
            DimSubscription.subscription_id,
            DimSubscription.subscription_name,
            func.sum(FactBillingLine.cost_in_billing_currency).label('total_cost'),
        )
        .join(FactBillingLine, FactBillingLine.subscription_fk == DimSubscription.id)
        .where(*month_filters)
        .group_by(DimSubscription.id)
        .order_by(func.sum(FactBillingLine.cost_in_billing_currency).desc())
        .limit(10)
    ).all()

    # Total cost
    total_cost = db.session.execute(
        select(func.sum(FactBillingLine.cost_in_billing_currency))
        .where(*month_filters)
    ).scalar() or 0

    # Recent import batches
    recent_batches = db.session.execute(
        select(ImportBatch).order_by(ImportBatch.started_at.desc()).limit(5)
    ).scalars().all()

    return render_template(
        'dashboard/index.html',
        service_costs=service_costs,
        top_subs=top_subs,
        total_cost=total_cost,
        recent_batches=recent_batches,
    )


@bp.route('/families')
def family_detail():
    family = request.args.get('family', '')
    page = request.args.get('page', 1, type=int)

    month_filters = []
    if g.active_month:
        month_filters = [
            FactBillingLine.charge_date >= g.active_month,
            FactBillingLine.charge_date < g.next_month,
        ]

    product_costs = db.session.query(
        DimProduct.product_name,
        func.sum(FactBillingLine.cost_in_billing_currency).label('total_cost'),
        func.count(FactBillingLine.id).label('line_count'),
    ).join(FactBillingLine, FactBillingLine.product_fk == DimProduct.id
    ).join(DimService, FactBillingLine.service_fk == DimService.id
    ).filter(DimService.service_family == family, *month_filters
    ).group_by(DimProduct.product_name
    ).order_by(func.sum(FactBillingLine.cost_in_billing_currency).desc()
    ).paginate(page=page, per_page=50, error_out=False)

    total_cost = db.session.execute(
        select(func.sum(FactBillingLine.cost_in_billing_currency))
        .join(DimService, FactBillingLine.service_fk == DimService.id)
        .where(DimService.service_family == family, *month_filters)
    ).scalar() or 0

    return render_template(
        'dashboard/family.html',
        family=family,
        product_costs=product_costs,
        total_cost=total_cost,
        pagination=product_costs,
    )
