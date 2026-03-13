from datetime import date

from flask import Blueprint, g, render_template, request
from sqlalchemy import func, select, text

from app.extensions import db
from app.models.dimensions import DimProduct, DimService, DimSubscription
from app.models.fact import FactBillingLine
from app.models.import_batch import ImportBatch

bp = Blueprint('dashboard', __name__)


def _enrich_rows(rows, is_rg=False):
    result = []
    for r in rows:
        label = r.label
        cost_a, cost_b, delta = float(r.cost_a), float(r.cost_b), float(r.delta)
        row = {
            'label': label or '(none)',
            'cost_a': cost_a,
            'cost_b': cost_b,
            'delta': delta,
            'row_class': 'table-danger' if delta > 0 else ('table-success' if delta < 0 else ''),
            'sign': '+' if delta > 0 else '',
            'pct_change': None if cost_a == 0 else float(delta / cost_a * 100),
        }
        if is_rg:
            row['is_rg'] = True
        result.append(row)
    max_abs = max((abs(r['delta']) for r in result), default=1) or 1
    for r in result:
        r['bar_width'] = int(abs(r['delta']) / max_abs * 100)
    return result


def _delta_by_service(month_a, next_a, month_b, next_b):
    rows = db.session.execute(text("""
        WITH a AS (
            SELECT s.service_family, SUM(f.cost_in_billing_currency) AS cost_a
            FROM fact_billing_line f JOIN dim_service s ON s.id = f.service_fk
            WHERE f.charge_date >= :start_a AND f.charge_date < :end_a
            GROUP BY s.service_family
        ),
        b AS (
            SELECT s.service_family, SUM(f.cost_in_billing_currency) AS cost_b
            FROM fact_billing_line f JOIN dim_service s ON s.id = f.service_fk
            WHERE f.charge_date >= :start_b AND f.charge_date < :end_b
            GROUP BY s.service_family
        )
        SELECT COALESCE(a.service_family, b.service_family) AS label,
               COALESCE(a.cost_a, 0) AS cost_a,
               COALESCE(b.cost_b, 0) AS cost_b,
               COALESCE(b.cost_b, 0) - COALESCE(a.cost_a, 0) AS delta
        FROM a FULL OUTER JOIN b ON a.service_family = b.service_family
        ORDER BY ABS(COALESCE(b.cost_b, 0) - COALESCE(a.cost_a, 0)) DESC
    """), {'start_a': month_a, 'end_a': next_a, 'start_b': month_b, 'end_b': next_b}).all()
    return _enrich_rows(rows)


def _delta_by_subscription(month_a, next_a, month_b, next_b):
    rows = db.session.execute(text("""
        WITH a AS (
            SELECT s.subscription_name, SUM(f.cost_in_billing_currency) AS cost_a
            FROM fact_billing_line f JOIN dim_subscription s ON s.id = f.subscription_fk
            WHERE f.charge_date >= :start_a AND f.charge_date < :end_a
            GROUP BY s.subscription_name
        ),
        b AS (
            SELECT s.subscription_name, SUM(f.cost_in_billing_currency) AS cost_b
            FROM fact_billing_line f JOIN dim_subscription s ON s.id = f.subscription_fk
            WHERE f.charge_date >= :start_b AND f.charge_date < :end_b
            GROUP BY s.subscription_name
        )
        SELECT COALESCE(a.subscription_name, b.subscription_name) AS label,
               COALESCE(a.cost_a, 0) AS cost_a,
               COALESCE(b.cost_b, 0) AS cost_b,
               COALESCE(b.cost_b, 0) - COALESCE(a.cost_a, 0) AS delta
        FROM a FULL OUTER JOIN b ON a.subscription_name = b.subscription_name
        ORDER BY ABS(COALESCE(b.cost_b, 0) - COALESCE(a.cost_a, 0)) DESC
    """), {'start_a': month_a, 'end_a': next_a, 'start_b': month_b, 'end_b': next_b}).all()
    return _enrich_rows(rows)


def _delta_by_resource_group(month_a, next_a, month_b, next_b):
    rows = db.session.execute(text("""
        WITH a AS (
            SELECT rg.resource_group_name, SUM(f.cost_in_billing_currency) AS cost_a
            FROM fact_billing_line f JOIN dim_resource_group rg ON rg.id = f.resource_group_fk
            WHERE f.charge_date >= :start_a AND f.charge_date < :end_a
            GROUP BY rg.resource_group_name
        ),
        b AS (
            SELECT rg.resource_group_name, SUM(f.cost_in_billing_currency) AS cost_b
            FROM fact_billing_line f JOIN dim_resource_group rg ON rg.id = f.resource_group_fk
            WHERE f.charge_date >= :start_b AND f.charge_date < :end_b
            GROUP BY rg.resource_group_name
        )
        SELECT COALESCE(a.resource_group_name, b.resource_group_name) AS label,
               COALESCE(a.cost_a, 0) AS cost_a,
               COALESCE(b.cost_b, 0) AS cost_b,
               COALESCE(b.cost_b, 0) - COALESCE(a.cost_a, 0) AS delta
        FROM a FULL OUTER JOIN b ON a.resource_group_name = b.resource_group_name
        ORDER BY ABS(COALESCE(b.cost_b, 0) - COALESCE(a.cost_a, 0)) DESC
        LIMIT 100
    """), {'start_a': month_a, 'end_a': next_a, 'start_b': month_b, 'end_b': next_b}).all()
    return _enrich_rows(rows, is_rg=True)


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


@bp.route('/delta')
def delta():
    def _parse_month(param, fallback):
        try:
            y, m = map(int, request.args.get(param, '').split('-'))
            return date(y, m, 1)
        except (ValueError, AttributeError):
            return fallback

    months = g.available_months
    default_a = months[1] if len(months) >= 2 else (months[0] if months else None)
    default_b = months[0] if months else None

    month_a = _parse_month('month_a', default_a)
    month_b = _parse_month('month_b', default_b)

    def _next(d):
        if not d:
            return None
        return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)

    next_a, next_b = _next(month_a), _next(month_b)

    service_rows = _delta_by_service(month_a, next_a, month_b, next_b)
    sub_rows = _delta_by_subscription(month_a, next_a, month_b, next_b)
    rg_rows = _delta_by_resource_group(month_a, next_a, month_b, next_b)

    return render_template(
        'dashboard/delta.html',
        month_a=month_a,
        month_b=month_b,
        service_rows=service_rows,
        sub_rows=sub_rows,
        rg_rows=rg_rows,
    )
