from collections import defaultdict
from decimal import Decimal

from flask import Blueprint, render_template, request
from sqlalchemy import func

from app.extensions import db
from app.models.dimensions import DimMeter, DimResourceGroup
from app.models.fact import FactBillingLine

bp = Blueprint('storage', __name__, url_prefix='/storage')

_STORAGE_FILTER = '%/Microsoft.Storage/storageAccounts/%'

_SORT_MAP = {
    'name':         DimResourceGroup.resource_id,
    'rg':           DimResourceGroup.resource_group_name,
    'transactions': func.sum(FactBillingLine.quantity),
    'total_cost':   func.sum(FactBillingLine.cost_in_billing_currency),
    'line_count':   func.count(FactBillingLine.id),
}


def _account_name(resource_id):
    """Extract storage account name from a resource_id path."""
    parts = resource_id.split('/storageAccounts/')
    if len(parts) > 1:
        return parts[1].split('/')[0]
    return resource_id


@bp.route('/')
def index():
    page      = request.args.get('page', 1, type=int)
    sort      = request.args.get('sort', 'total_cost')
    direction = request.args.get('dir', 'desc')
    name      = request.args.get('name', '').strip()
    rg        = request.args.get('rg', '').strip()
    min_cost  = request.args.get('min_cost', type=float)
    max_cost  = request.args.get('max_cost', type=float)
    min_tx    = request.args.get('min_tx', type=float)
    max_tx    = request.args.get('max_tx', type=float)

    # --- Heatmap query ---
    # Top 15 storage accounts by total cost, broken down by meter_sub_category
    heatmap_rows = (
        db.session.query(
            DimResourceGroup.resource_id,
            DimMeter.meter_sub_category,
            func.sum(FactBillingLine.quantity).label('total_qty'),
            func.sum(FactBillingLine.cost_in_billing_currency).label('total_cost'),
        )
        .join(FactBillingLine, FactBillingLine.resource_group_fk == DimResourceGroup.id)
        .outerjoin(DimMeter, FactBillingLine.meter_fk == DimMeter.id)
        .filter(DimResourceGroup.resource_id.ilike(_STORAGE_FILTER))
        .filter(FactBillingLine.meter_fk.isnot(None))
        .group_by(DimResourceGroup.resource_id, DimMeter.meter_sub_category)
        .all()
    )

    # Build per-account cost totals to pick top 15
    account_cost = defaultdict(float)
    for row in heatmap_rows:
        account_cost[_account_name(row.resource_id)] += float(row.total_cost or 0)

    top_accounts = [a for a, _ in sorted(account_cost.items(), key=lambda x: x[1], reverse=True)[:15]]

    # Build per-column volume totals to pick top 10 columns
    col_vol = defaultdict(float)
    for row in heatmap_rows:
        if _account_name(row.resource_id) in top_accounts:
            col_vol[row.meter_sub_category or '(none)'] += float(row.total_qty or 0)

    top_cols = [c for c, _ in sorted(col_vol.items(), key=lambda x: x[1], reverse=True)[:10]]

    # Pivot into {account: {col: qty}}
    heatmap_data = defaultdict(lambda: defaultdict(float))
    for row in heatmap_rows:
        acct = _account_name(row.resource_id)
        col  = row.meter_sub_category or '(none)'
        if acct in top_accounts and col in top_cols:
            heatmap_data[acct][col] += float(row.total_qty or 0)

    heatmap_max = max(
        (heatmap_data[a][c] for a in top_accounts for c in top_cols),
        default=0,
    )

    # --- List query ---
    list_query = (
        db.session.query(
            DimResourceGroup.id,
            DimResourceGroup.resource_id,
            DimResourceGroup.resource_group_name,
            func.sum(FactBillingLine.cost_in_billing_currency).label('total_cost'),
            func.sum(FactBillingLine.quantity).label('transactions'),
            func.count(FactBillingLine.id).label('line_count'),
        )
        .join(FactBillingLine, FactBillingLine.resource_group_fk == DimResourceGroup.id)
        .filter(DimResourceGroup.resource_id.ilike(_STORAGE_FILTER))
        .group_by(DimResourceGroup.id)
    )

    if name:     list_query = list_query.filter(DimResourceGroup.resource_id.ilike(f'%{name}%'))
    if rg:       list_query = list_query.filter(DimResourceGroup.resource_group_name.ilike(f'%{rg}%'))
    if min_cost: list_query = list_query.having(func.sum(FactBillingLine.cost_in_billing_currency) >= min_cost)
    if max_cost: list_query = list_query.having(func.sum(FactBillingLine.cost_in_billing_currency) <= max_cost)
    if min_tx:   list_query = list_query.having(func.sum(FactBillingLine.quantity) >= min_tx)
    if max_tx:   list_query = list_query.having(func.sum(FactBillingLine.quantity) <= max_tx)

    col = _SORT_MAP.get(sort, _SORT_MAP['total_cost'])
    list_query = list_query.order_by(col.desc() if direction == 'desc' else col.asc())

    pagination = list_query.paginate(page=page, per_page=20, error_out=False)

    filters = dict(name=name, rg=rg, min_cost=min_cost, max_cost=max_cost, min_tx=min_tx, max_tx=max_tx)

    return render_template(
        'storage/index.html',
        heatmap_accounts=top_accounts,
        heatmap_cols=top_cols,
        heatmap_data=heatmap_data,
        heatmap_max=heatmap_max,
        pagination=pagination,
        sort=sort,
        dir=direction,
        filters=filters,
        account_name=_account_name,
    )


@bp.route('/<int:pk>')
def detail(pk):
    rg = db.get_or_404(DimResourceGroup, pk)

    rows = (
        db.session.query(
            func.date_trunc('month', FactBillingLine.charge_date).label('month'),
            DimMeter.meter_name,
            func.sum(FactBillingLine.cost_in_billing_currency).label('total_cost'),
        )
        .select_from(DimResourceGroup)
        .join(FactBillingLine, FactBillingLine.resource_group_fk == DimResourceGroup.id)
        .outerjoin(DimMeter, FactBillingLine.meter_fk == DimMeter.id)
        .filter(DimResourceGroup.id == pk)
        .group_by(
            func.date_trunc('month', FactBillingLine.charge_date),
            DimMeter.meter_name,
        )
        .all()
    )

    months = sorted({r.month for r in rows}, reverse=True)
    meter_totals = defaultdict(Decimal)
    for r in rows:
        meter_totals[r.meter_name or '(none)'] += r.total_cost or Decimal(0)
    meters = sorted(meter_totals, key=lambda m: meter_totals[m], reverse=True)

    pivot = defaultdict(lambda: defaultdict(Decimal))
    month_totals = defaultdict(Decimal)
    for r in rows:
        meter = r.meter_name or '(none)'
        pivot[meter][r.month] += r.total_cost or Decimal(0)
        month_totals[r.month] += r.total_cost or Decimal(0)

    grand_total = sum(meter_totals.values(), Decimal(0))

    return render_template(
        'storage/detail.html',
        rg=rg,
        account_name=_account_name(rg.resource_id),
        months=months,
        meters=meters,
        pivot=pivot,
        meter_totals=meter_totals,
        month_totals=month_totals,
        grand_total=grand_total,
    )
