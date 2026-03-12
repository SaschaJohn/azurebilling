import io
import csv
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.fact import FactBillingLine
from app.services.dimension_cache import DimensionCache
from app.services.hash_utils import compute_row_hash

BATCH_SIZE = 500


def parse_date(s):
    if not s or not s.strip():
        return None
    try:
        return datetime.strptime(s.strip(), '%m/%d/%Y').date()
    except ValueError:
        return None


def parse_decimal(s):
    if not s or not s.strip():
        return None
    try:
        return Decimal(s.strip())
    except InvalidOperation:
        return None


def parse_json(s):
    if not s or not s.strip():
        return None
    try:
        return json.loads(s)
    except (json.JSONDecodeError, ValueError):
        return None


def parse_bool(s):
    if not s:
        return None
    return s.strip().lower() == 'true'


def import_csv(stream, batch_id, session, progress_cb=None) -> tuple[int, int]:
    """
    Stream a CSV from a binary file-like object into the DB.
    Returns (row_count, skipped_count).
    progress_cb, if provided, is called with the total rows processed every 10k rows.
    """
    text_stream = io.TextIOWrapper(stream, encoding='utf-8-sig')
    reader = csv.DictReader(text_stream)

    cache = DimensionCache(session)

    pending = []
    row_count = 0   # rows actually inserted (new)
    skipped_count = 0  # rows with parse errors or duplicate hashes
    total_processed = 0

    for raw_row in reader:
        try:
            fact = _process_row(raw_row, batch_id, cache)
            pending.append(fact)
        except Exception:
            skipped_count += 1
            total_processed += 1
            continue

        total_processed += 1

        if len(pending) >= BATCH_SIZE:
            inserted, dupes = _flush(session, pending)
            row_count += inserted
            skipped_count += dupes
            pending = []
            if progress_cb and total_processed % 10000 < BATCH_SIZE:
                progress_cb(total_processed)

    if pending:
        inserted, dupes = _flush(session, pending)
        row_count += inserted
        skipped_count += dupes

    return row_count, skipped_count


def _flush(session, batch: list) -> tuple[int, int]:
    """Insert a batch; returns (inserted_count, duplicate_count)."""
    stmt = (
        pg_insert(FactBillingLine)
        .values(batch)
        .on_conflict_do_nothing(index_elements=['row_hash'])
    )
    result = session.execute(stmt)
    session.commit()
    inserted = result.rowcount
    dupes = len(batch) - inserted
    return inserted, dupes


def _process_row(row: dict, batch_id, cache: DimensionCache) -> dict:
    row_hash = compute_row_hash(row)

    charge_date = parse_date(row.get('date'))
    if charge_date is None:
        raise ValueError('Missing charge date')

    # Subscription (needed before resource group)
    subscription_id = row.get('SubscriptionId', '') or ''
    subscription_fk = cache.get_subscription(
        subscription_id,
        row.get('subscriptionName', '') or '',
    )

    # Billing hierarchy
    billing_account_fk = cache.get_billing_account(
        row.get('billingAccountId', '') or '',
        row.get('billingAccountName', '') or '',
    )
    billing_profile_fk = cache.get_billing_profile(
        row.get('billingProfileId', '') or '',
        row.get('billingProfileName', '') or '',
        billing_account_fk,
    )
    invoice_section_fk = cache.get_invoice_section(
        row.get('invoiceSectionId', '') or '',
        row.get('invoiceSectionName', '') or '',
        billing_profile_fk,
    )

    # Reseller (nullable)
    reseller_name = row.get('resellerName', '') or ''
    reseller_fk = (
        cache.get_reseller(reseller_name, row.get('resellerMpnId', '') or '')
        if reseller_name
        else None
    )

    # Publisher
    publisher_fk = cache.get_publisher(
        row.get('publisherType', '') or '',
        row.get('publisherId', '') or '',
        row.get('publisherName', '') or '',
    )

    # Product
    product_fk = cache.get_product(
        row.get('ProductId', '') or '',
        row.get('productOrderId', '') or '',
        row.get('ProductName', '') or '',
        row.get('productOrderName', '') or '',
    )

    # Meter (nullable when meterId is empty — common for SaaS rows)
    meter_id = row.get('meterId', '') or ''
    meter_fk = (
        cache.get_meter(
            meter_id,
            row.get('meterName', '') or '',
            row.get('meterCategory', '') or '',
            row.get('meterSubCategory', '') or '',
            row.get('meterRegion', '') or '',
        )
        if meter_id
        else None
    )

    # Service
    service_fk = cache.get_service(
        row.get('serviceFamily', '') or '',
        row.get('consumedService', '') or '',
    )

    # Resource group (nullable when ResourceId is empty)
    resource_id = row.get('ResourceId', '') or ''
    resource_group_fk = (
        cache.get_resource_group(
            resource_id,
            row.get('resourceGroupName', '') or '',
            subscription_fk,
        )
        if resource_id
        else None
    )

    # Invoice (nullable when invoiceId is empty — common for usage rows)
    invoice_id = row.get('invoiceId', '') or ''
    invoice_fk = (
        cache.get_invoice(invoice_id, row.get('previousInvoiceId', '') or '')
        if invoice_id
        else None
    )

    # Benefit (nullable)
    benefit_id = row.get('benefitId', '') or ''
    reservation_id = row.get('reservationId', '') or ''
    benefit_fk = (
        cache.get_benefit(
            benefit_id,
            reservation_id,
            row.get('benefitName', '') or '',
            row.get('reservationName', '') or '',
        )
        if (benefit_id or reservation_id)
        else None
    )

    return {
        'billing_account_fk': billing_account_fk,
        'billing_profile_fk': billing_profile_fk,
        'invoice_section_fk': invoice_section_fk,
        'subscription_fk': subscription_fk,
        'reseller_fk': reseller_fk,
        'publisher_fk': publisher_fk,
        'product_fk': product_fk,
        'meter_fk': meter_fk,
        'service_fk': service_fk,
        'resource_group_fk': resource_group_fk,
        'invoice_fk': invoice_fk,
        'benefit_fk': benefit_fk,
        'import_batch_id': batch_id,
        'billing_period_start_date': parse_date(row.get('billingPeriodStartDate')),
        'billing_period_end_date': parse_date(row.get('billingPeriodEndDate')),
        'service_period_start_date': parse_date(row.get('servicePeriodStartDate')),
        'service_period_end_date': parse_date(row.get('servicePeriodEndDate')),
        'charge_date': charge_date,
        'exchange_rate_date': parse_date(row.get('exchangeRateDate')),
        'effective_price': parse_decimal(row.get('effectivePrice')),
        'quantity': parse_decimal(row.get('quantity')),
        'cost_in_billing_currency': parse_decimal(row.get('costInBillingCurrency')),
        'cost_in_pricing_currency': parse_decimal(row.get('costInPricingCurrency')),
        'cost_in_usd': parse_decimal(row.get('costInUsd')),
        'payg_cost_in_billing_currency': parse_decimal(row.get('paygCostInBillingCurrency')),
        'payg_cost_in_usd': parse_decimal(row.get('paygCostInUsd')),
        'exchange_rate_pricing_to_billing': parse_decimal(row.get('exchangeRatePricingToBilling')),
        'pay_g_price': parse_decimal(row.get('PayGPrice')),
        'unit_price': parse_decimal(row.get('unitPrice')),
        'unit_of_measure': row.get('unitOfMeasure') or None,
        'charge_type': row.get('chargeType') or None,
        'billing_currency': row.get('billingCurrency') or None,
        'pricing_currency': row.get('pricingCurrency') or None,
        'is_azure_credit_eligible': parse_bool(row.get('isAzureCreditEligible')),
        'service_info1': row.get('serviceInfo1') or None,
        'service_info2': row.get('serviceInfo2') or None,
        'additional_info': parse_json(row.get('additionalInfo')),
        'tags': parse_json(row.get('tags')),
        'frequency': row.get('frequency') or None,
        'term': row.get('term') or None,
        'pricing_model': row.get('pricingModel') or None,
        'cost_allocation_rule_name': row.get('costAllocationRuleName') or None,
        'provider': row.get('provider') or None,
        'cost_center': row.get('costCenter') or None,
        'resource_location': row.get('resourceLocation') or None,
        'location': row.get('location') or None,
        'row_hash': row_hash,
    }
