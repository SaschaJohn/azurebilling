import hashlib


def compute_row_hash(row: dict) -> str:
    """SHA-256 of the stable identifying fields for idempotent re-import."""
    fields = [
        row.get('invoiceId', '') or '',
        row.get('date', '') or '',
        row.get('billingPeriodStartDate', '') or '',
        row.get('billingPeriodEndDate', '') or '',
        row.get('servicePeriodStartDate', '') or '',
        row.get('servicePeriodEndDate', '') or '',
        row.get('SubscriptionId', '') or '',
        row.get('ResourceId', '') or '',
        row.get('meterId', '') or '',
        row.get('ProductId', '') or '',
        row.get('productOrderId', '') or '',
        row.get('quantity', '') or '',
        row.get('effectivePrice', '') or '',
        row.get('chargeType', '') or '',
        row.get('costInBillingCurrency', '') or '',
        row.get('additionalInfo', '') or '',
        row.get('tags', '') or '',
    ]
    content = '|'.join(fields)
    return hashlib.sha256(content.encode('utf-8')).hexdigest()
