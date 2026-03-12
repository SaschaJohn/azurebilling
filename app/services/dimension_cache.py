from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.dimensions import (
    DimBillingAccount,
    DimBillingProfile,
    DimInvoiceSection,
    DimSubscription,
    DimReseller,
    DimPublisher,
    DimProduct,
    DimMeter,
    DimService,
    DimResourceGroup,
    DimInvoice,
    DimBenefit,
)


class DimensionCache:
    """In-memory FK lookup cache with upsert-on-miss for all dimension tables."""

    def __init__(self, session):
        self.session = session
        self._billing_accounts: dict = {}
        self._billing_profiles: dict = {}
        self._invoice_sections: dict = {}
        self._subscriptions: dict = {}
        self._resellers: dict = {}
        self._publishers: dict = {}
        self._products: dict = {}
        self._meters: dict = {}
        self._services: dict = {}
        self._resource_groups: dict = {}
        self._invoices: dict = {}
        self._benefits: dict = {}
        self._warm()

    def _warm(self):
        s = self.session
        for row in s.execute(select(DimBillingAccount.billing_account_id, DimBillingAccount.id)).all():
            self._billing_accounts[row.billing_account_id] = row.id
        for row in s.execute(select(DimBillingProfile.billing_profile_id, DimBillingProfile.id)).all():
            self._billing_profiles[row.billing_profile_id] = row.id
        for row in s.execute(select(DimInvoiceSection.invoice_section_id, DimInvoiceSection.id)).all():
            self._invoice_sections[row.invoice_section_id] = row.id
        for row in s.execute(select(DimSubscription.subscription_id, DimSubscription.id)).all():
            self._subscriptions[row.subscription_id] = row.id
        for row in s.execute(select(DimReseller.name, DimReseller.mpn_id, DimReseller.id)).all():
            self._resellers[(row.name, row.mpn_id)] = row.id
        for row in s.execute(select(DimPublisher.publisher_type, DimPublisher.publisher_id, DimPublisher.id)).all():
            self._publishers[(row.publisher_type, row.publisher_id)] = row.id
        for row in s.execute(select(DimProduct.product_id, DimProduct.product_order_id, DimProduct.id)).all():
            self._products[(row.product_id, row.product_order_id)] = row.id
        for row in s.execute(select(DimMeter.meter_id, DimMeter.id)).all():
            self._meters[row.meter_id] = row.id
        for row in s.execute(select(DimService.service_family, DimService.consumed_service, DimService.id)).all():
            self._services[(row.service_family, row.consumed_service)] = row.id
        for row in s.execute(select(DimResourceGroup.resource_id, DimResourceGroup.id)).all():
            self._resource_groups[row.resource_id] = row.id
        for row in s.execute(select(DimInvoice.invoice_id, DimInvoice.id)).all():
            self._invoices[row.invoice_id] = row.id
        for row in s.execute(select(DimBenefit.benefit_id, DimBenefit.reservation_id, DimBenefit.id)).all():
            self._benefits[(row.benefit_id, row.reservation_id)] = row.id

    def _upsert(self, model, values: dict, conflict_cols: list, update_cols: list) -> int:
        update_set = {col: values[col] for col in update_cols if col in values}
        # on_conflict_do_update requires a non-empty set_; fall back to a no-op
        # update on the first conflict column so RETURNING still yields the id.
        if not update_set:
            update_set = {conflict_cols[0]: values[conflict_cols[0]]}
        stmt = (
            pg_insert(model)
            .values(**values)
            .on_conflict_do_update(
                index_elements=conflict_cols,
                set_=update_set,
            )
            .returning(model.id)
        )
        return self.session.execute(stmt).scalar()

    # ---------- Public get_X methods ----------

    def get_billing_account(self, billing_account_id: str, billing_account_name: str) -> int:
        key = billing_account_id
        if key not in self._billing_accounts:
            pk = self._upsert(
                DimBillingAccount,
                {'billing_account_id': billing_account_id, 'billing_account_name': billing_account_name},
                ['billing_account_id'],
                ['billing_account_name'],
            )
            self._billing_accounts[key] = pk
        return self._billing_accounts[key]

    def get_billing_profile(self, billing_profile_id: str, billing_profile_name: str, billing_account_fk: int) -> int:
        key = billing_profile_id
        if key not in self._billing_profiles:
            pk = self._upsert(
                DimBillingProfile,
                {
                    'billing_profile_id': billing_profile_id,
                    'billing_profile_name': billing_profile_name,
                    'billing_account_fk': billing_account_fk,
                },
                ['billing_profile_id'],
                ['billing_profile_name', 'billing_account_fk'],
            )
            self._billing_profiles[key] = pk
        return self._billing_profiles[key]

    def get_invoice_section(self, invoice_section_id: str, invoice_section_name: str, billing_profile_fk: int) -> int:
        key = invoice_section_id
        if key not in self._invoice_sections:
            pk = self._upsert(
                DimInvoiceSection,
                {
                    'invoice_section_id': invoice_section_id,
                    'invoice_section_name': invoice_section_name,
                    'billing_profile_fk': billing_profile_fk,
                },
                ['invoice_section_id'],
                ['invoice_section_name', 'billing_profile_fk'],
            )
            self._invoice_sections[key] = pk
        return self._invoice_sections[key]

    def get_subscription(self, subscription_id: str, subscription_name: str) -> int:
        key = subscription_id
        if key not in self._subscriptions:
            pk = self._upsert(
                DimSubscription,
                {'subscription_id': subscription_id, 'subscription_name': subscription_name},
                ['subscription_id'],
                ['subscription_name'],
            )
            self._subscriptions[key] = pk
        return self._subscriptions[key]

    def get_reseller(self, name: str, mpn_id: str) -> int:
        key = (name, mpn_id)
        if key not in self._resellers:
            pk = self._upsert(
                DimReseller,
                {'name': name, 'mpn_id': mpn_id},
                ['name', 'mpn_id'],
                [],
            )
            self._resellers[key] = pk
        return self._resellers[key]

    def get_publisher(self, publisher_type: str, publisher_id: str, publisher_name: str) -> int:
        key = (publisher_type, publisher_id)
        if key not in self._publishers:
            pk = self._upsert(
                DimPublisher,
                {
                    'publisher_type': publisher_type,
                    'publisher_id': publisher_id,
                    'publisher_name': publisher_name,
                },
                ['publisher_type', 'publisher_id'],
                ['publisher_name'],
            )
            self._publishers[key] = pk
        return self._publishers[key]

    def get_product(self, product_id: str, product_order_id: str, product_name: str, product_order_name: str) -> int:
        key = (product_id, product_order_id)
        if key not in self._products:
            pk = self._upsert(
                DimProduct,
                {
                    'product_id': product_id,
                    'product_order_id': product_order_id,
                    'product_name': product_name,
                    'product_order_name': product_order_name,
                },
                ['product_id', 'product_order_id'],
                ['product_name', 'product_order_name'],
            )
            self._products[key] = pk
        return self._products[key]

    def get_meter(self, meter_id: str, meter_name: str, meter_category: str, meter_sub_category: str, meter_region: str) -> int:
        key = meter_id
        if key not in self._meters:
            pk = self._upsert(
                DimMeter,
                {
                    'meter_id': meter_id,
                    'meter_name': meter_name,
                    'meter_category': meter_category,
                    'meter_sub_category': meter_sub_category,
                    'meter_region': meter_region,
                },
                ['meter_id'],
                ['meter_name', 'meter_category', 'meter_sub_category', 'meter_region'],
            )
            self._meters[key] = pk
        return self._meters[key]

    def get_service(self, service_family: str, consumed_service: str) -> int:
        key = (service_family, consumed_service)
        if key not in self._services:
            pk = self._upsert(
                DimService,
                {'service_family': service_family, 'consumed_service': consumed_service},
                ['service_family', 'consumed_service'],
                [],
            )
            self._services[key] = pk
        return self._services[key]

    def get_resource_group(self, resource_id: str, resource_group_name: str, subscription_fk: int) -> int:
        key = resource_id
        if key not in self._resource_groups:
            pk = self._upsert(
                DimResourceGroup,
                {
                    'resource_id': resource_id,
                    'resource_group_name': resource_group_name,
                    'subscription_fk': subscription_fk,
                },
                ['resource_id'],
                ['resource_group_name', 'subscription_fk'],
            )
            self._resource_groups[key] = pk
        return self._resource_groups[key]

    def get_invoice(self, invoice_id: str, previous_invoice_id: str) -> int:
        key = invoice_id
        if key not in self._invoices:
            pk = self._upsert(
                DimInvoice,
                {'invoice_id': invoice_id, 'previous_invoice_id': previous_invoice_id or None},
                ['invoice_id'],
                ['previous_invoice_id'],
            )
            self._invoices[key] = pk
        return self._invoices[key]

    def get_benefit(self, benefit_id: str, reservation_id: str, benefit_name: str, reservation_name: str) -> int:
        key = (benefit_id, reservation_id)
        if key not in self._benefits:
            pk = self._upsert(
                DimBenefit,
                {
                    'benefit_id': benefit_id,
                    'reservation_id': reservation_id,
                    'benefit_name': benefit_name or None,
                    'reservation_name': reservation_name or None,
                },
                ['benefit_id', 'reservation_id'],
                ['benefit_name', 'reservation_name'],
            )
            self._benefits[key] = pk
        return self._benefits[key]
