"""Master data change generator: address, segment, payment terms, carrier rates, SKU discontinuations.

Emits MasterDataChangeEvent records and mutates state in-place.
Runs on business days only.
"""
from __future__ import annotations

import dataclasses
from datetime import date, timedelta
from typing import Any, Literal

from faker import Faker
from pydantic import BaseModel

from flowform.calendar import is_business_day
from flowform.config import Config
from flowform.master_data.customers import SEGMENTS
from flowform.state import SimulationState

_CARRIER_CODES = ["DHL", "DBSC", "RABEN", "GEODIS", "BALTIC"]
_PAYMENT_TERMS_OPTIONS = [30, 60, 90]

# Daily Bernoulli probabilities
_PROB_ADDRESS = 2 / 22        # ~0.091
_PROB_SEGMENT = 1 / 66        # ~0.015
_PROB_PAYMENT_TERMS = 1 / 66  # ~0.015
_PROB_SKU_DISCONTINUATION = 5 / 66  # ~0.076


class MasterDataChangeEvent(BaseModel):
    event_type: Literal["master_data_change"] = "master_data_change"
    change_id: str           # "MDC-20260315-0001"
    timestamp: str           # sim_date at 09:00:00Z
    entity_type: str         # "customer" | "carrier" | "product"
    entity_id: str           # "CUST-0037" | "DHL" | "FF-BV50-..."
    field_changed: str       # field name
    old_value: Any
    new_value: Any
    effective_date: str      # sim_date + 1 day ISO string
    reason: str


def _next_change_id(state: SimulationState, sim_date: date) -> str:
    """Allocate the next MDC change ID."""
    if "mdc" not in state.counters:
        state.counters["mdc"] = 0
    seq = state.next_id("mdc")
    return f"MDC-{sim_date.strftime('%Y%m%d')}-{seq:04d}"


def _make_event(
    state: SimulationState,
    sim_date: date,
    entity_type: str,
    entity_id: str,
    field_changed: str,
    old_value: Any,
    new_value: Any,
    reason: str,
) -> MasterDataChangeEvent:
    effective = (sim_date + timedelta(days=1)).isoformat()
    return MasterDataChangeEvent(
        change_id=_next_change_id(state, sim_date),
        timestamp=f"{sim_date}T09:00:00Z",
        entity_type=entity_type,
        entity_id=entity_id,
        field_changed=field_changed,
        old_value=old_value,
        new_value=new_value,
        effective_date=effective,
        reason=reason,
    )


def _try_address_change(
    state: SimulationState, sim_date: date
) -> MasterDataChangeEvent | None:
    """Emit a delivery_address_primary change for a random customer."""
    if not state.rng.random() < _PROB_ADDRESS:
        return None

    customer = state.rng.choice(state.customers)
    fake = Faker("pl_PL")
    fake.seed_instance(state.rng.randint(0, 2**31))

    old_addr = {
        "street": customer.primary_address.street,
        "city": customer.primary_address.city,
        "postal_code": customer.primary_address.postal_code,
    }
    new_street = fake.street_address()
    new_city = fake.city()
    new_postal = fake.postcode()
    new_addr = {
        "street": new_street,
        "city": new_city,
        "postal_code": new_postal,
    }

    # Mutate in-place
    customer.primary_address.street = new_street
    customer.primary_address.city = new_city
    customer.primary_address.postal_code = new_postal

    return _make_event(
        state, sim_date,
        entity_type="customer",
        entity_id=customer.customer_id,
        field_changed="delivery_address_primary",
        old_value=old_addr,
        new_value=new_addr,
        reason="office_relocation",
    )


def _try_segment_change(
    state: SimulationState, sim_date: date
) -> MasterDataChangeEvent | None:
    """Emit a segment reclassification for a random customer."""
    if not state.rng.random() < _PROB_SEGMENT:
        return None

    customer = state.rng.choice(state.customers)
    old_segment = customer.segment
    available = [s for s in SEGMENTS if s != old_segment]
    new_segment = state.rng.choice(available)

    customer.segment = new_segment

    return _make_event(
        state, sim_date,
        entity_type="customer",
        entity_id=customer.customer_id,
        field_changed="segment",
        old_value=old_segment,
        new_value=new_segment,
        reason="account_reclassification",
    )


def _try_payment_terms_change(
    state: SimulationState, sim_date: date
) -> MasterDataChangeEvent | None:
    """Emit a payment_terms_days change for a random customer."""
    if not state.rng.random() < _PROB_PAYMENT_TERMS:
        return None

    customer = state.rng.choice(state.customers)
    old_terms = customer.payment_terms_days
    available = [t for t in _PAYMENT_TERMS_OPTIONS if t != old_terms]
    new_terms = state.rng.choice(available)

    customer.payment_terms_days = new_terms

    return _make_event(
        state, sim_date,
        entity_type="customer",
        entity_id=customer.customer_id,
        field_changed="payment_terms_days",
        old_value=old_terms,
        new_value=new_terms,
        reason="contract_renewal",
    )


def _try_carrier_rate_change(
    state: SimulationState, sim_date: date
) -> MasterDataChangeEvent | None:
    """Fire exactly once on the first business day at or after sim_day 90."""
    if state.sim_day < 90:
        return None
    if state.counters.get("carrier_rate_changed", 0) != 0:
        return None

    carrier_code = state.rng.choice(_CARRIER_CODES)
    # ±5–15% rate change
    pct = state.rng.uniform(0.05, 0.15)
    direction = state.rng.choice([-1, 1])
    old_rate = 1.0  # normalised base rate
    new_rate = round(old_rate * (1 + direction * pct), 4)

    state.counters["carrier_rate_changed"] = 1

    return _make_event(
        state, sim_date,
        entity_type="carrier",
        entity_id=carrier_code,
        field_changed="carrier_base_rate",
        old_value=old_rate,
        new_value=new_rate,
        reason="annual_rate_review",
    )


def _try_sku_discontinuation(
    state: SimulationState, sim_date: date
) -> MasterDataChangeEvent | None:
    """Discontinue a random active SKU."""
    if not state.rng.random() < _PROB_SKU_DISCONTINUATION:
        return None

    active_entries = [(i, e) for i, e in enumerate(state.catalog) if e.is_active]
    if not active_entries:
        return None

    idx, entry = state.rng.choice(active_entries)
    # CatalogEntry is frozen; replace it in the list with is_active=False
    state.catalog[idx] = dataclasses.replace(entry, is_active=False)

    return _make_event(
        state, sim_date,
        entity_type="product",
        entity_id=entry.sku,
        field_changed="sku_status",
        old_value="active",
        new_value="discontinued",
        reason="product_lifecycle",
    )


def run(state: SimulationState, config: Config, sim_date: date) -> list[MasterDataChangeEvent]:
    """Run the master data change engine for *sim_date*.

    Returns a (possibly empty) list of MasterDataChangeEvent records.
    All state mutations happen in-place before events are returned.
    """
    if not is_business_day(sim_date):
        return []

    events: list[MasterDataChangeEvent] = []

    result = _try_address_change(state, sim_date)
    if result is not None:
        events.append(result)

    result = _try_segment_change(state, sim_date)
    if result is not None:
        events.append(result)

    result = _try_payment_terms_change(state, sim_date)
    if result is not None:
        events.append(result)

    result = _try_carrier_rate_change(state, sim_date)
    if result is not None:
        events.append(result)

    result = _try_sku_discontinuation(state, sim_date)
    if result is not None:
        events.append(result)

    return events
