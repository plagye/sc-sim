"""Order generation engine for FlowForm Industries.

Generates daily customer_order events based on segment profiles, SKU affinity
weights, seasonal multipliers, day-of-week multipliers, and priority rules.

Business day: all active customers can order.
Weekend (Sat/Sun): only Industrial Distributors place EDI auto-orders.
Holidays that fall on weekdays: orders still arrive (EDI runs 24/7).

Engine entry point: ``run(state, config, date) -> list[CustomerOrderEvent]``
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from flowform.calendar import (
    is_holiday,
    is_weekend,
    order_multiplier,
    polish_holidays,
)
from flowform.catalog import pricing
from flowform.catalog.generator import CatalogEntry
from flowform.config import Config
from flowform.master_data.customers import Customer
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Event schema
# ---------------------------------------------------------------------------


class CustomerOrderEvent(BaseModel):
    """A customer order with one or more line items.

    ``extra="allow"`` enables schema evolution fields (sales_channel,
    incoterms) to be attached dynamically without fixed model fields.
    """

    model_config = ConfigDict(extra="allow")

    event_type: Literal["customer_order"]
    event_id: str
    order_id: str
    customer_id: str
    simulation_date: str
    timestamp: str
    currency: str
    priority: Literal["standard", "express", "critical"]
    status: Literal["confirmed"]
    requested_delivery_date: str
    lines: list[dict[str, Any]]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DISTRIBUTOR_SEGMENT = "Industrial Distributors"

# Lines per order: weights for 1, 2, 3, 4, 5 lines
_LINE_COUNT_WEIGHTS: dict[str, list[float]] = {
    "Industrial Distributors": [40, 30, 15, 10, 5],
    "Oil & Gas":               [10, 20, 25, 25, 20],
    "Shipyards":               [10, 20, 25, 25, 20],
    "_default":                [30, 30, 20, 15, 5],
}

# Delivery lead time in business days by priority (min, max)
_DELIVERY_DAYS: dict[str, tuple[int, int]] = {
    "critical": (2, 4),
    "express":  (5, 10),
    "standard": (10, 21),
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _add_business_days(start: date, n: int) -> date:
    """Return the date that is *n* business days after *start*.

    Skips weekends and Polish public holidays.

    Args:
        start: Starting date (not counted as day 1).
        n:     Number of business days to advance.

    Returns:
        Date that is exactly n business days after start.
    """
    current = start
    days_added = 0
    while days_added < n:
        current += timedelta(days=1)
        if not is_weekend(current) and not is_holiday(current):
            days_added += 1
    return current


def _first_business_day_of_year(year: int) -> date:
    """Return the first business day of January in *year*."""
    candidate = date(year, 1, 1)
    holidays = polish_holidays(year)
    while candidate.weekday() >= 5 or candidate in holidays:
        candidate += timedelta(days=1)
    return candidate


def _build_sku_weights(
    customer: Customer,
    catalog: list[CatalogEntry],
) -> list[float]:
    """Compute combined affinity weight for every catalog entry for this customer.

    For each dimension in the customer's sku_affinity dict, look up the SKU's
    spec value.  If the spec value isn't in the affinity dict, fall back to
    weight=1.0 (equal).  Multiply all per-dimension weights together.

    Args:
        customer: Customer with an ordering_profile.sku_affinity dict.
        catalog:  Full list of CatalogEntry objects.

    Returns:
        List of floats (same length as catalog), one weight per entry.
        Entries with a zero affinity value will never be sampled.
    """
    affinity = customer.ordering_profile.sku_affinity
    weights: list[float] = []

    for entry in catalog:
        spec = entry.spec
        combined = 1.0

        for dim, dim_weights in affinity.items():
            if dim == "dn":
                val: Any = spec.dn
            elif dim == "material":
                val = spec.material
            elif dim == "valve_type":
                val = spec.valve_type
            elif dim in ("pressure", "pressure_class"):
                val = spec.pressure_class
            elif dim == "connection":
                val = spec.connection
            elif dim == "actuation":
                val = spec.actuation
            else:
                continue  # unknown dimension — skip

            combined *= dim_weights.get(val, 1.0)

        weights.append(max(0.0, combined))

    return weights


def _distribute_quantity(total: int, n: int, rng: Any) -> list[int]:
    """Distribute *total* units across *n* lines with random variance.

    Random weights in [0.5, 1.5], proportional split, last line absorbs
    rounding remainder.  Each line gets at least 1 unit.

    Args:
        total: Total quantity to distribute.
        n:     Number of lines.
        rng:   Seeded random.Random instance.

    Returns:
        List of length n with positive integers summing to at least n.
    """
    if n == 1:
        return [total]

    w = [rng.uniform(0.5, 1.5) for _ in range(n)]
    w_sum = sum(w)
    quantities: list[int] = []
    remaining = total
    for i in range(n):
        if i == n - 1:
            quantities.append(max(1, remaining))
        else:
            q = max(1, round(w[i] / w_sum * total))
            quantities.append(q)
            remaining -= q
    return quantities


def _make_rates_dict(state: SimulationState) -> dict[str, float]:
    """Convert state.exchange_rates keys to the format pricing.convert_price expects.

    state.exchange_rates uses {"EUR": rate, "USD": rate}.
    pricing.convert_price expects {"EUR_PLN": rate, "USD_PLN": rate}.
    """
    return {
        "EUR_PLN": state.exchange_rates["EUR"],
        "USD_PLN": state.exchange_rates["USD"],
    }


def _order_timestamp(sim_date: date, is_weekend_day: bool, rng: Any) -> str:
    """Build an ISO datetime string for the order.

    Business days: random hour 08–16.
    Weekends (EDI overnight batch): random hour 00–05.
    """
    if is_weekend_day:
        hour = rng.randint(0, 5)
    else:
        hour = rng.randint(8, 16)
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    return f"{sim_date.isoformat()}T{hour:02d}:{minute:02d}:{second:02d}"


# ---------------------------------------------------------------------------
# Engine entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[CustomerOrderEvent]:
    """Generate customer order events for *sim_date*.

    On business days all active customers may order.  On weekends only
    Industrial Distributors place EDI auto-orders.  On holidays that fall on
    weekdays, the normal weekday DOW multiplier is applied (EDI still arrives).

    Args:
        state:    Mutable simulation state.  ``state.open_orders`` is updated
                  in place with each new order keyed by order_id.
        config:   Validated simulation config.
        sim_date: Current simulation date.

    Returns:
        List of :class:`CustomerOrderEvent` instances (may be empty).
    """
    weekend_day = is_weekend(sim_date)

    # Pre-compute shared values for this day
    month_mult = config.demand.base_monthly_multipliers[sim_date.month]
    dow_mult = order_multiplier(sim_date)  # holidays do NOT suppress orders
    rates = _make_rates_dict(state)

    # Per-customer SKU weight cache — rebuilt each run() call (acceptable cost)
    sku_weight_cache: dict[str, list[float]] = {}

    events: list[CustomerOrderEvent] = []

    for customer in state.customers:
        if not customer.active:
            continue

        # Weekends: only Distributors can order
        if weekend_day and customer.segment != _DISTRIBUTOR_SEGMENT:
            continue

        # Effective daily ordering probability
        base_freq = customer.ordering_profile.base_order_frequency_per_month
        seasonal = customer.seasonal_profile[sim_date.month]
        p = (base_freq / 22.0) * month_mult * seasonal * dow_mult

        if state.rng.random() >= p:
            continue  # No order today for this customer

        # --- Generate one order for this customer ---
        order_id = f"ORD-{sim_date.year}-{state.next_id('order'):06d}"

        # Priority
        priority_roll = state.rng.random()
        if priority_roll < config.orders.critical_share:
            priority: Literal["standard", "express", "critical"] = "critical"
        elif priority_roll < config.orders.critical_share + config.orders.express_share:
            priority = "express"
        else:
            priority = "standard"

        # Requested delivery date (in business days from order date)
        lo_days, hi_days = _DELIVERY_DAYS[priority]
        n_biz_days = state.rng.randint(lo_days, hi_days)
        delivery_date = _add_business_days(sim_date, n_biz_days)

        # December delivery restriction
        if (
            not customer.accepts_deliveries_december
            and delivery_date.month == 12
            and delivery_date.day > 20
        ):
            delivery_date = _first_business_day_of_year(delivery_date.year + 1)

        # Total quantity for this order
        lo_qty, hi_qty = customer.ordering_profile.order_size_range
        total_qty = state.rng.randint(lo_qty, hi_qty)

        # Number of lines
        line_weights = _LINE_COUNT_WEIGHTS.get(
            customer.segment, _LINE_COUNT_WEIGHTS["_default"]
        )
        n_lines = state.rng.choices(range(1, 6), weights=line_weights, k=1)[0]

        # SKU selection using affinity weights
        if customer.customer_id not in sku_weight_cache:
            sku_weight_cache[customer.customer_id] = _build_sku_weights(
                customer, state.catalog
            )
        sku_weights = sku_weight_cache[customer.customer_id]

        selected_entries = state.rng.choices(
            state.catalog, weights=sku_weights, k=n_lines
        )
        quantities = _distribute_quantity(total_qty, n_lines, state.rng)

        # Build line items
        lines: list[dict[str, Any]] = []
        for i, (entry, qty) in enumerate(zip(selected_entries, quantities)):
            pln_price = pricing.base_price_pln(entry.spec)
            discounted = pricing.apply_customer_discount(
                pln_price, customer.contract_discount_pct
            )
            unit_price = pricing.convert_price(discounted, customer.currency, rates)
            lines.append(
                {
                    "line_id": f"{order_id}-L{i + 1}",
                    "sku": entry.sku,
                    "quantity_ordered": qty,
                    "quantity_allocated": 0,
                    "quantity_shipped": 0,
                    "quantity_backordered": 0,
                    "unit_price": unit_price,
                    "line_status": "open",
                }
            )

        # Timestamp
        timestamp = _order_timestamp(sim_date, weekend_day, state.rng)

        # Assemble event dict (base fields first)
        event_data: dict[str, Any] = {
            "event_type": "customer_order",
            "event_id": str(uuid.uuid4()),
            "order_id": order_id,
            "customer_id": customer.customer_id,
            "simulation_date": sim_date.isoformat(),
            "timestamp": timestamp,
            "currency": customer.currency,
            "priority": priority,
            "status": "confirmed",
            "requested_delivery_date": delivery_date.isoformat(),
            "lines": lines,
        }

        # Schema evolution: sales_channel appears from day threshold
        if state.sim_day >= config.schema_evolution.sales_channel_from_day:
            if weekend_day:
                event_data["sales_channel"] = "edi"
            else:
                event_data["sales_channel"] = state.rng.choices(
                    ["direct", "edi", "portal"],
                    weights=[0.5, 0.3, 0.2],
                    k=1,
                )[0]

        # Schema evolution: incoterms appears from day threshold
        if state.sim_day >= config.schema_evolution.incoterms_from_day:
            event_data["incoterms"] = state.rng.choices(
                ["EXW", "FCA", "DAP", "DDP"],
                weights=[0.3, 0.3, 0.25, 0.15],
                k=1,
            )[0]

        event = CustomerOrderEvent(**event_data)
        events.append(event)

        # Persist order to state
        state.open_orders[order_id] = event.model_dump()

    return events
