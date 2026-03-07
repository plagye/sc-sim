"""Credit check engine for FlowForm Industries.

Evaluates open customer orders against credit limits. Orders that push a
customer's total exposure (unpaid balance + open order value) over their
credit limit are placed on ``credit_hold``. Orders already on hold are
re-evaluated and released if a payment has brought the exposure back below
the limit.

Runs on business days only, after order generation in the day loop.

Engine entry point: ``run(state, config, sim_date) -> list[CreditHoldEvent | CreditReleaseEvent]``
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Any, Literal

from pydantic import BaseModel

from flowform.calendar import is_business_day
from flowform.config import Config
from flowform.master_data.customers import Customer
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Event schemas
# ---------------------------------------------------------------------------

# Statuses that represent "open" orders for exposure purposes
_OPEN_STATUSES: frozenset[str] = frozenset({"confirmed", "credit_hold"})

# Statuses to skip entirely (nothing to evaluate or release)
_SKIP_STATUSES: frozenset[str] = frozenset({"shipped", "cancelled", "partially_shipped"})


class CreditHoldEvent(BaseModel):
    """Emitted when an order is placed on credit hold."""

    event_type: Literal["credit_hold"]
    event_id: str
    simulation_date: str
    order_id: str
    customer_id: str
    customer_balance_pln: float
    open_orders_value_pln: float
    credit_limit_pln: float
    exposure_pln: float


class CreditReleaseEvent(BaseModel):
    """Emitted when a credit-held order is released back to confirmed."""

    event_type: Literal["credit_release"]
    event_id: str
    simulation_date: str
    order_id: str
    customer_id: str
    customer_balance_pln: float
    open_orders_value_pln: float
    credit_limit_pln: float
    exposure_pln: float


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _order_value_pln(
    order: dict[str, Any],
    exchange_rates: dict[str, float],
) -> float:
    """Calculate the total PLN value of all lines in an order dict.

    Converts non-PLN order values using the provided exchange rates.

    Args:
        order:          Order dict as stored in ``state.open_orders``.
        exchange_rates: ``{"EUR": rate, "USD": rate}`` from state.

    Returns:
        Total order value in PLN, as a float.
    """
    currency: str = order.get("currency", "PLN")
    if currency == "PLN":
        rate = 1.0
    elif currency == "EUR":
        rate = exchange_rates["EUR"]
    elif currency == "USD":
        rate = exchange_rates["USD"]
    else:
        rate = 1.0  # fallback for unknown currency

    total = 0.0
    for line in order.get("lines", []):
        qty: int = line.get("quantity_ordered", 0)
        unit_price: float = line.get("unit_price", 0.0)
        total += qty * unit_price

    return total * rate


def _build_customer_index(customers: list[Customer]) -> dict[str, Customer]:
    """Build a lookup dict from customer_id to Customer.

    Args:
        customers: List of Customer objects.

    Returns:
        Dict keyed by customer_id.
    """
    return {c.customer_id: c for c in customers}


def _compute_open_orders_value_pln(
    customer_id: str,
    open_orders: dict[str, dict[str, Any]],
    exchange_rates: dict[str, float],
) -> float:
    """Sum the PLN value of all open orders for a single customer.

    Includes both ``confirmed`` and ``credit_hold`` orders — a held order
    still represents goods committed to the customer (not yet shipped, so
    not yet invoiced separately).

    Args:
        customer_id:  Target customer.
        open_orders:  Full ``state.open_orders`` dict.
        exchange_rates: Rates from state.

    Returns:
        Total open order value in PLN.
    """
    total = 0.0
    for order in open_orders.values():
        if order.get("customer_id") != customer_id:
            continue
        status = order.get("status", "")
        if status not in _OPEN_STATUSES:
            continue
        total += _order_value_pln(order, exchange_rates)
    return total


# ---------------------------------------------------------------------------
# Engine entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[CreditHoldEvent | CreditReleaseEvent]:
    """Evaluate all open orders for credit holds and releases on *sim_date*.

    Runs on business days only. Orders already shipped/cancelled are skipped.
    For each customer, total exposure = unpaid balance + value of all open
    orders (confirmed + credit_hold). Orders are evaluated in insertion order;
    a held order still contributes to exposure for subsequent orders.

    Args:
        state:    Mutable simulation state. ``state.open_orders`` is updated
                  in place: order ``status`` may change to ``"credit_hold"``
                  or back to ``"confirmed"``.
        config:   Validated simulation config (not directly used, kept for
                  engine signature consistency).
        sim_date: Current simulation date.

    Returns:
        List of :class:`CreditHoldEvent` and :class:`CreditReleaseEvent`
        instances (may be empty on non-business days or when no action is
        needed).
    """
    if not is_business_day(sim_date):
        return []

    if not state.open_orders:
        return []

    # Build a customer lookup for fast access
    customer_index = _build_customer_index(state.customers)

    # Cache of already-computed open_orders_value_pln per customer to avoid
    # repeated iteration. This is computed fresh for each customer we encounter
    # (not pre-computed globally) so that order-by-order mutations don't
    # invalidate the cache. The spec says to evaluate in insertion order and
    # include held orders in exposure — we recompute per customer on demand.
    # Because the function includes ALL open orders regardless of which one
    # is currently being evaluated, we compute it once per customer and reuse.
    exposure_cache: dict[str, float] = {}

    events: list[CreditHoldEvent | CreditReleaseEvent] = []

    for order_id, order in state.open_orders.items():
        status: str = order.get("status", "")

        # Skip terminal / shipped statuses
        if status in _SKIP_STATUSES:
            continue

        # Only act on confirmed and credit_hold orders
        if status not in _OPEN_STATUSES:
            continue

        customer_id: str = order.get("customer_id", "")
        customer = customer_index.get(customer_id)
        if customer is None:
            continue  # orphan order — skip

        # Compute exposure for this customer (cached per customer per run)
        if customer_id not in exposure_cache:
            open_orders_value = _compute_open_orders_value_pln(
                customer_id, state.open_orders, state.exchange_rates
            )
            exposure_cache[customer_id] = open_orders_value

        open_orders_value_pln = exposure_cache[customer_id]
        balance_pln: float = state.customer_balances.get(customer_id, 0.0)
        exposure_pln = balance_pln + open_orders_value_pln
        credit_limit_pln: float = customer.credit_limit

        if status == "confirmed" and exposure_pln > credit_limit_pln:
            # Place on hold
            order["status"] = "credit_hold"
            events.append(
                CreditHoldEvent(
                    event_type="credit_hold",
                    event_id=str(uuid.uuid4()),
                    simulation_date=sim_date.isoformat(),
                    order_id=order_id,
                    customer_id=customer_id,
                    customer_balance_pln=round(balance_pln, 2),
                    open_orders_value_pln=round(open_orders_value_pln, 2),
                    credit_limit_pln=round(credit_limit_pln, 2),
                    exposure_pln=round(exposure_pln, 2),
                )
            )

        elif status == "credit_hold" and exposure_pln <= credit_limit_pln:
            # Release back to confirmed
            order["status"] = "confirmed"
            events.append(
                CreditReleaseEvent(
                    event_type="credit_release",
                    event_id=str(uuid.uuid4()),
                    simulation_date=sim_date.isoformat(),
                    order_id=order_id,
                    customer_id=customer_id,
                    customer_balance_pln=round(balance_pln, 2),
                    open_orders_value_pln=round(open_orders_value_pln, 2),
                    credit_limit_pln=round(credit_limit_pln, 2),
                    exposure_pln=round(exposure_pln, 2),
                )
            )

    return events
