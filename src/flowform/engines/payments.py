"""Payment processing engine for FlowForm Industries.

Processes customer payments from ``state.scheduled_payments``: fires all
entries whose ``due_date <= sim_date``, reduces customer balances, and
potentially releases credit holds when exposure drops below the limit.

Runs on business days only.

Engine entry point: ``run(state, config, sim_date) -> list[PaymentEvent | CreditReleaseEvent]``
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Any, Literal

from pydantic import BaseModel

from flowform.calendar import is_business_day
from flowform.config import Config
from flowform.engines.credit import (
    CreditReleaseEvent,
    _compute_open_orders_value_pln,
)
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Event schema
# ---------------------------------------------------------------------------


class PaymentEvent(BaseModel):
    """Emitted when a customer makes a payment on their outstanding balance."""

    event_type: Literal["payment"]
    event_id: str
    simulation_date: str
    customer_id: str
    payment_amount_pln: float       # amount paid, rounded to 2 dp
    balance_before_pln: float       # balance before this payment, rounded to 2 dp
    balance_after_pln: float        # balance after, rounded to 2 dp
    payment_behaviour: str          # "on_time" | "late" | "very_late"
    payment_terms_days: int         # customer.payment_terms_days
    invoice_ref: str                # shipment_confirmation_id or load_id


# ---------------------------------------------------------------------------
# Engine entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[PaymentEvent | CreditReleaseEvent]:
    """Process scheduled customer payments due on or before *sim_date*.

    Iterates ``state.scheduled_payments``, fires all entries whose
    ``due_date <= sim_date.isoformat()``, applies the behaviour-specific
    partial-pay multiplier, reduces customer balances, and removes fired
    entries from the schedule.

    After each payment, checks whether any credit-held orders for that
    customer can be released (exposure <= credit_limit).

    Args:
        state:    Mutable simulation state.  ``state.customer_balances`` is
                  reduced in place.  ``state.scheduled_payments`` is pruned.
                  ``state.open_orders`` may have statuses flipped from
                  ``"credit_hold"`` to ``"confirmed"``.
        config:   Validated simulation config.
        sim_date: Current simulation date.

    Returns:
        List of :class:`PaymentEvent` and :class:`CreditReleaseEvent`
        instances (empty on non-business days or when no payments are due).
    """
    if not is_business_day(sim_date):
        return []

    # Build customer lookup index
    customer_index = {c.customer_id: c for c in state.customers}

    events: list[PaymentEvent | CreditReleaseEvent] = []
    sim_date_str = sim_date.isoformat()

    # Collect entries to remove after iteration (avoid mutating while iterating)
    fired_indices: list[int] = []

    for idx, entry in enumerate(state.scheduled_payments):
        due_date: str = entry.get("due_date", "")
        if due_date > sim_date_str:
            # Not due yet
            continue

        customer_id: str = entry.get("customer_id", "")
        amount_pln: float = entry.get("amount_pln", 0.0)
        behaviour: str = entry.get("behaviour", "on_time")
        invoice_ref: str = entry.get("invoice_ref", "")

        customer = customer_index.get(customer_id)
        if customer is None:
            fired_indices.append(idx)
            continue

        balance = state.customer_balances.get(customer_id, 0.0)
        if balance <= 0.0:
            # Nothing to pay — entry is stale; remove it.
            fired_indices.append(idx)
            continue

        # Apply behaviour-specific partial-pay multiplier
        if behaviour == "on_time":
            fraction = 1.0
        elif behaviour == "late":
            fraction = state.rng.uniform(0.50, 0.80)
        else:  # very_late
            fraction = state.rng.uniform(0.10, 0.30)

        # The payment amount is the lesser of the scheduled invoice amount
        # (scaled by fraction) and the current outstanding balance.
        payment_amount = round(min(amount_pln * fraction, balance), 2)
        balance_before = round(balance, 2)
        balance_after = max(0.0, round(balance - payment_amount, 2))

        state.customer_balances[customer_id] = balance_after

        events.append(
            PaymentEvent(
                event_type="payment",
                event_id=str(uuid.uuid4()),
                simulation_date=sim_date_str,
                customer_id=customer_id,
                payment_amount_pln=payment_amount,
                balance_before_pln=balance_before,
                balance_after_pln=balance_after,
                payment_behaviour=behaviour,
                payment_terms_days=customer.payment_terms_days,
                invoice_ref=invoice_ref,
            )
        )

        fired_indices.append(idx)

        # Check if credit-held orders can now be released
        new_balance = balance_after
        open_orders_value = _compute_open_orders_value_pln(
            customer_id, state.open_orders, state.exchange_rates
        )
        exposure = new_balance + open_orders_value
        credit_limit: float = customer.credit_limit

        if exposure <= credit_limit:
            for order_id, order in state.open_orders.items():
                if order.get("customer_id") != customer_id:
                    continue
                if order.get("status") != "credit_hold":
                    continue
                order["status"] = "confirmed"
                events.append(
                    CreditReleaseEvent(
                        event_type="credit_release",
                        event_id=str(uuid.uuid4()),
                        simulation_date=sim_date_str,
                        order_id=order_id,
                        customer_id=customer_id,
                        customer_balance_pln=round(new_balance, 2),
                        open_orders_value_pln=round(open_orders_value, 2),
                        credit_limit_pln=round(credit_limit, 2),
                        exposure_pln=round(exposure, 2),
                    )
                )

    # Remove fired entries in reverse order to preserve indices
    for idx in reversed(fired_indices):
        del state.scheduled_payments[idx]

    return events
