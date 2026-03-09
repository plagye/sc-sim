"""Payment processing engine for FlowForm Industries.

Simulates customers paying outstanding invoices, reducing customer balances
and potentially releasing credit holds when exposure drops below the limit.

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


# ---------------------------------------------------------------------------
# Engine entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[PaymentEvent | CreditReleaseEvent]:
    """Process customer payments on *sim_date*.

    For each customer with a positive outstanding balance, probabilistically
    decides whether a payment arrives today. Classifies payments as on_time,
    late, or very_late, updating balances accordingly. After each payment,
    checks whether any credit-held orders for that customer can be released.

    Args:
        state:    Mutable simulation state. ``state.customer_balances`` is
                  reduced in place. ``state.open_orders`` may have order
                  statuses flipped from ``"credit_hold"`` to ``"confirmed"``.
        config:   Validated simulation config.
        sim_date: Current simulation date.

    Returns:
        List of :class:`PaymentEvent` and :class:`CreditReleaseEvent`
        instances (empty on non-business days or when no payments occur).
    """
    if not is_business_day(sim_date):
        return []

    # Build customer lookup index
    customer_index = {c.customer_id: c for c in state.customers}

    on_time_prob = config.payments.on_time_probability
    late_prob = config.payments.late_probability
    # very_late_prob is implicit (remainder), no need to read it separately

    events: list[PaymentEvent | CreditReleaseEvent] = []

    for customer_id, balance in list(state.customer_balances.items()):
        if balance <= 0.0:
            continue

        customer = customer_index.get(customer_id)
        if customer is None:
            continue

        # Daily payment probability derived from payment terms
        payment_terms_days: int = customer.payment_terms_days
        daily_prob = 1.0 / payment_terms_days

        # Decide whether this customer pays today
        if state.rng.random() >= daily_prob:
            continue

        # Determine payment behaviour
        roll = state.rng.random()
        if roll < on_time_prob:
            behaviour = "on_time"
            fraction = 1.0
        elif roll < on_time_prob + late_prob:
            behaviour = "late"
            fraction = state.rng.uniform(0.50, 0.80)
        else:
            behaviour = "very_late"
            fraction = state.rng.uniform(0.10, 0.30)

        # Calculate and apply payment
        balance_before = round(balance, 2)
        payment_amount = round(balance * fraction, 2)
        balance_after = max(0.0, round(balance - payment_amount, 2))

        state.customer_balances[customer_id] = balance_after

        events.append(
            PaymentEvent(
                event_type="payment",
                event_id=str(uuid.uuid4()),
                simulation_date=sim_date.isoformat(),
                customer_id=customer_id,
                payment_amount_pln=payment_amount,
                balance_before_pln=balance_before,
                balance_after_pln=balance_after,
                payment_behaviour=behaviour,
                payment_terms_days=payment_terms_days,
            )
        )

        # Check if credit-held orders can now be released
        new_balance = balance_after
        open_orders_value = _compute_open_orders_value_pln(
            customer_id, state.open_orders, state.exchange_rates
        )
        exposure = new_balance + open_orders_value
        credit_limit: float = customer.credit_limit

        if exposure <= credit_limit:
            # Release all credit-held orders for this customer
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
                        simulation_date=sim_date.isoformat(),
                        order_id=order_id,
                        customer_id=customer_id,
                        customer_balance_pln=round(new_balance, 2),
                        open_orders_value_pln=round(open_orders_value, 2),
                        credit_limit_pln=round(credit_limit, 2),
                        exposure_pln=round(exposure, 2),
                    )
                )

    return events
