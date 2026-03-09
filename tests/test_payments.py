"""Tests for the payment processing engine (Phase 4, Step 32)."""

from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from flowform.config import load_config
from flowform.engines.credit import CreditReleaseEvent
from flowform.engines.payments import PaymentEvent, run
from flowform.state import SimulationState


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def config():
    """Load the real config from config.yaml."""
    return load_config(Path("/home/coder/sc-sim/config.yaml"))


@pytest.fixture()
def state(config, tmp_path: Path) -> SimulationState:
    """Fresh SimulationState backed by a temp DB."""
    return SimulationState.from_new(config, db_path=tmp_path / "sim.db")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BUSINESS_DAY = date(2026, 1, 5)   # Monday
_SATURDAY = date(2026, 1, 3)
_HOLIDAY = date(2026, 1, 1)        # New Year's Day (Polish holiday)


def _make_order(
    order_id: str,
    customer_id: str,
    unit_price: float,
    quantity: int,
    currency: str = "PLN",
    status: str = "confirmed",
) -> dict[str, Any]:
    """Build a minimal order dict matching orders engine shape."""
    return {
        "event_type": "customer_order",
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "customer_id": customer_id,
        "simulation_date": "2026-01-05",
        "timestamp": "2026-01-05T10:00:00",
        "currency": currency,
        "priority": "standard",
        "status": status,
        "requested_delivery_date": "2026-01-20",
        "lines": [
            {
                "line_id": f"{order_id}-L1",
                "sku": "FF-GA15-CS-PN16-FL-MN",
                "quantity_ordered": quantity,
                "quantity_allocated": 0,
                "quantity_shipped": 0,
                "quantity_backordered": 0,
                "unit_price": unit_price,
                "line_status": "open",
            }
        ],
    }


def _force_payment_for(state: SimulationState, customer_id: str, behaviour: str) -> None:
    """Seed the RNG so the next run() call produces the desired payment behaviour.

    This works by replacing state.rng with a deterministic object whose
    random() calls return controlled values.
    """
    import random

    class _FixedRNG:
        """Returns a fixed sequence then falls back to a real RNG."""

        def __init__(self, values: list[float], fallback_seed: int = 0) -> None:
            self._values = list(values)
            self._idx = 0
            self._fallback = random.Random(fallback_seed)

        def random(self) -> float:
            if self._idx < len(self._values):
                v = self._values[self._idx]
                self._idx += 1
                return v
            return self._fallback.random()

        def uniform(self, a: float, b: float) -> float:
            if self._idx < len(self._values):
                v = self._values[self._idx]
                self._idx += 1
                # scale the value to the [a, b] range
                return a + v * (b - a)
            return self._fallback.uniform(a, b)

    # daily_prob = 1 / payment_terms_days.  Use 0.0 to always trigger payment.
    # Then the behaviour roll:
    #   on_time:    roll < 0.82
    #   late:       0.82 <= roll < 0.97
    #   very_late:  roll >= 0.97
    if behaviour == "on_time":
        behaviour_roll = 0.50
    elif behaviour == "late":
        behaviour_roll = 0.85
    else:  # very_late
        behaviour_roll = 0.99

    # 0.0 < daily_prob for any customer, so 0.0 always triggers
    state.rng = _FixedRNG([0.0, behaviour_roll, 0.65])


# ---------------------------------------------------------------------------
# Test 1: no payment on weekend
# ---------------------------------------------------------------------------


def test_no_payment_on_weekend(state, config):
    """Engine must return [] on Saturday and leave balances unchanged."""
    customer = state.customers[0]
    state.customer_balances[customer.customer_id] = 5000.0

    result = run(state, config, _SATURDAY)

    assert result == []
    assert state.customer_balances[customer.customer_id] == 5000.0

    # Cleanup
    state.customer_balances[customer.customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 2: no payment on holiday
# ---------------------------------------------------------------------------


def test_no_payment_on_holiday(state, config):
    """Engine must return [] on Polish public holiday."""
    customer = state.customers[0]
    state.customer_balances[customer.customer_id] = 5000.0

    result = run(state, config, _HOLIDAY)

    assert result == []
    assert state.customer_balances[customer.customer_id] == 5000.0

    # Cleanup
    state.customer_balances[customer.customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 3: no payment when all balances zero
# ---------------------------------------------------------------------------


def test_no_payment_when_all_balances_zero(state, config):
    """Engine must return [] when no customer has a positive balance."""
    # Ensure every customer has a zero balance
    for customer_id in list(state.customer_balances.keys()):
        state.customer_balances[customer_id] = 0.0

    result = run(state, config, _BUSINESS_DAY)

    assert result == []


# ---------------------------------------------------------------------------
# Test 4: PaymentEvent schema correctness
# ---------------------------------------------------------------------------


def test_payment_event_schema(state, config):
    """A PaymentEvent returned must have correct fields and sensible values."""
    customer = state.customers[0]
    customer_id = customer.customer_id
    state.customer_balances[customer_id] = 50000.0

    _force_payment_for(state, customer_id, "on_time")

    result = run(state, config, _BUSINESS_DAY)

    payment_events = [e for e in result if isinstance(e, PaymentEvent)]
    assert len(payment_events) >= 1, "Expected at least one PaymentEvent"

    evt = next(e for e in payment_events if e.customer_id == customer_id)

    assert evt.event_type == "payment"
    assert evt.customer_id == customer_id
    assert evt.balance_before_pln > 0.0
    assert evt.balance_after_pln < evt.balance_before_pln
    assert evt.balance_after_pln >= 0.0
    assert evt.payment_amount_pln > 0.0
    assert isinstance(evt.payment_terms_days, int)
    assert evt.simulation_date == _BUSINESS_DAY.isoformat()

    # Cleanup
    state.customer_balances[customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 5: on_time payment clears full balance
# ---------------------------------------------------------------------------


def test_on_time_payment_clears_full_balance(state, config):
    """An on_time payment must pay 100% of the outstanding balance."""
    customer = state.customers[0]
    customer_id = customer.customer_id
    initial_balance = 12345.67
    state.customer_balances[customer_id] = initial_balance

    _force_payment_for(state, customer_id, "on_time")

    result = run(state, config, _BUSINESS_DAY)

    payment_events = [
        e for e in result
        if isinstance(e, PaymentEvent) and e.customer_id == customer_id
    ]
    assert len(payment_events) == 1

    evt = payment_events[0]
    assert evt.payment_behaviour == "on_time"
    assert evt.balance_after_pln == 0.0
    assert state.customer_balances[customer_id] == 0.0


# ---------------------------------------------------------------------------
# Test 6: late payment pays 50–80% of balance
# ---------------------------------------------------------------------------


def test_late_payment_partial_fraction(state, config):
    """A late payment must pay between 50% and 80% of the outstanding balance."""
    customer = state.customers[0]
    customer_id = customer.customer_id
    initial_balance = 20000.0
    state.customer_balances[customer_id] = initial_balance

    _force_payment_for(state, customer_id, "late")

    result = run(state, config, _BUSINESS_DAY)

    payment_events = [
        e for e in result
        if isinstance(e, PaymentEvent) and e.customer_id == customer_id
    ]
    assert len(payment_events) == 1

    evt = payment_events[0]
    assert evt.payment_behaviour == "late"

    min_expected = round(0.50 * initial_balance, 2)
    max_expected = round(0.80 * initial_balance, 2)
    assert min_expected <= evt.payment_amount_pln <= max_expected, (
        f"Late payment {evt.payment_amount_pln} not in [{min_expected}, {max_expected}]"
    )
    assert evt.balance_after_pln > 0.0

    # Cleanup
    state.customer_balances[customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 7: very_late payment pays 10–30% of balance
# ---------------------------------------------------------------------------


def test_very_late_payment_partial_fraction(state, config):
    """A very_late payment must pay between 10% and 30% of the balance."""
    customer = state.customers[0]
    customer_id = customer.customer_id
    initial_balance = 30000.0
    state.customer_balances[customer_id] = initial_balance

    _force_payment_for(state, customer_id, "very_late")

    result = run(state, config, _BUSINESS_DAY)

    payment_events = [
        e for e in result
        if isinstance(e, PaymentEvent) and e.customer_id == customer_id
    ]
    assert len(payment_events) == 1

    evt = payment_events[0]
    assert evt.payment_behaviour == "very_late"

    min_expected = round(0.10 * initial_balance, 2)
    max_expected = round(0.30 * initial_balance, 2)
    assert min_expected <= evt.payment_amount_pln <= max_expected, (
        f"Very late payment {evt.payment_amount_pln} not in [{min_expected}, {max_expected}]"
    )

    # Cleanup
    state.customer_balances[customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 8: balance never goes negative
# ---------------------------------------------------------------------------


def test_balance_never_goes_negative(state, config):
    """Even a very small positive balance must not result in a negative balance."""
    customer = state.customers[0]
    customer_id = customer.customer_id
    state.customer_balances[customer_id] = 0.01

    _force_payment_for(state, customer_id, "on_time")

    run(state, config, _BUSINESS_DAY)

    assert state.customer_balances[customer_id] >= 0.0


# ---------------------------------------------------------------------------
# Test 9: credit hold released after payment reduces exposure below limit
# ---------------------------------------------------------------------------


def test_credit_hold_released_after_payment(state, config):
    """Credit-held orders are released when on_time payment drops exposure below limit."""
    customer = state.customers[0]
    customer_id = customer.customer_id
    credit_limit = customer.credit_limit

    # Set balance just above limit with a small held order value
    small_order_value = 100.0
    state.customer_balances[customer_id] = credit_limit + 1000.0

    order_id = "ORD-PAY-HOLD-001"
    order = _make_order(
        order_id=order_id,
        customer_id=customer_id,
        unit_price=small_order_value,
        quantity=1,
        status="credit_hold",
    )
    state.open_orders[order_id] = order

    # Force on_time payment that wipes the full balance
    _force_payment_for(state, customer_id, "on_time")

    result = run(state, config, _BUSINESS_DAY)

    payment_events = [e for e in result if isinstance(e, PaymentEvent)]
    release_events = [e for e in result if isinstance(e, CreditReleaseEvent)]

    assert len(payment_events) >= 1
    # After on_time payment, balance = 0; exposure = small_order_value < limit
    assert len(release_events) >= 1, "Credit release expected after balance cleared"
    assert any(e.order_id == order_id for e in release_events)
    assert state.open_orders[order_id]["status"] == "confirmed"

    # Cleanup
    del state.open_orders[order_id]
    state.customer_balances[customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 10: credit hold NOT released if exposure still over limit after partial payment
# ---------------------------------------------------------------------------


def test_credit_hold_not_released_if_still_over_limit(state, config):
    """Partial late payment that still leaves exposure above limit must not release holds."""
    customer = state.customers[0]
    customer_id = customer.customer_id
    credit_limit = customer.credit_limit

    # Balance much higher than the limit so even a 80% payment leaves balance > limit
    state.customer_balances[customer_id] = credit_limit * 10.0

    order_id = "ORD-PAY-HOLD-002"
    order = _make_order(
        order_id=order_id,
        customer_id=customer_id,
        unit_price=100.0,
        quantity=1,
        status="credit_hold",
    )
    state.open_orders[order_id] = order

    _force_payment_for(state, customer_id, "late")

    result = run(state, config, _BUSINESS_DAY)

    release_events = [
        e for e in result
        if isinstance(e, CreditReleaseEvent) and e.order_id == order_id
    ]
    assert len(release_events) == 0, "Must NOT release when exposure still over limit"
    assert state.open_orders[order_id]["status"] == "credit_hold"

    # Cleanup
    del state.open_orders[order_id]
    state.customer_balances[customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 11: only customers with positive balance can pay
# ---------------------------------------------------------------------------


def test_only_customers_with_positive_balance_can_pay(state, config):
    """Customers with zero balance must never appear in the event list."""
    customer_zero = state.customers[0]
    customer_pos = state.customers[1]

    state.customer_balances[customer_zero.customer_id] = 0.0
    state.customer_balances[customer_pos.customer_id] = 10000.0

    # Force payment for the positive-balance customer
    _force_payment_for(state, customer_pos.customer_id, "on_time")

    result = run(state, config, _BUSINESS_DAY)

    payment_events = [e for e in result if isinstance(e, PaymentEvent)]
    paying_customers = {e.customer_id for e in payment_events}

    assert customer_zero.customer_id not in paying_customers, (
        "Customer with zero balance must not appear in payment events"
    )

    # Cleanup
    state.customer_balances[customer_pos.customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 12: reproducibility — same seed produces same results
# ---------------------------------------------------------------------------


def test_reproducibility(state, config):
    """Running twice from the same RNG seed must produce identical payment amounts."""
    import random

    customer = state.customers[0]
    customer_id = customer.customer_id
    initial_balance = 25000.0

    def run_once() -> list[float]:
        state.customer_balances[customer_id] = initial_balance
        state.rng = random.Random(42)
        result = run(state, config, _BUSINESS_DAY)
        return [e.payment_amount_pln for e in result if isinstance(e, PaymentEvent)]

    first = run_once()
    # Reset balance to initial for second run
    second = run_once()

    assert first == second, f"Reproducibility failed: {first} != {second}"

    # Cleanup
    state.customer_balances[customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 13: PaymentEvent round-trips via model_dump
# ---------------------------------------------------------------------------


def test_payment_event_roundtrips_via_model_dump(state, config):
    """PaymentEvent.model_dump() must include all required keys with correct types."""
    customer = state.customers[0]
    customer_id = customer.customer_id
    state.customer_balances[customer_id] = 8000.0

    _force_payment_for(state, customer_id, "on_time")

    result = run(state, config, _BUSINESS_DAY)

    payment_events = [e for e in result if isinstance(e, PaymentEvent)]
    assert len(payment_events) >= 1

    d = payment_events[0].model_dump()

    expected_keys = {
        "event_type",
        "event_id",
        "simulation_date",
        "customer_id",
        "payment_amount_pln",
        "balance_before_pln",
        "balance_after_pln",
        "payment_behaviour",
        "payment_terms_days",
    }
    assert expected_keys.issubset(d.keys()), (
        f"Missing keys in model_dump: {expected_keys - d.keys()}"
    )
    assert d["event_type"] == "payment"
    assert isinstance(d["payment_amount_pln"], float)
    assert isinstance(d["payment_terms_days"], int)

    # Cleanup
    state.customer_balances[customer_id] = 0.0
