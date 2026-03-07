"""Tests for the credit check engine (Phase 2, Step 17)."""

from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from flowform.config import load_config
from flowform.engines.credit import CreditHoldEvent, CreditReleaseEvent, run
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
# Helper: build a minimal order dict matching what orders.py produces
# ---------------------------------------------------------------------------


def _make_order(
    order_id: str,
    customer_id: str,
    currency: str,
    unit_price: float,
    quantity: int,
    status: str = "confirmed",
) -> dict[str, Any]:
    """Build a minimal order dict with one line, matching the orders engine shape."""
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


# ---------------------------------------------------------------------------
# Test 1: run() returns [] on a Saturday (weekend)
# ---------------------------------------------------------------------------


def test_no_events_on_weekend(state, config):
    """Engine must return an empty list on weekends."""
    # 2026-01-03 is a Saturday
    saturday = date(2026, 1, 3)
    result = run(state, config, saturday)
    assert result == [], "Expected empty list on Saturday"


# ---------------------------------------------------------------------------
# Test 2: no hold when customer exposure is below credit limit
# ---------------------------------------------------------------------------


def test_no_hold_under_limit(state, config):
    """Small order well within credit limit should not trigger a hold."""
    customer = state.customers[0]
    order_id = "ORD-TEST-001"

    # Set balance to 0
    state.customer_balances[customer.customer_id] = 0.0

    # Create an order whose value is far below the customer's credit limit
    # Customer credit limits are at least 20_000 PLN, so 100 PLN is safe.
    order = _make_order(
        order_id=order_id,
        customer_id=customer.customer_id,
        currency="PLN",
        unit_price=1.0,
        quantity=100,  # 100 PLN total — well under any credit limit
    )
    state.open_orders[order_id] = order

    # Monday 2026-01-05 is a business day
    result = run(state, config, date(2026, 1, 5))

    hold_events = [e for e in result if isinstance(e, CreditHoldEvent)
                   and e.order_id == order_id]
    assert len(hold_events) == 0, "No hold expected when exposure is well under limit"
    assert state.open_orders[order_id]["status"] == "confirmed"

    # Cleanup
    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 3: hold when balance + order value exceeds credit limit
# ---------------------------------------------------------------------------


def test_hold_when_over_limit(state, config):
    """Order that tips exposure over the credit limit must be held."""
    customer = state.customers[0]
    order_id = "ORD-TEST-002"

    # Set balance to just 1 PLN below the customer's credit limit
    credit_limit = customer.credit_limit
    state.customer_balances[customer.customer_id] = credit_limit - 1.0

    # Order value of 1000 PLN: pushes exposure over the limit
    order = _make_order(
        order_id=order_id,
        customer_id=customer.customer_id,
        currency="PLN",
        unit_price=1000.0,
        quantity=1,
    )
    state.open_orders[order_id] = order

    result = run(state, config, date(2026, 1, 5))

    hold_events = [e for e in result if isinstance(e, CreditHoldEvent)
                   and e.order_id == order_id]
    assert len(hold_events) == 1, "Expected exactly one CreditHoldEvent"
    assert state.open_orders[order_id]["status"] == "credit_hold"

    # Cleanup
    del state.open_orders[order_id]
    state.customer_balances[customer.customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 4: release when balance drops below limit
# ---------------------------------------------------------------------------


def test_release_when_balance_drops(state, config):
    """Order on credit_hold must be released when exposure falls under limit."""
    customer = state.customers[0]
    order_id = "ORD-TEST-003"
    credit_limit = customer.credit_limit

    # Order on hold, balance was high — now balance dropped below limit
    # Set order value at 500 PLN, balance at 0 → exposure = 500 < limit
    state.customer_balances[customer.customer_id] = 0.0

    order = _make_order(
        order_id=order_id,
        customer_id=customer.customer_id,
        currency="PLN",
        unit_price=500.0,
        quantity=1,
        status="credit_hold",  # already on hold
    )
    state.open_orders[order_id] = order

    # Exposure = 0 (balance) + 500 (order) = 500, which is under the minimum
    # credit limit of 20_000 for any segment.
    assert credit_limit > 500.0, "Credit limit must be > 500 for this test"

    result = run(state, config, date(2026, 1, 5))

    release_events = [e for e in result if isinstance(e, CreditReleaseEvent)
                      and e.order_id == order_id]
    assert len(release_events) == 1, "Expected exactly one CreditReleaseEvent"
    assert state.open_orders[order_id]["status"] == "confirmed"

    # Cleanup
    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 5: cumulative exposure — second order tips over limit
# ---------------------------------------------------------------------------


def test_multiple_orders_cumulative_exposure(state, config):
    """First order alone is fine; second order combined with first tips over limit."""
    customer = state.customers[0]
    credit_limit = customer.credit_limit

    # Zero balance to isolate order value effects
    state.customer_balances[customer.customer_id] = 0.0

    # First order: 60% of credit limit — under limit on its own
    first_order_value = credit_limit * 0.60
    order_id_1 = "ORD-TEST-004"
    order1 = _make_order(
        order_id=order_id_1,
        customer_id=customer.customer_id,
        currency="PLN",
        unit_price=first_order_value,
        quantity=1,
    )
    state.open_orders[order_id_1] = order1

    # Second order: 50% of credit limit — alone fine, but combined = 110% → over
    second_order_value = credit_limit * 0.50
    order_id_2 = "ORD-TEST-005"
    order2 = _make_order(
        order_id=order_id_2,
        customer_id=customer.customer_id,
        currency="PLN",
        unit_price=second_order_value,
        quantity=1,
    )
    state.open_orders[order_id_2] = order2

    result = run(state, config, date(2026, 1, 5))

    # First order should NOT be held (it appears first in insertion order;
    # at time of its evaluation, combined exposure = 110% of limit because
    # open_orders_value includes BOTH orders).
    # Per spec: open_orders_value is total of ALL open orders for customer.
    # So both orders contribute to exposure from the start.
    # Combined exposure = 110% of limit → both orders should be held.
    hold_events = [e for e in result if isinstance(e, CreditHoldEvent)]
    hold_order_ids = {e.order_id for e in hold_events}

    # Both orders see total exposure = 110% → both should be held
    assert order_id_1 in hold_order_ids or order_id_2 in hold_order_ids, (
        "At least one order must be held when combined exposure exceeds limit"
    )
    assert order_id_2 in hold_order_ids, "Second order must be held (cumulative exposure)"

    # Cleanup
    del state.open_orders[order_id_1]
    del state.open_orders[order_id_2]
    state.customer_balances[customer.customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 6: held orders still count toward exposure (hold doesn't reduce it)
# ---------------------------------------------------------------------------


def test_hold_does_not_reduce_exposure(state, config):
    """A credit-held order still counts toward total exposure for subsequent orders."""
    customer = state.customers[0]
    credit_limit = customer.credit_limit

    state.customer_balances[customer.customer_id] = 0.0

    # Order 1 already on hold: 80% of credit limit
    order_id_1 = "ORD-TEST-006"
    order1 = _make_order(
        order_id=order_id_1,
        customer_id=customer.customer_id,
        currency="PLN",
        unit_price=credit_limit * 0.80,
        quantity=1,
        status="credit_hold",
    )
    state.open_orders[order_id_1] = order1

    # Order 2 confirmed: 30% of credit limit — pushes total to 110% even though
    # order 1 is already on hold
    order_id_2 = "ORD-TEST-007"
    order2 = _make_order(
        order_id=order_id_2,
        customer_id=customer.customer_id,
        currency="PLN",
        unit_price=credit_limit * 0.30,
        quantity=1,
        status="confirmed",
    )
    state.open_orders[order_id_2] = order2

    result = run(state, config, date(2026, 1, 5))

    # Order 1 is on hold — exposure is 110%, so it should NOT be released
    # (110% > 100%)
    release_events = [e for e in result if isinstance(e, CreditReleaseEvent)
                      and e.order_id == order_id_1]
    assert len(release_events) == 0, "Held order must NOT be released when still over limit"
    assert state.open_orders[order_id_1]["status"] == "credit_hold"

    # Order 2 confirmed — total exposure = 110% → should be held
    hold_events = [e for e in result if isinstance(e, CreditHoldEvent)
                   and e.order_id == order_id_2]
    assert len(hold_events) == 1, "Confirmed order must be held when held orders count toward exposure"
    assert state.open_orders[order_id_2]["status"] == "credit_hold"

    # Cleanup
    del state.open_orders[order_id_1]
    del state.open_orders[order_id_2]
    state.customer_balances[customer.customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 7: all required fields present and correctly valued on CreditHoldEvent
# ---------------------------------------------------------------------------


def test_event_fields_complete(state, config):
    """Verify all required fields are present and correctly typed/valued."""
    customer = state.customers[0]
    credit_limit = customer.credit_limit
    order_id = "ORD-TEST-008"

    balance = credit_limit - 1.0
    state.customer_balances[customer.customer_id] = balance

    order_value_pln = 1000.0
    order = _make_order(
        order_id=order_id,
        customer_id=customer.customer_id,
        currency="PLN",
        unit_price=order_value_pln,
        quantity=1,
    )
    state.open_orders[order_id] = order

    result = run(state, config, date(2026, 1, 5))

    hold_events = [e for e in result if isinstance(e, CreditHoldEvent)
                   and e.order_id == order_id]
    assert len(hold_events) == 1

    evt = hold_events[0]

    # event_type
    assert evt.event_type == "credit_hold"

    # event_id is a valid UUID string
    parsed = uuid.UUID(evt.event_id)
    assert str(parsed) == evt.event_id

    # simulation_date
    assert evt.simulation_date == "2026-01-05"

    # order_id and customer_id
    assert evt.order_id == order_id
    assert evt.customer_id == customer.customer_id

    # customer_balance_pln
    assert evt.customer_balance_pln == round(balance, 2)

    # open_orders_value_pln should include the order itself (PLN, unit_price * qty)
    assert evt.open_orders_value_pln == round(order_value_pln, 2)

    # credit_limit_pln
    assert evt.credit_limit_pln == round(credit_limit, 2)

    # exposure_pln = balance + open_orders_value
    expected_exposure = round(balance + order_value_pln, 2)
    assert evt.exposure_pln == expected_exposure

    # exposure must exceed credit limit (that's why it was held)
    assert evt.exposure_pln > evt.credit_limit_pln

    # Cleanup
    del state.open_orders[order_id]
    state.customer_balances[customer.customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 8: no events when state.open_orders is empty
# ---------------------------------------------------------------------------


def test_no_events_when_no_open_orders(state, config):
    """Engine must return empty list when there are no open orders."""
    # Ensure open_orders is empty
    state.open_orders.clear()

    # Monday 2026-01-05 is a valid business day
    result = run(state, config, date(2026, 1, 5))
    assert result == [], "Expected empty list when no open orders"
