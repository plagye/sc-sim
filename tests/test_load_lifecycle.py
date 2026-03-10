"""Tests for the load lifecycle engine (Step 24).

Covers:
- Empty active_loads -> no events
- load_created -> in_transit on a business day
- estimated_arrival date is set and valid after in_transit transition
- Reliability failure blocks transition and increments delay_days
- delay_days increment on failed reliability check
- out_for_delivery -> delivered transition
- actual_arrival set on delivery
- Delivered load stays in active_loads (not removed)
- Order lines updated to shipped on delivery
- Weekend processing (carriers run 7 days/week)
- sync_window inherited from load record
- BALTIC carrier transit range
- No double transition in a single run() call
- Terminal status loads are skipped
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from flowform.config import load_config
from flowform.engines import load_lifecycle
from flowform.engines.load_planning import LoadEvent
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"

# Patch target for carrier reliability (module-level function in load_lifecycle)
_RELIABILITY_PATCH = "flowform.engines.load_lifecycle._carrier_reliability"


@pytest.fixture()
def config():
    return load_config(_CONFIG_PATH)


@pytest.fixture()
def state(config, tmp_path):
    """Fresh simulation state isolated in a temp directory."""
    db = tmp_path / "sim.db"
    return SimulationState.from_new(config, db_path=db)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BIZ_DAY = date(2026, 1, 5)   # Monday — business day
_WEEKEND = date(2026, 1, 10)  # Saturday


def _make_load(
    state: SimulationState,
    load_id: str = "LOAD-9001",
    carrier_code: str = "RABEN",
    status: str = "load_created",
    planned_date: date | None = None,
    estimated_arrival: date | None = None,
    out_for_delivery_date: date | None = None,
    delivery_window: int = 1,
    sync_window: str = "08",
    order_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Insert a load dict into state.active_loads and return it."""
    if planned_date is None:
        planned_date = _BIZ_DAY
    if order_ids is None:
        order_ids = []

    record: dict[str, Any] = {
        "load_id": load_id,
        "status": status,
        "carrier_code": carrier_code,
        "source_warehouse_id": "W01",
        "planned_date": planned_date.isoformat(),
        "estimated_departure": planned_date.isoformat(),
        "estimated_arrival": (
            estimated_arrival.isoformat() if estimated_arrival else None
        ),
        "actual_departure": None,
        "actual_arrival": None,
        "delay_days": 0,
        "shipment_ids": ["SHP-9001"],
        "order_ids": order_ids,
        "total_weight_kg": 100.0,
        "weight_unit": "kg",
        "total_weight_reported": 100.0,
        "sync_window": sync_window,
        "priority": "standard",
        "customer_ids": ["CUST-0001"],
        "events": [],
    }

    if out_for_delivery_date is not None:
        record["out_for_delivery_date"] = out_for_delivery_date.isoformat()
        record["delivery_window"] = delivery_window

    state.active_loads[load_id] = record
    return record


def _make_order_with_allocated_lines(
    state: SimulationState,
    order_id: str,
    qty: int = 10,
) -> None:
    """Insert a minimal order with one allocated line into state.open_orders."""
    sku = state.catalog[0].sku
    state.open_orders[order_id] = {
        "event_type": "customer_order",
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "customer_id": state.customers[0].customer_id,
        "simulation_date": _BIZ_DAY.isoformat(),
        "status": "allocated",
        "priority": "standard",
        "currency": "PLN",
        "load_id": None,
        "shipment_id": None,
        "lines": [
            {
                "line_id": f"{order_id}-L1",
                "sku": sku,
                "quantity_ordered": qty,
                "quantity_allocated": qty,
                "quantity_shipped": 0,
                "quantity_backordered": 0,
                "unit_price": 500.0,
                "line_status": "allocated",
                "warehouse_id": "W01",
            }
        ],
    }


# ---------------------------------------------------------------------------
# Test 1: empty state -> no events
# ---------------------------------------------------------------------------


def test_empty_state_returns_no_events(state, config):
    """With no active loads, run() must return an empty list."""
    events = load_lifecycle.run(state, config, _BIZ_DAY)
    assert events == []


# ---------------------------------------------------------------------------
# Test 2: load_created -> in_transit on a business day
# ---------------------------------------------------------------------------


def test_load_created_transitions_to_in_transit_on_business_day(state, config):
    """A load_created load with planned_date == today on a business day must
    transition to in_transit and emit exactly one LoadEvent."""
    _make_load(state, planned_date=_BIZ_DAY)

    events = load_lifecycle.run(state, config, _BIZ_DAY)

    assert len(events) == 1
    ev = events[0]
    assert isinstance(ev, LoadEvent)
    assert ev.event_subtype == "in_transit"
    assert ev.status == "in_transit"

    load = state.active_loads["LOAD-9001"]
    assert load["status"] == "in_transit"
    assert load["actual_departure"] == _BIZ_DAY.isoformat()
    assert load["estimated_arrival"] is not None


# ---------------------------------------------------------------------------
# Test 3: estimated_arrival is a valid future date after in_transit
# ---------------------------------------------------------------------------


def test_in_transit_estimated_arrival_is_set_from_transit_days(state, config):
    """After transitioning to in_transit, estimated_arrival must be a valid ISO
    date on or after sim_date."""
    _make_load(state, planned_date=_BIZ_DAY)

    load_lifecycle.run(state, config, _BIZ_DAY)

    load = state.active_loads["LOAD-9001"]
    arrival_str = load["estimated_arrival"]
    assert arrival_str is not None

    arrival = date.fromisoformat(arrival_str)
    assert arrival >= _BIZ_DAY


# ---------------------------------------------------------------------------
# Test 4: reliability failure blocks transition
# ---------------------------------------------------------------------------


def test_no_transition_when_reliability_fails(state, config):
    """When carrier reliability returns 0.0, the in_transit -> out_for_delivery
    transition must be blocked and no event emitted."""
    # Seed the RNG so that random() produces a value reliably above 0.0
    state.rng.seed(42)

    _make_load(
        state,
        status="in_transit",
        estimated_arrival=_BIZ_DAY,  # arrival date reached — transition is due
    )

    # Patch the module-level reliability function to always return 0.0
    with patch(_RELIABILITY_PATCH, return_value=0.0):
        events = load_lifecycle.run(state, config, _BIZ_DAY)

    assert events == []
    assert state.active_loads["LOAD-9001"]["status"] == "in_transit"


# ---------------------------------------------------------------------------
# Test 5: delay_days increments on a delay
# ---------------------------------------------------------------------------


def test_delay_days_increments_on_delay(state, config):
    """After a blocked reliability check, delay_days must be 1."""
    state.rng.seed(42)

    load = _make_load(
        state,
        status="in_transit",
        estimated_arrival=_BIZ_DAY,
    )
    load["delay_days"] = 0

    with patch(_RELIABILITY_PATCH, return_value=0.0):
        load_lifecycle.run(state, config, _BIZ_DAY)

    assert state.active_loads["LOAD-9001"]["delay_days"] == 1


# ---------------------------------------------------------------------------
# Test 6: out_for_delivery -> delivered
# ---------------------------------------------------------------------------


def test_out_for_delivery_transitions_to_delivered(state, config):
    """A load in out_for_delivery with its delivery window elapsed must
    transition to delivered and emit one event."""
    ofd_date = _BIZ_DAY - timedelta(days=2)  # window=1, delivery_due is yesterday
    _make_load(
        state,
        status="out_for_delivery",
        out_for_delivery_date=ofd_date,
        delivery_window=1,
    )

    with patch(_RELIABILITY_PATCH, return_value=1.0):
        events = load_lifecycle.run(state, config, _BIZ_DAY)

    assert len(events) == 1
    ev = events[0]
    assert ev.event_subtype == "delivered"
    assert ev.status == "delivered"
    assert state.active_loads["LOAD-9001"]["status"] == "delivered"


# ---------------------------------------------------------------------------
# Test 7: actual_arrival is set on delivery
# ---------------------------------------------------------------------------


def test_delivered_sets_actual_arrival(state, config):
    """After delivery, load['actual_arrival'] must equal sim_date.isoformat()."""
    ofd_date = _BIZ_DAY - timedelta(days=2)
    _make_load(
        state,
        status="out_for_delivery",
        out_for_delivery_date=ofd_date,
        delivery_window=1,
    )

    with patch(_RELIABILITY_PATCH, return_value=1.0):
        load_lifecycle.run(state, config, _BIZ_DAY)

    assert state.active_loads["LOAD-9001"]["actual_arrival"] == _BIZ_DAY.isoformat()


# ---------------------------------------------------------------------------
# Test 8: delivered load stays in active_loads
# ---------------------------------------------------------------------------


def test_delivered_load_remains_in_active_loads(state, config):
    """After delivery, the load must still be present in state.active_loads."""
    ofd_date = _BIZ_DAY - timedelta(days=2)
    _make_load(
        state,
        status="out_for_delivery",
        out_for_delivery_date=ofd_date,
        delivery_window=1,
    )

    with patch(_RELIABILITY_PATCH, return_value=1.0):
        load_lifecycle.run(state, config, _BIZ_DAY)

    assert "LOAD-9001" in state.active_loads


# ---------------------------------------------------------------------------
# Test 9: order lines updated to shipped on delivery
# ---------------------------------------------------------------------------


def test_order_lines_set_to_shipped_on_delivery(state, config):
    """On delivery, all allocated order lines for orders in the load must be
    transitioned to line_status='shipped' with quantity_shipped set."""
    order_id = "ORD-LC001"
    _make_order_with_allocated_lines(state, order_id, qty=10)

    ofd_date = _BIZ_DAY - timedelta(days=2)
    _make_load(
        state,
        status="out_for_delivery",
        out_for_delivery_date=ofd_date,
        delivery_window=1,
        order_ids=[order_id],
    )

    with patch(_RELIABILITY_PATCH, return_value=1.0):
        load_lifecycle.run(state, config, _BIZ_DAY)

    order = state.open_orders[order_id]
    for line in order["lines"]:
        assert line["line_status"] == "shipped"
        assert line["quantity_shipped"] == 10


# ---------------------------------------------------------------------------
# Test 10: weekend processing — carriers run 7 days/week
# ---------------------------------------------------------------------------


def test_weekend_day_still_processes_loads(state, config):
    """An in_transit load whose estimated_arrival <= Saturday should transition
    to out_for_delivery on a Saturday (carriers operate weekends)."""
    _make_load(
        state,
        status="in_transit",
        estimated_arrival=_WEEKEND - timedelta(days=1),
    )

    with patch(_RELIABILITY_PATCH, return_value=1.0):
        events = load_lifecycle.run(state, config, _WEEKEND)

    assert len(events) == 1
    assert events[0].event_subtype == "out_for_delivery"


# ---------------------------------------------------------------------------
# Test 11: sync_window inherited from load record
# ---------------------------------------------------------------------------


def test_load_event_sync_window_inherited_from_load_record(state, config):
    """The emitted LoadEvent must carry the same sync_window as stored in the
    load record."""
    _make_load(state, sync_window="14", planned_date=_BIZ_DAY)

    events = load_lifecycle.run(state, config, _BIZ_DAY)

    assert len(events) == 1
    assert events[0].sync_window == "14"


# ---------------------------------------------------------------------------
# Test 12: BALTIC carrier has long transit range (4-7 days)
# ---------------------------------------------------------------------------


def test_baltic_load_has_long_transit_range(state, config):
    """For a BALTIC load, estimated_arrival must be >= sim_date + 4 days after
    the load_created -> in_transit transition."""
    state.rng.seed(0)

    _make_load(state, carrier_code="BALTIC", planned_date=_BIZ_DAY)

    load_lifecycle.run(state, config, _BIZ_DAY)

    load = state.active_loads["LOAD-9001"]
    arrival = date.fromisoformat(load["estimated_arrival"])
    assert arrival >= _BIZ_DAY + timedelta(days=4)


# ---------------------------------------------------------------------------
# Test 13: no double transition in a single run() call
# ---------------------------------------------------------------------------


def test_no_double_transition_in_one_day(state, config):
    """A load_created load must only reach in_transit after one run() call,
    not jump directly to out_for_delivery or beyond."""
    _make_load(state, status="load_created", planned_date=_BIZ_DAY)

    events = load_lifecycle.run(state, config, _BIZ_DAY)

    assert len(events) == 1
    assert events[0].event_subtype == "in_transit"
    assert state.active_loads["LOAD-9001"]["status"] == "in_transit"


# ---------------------------------------------------------------------------
# Test 14: terminal status loads are skipped
# ---------------------------------------------------------------------------


def test_terminal_status_loads_skipped(state, config):
    """A load already in 'delivered' status must not be processed and run()
    must return an empty list."""
    load = _make_load(state, status="delivered")
    load["actual_arrival"] = _BIZ_DAY.isoformat()

    events = load_lifecycle.run(state, config, _BIZ_DAY + timedelta(days=1))

    assert events == []
    assert state.active_loads["LOAD-9001"]["status"] == "delivered"


# ---------------------------------------------------------------------------
# Pre-Fix 12: delivered load awaiting POD must not be re-advanced
# ---------------------------------------------------------------------------


def test_delivered_load_awaiting_pod_not_re_advanced(state, config):
    """A load that reached 'delivered' status (and is in pending_pod) must
    remain at 'delivered' for subsequent load_lifecycle.run() calls.

    This verifies that the pod.py engine — not load_lifecycle — is responsible
    for removing the load from active_loads after POD fires.
    """
    load = _make_load(state, status="delivered")
    load["actual_arrival"] = _BIZ_DAY.isoformat()

    # Simulate load being added to pending_pod (as the lifecycle engine does on delivery)
    state.pending_pod["LOAD-9001"] = {
        "load_id": "LOAD-9001",
        "carrier_code": load["carrier_code"],
        "source_warehouse_id": load["source_warehouse_id"],
        "shipment_ids": load["shipment_ids"],
        "order_ids": load["order_ids"],
        "total_weight_kg": load["total_weight_kg"],
        "weight_unit": load["weight_unit"],
        "total_weight_reported": load["total_weight_reported"],
        "sync_window": load["sync_window"],
        "priority": load["priority"],
        "customer_ids": load["customer_ids"],
        "planned_date": load["planned_date"],
        "delivery_date": _BIZ_DAY.isoformat(),
        "pod_due_date": (_BIZ_DAY + timedelta(days=3)).isoformat(),
    }

    # Run load_lifecycle for 2 more days — load must stay at "delivered"
    day2 = _BIZ_DAY + timedelta(days=1)
    day3 = _BIZ_DAY + timedelta(days=2)
    for d in [day2, day3]:
        events = load_lifecycle.run(state, config, d)
        assert events == [], f"Expected no events for delivered load on {d}, got {events}"
        assert state.active_loads["LOAD-9001"]["status"] == "delivered", (
            f"Load should remain 'delivered' on {d}, not advance further"
        )


# ---------------------------------------------------------------------------
# Test 16 (Fix 1): state.allocated entry cleaned up when lines ship
# ---------------------------------------------------------------------------


def test_allocated_cleared_on_shipment(state, config):
    """On delivery, lines that transition to 'shipped' must be removed from
    state.allocated so stale allocation entries don't accumulate."""
    order_id = "ORD-LC-FIX1"
    _make_order_with_allocated_lines(state, order_id, qty=10)

    # Seed the allocation tracking entry (mirrors what allocation.py does)
    sku = state.open_orders[order_id]["lines"][0]["sku"]
    line_id = state.open_orders[order_id]["lines"][0]["line_id"]
    state.allocated[order_id] = {line_id: 10}

    ofd_date = _BIZ_DAY - timedelta(days=2)
    _make_load(
        state,
        load_id="LOAD-FIX1",
        status="out_for_delivery",
        out_for_delivery_date=ofd_date,
        delivery_window=1,
        order_ids=[order_id],
    )

    with patch(_RELIABILITY_PATCH, return_value=1.0):
        load_lifecycle.run(state, config, _BIZ_DAY)

    # The line should be shipped and the allocated entry cleaned up
    order = state.open_orders[order_id]
    assert order["lines"][0]["line_status"] == "shipped"

    # state.allocated entry for this order/line must be gone
    remaining = state.allocated.get(order_id, {})
    assert line_id not in remaining, (
        f"Expected allocated entry for {line_id} to be removed after shipment"
    )


# ---------------------------------------------------------------------------
# Test 17 (Fix 4): customer balance invoiced in PLN with currency conversion
# ---------------------------------------------------------------------------


def test_balance_invoiced_in_pln(state, config):
    """When a EUR-currency order is delivered, the customer balance must be
    updated in PLN (unit_price * qty * exchange_rate), not in EUR."""
    order_id = "ORD-LC-FIX4"
    sku = state.catalog[0].sku
    customer_id = state.customers[0].customer_id

    # Seed exchange rate: 1 EUR = 4.50 PLN
    state.exchange_rates["EUR"] = 4.50

    # Create a EUR order: 10 units at 100 EUR each → expected PLN = 4500.0
    state.open_orders[order_id] = {
        "event_type": "customer_order",
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "customer_id": customer_id,
        "simulation_date": _BIZ_DAY.isoformat(),
        "status": "allocated",
        "priority": "standard",
        "currency": "EUR",
        "load_id": None,
        "shipment_id": None,
        "lines": [
            {
                "line_id": f"{order_id}-L1",
                "sku": sku,
                "quantity_ordered": 10,
                "quantity_allocated": 10,
                "quantity_shipped": 0,
                "quantity_backordered": 0,
                "unit_price": 100.0,
                "line_status": "allocated",
                "warehouse_id": "W01",
            }
        ],
    }

    # Ensure customer starts with zero balance
    state.customer_balances[customer_id] = 0.0

    ofd_date = _BIZ_DAY - timedelta(days=2)
    _make_load(
        state,
        load_id="LOAD-FIX4",
        status="out_for_delivery",
        out_for_delivery_date=ofd_date,
        delivery_window=1,
        order_ids=[order_id],
    )

    with patch(_RELIABILITY_PATCH, return_value=1.0):
        load_lifecycle.run(state, config, _BIZ_DAY)

    expected_pln = 10 * 100.0 * 4.50  # = 4500.0
    actual_balance = state.customer_balances[customer_id]
    assert abs(actual_balance - expected_pln) < 0.01, (
        f"Expected balance {expected_pln} PLN, got {actual_balance}"
    )
