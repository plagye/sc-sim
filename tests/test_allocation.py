"""Tests for the allocation engine (Phase 2, Step 19).

Covers:
1. Returns [] on weekend
2. Returns [] on holiday
3. Full allocation when stock sufficient
4. Zero stock → BackorderEvent, inventory unchanged
5. Partial fill → AllocationEvent + BackorderEvent, correct quantities
6. Priority ordering: critical before standard with limited stock
7. Backorder escalation: >14 days standard → express
8. Backorder cancellation: >30 days, forced 100% dice roll
9. Credit-hold orders skipped entirely
10. Invariant: allocated + shipped + backordered <= ordered always holds
11. Multiple orders, multiple lines — per-SKU inventory accounting
12. backorder_days increments each day for waiting lines
"""

from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from flowform.config import load_config
from flowform.engines.allocation import (
    BackorderCancellationEvent,
    BackorderEvent,
    run,
)
from flowform.engines.inventory_movements import InventoryMovementEvent
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
# Helper: build minimal order / line dicts
# ---------------------------------------------------------------------------

_SKU_A = "FF-GA15-CS-PN16-FL-MN"
_SKU_B = "FF-BA25-CS-PN16-FL-MN"
_WAREHOUSE = "W01"
_BUSINESS_DAY = date(2026, 1, 5)    # Monday
_SATURDAY = date(2026, 1, 3)
_HOLIDAY = date(2026, 1, 1)         # New Year's Day


def _make_order(
    order_id: str,
    customer_id: str,
    *,
    priority: str = "standard",
    status: str = "confirmed",
    requested_delivery_date: str = "2026-01-20",
    simulation_date: str = "2026-01-05",
    lines: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a minimal order dict matching the shape produced by orders.py."""
    return {
        "event_type": "customer_order",
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "customer_id": customer_id,
        "simulation_date": simulation_date,
        "timestamp": f"{simulation_date}T10:00:00",
        "currency": "PLN",
        "priority": priority,
        "status": status,
        "requested_delivery_date": requested_delivery_date,
        "lines": lines or [],
    }


def _make_line(
    order_id: str,
    line_num: int,
    sku: str,
    quantity_ordered: int,
    *,
    quantity_allocated: int = 0,
    quantity_shipped: int = 0,
    quantity_backordered: int = 0,
    line_status: str = "open",
    backorder_days: int = 0,
) -> dict[str, Any]:
    """Build a minimal order line dict."""
    return {
        "line_id": f"{order_id}-L{line_num}",
        "sku": sku,
        "quantity_ordered": quantity_ordered,
        "quantity_allocated": quantity_allocated,
        "quantity_shipped": quantity_shipped,
        "quantity_backordered": quantity_backordered,
        "unit_price": 100.0,
        "line_status": line_status,
        "backorder_days": backorder_days,
    }


# ---------------------------------------------------------------------------
# Test 1: Returns [] on weekend
# ---------------------------------------------------------------------------


def test_returns_empty_on_weekend(state, config):
    """Engine must return [] on a Saturday."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-2026-900001"
    line = _make_line(order_id, 1, _SKU_A, 10)
    state.open_orders[order_id] = _make_order(order_id, customer_id, lines=[line])
    state.inventory.setdefault(_WAREHOUSE, {})[_SKU_A] = 100

    result = run(state, config, _SATURDAY)

    assert result == [], f"Expected [] on Saturday, got {result}"

    # Cleanup
    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 2: Returns [] on holiday
# ---------------------------------------------------------------------------


def test_returns_empty_on_holiday(state, config):
    """Engine must return [] on a Polish public holiday."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-2026-900002"
    line = _make_line(order_id, 1, _SKU_A, 10)
    state.open_orders[order_id] = _make_order(order_id, customer_id, lines=[line])
    state.inventory.setdefault(_WAREHOUSE, {})[_SKU_A] = 100

    result = run(state, config, _HOLIDAY)

    assert result == [], f"Expected [] on holiday, got {result}"

    # Cleanup
    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 3: Full allocation when stock sufficient
# ---------------------------------------------------------------------------


def test_full_allocation_when_stock_sufficient(state, config):
    """When inventory >= quantity_ordered, line is fully allocated.

    Expects: one InventoryMovementEvent (pick), inventory decreases, line
    status → "allocated".
    """
    customer_id = state.customers[0].customer_id
    order_id = "ORD-2026-900003"

    # Set up inventory: 100 units on hand
    state.inventory.setdefault(_WAREHOUSE, {})[_SKU_A] = 100
    initial_inv = state.inventory[_WAREHOUSE][_SKU_A]

    line = _make_line(order_id, 1, _SKU_A, 50)  # order 50 units
    order = _make_order(order_id, customer_id, lines=[line])
    state.open_orders[order_id] = order

    events = run(state, config, _BUSINESS_DAY)

    # Filter to pick events for this order
    picks = [
        e for e in events
        if isinstance(e, InventoryMovementEvent)
        and e.movement_type == "pick"
        and e.order_id == order_id
    ]

    assert len(picks) == 1, f"Expected 1 pick event, got {len(picks)}"
    pick = picks[0]
    assert pick.sku == _SKU_A
    assert pick.quantity == 50
    assert pick.quantity_direction == "out"
    assert pick.warehouse_id == _WAREHOUSE

    # Inventory must have decreased
    assert state.inventory[_WAREHOUSE][_SKU_A] == initial_inv - 50

    # Line status must be "allocated"
    updated_line = state.open_orders[order_id]["lines"][0]
    assert updated_line["line_status"] == "allocated"
    assert updated_line["quantity_allocated"] == 50
    assert updated_line["quantity_backordered"] == 0

    # Order status must be "allocated"
    assert state.open_orders[order_id]["status"] == "allocated"

    # Cleanup
    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 4: Zero stock → BackorderEvent, inventory unchanged
# ---------------------------------------------------------------------------


def test_zero_stock_creates_backorder(state, config):
    """When inventory == 0, a BackorderEvent is emitted; inventory unchanged."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-2026-900004"

    # Ensure zero inventory for this SKU
    state.inventory.setdefault(_WAREHOUSE, {})[_SKU_B] = 0

    line = _make_line(order_id, 1, _SKU_B, 30)
    state.open_orders[order_id] = _make_order(order_id, customer_id, lines=[line])

    events = run(state, config, _BUSINESS_DAY)

    backorders = [e for e in events if isinstance(e, BackorderEvent) and e.order_id == order_id]
    picks = [e for e in events if isinstance(e, InventoryMovementEvent) and e.order_id == order_id]

    assert len(backorders) == 1, f"Expected 1 BackorderEvent, got {len(backorders)}"
    assert len(picks) == 0, "No pick should be emitted when stock is zero"

    bo = backorders[0]
    assert bo.backorder_reason == "no_stock"
    assert bo.quantity_backordered == 30
    assert bo.quantity_allocated == 0

    # Inventory unchanged
    assert state.inventory[_WAREHOUSE].get(_SKU_B, 0) == 0

    # Line status
    updated_line = state.open_orders[order_id]["lines"][0]
    assert updated_line["line_status"] == "backordered"
    assert updated_line["quantity_backordered"] == 30

    # Cleanup
    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 5: Partial fill → AllocationEvent + BackorderEvent
# ---------------------------------------------------------------------------


def test_partial_fill(state, config):
    """When stock < need, partial fill: pick + backorder events with correct quantities."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-2026-900005"

    # Set 15 units available, order 40
    state.inventory.setdefault(_WAREHOUSE, {})[_SKU_A] = 15

    line = _make_line(order_id, 1, _SKU_A, 40)
    state.open_orders[order_id] = _make_order(order_id, customer_id, lines=[line])

    events = run(state, config, _BUSINESS_DAY)

    picks = [
        e for e in events
        if isinstance(e, InventoryMovementEvent)
        and e.movement_type == "pick"
        and e.order_id == order_id
    ]
    backorders = [
        e for e in events
        if isinstance(e, BackorderEvent)
        and e.order_id == order_id
    ]

    assert len(picks) == 1, f"Expected 1 pick event, got {len(picks)}"
    assert len(backorders) == 1, f"Expected 1 backorder event, got {len(backorders)}"

    pick = picks[0]
    bo = backorders[0]

    assert pick.quantity == 15, f"Pick quantity should be 15 (available), got {pick.quantity}"
    assert bo.quantity_backordered == 25, f"Backorder qty should be 25 (40-15), got {bo.quantity_backordered}"
    assert bo.backorder_reason == "insufficient_stock"

    # Inventory should now be 0
    assert state.inventory[_WAREHOUSE][_SKU_A] == 0

    # Line status: partially_allocated
    updated_line = state.open_orders[order_id]["lines"][0]
    assert updated_line["line_status"] == "partially_allocated"
    assert updated_line["quantity_allocated"] == 15
    assert updated_line["quantity_backordered"] == 25

    # Cleanup
    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 6: Priority ordering — critical before standard with limited stock
# ---------------------------------------------------------------------------


def test_priority_ordering_critical_before_standard(state, config):
    """Critical orders must be allocated before standard when stock is limited."""
    customer_id = state.customers[0].customer_id
    sku = "FF-BA50-CS-PN16-FL-MN"

    # Only 30 units available — first-come first-serve would be wrong
    state.inventory.setdefault(_WAREHOUSE, {})[sku] = 30

    # Standard order (registered first but lower priority)
    order_std_id = "ORD-2026-900006"
    line_std = _make_line(order_std_id, 1, sku, 30)
    state.open_orders[order_std_id] = _make_order(
        order_std_id,
        customer_id,
        priority="standard",
        lines=[line_std],
    )

    # Critical order (registered second but higher priority)
    order_crit_id = "ORD-2026-900007"
    line_crit = _make_line(order_crit_id, 1, sku, 30)
    state.open_orders[order_crit_id] = _make_order(
        order_crit_id,
        customer_id,
        priority="critical",
        lines=[line_crit],
    )

    run(state, config, _BUSINESS_DAY)

    # Critical order should have been fully allocated
    crit_order = state.open_orders[order_crit_id]
    crit_line = crit_order["lines"][0]

    std_order = state.open_orders[order_std_id]
    std_line = std_order["lines"][0]

    # Critical should be allocated; standard should be backordered (no stock left)
    assert crit_line["quantity_allocated"] == 30, (
        f"Critical line should have been allocated 30 units, got {crit_line['quantity_allocated']}"
    )
    assert crit_line["line_status"] == "allocated"

    # Standard order cannot be filled — it should be backordered
    assert std_line["line_status"] in ("backordered", "partially_allocated"), (
        f"Standard line should be backordered, got status {std_line['line_status']}"
    )
    assert std_line["quantity_allocated"] == 0 or (
        std_line["quantity_backordered"] > 0
    ), "Standard line should have backorder quantity"

    # Total inventory used should not exceed 30
    assert state.inventory[_WAREHOUSE].get(sku, 0) == 0, "Inventory should be depleted"

    # Cleanup
    del state.open_orders[order_std_id]
    del state.open_orders[order_crit_id]


# ---------------------------------------------------------------------------
# Test 7: Backorder escalation: >14 days, standard → express
# ---------------------------------------------------------------------------


def test_backorder_escalation_after_14_days(state, config):
    """A standard-priority line backordered for >14 days must be escalated to express."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-2026-900008"
    sku = "FF-GA15-SS304-PN16-FL-MN"

    # No stock — line will remain backordered
    state.inventory.setdefault(_WAREHOUSE, {})[sku] = 0

    line = _make_line(
        order_id, 1, sku, 20,
        line_status="backordered",
        quantity_backordered=20,
        backorder_days=15,  # >14 → should escalate
    )
    state.open_orders[order_id] = _make_order(
        order_id, customer_id,
        priority="standard",
        lines=[line],
    )

    events = run(state, config, _BUSINESS_DAY)

    # The escalation event should appear
    escalation_events = [
        e for e in events
        if isinstance(e, BackorderEvent)
        and e.order_id == order_id
        and e.escalated_to_express is True
    ]
    assert len(escalation_events) >= 1, (
        "Expected at least one BackorderEvent with escalated_to_express=True"
    )

    # Order priority should be updated to express
    assert state.open_orders[order_id]["priority"] == "express", (
        f"Expected priority 'express', got '{state.open_orders[order_id]['priority']}'"
    )

    # Cleanup
    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 8: Backorder cancellation: >30 days, forced dice roll
# ---------------------------------------------------------------------------


def test_backorder_cancellation_after_30_days(state, config):
    """Lines backordered >30 days with a 100% cancel roll emit BackorderCancellationEvent."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-2026-900009"
    sku = "FF-BA25-SS304-PN16-FL-MN"

    state.inventory.setdefault(_WAREHOUSE, {})[sku] = 0

    line = _make_line(
        order_id, 1, sku, 15,
        line_status="backordered",
        quantity_backordered=15,
        backorder_days=31,  # >30 days
    )
    state.open_orders[order_id] = _make_order(
        order_id, customer_id,
        priority="standard",
        lines=[line],
    )

    # Force the cancellation by patching rng.random to always return 0.0 (< 0.15)
    original_random = state.rng.random
    state.rng.random = lambda: 0.0

    try:
        events = run(state, config, _BUSINESS_DAY)
    finally:
        state.rng.random = original_random  # restore

    cancellations = [
        e for e in events
        if isinstance(e, BackorderCancellationEvent)
        and e.order_id == order_id
    ]
    assert len(cancellations) == 1, (
        f"Expected 1 BackorderCancellationEvent, got {len(cancellations)}"
    )

    canc = cancellations[0]
    assert canc.quantity_cancelled == 15
    assert canc.reason == "backorder_timeout"
    assert canc.days_backordered > 30

    # Line should be marked cancelled
    updated_line = state.open_orders[order_id]["lines"][0]
    assert updated_line["line_status"] == "cancelled"
    assert updated_line["quantity_backordered"] == 0

    # Cleanup
    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test: BackorderEvent has event_type "backorder" (Fix 4)
# ---------------------------------------------------------------------------


def test_backorder_event_type_is_backorder():
    """BackorderEvent.event_type must be 'backorder', not 'customer_orders'.

    This ensures backorder events are routed to backorder_events.json and do
    not pollute customer_orders.json with a different event shape.
    """
    event = BackorderEvent(
        event_id="test-uuid",
        backorder_date="2026-01-05",
        order_id="ORD-2026-999999",
        order_line_id="ORD-2026-999999-L1",
        sku="FF-GA15-CS-PN16-FL-MN",
        quantity_backordered=5,
        quantity_allocated=0,
        backorder_reason="no_stock",
        days_backordered=0,
    )
    assert event.event_type == "backorder", (
        f"Expected event_type='backorder', got '{event.event_type}'"
    )


# ---------------------------------------------------------------------------
# Test 9: Credit-hold orders skipped entirely
# ---------------------------------------------------------------------------


def test_credit_hold_orders_skipped(state, config):
    """Orders with status "credit_hold" must not be allocated."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-2026-900010"
    sku = "FF-GA40-CS-PN16-FL-MN"

    state.inventory.setdefault(_WAREHOUSE, {})[sku] = 200
    initial_inv = state.inventory[_WAREHOUSE][sku]

    line = _make_line(order_id, 1, sku, 50)
    state.open_orders[order_id] = _make_order(
        order_id, customer_id,
        status="credit_hold",
        lines=[line],
    )

    events = run(state, config, _BUSINESS_DAY)

    # No picks or backorders for this order
    order_events = [
        e for e in events
        if hasattr(e, "order_id") and e.order_id == order_id
    ]
    assert len(order_events) == 0, (
        f"Expected no events for credit_hold order, got {order_events}"
    )

    # Inventory unchanged
    assert state.inventory[_WAREHOUSE][sku] == initial_inv

    # Line unchanged
    assert state.open_orders[order_id]["lines"][0]["quantity_allocated"] == 0

    # Cleanup
    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 10: Invariant check: allocated + shipped + backordered <= ordered
# ---------------------------------------------------------------------------


def test_invariant_allocated_plus_backorder_never_exceeds_ordered(state, config):
    """After allocation, the invariant qty_allocated + qty_shipped + qty_backordered <= qty_ordered holds."""
    customer_id = state.customers[0].customer_id

    # Set limited stock for multiple SKUs
    skus = [_SKU_A, _SKU_B]
    state.inventory.setdefault(_WAREHOUSE, {})[_SKU_A] = 7
    state.inventory.setdefault(_WAREHOUSE, {})[_SKU_B] = 0

    orders_created: list[str] = []
    for i, sku in enumerate(skus):
        order_id = f"ORD-2026-9000{11 + i:02d}"
        line = _make_line(order_id, 1, sku, 20)
        state.open_orders[order_id] = _make_order(order_id, customer_id, lines=[line])
        orders_created.append(order_id)

    run(state, config, _BUSINESS_DAY)

    for order_id in orders_created:
        order = state.open_orders[order_id]
        for line in order["lines"]:
            qty_ordered = line["quantity_ordered"]
            qty_alloc = line["quantity_allocated"]
            qty_shipped = line["quantity_shipped"]
            qty_bo = line["quantity_backordered"]

            total = qty_alloc + qty_shipped + qty_bo
            assert total <= qty_ordered, (
                f"Invariant violated for {order_id}: "
                f"allocated({qty_alloc}) + shipped({qty_shipped}) + backordered({qty_bo}) "
                f"= {total} > ordered({qty_ordered})"
            )

    # Cleanup
    for order_id in orders_created:
        del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 11: Multiple orders, multiple lines — per-SKU inventory accounting
# ---------------------------------------------------------------------------


def test_multiple_orders_multiple_lines_inventory_accounting(state, config):
    """Multiple orders with multiple lines correctly deplete per-SKU inventory."""
    customer_id = state.customers[0].customer_id
    sku1 = "FF-GA80-CS-PN16-FL-MN"
    sku2 = "FF-BA100-CS-PN16-FL-MN"

    # Set known inventory
    state.inventory.setdefault(_WAREHOUSE, {})[sku1] = 50
    state.inventory.setdefault(_WAREHOUSE, {})[sku2] = 30

    # Two orders, each with two lines
    orders_created: list[str] = []

    order_id_1 = "ORD-2026-900013"
    state.open_orders[order_id_1] = _make_order(
        order_id_1, customer_id,
        lines=[
            _make_line(order_id_1, 1, sku1, 20),
            _make_line(order_id_1, 2, sku2, 10),
        ],
    )
    orders_created.append(order_id_1)

    order_id_2 = "ORD-2026-900014"
    state.open_orders[order_id_2] = _make_order(
        order_id_2, customer_id,
        lines=[
            _make_line(order_id_2, 1, sku1, 20),
            _make_line(order_id_2, 2, sku2, 25),
        ],
    )
    orders_created.append(order_id_2)

    run(state, config, _BUSINESS_DAY)

    # Total demand: sku1 = 40 (≤ 50 available), sku2 = 35 (> 30 available)
    # sku1: both lines should be fully allocated, inventory = 10 remaining
    # sku2: first 30 allocated then 0 left; second line partially or backordered

    total_allocated_sku1 = sum(
        line["quantity_allocated"]
        for oid in orders_created
        for line in state.open_orders[oid]["lines"]
        if line["sku"] == sku1
    )
    total_allocated_sku2 = sum(
        line["quantity_allocated"]
        for oid in orders_created
        for line in state.open_orders[oid]["lines"]
        if line["sku"] == sku2
    )

    # sku1: 40 units demanded, 50 available → 40 allocated
    assert total_allocated_sku1 == 40, (
        f"Expected 40 units of {sku1} allocated, got {total_allocated_sku1}"
    )
    # sku2: 35 demanded, 30 available → 30 allocated max
    assert total_allocated_sku2 == 30, (
        f"Expected 30 units of {sku2} allocated, got {total_allocated_sku2}"
    )

    # Inventory levels
    assert state.inventory[_WAREHOUSE][sku1] == 10  # 50 - 40
    assert state.inventory[_WAREHOUSE][sku2] == 0   # 30 - 30

    # Cleanup
    for order_id in orders_created:
        del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 14 (Pre-Fix 5): backordered line retried when stock is replenished
# ---------------------------------------------------------------------------


def test_backordered_line_retried_when_stock_arrives(state, config):
    """A line backordered on day 1 must be allocated on day 2 when stock arrives.

    This is the regression test for the order eligibility filter: orders with
    status 'backordered' or 'partially_allocated' must remain in the allocation
    queue so they are retried each business day.
    """
    customer_id = state.customers[0].customer_id
    order_id = "ORD-2026-900015"
    sku = "FF-BA80-CS-PN16-FL-MN"

    # Day 1: zero stock → line becomes backordered
    state.inventory.setdefault(_WAREHOUSE, {})[sku] = 0
    line = _make_line(order_id, 1, sku, 10)
    state.open_orders[order_id] = _make_order(order_id, customer_id, lines=[line])

    day1 = date(2026, 1, 5)
    run(state, config, day1)

    # Verify backorder state after day 1
    updated_line = state.open_orders[order_id]["lines"][0]
    assert updated_line["line_status"] == "backordered", (
        f"Expected 'backordered' after day 1, got '{updated_line['line_status']}'"
    )

    # Day 2: stock arrives
    state.inventory[_WAREHOUSE][sku] = 50

    day2 = date(2026, 1, 7)  # Wednesday (skip Jan 6 holiday)
    run(state, config, day2)

    # Line should now be allocated
    updated_line = state.open_orders[order_id]["lines"][0]
    assert updated_line["line_status"] == "allocated", (
        f"Expected 'allocated' after stock arrived on day 2, "
        f"got '{updated_line['line_status']}'"
    )
    assert updated_line["quantity_allocated"] == 10

    # Cleanup
    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 13 (Pre-Fix 4): state.allocated populated after full allocation
# ---------------------------------------------------------------------------


def test_state_allocated_populated_after_full_allocation(state, config):
    """After a full allocation, state.allocated[order_id][line_id] == qty_ordered."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-2026-900016"
    qty = 25

    state.inventory.setdefault(_WAREHOUSE, {})[_SKU_A] = 100
    line = _make_line(order_id, 1, _SKU_A, qty)
    state.open_orders[order_id] = _make_order(order_id, customer_id, lines=[line])

    run(state, config, _BUSINESS_DAY)

    line_id = f"{order_id}-L1"
    assert order_id in state.allocated, "order_id should appear in state.allocated"
    assert line_id in state.allocated[order_id], "line_id should appear in state.allocated[order_id]"
    assert state.allocated[order_id][line_id] == qty, (
        f"Expected state.allocated[{order_id!r}][{line_id!r}] == {qty}, "
        f"got {state.allocated[order_id][line_id]}"
    )

    # Cleanup
    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 12: backorder_days increments each day for waiting lines
# ---------------------------------------------------------------------------


def test_backorder_days_increments_each_business_day(state, config):
    """A backordered line's backorder_days counter must increment on each business day."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-2026-900017"
    sku = "FF-GA15-CS-PN25-FL-MN"

    # No stock so line stays backordered
    state.inventory.setdefault(_WAREHOUSE, {})[sku] = 0

    line = _make_line(
        order_id, 1, sku, 10,
        line_status="backordered",
        quantity_backordered=10,
        backorder_days=0,
    )
    state.open_orders[order_id] = _make_order(order_id, customer_id, lines=[line])

    # Run two consecutive business days
    day1 = date(2026, 1, 5)   # Monday
    day2 = date(2026, 1, 6)   # Tuesday (Jan 6 is Epiphany in PL — but is_business_day handles it)
    # Use a day pair that are guaranteed business days
    day2 = date(2026, 1, 7)   # Wednesday

    run(state, config, day1)
    days_after_day1 = state.open_orders[order_id]["lines"][0]["backorder_days"]

    run(state, config, day2)
    days_after_day2 = state.open_orders[order_id]["lines"][0]["backorder_days"]

    assert days_after_day1 == 1, f"Expected backorder_days=1 after day1, got {days_after_day1}"
    assert days_after_day2 == 2, f"Expected backorder_days=2 after day2, got {days_after_day2}"

    # Cleanup
    del state.open_orders[order_id]
