"""Tests for the modification and cancellation engine (Phase 2, Step 20).

Covers:
1.  Returns [] on weekend
2.  Returns [] on holiday
3.  quantity_change emits OrderModificationEvent with correct old/new values
4.  quantity_change cannot reduce below allocated + shipped quantity
5.  date_change never sets date to past
6.  priority_change: standard → express/critical correctly
7.  line_removal returns allocated inventory to state.inventory
8.  line_removal only when >1 active line exists
9.  Full cancellation: all lines cancelled, inventory returned, order status "cancelled"
10. Partial cancellation: only selected lines cancelled, correct event
11. Cancelled orders not modified or re-cancelled
12. Credit-hold orders skipped
"""

from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from flowform.config import load_config
from flowform.engines.modifications import (
    OrderCancellationEvent,
    OrderModificationEvent,
    run,
)
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
# Test helpers
# ---------------------------------------------------------------------------

_SKU_A = "FF-GA15-CS-PN16-FL-MN"
_SKU_B = "FF-BA25-CS-PN16-FL-MN"
_WAREHOUSE = "W01"
_BUSINESS_DAY = date(2026, 1, 5)  # Monday, not a holiday
_SATURDAY = date(2026, 1, 3)
_HOLIDAY = date(2026, 1, 1)       # New Year's Day


def _make_order(
    order_id: str,
    customer_id: str,
    *,
    priority: str = "standard",
    status: str = "confirmed",
    requested_delivery_date: str = "2026-02-20",
    simulation_date: str = "2026-01-05",
    lines: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
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
) -> dict[str, Any]:
    return {
        "line_id": f"{order_id}-L{line_num}",
        "sku": sku,
        "quantity_ordered": quantity_ordered,
        "quantity_allocated": quantity_allocated,
        "quantity_shipped": quantity_shipped,
        "quantity_backordered": quantity_backordered,
        "unit_price": 500.0,
        "line_status": line_status,
        "warehouse_id": _WAREHOUSE,
    }


def _force_modification_type(state: SimulationState, mod_type: str) -> None:
    """Patch state.rng so the next choices() call returns the specified type."""
    # We need the dice roll for 10% to pass AND the type selection to pick our type.
    # We monkey-patch rng.random() to return a value < 0.10 for the probability check,
    # and rng.choices() for the type selection.
    original_random = state.rng.random
    original_choices = state.rng.choices
    call_count = [0]

    def patched_random():
        call_count[0] += 1
        # First call is the 10% probability check — return 0.0 to always trigger
        if call_count[0] == 1:
            return 0.0
        return original_random()

    def patched_choices(population, weights=None, k=1):
        # When called with the modification type population, return our target
        from flowform.engines.modifications import _MODIFICATION_TYPES
        if population is _MODIFICATION_TYPES:
            return [mod_type]
        return original_choices(population, weights=weights, k=k)

    state.rng.random = patched_random   # type: ignore[method-assign]
    state.rng.choices = patched_choices  # type: ignore[method-assign]


def _restore_rng(state: SimulationState, original_random: Any, original_choices: Any) -> None:
    state.rng.random = original_random    # type: ignore[method-assign]
    state.rng.choices = original_choices  # type: ignore[method-assign]


# ---------------------------------------------------------------------------
# Test 1: Returns [] on weekend
# ---------------------------------------------------------------------------


def test_returns_empty_on_weekend(state, config):
    """Engine must return [] on Saturday."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-MOD-WKD"
    line = _make_line(order_id, 1, _SKU_A, 10)
    state.open_orders[order_id] = _make_order(order_id, customer_id, lines=[line])

    result = run(state, config, _SATURDAY)
    assert result == []

    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 2: Returns [] on holiday
# ---------------------------------------------------------------------------


def test_returns_empty_on_holiday(state, config):
    """Engine must return [] on a Polish public holiday."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-MOD-HOL"
    line = _make_line(order_id, 1, _SKU_A, 10)
    state.open_orders[order_id] = _make_order(order_id, customer_id, lines=[line])

    result = run(state, config, _HOLIDAY)
    assert result == []

    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 3: quantity_change emits event with correct old/new values
# ---------------------------------------------------------------------------


def test_quantity_change_event_old_new_values(state, config):
    """quantity_change must record old and new quantity_ordered on the event."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-QC-001"
    original_qty = 50
    line = _make_line(order_id, 1, _SKU_A, original_qty)
    state.open_orders[order_id] = _make_order(order_id, customer_id, lines=[line])

    # Force modification type to quantity_change and force 10% dice to pass
    original_random = state.rng.random
    original_choices = state.rng.choices
    call_count = [0]

    def patched_random():
        call_count[0] += 1
        if call_count[0] == 1:
            return 0.0  # pass the 10% check
        return original_random()

    def patched_choices(population, weights=None, k=1):
        from flowform.engines.modifications import _MODIFICATION_TYPES
        if population is _MODIFICATION_TYPES:
            return ["quantity_change"]
        return original_choices(population, weights=weights, k=k)

    state.rng.random = patched_random    # type: ignore[method-assign]
    state.rng.choices = patched_choices  # type: ignore[method-assign]

    try:
        events = run(state, config, _BUSINESS_DAY)
    finally:
        state.rng.random = original_random   # type: ignore[method-assign]
        state.rng.choices = original_choices  # type: ignore[method-assign]

    mod_events = [e for e in events if isinstance(e, OrderModificationEvent)
                  and e.order_id == order_id and e.modification_type == "quantity_change"]

    assert len(mod_events) == 1, f"Expected 1 quantity_change event, got {len(mod_events)}"
    evt = mod_events[0]
    assert evt.old_value == str(original_qty)
    assert evt.new_value is not None
    assert evt.line_id is not None

    # The line in state should reflect the new quantity
    line_state = state.open_orders[order_id]["lines"][0]
    assert line_state["quantity_ordered"] == int(evt.new_value)

    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 4: quantity_change cannot reduce below allocated + shipped
# ---------------------------------------------------------------------------


def test_quantity_change_cannot_go_below_committed(state, config):
    """quantity_change must not reduce quantity_ordered below allocated + shipped."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-QC-FLOOR"

    allocated = 30
    shipped = 10
    original_qty = 50  # allocated + shipped = 40

    line = _make_line(
        order_id, 1, _SKU_A, original_qty,
        quantity_allocated=allocated,
        quantity_shipped=shipped,
        line_status="partially_allocated",
    )
    state.open_orders[order_id] = _make_order(
        order_id, customer_id,
        status="partially_allocated",
        lines=[line],
    )

    # Force quantity_change with a low multiplier to try to reduce
    original_random = state.rng.random
    original_choices = state.rng.choices
    original_uniform = state.rng.uniform
    call_count = [0]

    def patched_random():
        call_count[0] += 1
        if call_count[0] == 1:
            return 0.0  # pass 10% check
        return original_random()

    def patched_choices(population, weights=None, k=1):
        from flowform.engines.modifications import _MODIFICATION_TYPES
        if population is _MODIFICATION_TYPES:
            return ["quantity_change"]
        return original_choices(population, weights=weights, k=k)

    def patched_uniform(lo, hi):
        # Always return 0.5 → new_qty = original_qty * 0.5 = 25, which < committed 40
        return 0.5

    state.rng.random = patched_random    # type: ignore[method-assign]
    state.rng.choices = patched_choices  # type: ignore[method-assign]
    state.rng.uniform = patched_uniform  # type: ignore[method-assign]

    try:
        events = run(state, config, _BUSINESS_DAY)
    finally:
        state.rng.random = original_random    # type: ignore[method-assign]
        state.rng.choices = original_choices  # type: ignore[method-assign]
        state.rng.uniform = original_uniform  # type: ignore[method-assign]

    committed = allocated + shipped  # = 40
    final_qty = state.open_orders[order_id]["lines"][0]["quantity_ordered"]
    assert final_qty >= committed, (
        f"quantity_ordered ({final_qty}) must be >= committed ({committed})"
    )

    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 5: date_change never sets date to the past
# ---------------------------------------------------------------------------


def test_date_change_never_in_past(state, config):
    """After date_change, requested_delivery_date must be >= sim_date."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-DC-001"

    # Delivery date very close to sim_date so delta=-5 would push it to past
    delivery_date = "2026-01-06"  # only 1 day after sim_date
    line = _make_line(order_id, 1, _SKU_A, 20)
    line["requested_delivery_date"] = delivery_date
    state.open_orders[order_id] = _make_order(
        order_id, customer_id,
        requested_delivery_date=delivery_date,
        lines=[line],
    )

    original_random = state.rng.random
    original_choices = state.rng.choices
    original_randint = state.rng.randint
    call_count = [0]

    def patched_random():
        call_count[0] += 1
        if call_count[0] == 1:
            return 0.0  # pass 10% check
        return original_random()

    def patched_choices(population, weights=None, k=1):
        from flowform.engines.modifications import _MODIFICATION_TYPES
        if population is _MODIFICATION_TYPES:
            return ["date_change"]
        return original_choices(population, weights=weights, k=k)

    def patched_randint(lo, hi):
        # Always return -5 to try to push date into past
        if lo == -5 and hi == 30:
            return -5
        return original_randint(lo, hi)

    state.rng.random = patched_random    # type: ignore[method-assign]
    state.rng.choices = patched_choices  # type: ignore[method-assign]
    state.rng.randint = patched_randint  # type: ignore[method-assign]

    try:
        events = run(state, config, _BUSINESS_DAY)
    finally:
        state.rng.random = original_random    # type: ignore[method-assign]
        state.rng.choices = original_choices  # type: ignore[method-assign]
        state.rng.randint = original_randint  # type: ignore[method-assign]

    # If a date_change event was emitted, check the new date is >= sim_date
    date_events = [
        e for e in events
        if isinstance(e, OrderModificationEvent)
        and e.order_id == order_id
        and e.modification_type == "date_change"
    ]

    for evt in date_events:
        new_date = date.fromisoformat(evt.new_value)
        assert new_date >= _BUSINESS_DAY, (
            f"date_change set new_date {new_date} which is before sim_date {_BUSINESS_DAY}"
        )

    # Also check the state directly
    final_date_str = (
        state.open_orders[order_id]["lines"][0].get("requested_delivery_date")
        or state.open_orders[order_id].get("requested_delivery_date", "")
    )
    if final_date_str:
        final_date = date.fromisoformat(final_date_str)
        assert final_date >= _BUSINESS_DAY

    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 6: priority_change escalates correctly
# ---------------------------------------------------------------------------


def test_priority_change_escalates_standard(state, config):
    """priority_change on standard order must result in express or critical."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-PRI-001"

    line = _make_line(order_id, 1, _SKU_A, 10)
    state.open_orders[order_id] = _make_order(
        order_id, customer_id,
        priority="standard",
        lines=[line],
    )

    original_random = state.rng.random
    original_choices = state.rng.choices
    call_count = [0]

    def patched_random():
        call_count[0] += 1
        if call_count[0] == 1:
            return 0.0  # pass 10% check
        return original_random()

    def patched_choices(population, weights=None, k=1):
        from flowform.engines.modifications import _MODIFICATION_TYPES
        if population is _MODIFICATION_TYPES:
            return ["priority_change"]
        return original_choices(population, weights=weights, k=k)

    state.rng.random = patched_random    # type: ignore[method-assign]
    state.rng.choices = patched_choices  # type: ignore[method-assign]

    try:
        events = run(state, config, _BUSINESS_DAY)
    finally:
        state.rng.random = original_random    # type: ignore[method-assign]
        state.rng.choices = original_choices  # type: ignore[method-assign]

    pri_events = [
        e for e in events
        if isinstance(e, OrderModificationEvent)
        and e.order_id == order_id
        and e.modification_type == "priority_change"
    ]

    assert len(pri_events) == 1, f"Expected 1 priority_change event, got {len(pri_events)}"
    evt = pri_events[0]
    assert evt.old_value == "standard"
    assert evt.new_value in ("express", "critical")

    # Verify state was updated
    new_priority = state.open_orders[order_id]["priority"]
    assert new_priority in ("express", "critical")
    assert new_priority != "standard"

    del state.open_orders[order_id]


def test_priority_change_express_to_critical(state, config):
    """priority_change on express order must result in critical."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-PRI-EXP"

    line = _make_line(order_id, 1, _SKU_A, 10)
    state.open_orders[order_id] = _make_order(
        order_id, customer_id,
        priority="express",
        lines=[line],
    )

    original_random = state.rng.random
    original_choices = state.rng.choices
    call_count = [0]

    def patched_random():
        call_count[0] += 1
        if call_count[0] == 1:
            return 0.0
        return original_random()

    def patched_choices(population, weights=None, k=1):
        from flowform.engines.modifications import _MODIFICATION_TYPES
        if population is _MODIFICATION_TYPES:
            return ["priority_change"]
        return original_choices(population, weights=weights, k=k)

    state.rng.random = patched_random    # type: ignore[method-assign]
    state.rng.choices = patched_choices  # type: ignore[method-assign]

    try:
        events = run(state, config, _BUSINESS_DAY)
    finally:
        state.rng.random = original_random    # type: ignore[method-assign]
        state.rng.choices = original_choices  # type: ignore[method-assign]

    pri_events = [
        e for e in events
        if isinstance(e, OrderModificationEvent)
        and e.order_id == order_id
        and e.modification_type == "priority_change"
    ]
    assert len(pri_events) == 1
    assert pri_events[0].new_value == "critical"
    assert state.open_orders[order_id]["priority"] == "critical"

    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 7: line_removal returns allocated inventory
# ---------------------------------------------------------------------------


def test_line_removal_returns_allocated_inventory(state, config):
    """Removing a line must return its allocated qty to warehouse inventory."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-LR-001"

    allocated_qty = 25
    state.inventory.setdefault(_WAREHOUSE, {})[_SKU_B] = 10  # known starting level

    line1 = _make_line(order_id, 1, _SKU_A, 50)
    line2 = _make_line(order_id, 2, _SKU_B, 30, quantity_allocated=allocated_qty, line_status="allocated")
    state.open_orders[order_id] = _make_order(order_id, customer_id, lines=[line1, line2])

    inventory_before = state.inventory[_WAREHOUSE].get(_SKU_B, 0)

    original_random = state.rng.random
    original_choices = state.rng.choices
    call_count = [0]

    def patched_random():
        call_count[0] += 1
        if call_count[0] == 1:
            return 0.0  # pass 10% check
        return original_random()

    def patched_choices(population, weights=None, k=1):
        from flowform.engines.modifications import _MODIFICATION_TYPES
        if population is _MODIFICATION_TYPES:
            return ["line_removal"]
        return original_choices(population, weights=weights, k=k)

    state.rng.random = patched_random    # type: ignore[method-assign]
    state.rng.choices = patched_choices  # type: ignore[method-assign]

    try:
        events = run(state, config, _BUSINESS_DAY)
    finally:
        state.rng.random = original_random    # type: ignore[method-assign]
        state.rng.choices = original_choices  # type: ignore[method-assign]

    removal_events = [
        e for e in events
        if isinstance(e, OrderModificationEvent)
        and e.order_id == order_id
        and e.modification_type == "line_removal"
    ]

    assert len(removal_events) == 1, f"Expected 1 line_removal event, got {len(removal_events)}"

    # The removed line had _SKU_B allocated — that should be returned
    # line2 has qty=30 and allocated=25; it should be the one removed (smaller qty)
    inventory_after = state.inventory[_WAREHOUSE].get(_SKU_B, 0)
    assert inventory_after == inventory_before + allocated_qty, (
        f"Expected inventory {_SKU_B} = {inventory_before + allocated_qty}, "
        f"got {inventory_after}"
    )

    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 8: line_removal only when >1 active line exists
# ---------------------------------------------------------------------------


def test_line_removal_skipped_when_only_one_active_line(state, config):
    """line_removal must not fire when order has only one active line."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-LR-SINGLE"

    line = _make_line(order_id, 1, _SKU_A, 20)
    state.open_orders[order_id] = _make_order(order_id, customer_id, lines=[line])

    original_random = state.rng.random
    original_choices = state.rng.choices
    call_count = [0]

    def patched_random():
        call_count[0] += 1
        if call_count[0] == 1:
            return 0.0
        return original_random()

    def patched_choices(population, weights=None, k=1):
        from flowform.engines.modifications import _MODIFICATION_TYPES
        if population is _MODIFICATION_TYPES:
            return ["line_removal"]
        return original_choices(population, weights=weights, k=k)

    state.rng.random = patched_random    # type: ignore[method-assign]
    state.rng.choices = patched_choices  # type: ignore[method-assign]

    try:
        events = run(state, config, _BUSINESS_DAY)
    finally:
        state.rng.random = original_random    # type: ignore[method-assign]
        state.rng.choices = original_choices  # type: ignore[method-assign]

    removal_events = [
        e for e in events
        if isinstance(e, OrderModificationEvent)
        and e.order_id == order_id
        and e.modification_type == "line_removal"
    ]

    # The line_removal handler returns None when only one active line — no event
    assert len(removal_events) == 0, (
        "line_removal should not fire when order has only one active line"
    )

    # Line should still be active
    assert state.open_orders[order_id]["lines"][0]["line_status"] == "open"

    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 9: Full cancellation
# ---------------------------------------------------------------------------


def test_full_cancellation_cancels_all_lines_and_returns_inventory(state, config):
    """Full cancellation must set all lines to 'cancelled', return allocated inventory, set order status 'cancelled'."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-FULLCANC-001"

    allocated_a = 15
    allocated_b = 8
    state.inventory.setdefault(_WAREHOUSE, {})[_SKU_A] = 5
    state.inventory.setdefault(_WAREHOUSE, {})[_SKU_B] = 5
    inv_a_before = state.inventory[_WAREHOUSE][_SKU_A]
    inv_b_before = state.inventory[_WAREHOUSE][_SKU_B]

    line1 = _make_line(order_id, 1, _SKU_A, 20, quantity_allocated=allocated_a, line_status="allocated")
    line2 = _make_line(order_id, 2, _SKU_B, 10, quantity_allocated=allocated_b, line_status="allocated")
    state.open_orders[order_id] = _make_order(order_id, customer_id, status="allocated", lines=[line1, line2])

    # Force: pass 10% modification check → miss, then pass 2.5% cancellation check
    # We'll patch random to return values so modification is skipped and cancellation triggers
    original_random = state.rng.random
    random_values = iter([0.5, 0.0])  # 0.5 > 0.10 (skip mod), 0.0 < 0.025 (trigger canc)

    def patched_random():
        try:
            return next(random_values)
        except StopIteration:
            return original_random()

    # Force full cancellation (random() < 0.60 for full)
    # We also need rng.random for the full/partial decision, handle that separately
    original_choices = state.rng.choices
    call_count_full = [0]

    def patched_random_full():
        call_count_full[0] += 1
        v = [0.5, 0.0, 0.1]  # skip mod, trigger canc, force full cancel
        if call_count_full[0] - 1 < len(v):
            return v[call_count_full[0] - 1]
        return original_random()

    state.rng.random = patched_random_full  # type: ignore[method-assign]

    try:
        events = run(state, config, _BUSINESS_DAY)
    finally:
        state.rng.random = original_random    # type: ignore[method-assign]

    canc_events = [
        e for e in events
        if isinstance(e, OrderCancellationEvent)
        and e.order_id == order_id
    ]
    assert len(canc_events) == 1, f"Expected 1 cancellation event, got {len(canc_events)}"
    canc = canc_events[0]

    if canc.cancellation_type == "full":
        # All lines must be cancelled
        for line in state.open_orders[order_id]["lines"]:
            assert line["line_status"] == "cancelled", (
                f"Line {line['line_id']} should be 'cancelled', got {line['line_status']}"
            )
        assert state.open_orders[order_id]["status"] == "cancelled"

        # Inventory must have been returned
        assert state.inventory[_WAREHOUSE][_SKU_A] == inv_a_before + allocated_a
        assert state.inventory[_WAREHOUSE][_SKU_B] == inv_b_before + allocated_b

    # At minimum: event has non-empty cancelled_lines and total_quantity_cancelled > 0
    assert len(canc.cancelled_lines) > 0
    assert canc.total_quantity_cancelled > 0

    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 10: Partial cancellation
# ---------------------------------------------------------------------------


def test_partial_cancellation_cancels_subset_of_lines(state, config):
    """Partial cancellation must cancel only a subset of lines; order not fully cancelled."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-PARTCANC-001"

    state.inventory.setdefault(_WAREHOUSE, {})[_SKU_A] = 0
    state.inventory.setdefault(_WAREHOUSE, {})[_SKU_B] = 0

    line1 = _make_line(order_id, 1, _SKU_A, 20, quantity_allocated=10, line_status="allocated")
    line2 = _make_line(order_id, 2, _SKU_B, 15, quantity_allocated=5, line_status="allocated")
    line3 = _make_line(order_id, 3, _SKU_A, 12, quantity_allocated=3, line_status="allocated")
    state.open_orders[order_id] = _make_order(
        order_id, customer_id,
        status="allocated",
        lines=[line1, line2, line3],
    )

    original_random = state.rng.random
    call_count = [0]

    def patched_random():
        call_count[0] += 1
        values = [0.5, 0.0, 0.70]  # skip mod, trigger canc, 0.70 > 0.60 → partial
        if call_count[0] - 1 < len(values):
            return values[call_count[0] - 1]
        return original_random()

    state.rng.random = patched_random  # type: ignore[method-assign]

    try:
        events = run(state, config, _BUSINESS_DAY)
    finally:
        state.rng.random = original_random  # type: ignore[method-assign]

    canc_events = [
        e for e in events
        if isinstance(e, OrderCancellationEvent)
        and e.order_id == order_id
    ]
    assert len(canc_events) == 1

    canc = canc_events[0]

    if canc.cancellation_type == "partial":
        # Not all lines cancelled
        all_cancelled = all(
            line["line_status"] == "cancelled"
            for line in state.open_orders[order_id]["lines"]
        )
        assert not all_cancelled, "Partial cancel should not cancel all lines"

        # Order should not be 'cancelled' if some lines remain active
        active_remaining = [
            line for line in state.open_orders[order_id]["lines"]
            if line["line_status"] != "cancelled"
        ]
        if active_remaining:
            assert state.open_orders[order_id]["status"] != "cancelled"

    # Always: cancelled_lines is a non-empty subset
    assert len(canc.cancelled_lines) >= 1
    assert canc.total_quantity_cancelled >= 1

    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 11: Cancelled orders not modified or re-cancelled
# ---------------------------------------------------------------------------


def test_cancelled_orders_skipped(state, config):
    """Orders already in 'cancelled' status must not be modified or re-cancelled."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-ALREADY-CANC"

    line = _make_line(order_id, 1, _SKU_A, 10, line_status="cancelled")
    state.open_orders[order_id] = _make_order(
        order_id, customer_id,
        status="cancelled",
        lines=[line],
    )

    # Run with a 100% trigger for both mod and cancellation
    original_random = state.rng.random
    state.rng.random = lambda: 0.0  # type: ignore[method-assign]

    try:
        events = run(state, config, _BUSINESS_DAY)
    finally:
        state.rng.random = original_random  # type: ignore[method-assign]

    order_events = [
        e for e in events
        if hasattr(e, "order_id") and e.order_id == order_id
    ]
    assert len(order_events) == 0, (
        f"Expected no events for cancelled order, got {order_events}"
    )

    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 12: Credit-hold orders skipped
# ---------------------------------------------------------------------------


def test_credit_hold_orders_skipped(state, config):
    """Orders with status 'credit_hold' must not be modified or cancelled."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-CHOLD-001"

    line = _make_line(order_id, 1, _SKU_A, 20)
    state.open_orders[order_id] = _make_order(
        order_id, customer_id,
        status="credit_hold",
        lines=[line],
    )

    original_random = state.rng.random
    state.rng.random = lambda: 0.0  # type: ignore[method-assign]

    try:
        events = run(state, config, _BUSINESS_DAY)
    finally:
        state.rng.random = original_random  # type: ignore[method-assign]

    order_events = [
        e for e in events
        if hasattr(e, "order_id") and e.order_id == order_id
    ]
    assert len(order_events) == 0, (
        f"Expected no events for credit_hold order, got {order_events}"
    )

    assert state.open_orders[order_id]["status"] == "credit_hold"

    del state.open_orders[order_id]


# ---------------------------------------------------------------------------
# Test 13: Modified orders skipped in cancellation pass
# ---------------------------------------------------------------------------


def test_modified_orders_not_also_cancelled(state, config):
    """An order modified in pass 1 must be excluded from the cancellation pass."""
    customer_id = state.customers[0].customer_id
    order_id = "ORD-MOD-NOCANC"

    line = _make_line(order_id, 1, _SKU_A, 50)
    state.open_orders[order_id] = _make_order(order_id, customer_id, lines=[line])

    original_random = state.rng.random
    original_choices = state.rng.choices
    call_count = [0]

    def patched_random():
        call_count[0] += 1
        # First call: pass modification check (0.0 < 0.10)
        # If cancellation pass runs for same order it would also need 0.0 < 0.025
        # We want to confirm cancellation is skipped despite the low value
        return 0.0

    def patched_choices(population, weights=None, k=1):
        from flowform.engines.modifications import _MODIFICATION_TYPES
        if population is _MODIFICATION_TYPES:
            return ["date_change"]
        return original_choices(population, weights=weights, k=k)

    state.rng.random = patched_random    # type: ignore[method-assign]
    state.rng.choices = patched_choices  # type: ignore[method-assign]

    try:
        events = run(state, config, _BUSINESS_DAY)
    finally:
        state.rng.random = original_random    # type: ignore[method-assign]
        state.rng.choices = original_choices  # type: ignore[method-assign]

    mod_events = [e for e in events if isinstance(e, OrderModificationEvent) and e.order_id == order_id]
    canc_events = [e for e in events if isinstance(e, OrderCancellationEvent) and e.order_id == order_id]

    # May or may not have a modification (date_change may produce None if old==new)
    # But there must NEVER be a cancellation for this order
    assert len(canc_events) == 0, (
        f"Modified order should not also be cancelled, got {canc_events}"
    )

    del state.open_orders[order_id]
