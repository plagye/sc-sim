"""Tests for the inventory snapshots engine (Phase 4, Step 38)."""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest

from flowform.config import load_config
from flowform.engines.snapshots import InventorySnapshotEvent, run
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
# Date constants
# ---------------------------------------------------------------------------

_BUSINESS_DAY = date(2026, 1, 5)   # Monday
_SATURDAY = date(2026, 1, 3)
_HOLIDAY = date(2026, 1, 1)        # New Year's Day (Polish holiday)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_returns_empty_on_weekend(state, config):
    events = run(state, config, _SATURDAY)
    assert events == []


def test_returns_empty_on_holiday(state, config):
    events = run(state, config, _HOLIDAY)
    assert events == []


def test_one_event_per_warehouse(state, config):
    events = run(state, config, _BUSINESS_DAY)
    assert len(events) == 2
    warehouses = {e.warehouse for e in events}
    assert warehouses == {"W01", "W02"}


def test_snapshot_id_format(state, config):
    events = run(state, config, _BUSINESS_DAY)
    pattern = re.compile(r"^SNAP-\d{8}-W0[12]$")
    for event in events:
        assert pattern.match(event.snapshot_id), (
            f"Unexpected snapshot_id: {event.snapshot_id}"
        )


def test_timestamp_is_end_of_day(state, config):
    events = run(state, config, _BUSINESS_DAY)
    for event in events:
        assert event.timestamp.endswith("T23:59:00Z"), (
            f"Unexpected timestamp: {event.timestamp}"
        )


def test_event_type_literal(state, config):
    events = run(state, config, _BUSINESS_DAY)
    for event in events:
        assert event.event_type == "inventory_snapshot"


def test_positions_contain_seeded_inventory(state, config):
    """After from_new(), W01 should have many positions (initial inventory seeded ~65% of SKUs)."""
    events = run(state, config, _BUSINESS_DAY)
    w01_event = next(e for e in events if e.warehouse == "W01")
    assert len(w01_event.positions) > 0


def test_quantity_on_hand_matches_state(state, config):
    """For a seeded SKU in W01, position.quantity_on_hand must match state inventory + allocated."""
    # Pick first SKU in W01 inventory
    w01_inventory = state.inventory.get("W01", {})
    assert w01_inventory, "Expected W01 to have seeded inventory"
    sku = next(iter(w01_inventory))
    expected_available = w01_inventory[sku]

    events = run(state, config, _BUSINESS_DAY)
    w01_event = next(e for e in events if e.warehouse == "W01")
    pos_map = {p.sku: p for p in w01_event.positions}

    assert sku in pos_map, f"SKU {sku} not in snapshot positions"
    pos = pos_map[sku]
    # No open orders in fresh state, so on_hand == available quantity in state
    assert pos.quantity_on_hand == expected_available


def test_quantity_allocated_zero_when_none_allocated(state, config):
    """Fresh state with no open orders → all positions have quantity_allocated == 0."""
    # state.open_orders is empty on fresh state
    assert len(state.open_orders) == 0
    events = run(state, config, _BUSINESS_DAY)
    for event in events:
        for pos in event.positions:
            assert pos.quantity_allocated == 0, (
                f"Expected 0 allocated for {pos.sku} in {event.warehouse}"
            )


def test_quantity_available_equals_onhand_minus_allocated(state, config):
    """Manually inject an allocated-but-not-shipped order line, then verify quantity_available."""
    w01_inventory = state.inventory.get("W01", {})
    sku = next(iter(w01_inventory))
    on_hand_available = w01_inventory[sku]

    # Inject a fake open order with an allocated line for this SKU in W01
    allocated_qty = min(5, on_hand_available)
    fake_order_id = "ORD-TEST-000001"
    state.open_orders[fake_order_id] = {
        "order_id": fake_order_id,
        "status": "allocated",
        "lines": [
            {
                "line_id": f"{fake_order_id}-L1",
                "sku": sku,
                "warehouse_id": "W01",
                "quantity_ordered": allocated_qty,
                "quantity_allocated": allocated_qty,
                "quantity_shipped": 0,
                "quantity_backordered": 0,
                "line_status": "allocated",
            }
        ],
    }

    try:
        events = run(state, config, _BUSINESS_DAY)
        w01_event = next(e for e in events if e.warehouse == "W01")
        pos_map = {p.sku: p for p in w01_event.positions}

        assert sku in pos_map
        pos = pos_map[sku]
        # on_hand = state.inventory (available) + allocated_qty
        expected_on_hand = on_hand_available + allocated_qty
        assert pos.quantity_on_hand == expected_on_hand
        assert pos.quantity_allocated == allocated_qty
        assert pos.quantity_available == expected_on_hand - allocated_qty
    finally:
        # Clean up injected order
        state.open_orders.pop(fake_order_id, None)


def test_quantity_available_clamped_to_zero(state, config):
    """Simulate fully-picked SKU: state.inventory == 0 but allocated units remain.

    After the allocation engine picks all available units (setting
    state.inventory to 0) and the order is still in-transit, the snapshot
    must report quantity_available == 0.
    """
    w01_inventory = state.inventory.get("W01", {})
    sku = next(iter(w01_inventory))
    original_qty = w01_inventory[sku]

    # Simulate that the allocation engine has picked all units (inventory → 0)
    state.inventory["W01"][sku] = 0

    # The order that holds those units is still open (not yet shipped)
    allocated_qty = original_qty
    fake_order_id = "ORD-TEST-000002"
    state.open_orders[fake_order_id] = {
        "order_id": fake_order_id,
        "status": "allocated",
        "lines": [
            {
                "line_id": f"{fake_order_id}-L1",
                "sku": sku,
                "warehouse_id": "W01",
                "quantity_ordered": allocated_qty,
                "quantity_allocated": allocated_qty,
                "quantity_shipped": 0,
                "quantity_backordered": 0,
                "line_status": "allocated",
            }
        ],
    }

    try:
        events = run(state, config, _BUSINESS_DAY)
        w01_event = next(e for e in events if e.warehouse == "W01")
        pos_map = {p.sku: p for p in w01_event.positions}

        assert sku in pos_map
        pos = pos_map[sku]
        # on_hand = 0 (state.inventory) + allocated_qty → all units are allocated
        # available = max(0, on_hand - allocated) = max(0, allocated_qty - allocated_qty) = 0
        assert pos.quantity_available == 0
        assert pos.quantity_allocated == allocated_qty
    finally:
        state.open_orders.pop(fake_order_id, None)
        state.inventory["W01"][sku] = original_qty


def test_zero_inventory_skus_excluded(state, config):
    """SKUs with qty == 0 in state.inventory should not appear in positions."""
    # Insert a zero-qty SKU into W01
    fake_sku = "FF-GATE-DN15-CS-PN16-FL-M-ZERO"
    state.inventory["W01"][fake_sku] = 0

    try:
        events = run(state, config, _BUSINESS_DAY)
        w01_event = next(e for e in events if e.warehouse == "W01")
        sku_set = {p.sku for p in w01_event.positions}
        assert fake_sku not in sku_set
    finally:
        state.inventory["W01"].pop(fake_sku, None)


def test_w02_positions_reflect_w02_state(state, config):
    """W02 positions should reflect W02 inventory, not W01."""
    w01_skus = set(state.inventory.get("W01", {}).keys())
    w02_skus = set(state.inventory.get("W02", {}).keys())

    events = run(state, config, _BUSINESS_DAY)
    w01_event = next(e for e in events if e.warehouse == "W01")
    w02_event = next(e for e in events if e.warehouse == "W02")

    w01_pos_skus = {p.sku for p in w01_event.positions}
    w02_pos_skus = {p.sku for p in w02_event.positions}

    # W01 positions must be a subset of W01 inventory
    # (filtered to qty > 0, which from_new guarantees)
    assert w01_pos_skus <= w01_skus

    # W02 positions must be a subset of W02 inventory
    assert w02_pos_skus <= w02_skus

    # There should be some W01-only SKUs not appearing in W02 snapshot
    # (since W02 gets ~50% of W01 SKUs at init)
    w01_only = w01_skus - w02_skus
    assert len(w01_only) > 0, "Expected some SKUs in W01 but not W02"
