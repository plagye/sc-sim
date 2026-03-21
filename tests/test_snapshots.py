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
    """For a seeded SKU in W01, position.quantity_on_hand must match state inventory on_hand."""
    # Pick first SKU in W01 inventory
    w01_inventory = state.inventory.get("W01", {})
    assert w01_inventory, "Expected W01 to have seeded inventory"
    sku = next(iter(w01_inventory))
    expected_on_hand = w01_inventory[sku]["on_hand"]

    events = run(state, config, _BUSINESS_DAY)
    w01_event = next(e for e in events if e.warehouse == "W01")
    pos_map = {p.sku: p for p in w01_event.positions}

    assert sku in pos_map, f"SKU {sku} not in snapshot positions"
    pos = pos_map[sku]
    # on_hand is read directly from state.inventory[wh][sku]["on_hand"]
    assert pos.quantity_on_hand == expected_on_hand


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
    """Directly set allocated in inventory position, verify snapshot quantity_available == on_hand - allocated."""
    w01_inventory = state.inventory.get("W01", {})
    sku = next(iter(w01_inventory))
    pos = w01_inventory[sku]
    on_hand = pos["on_hand"]

    # Inject allocated units into the inventory position directly
    allocated_qty = min(5, on_hand)
    original_allocated = pos["allocated"]
    pos["allocated"] = allocated_qty

    try:
        events = run(state, config, _BUSINESS_DAY)
        w01_event = next(e for e in events if e.warehouse == "W01")
        pos_map = {p.sku: p for p in w01_event.positions}

        assert sku in pos_map
        snap_pos = pos_map[sku]
        assert snap_pos.quantity_on_hand == on_hand
        assert snap_pos.quantity_allocated == allocated_qty
        assert snap_pos.quantity_available == max(0, on_hand - allocated_qty)
    finally:
        pos["allocated"] = original_allocated


def test_quantity_available_clamped_to_zero(state, config):
    """Simulate fully-allocated SKU: allocated >= on_hand → quantity_available clamped to 0."""
    w01_inventory = state.inventory.get("W01", {})
    sku = next(iter(w01_inventory))
    pos = w01_inventory[sku]
    original_allocated = pos["allocated"]
    on_hand = pos["on_hand"]

    # Set allocated > on_hand to force the clamp
    pos["allocated"] = on_hand + 5

    try:
        events = run(state, config, _BUSINESS_DAY)
        w01_event = next(e for e in events if e.warehouse == "W01")
        pos_map = {p.sku: p for p in w01_event.positions}

        assert sku in pos_map
        snap_pos = pos_map[sku]
        # available = max(0, on_hand - allocated) = max(0, on_hand - (on_hand+5)) = 0
        assert snap_pos.quantity_available == 0
        assert snap_pos.quantity_allocated == on_hand + 5
    finally:
        pos["allocated"] = original_allocated


def test_zero_inventory_skus_excluded(state, config):
    """SKUs with on_hand == 0 in state.inventory should not appear in positions."""
    # Insert a zero-qty SKU into W01
    fake_sku = "FF-GATE-DN15-CS-PN16-FL-M-ZERO"
    state.inventory["W01"][fake_sku] = {"on_hand": 0, "allocated": 0, "in_transit": 0}

    try:
        events = run(state, config, _BUSINESS_DAY)
        w01_event = next(e for e in events if e.warehouse == "W01")
        sku_set = {p.sku for p in w01_event.positions}
        assert fake_sku not in sku_set
    finally:
        state.inventory["W01"].pop(fake_sku, None)


def test_snapshot_available_never_negative(tmp_path):
    """Run a 30-day simulation and verify no snapshot position has quantity_available < 0."""
    from datetime import timedelta

    from flowform.cli import _run_day_loop
    from flowform.config import load_config
    from flowform.engines.snapshots import InventorySnapshotEvent
    from flowform.state import SimulationState

    config_path = Path("/home/coder/sc-sim/config.yaml")
    config = load_config(config_path)

    db_path = tmp_path / "sim.db"
    output_dir = tmp_path / "output"
    state = SimulationState.from_new(config, db_path=db_path)

    all_snapshots: list[InventorySnapshotEvent] = []
    for _ in range(30):
        next_date = state.current_date + timedelta(days=1)
        state.advance_day(next_date)
        events = _run_day_loop(state, config, next_date, output_dir=output_dir)
        for ev in events:
            if isinstance(ev, InventorySnapshotEvent):
                all_snapshots.append(ev)
        state.save()

    assert all_snapshots, "Expected at least one inventory_snapshot event in 30 days"
    for snap in all_snapshots:
        for pos in snap.positions:
            assert pos.quantity_available >= 0, (
                f"Negative quantity_available ({pos.quantity_available}) for SKU "
                f"{pos.sku} in warehouse {snap.warehouse}"
            )


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
