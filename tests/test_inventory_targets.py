"""Tests for the inventory targets engine (Phase 4, Step 36)."""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest

from flowform.config import load_config
from flowform.engines.inventory_targets import InventoryTargetEvent, run
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

_FIRST_BIZ_DAY = date(2026, 2, 2)   # Monday — first business day of February 2026
_MID_MONTH = date(2026, 2, 16)       # Monday — mid-month, not the first business day
_SATURDAY = date(2026, 1, 3)         # Saturday
_HOLIDAY = date(2026, 1, 1)          # New Year's Day (Polish holiday)


# ---------------------------------------------------------------------------
# Test 1: no events on non-first business day
# ---------------------------------------------------------------------------


def test_no_events_on_non_first_business_day(state, config):
    """Engine must return [] on a mid-month business day."""
    result = run(state, config, _MID_MONTH)
    assert result == []


# ---------------------------------------------------------------------------
# Test 2: no events on weekend
# ---------------------------------------------------------------------------


def test_no_events_on_weekend(state, config):
    """Engine must return [] on Saturday."""
    result = run(state, config, _SATURDAY)
    assert result == []


# ---------------------------------------------------------------------------
# Test 3: no events on holiday
# ---------------------------------------------------------------------------


def test_no_events_on_holiday(state, config):
    """Engine must return [] on a Polish public holiday."""
    result = run(state, config, _HOLIDAY)
    assert result == []


# ---------------------------------------------------------------------------
# Test 4: fires on first business day of month
# ---------------------------------------------------------------------------


def test_fires_on_first_business_day_of_month(state, config):
    """Engine must return a non-empty list on the first business day of the month."""
    result = run(state, config, _FIRST_BIZ_DAY)
    assert len(result) > 0


# ---------------------------------------------------------------------------
# Test 5: event_type is inventory_target
# ---------------------------------------------------------------------------


def test_event_type_is_inventory_target(state, config):
    """All returned events must have event_type == 'inventory_target'."""
    result = run(state, config, _FIRST_BIZ_DAY)
    for event in result:
        assert event.event_type == "inventory_target"


# ---------------------------------------------------------------------------
# Test 6: target_id format
# ---------------------------------------------------------------------------


def test_target_id_format(state, config):
    """All target_ids must match r'^TGT-\\d{8}-\\d{4}$'."""
    result = run(state, config, _FIRST_BIZ_DAY)
    pattern = re.compile(r"^TGT-\d{8}-\d{4}$")
    for event in result:
        assert pattern.match(event.target_id), (
            f"Bad target_id: {event.target_id!r}"
        )


# ---------------------------------------------------------------------------
# Test 7: both warehouses covered
# ---------------------------------------------------------------------------


def test_both_warehouses_covered(state, config):
    """Result must contain events for both 'W01' and 'W02'."""
    result = run(state, config, _FIRST_BIZ_DAY)
    warehouses = {event.warehouse for event in result}
    assert "W01" in warehouses
    assert "W02" in warehouses


# ---------------------------------------------------------------------------
# Test 8: stock levels ordered
# ---------------------------------------------------------------------------


def test_stock_levels_ordered(state, config):
    """For every event: safety_stock <= reorder_point <= target_stock."""
    result = run(state, config, _FIRST_BIZ_DAY)
    for event in result:
        assert event.safety_stock <= event.reorder_point, (
            f"safety_stock {event.safety_stock} > reorder_point {event.reorder_point} "
            f"for sku={event.sku}, warehouse={event.warehouse}"
        )
        assert event.reorder_point <= event.target_stock, (
            f"reorder_point {event.reorder_point} > target_stock {event.target_stock} "
            f"for sku={event.sku}, warehouse={event.warehouse}"
        )


# ---------------------------------------------------------------------------
# Test 9: all stock levels positive
# ---------------------------------------------------------------------------


def test_all_stock_levels_positive(state, config):
    """safety_stock, reorder_point, and target_stock must all be >= 1."""
    result = run(state, config, _FIRST_BIZ_DAY)
    for event in result:
        assert event.safety_stock >= 1, (
            f"safety_stock < 1 for sku={event.sku}, warehouse={event.warehouse}"
        )
        assert event.reorder_point >= 1, (
            f"reorder_point < 1 for sku={event.sku}, warehouse={event.warehouse}"
        )
        assert event.target_stock >= 1, (
            f"target_stock < 1 for sku={event.sku}, warehouse={event.warehouse}"
        )


# ---------------------------------------------------------------------------
# Test 10: review_trigger constant
# ---------------------------------------------------------------------------


def test_review_trigger_constant(state, config):
    """All events must have review_trigger == 'demand_plan_update'."""
    result = run(state, config, _FIRST_BIZ_DAY)
    for event in result:
        assert event.review_trigger == "demand_plan_update"


# ---------------------------------------------------------------------------
# Test 11: counter increments
# ---------------------------------------------------------------------------


def test_counter_increments(state, config):
    """state.counters['target'] must equal the number of events returned."""
    # Ensure counter starts at 0 for this test
    state.counters["target"] = 0
    result = run(state, config, _FIRST_BIZ_DAY)
    assert state.counters["target"] == len(result), (
        f"Counter {state.counters['target']} != event count {len(result)}"
    )


# ---------------------------------------------------------------------------
# Test 12: reproducibility
# ---------------------------------------------------------------------------


def test_reproducibility(config, tmp_path: Path):
    """Two states with the same seed must produce identical events on the same date."""
    import random

    def run_once(call_n: int) -> list[dict]:
        fresh_state = SimulationState.from_new(
            config, db_path=tmp_path / f"sim_repro_{call_n}.db"
        )
        fresh_state.rng = random.Random(42)
        fresh_state.counters["target"] = 0
        return [e.model_dump() for e in run(fresh_state, config, _FIRST_BIZ_DAY)]

    first = run_once(0)
    second = run_once(1)

    assert first == second, (
        "Reproducibility failed: identical seeds produced different output"
    )


# ---------------------------------------------------------------------------
# Test 13: sku_in_catalog
# ---------------------------------------------------------------------------


def test_sku_in_catalog(state, config):
    """Every event.sku must be present in the set of catalog SKU strings."""
    catalog_skus = {entry.sku for entry in state.catalog}
    result = run(state, config, _FIRST_BIZ_DAY)
    for event in result:
        assert event.sku in catalog_skus, (
            f"SKU {event.sku!r} not found in catalog"
        )


# ---------------------------------------------------------------------------
# Test 14: event_count_range
# ---------------------------------------------------------------------------


def test_event_count_range(state, config):
    """Between 20 and 40 events (10–20 SKUs x 2 warehouses) on a first-biz-day run."""
    result = run(state, config, _FIRST_BIZ_DAY)
    assert 20 <= len(result) <= 40, (
        f"Expected 20–40 events, got {len(result)}"
    )
