"""Tests for Step 29: Inter-warehouse transfer (replenishment) engine.

Covers demand-pull replenishment from W01 to W02:
- Business day and holiday guards
- Eligibility threshold (W02 < 30% of W01)
- Transfer quantity formula
- Inventory mutation
- Daily cap (_MAX_SKUS_PER_DAY = 5)
- Movement counter increments
- Edge cases (W02-only SKU, W01-only SKU)
- Determinism
"""

from __future__ import annotations

import copy
import math
import random
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from flowform.config import load_config
from flowform.engines import transfers
from flowform.engines.transfers import _find_eligible_skus, _REPLENISHMENT_PROB
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"

# A Monday — ordinary business day, no holidays nearby
_BIZ_DAY = date(2026, 3, 9)

# April 4 2026 is a Saturday
_SATURDAY = date(2026, 4, 4)

# May 1 2026 — Polish Labour Day (public holiday)
_HOLIDAY = date(2026, 5, 1)

# Seed 0: first random() = 0.844 >= 0.70 -> gate BLOCKS
_SEED_BLOCK = 0

# Seed 1: first random() = 0.134 < 0.70 -> gate PASSES
_SEED_PASS = 1


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def config():
    return load_config(_CONFIG_PATH)


@pytest.fixture()
def state(config, tmp_path):
    """Fresh simulation state isolated in a temp directory."""
    db = tmp_path / "sim.db"
    s = SimulationState.from_new(config, db_path=db)
    # Clear inventory to give tests a clean slate
    s.inventory.clear()
    return s


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _force_gate_pass(state: SimulationState) -> None:
    """Patch state.rng so that the first random() call returns 0.5 (< 0.70)."""
    state.rng = random.Random(_SEED_PASS)


def _force_gate_block(state: SimulationState) -> None:
    """Patch state.rng so that the first random() call returns 0.844 (>= 0.70)."""
    state.rng = random.Random(_SEED_BLOCK)


def _add_sku(state: SimulationState, sku: str, w01: int = 0, w02: int = 0) -> None:
    """Set inventory levels for a single SKU in W01 and/or W02."""
    if w01 > 0:
        state.inventory.setdefault("W01", {})[sku] = {"on_hand": w01, "allocated": 0, "in_transit": 0}
    if w02 > 0:
        state.inventory.setdefault("W02", {})[sku] = {"on_hand": w02, "allocated": 0, "in_transit": 0}


def _inv(state: SimulationState, wh: str, sku: str) -> int:
    """Get on_hand quantity from the new dict format."""
    pos = state.inventory.get(wh, {}).get(sku)
    return pos["on_hand"] if pos else 0


# ---------------------------------------------------------------------------
# T1: Weekend guard
# ---------------------------------------------------------------------------


def test_weekend_guard_returns_empty(state, config):
    """run() on a Saturday must return []."""
    _force_gate_pass(state)
    result = transfers.run(state, config, _SATURDAY)
    assert result == []


# ---------------------------------------------------------------------------
# T2: Holiday guard
# ---------------------------------------------------------------------------


def test_holiday_guard_returns_empty(state, config):
    """run() on a Polish public holiday (May 1) must return []."""
    _force_gate_pass(state)
    result = transfers.run(state, config, _HOLIDAY)
    assert result == []


# ---------------------------------------------------------------------------
# T3: No eligible SKUs (W02 adequately stocked)
# ---------------------------------------------------------------------------


def test_no_eligible_skus_adequately_stocked(state, config):
    """W01=100, W02=40. threshold=ceil(0.30*100)=30. 40>=30 => not eligible."""
    sku = "FF-GT50-CS-PN16-FL-MN"
    state.inventory["W01"] = {sku: {"on_hand": 100, "allocated": 0, "in_transit": 0}}
    state.inventory["W02"] = {sku: {"on_hand": 40, "allocated": 0, "in_transit": 0}}

    with patch.object(state.rng, "random", return_value=0.5):
        result = transfers.run(state, config, _BIZ_DAY)

    assert result == []


# ---------------------------------------------------------------------------
# T4: Eligible SKU triggers transfer (basic case)
# ---------------------------------------------------------------------------


def test_eligible_sku_triggers_transfer(state, config):
    """W01=100, W02=10. threshold=30. 10<30 => eligible.
    transfer_qty = ceil(0.25*100) - 10 = 25 - 10 = 15.
    Expect 2 InventoryMovementEvent objects.
    """
    sku = "FF-GT50-CS-PN16-FL-MN"
    state.inventory["W01"] = {sku: {"on_hand": 100, "allocated": 0, "in_transit": 0}}
    state.inventory["W02"] = {sku: {"on_hand": 10, "allocated": 0, "in_transit": 0}}

    with patch.object(state.rng, "random", return_value=0.5):
        result = transfers.run(state, config, _BIZ_DAY)

    assert len(result) == 2

    outbound = result[0]
    inbound = result[1]

    # Both have correct movement_type and reason_code
    assert outbound.movement_type == "transfer"
    assert outbound.reason_code == "replenishment"
    assert inbound.movement_type == "transfer"
    assert inbound.reason_code == "replenishment"

    # Outbound leg fields
    assert outbound.warehouse_id == "W01"
    assert outbound.quantity_direction == "out"
    assert outbound.destination_warehouse_id == "W02"
    assert outbound.quantity == 15

    # Inbound leg fields
    assert inbound.warehouse_id == "W02"
    assert inbound.quantity_direction == "in"
    assert inbound.destination_warehouse_id is None
    assert inbound.quantity == 15


# ---------------------------------------------------------------------------
# T5: Inventory mutated correctly after transfer
# ---------------------------------------------------------------------------


def test_inventory_mutated_after_transfer(state, config):
    """After a 15-unit transfer: W01 should be 85, W02 should be 25."""
    sku = "FF-GT50-CS-PN16-FL-MN"
    state.inventory["W01"] = {sku: {"on_hand": 100, "allocated": 0, "in_transit": 0}}
    state.inventory["W02"] = {sku: {"on_hand": 10, "allocated": 0, "in_transit": 0}}

    with patch.object(state.rng, "random", return_value=0.5):
        transfers.run(state, config, _BIZ_DAY)

    assert _inv(state, "W01", sku) == 85
    assert _inv(state, "W02", sku) == 25


# ---------------------------------------------------------------------------
# T6: Transfer quantity capped at W01 availability
# ---------------------------------------------------------------------------


def test_transfer_capped_at_w01_availability(state, config):
    """W01=3, W02=0. threshold=ceil(0.30*3)=1. 0<1 => eligible.
    target=ceil(0.25*3)=1. transfer=max(1, 1-0)=1. cap=min(1,3)=1.
    W01 -> 2, W02 -> 1.
    """
    sku = "FF-GT50-CS-PN16-FL-MN"
    state.inventory["W01"] = {sku: {"on_hand": 3, "allocated": 0, "in_transit": 0}}
    state.inventory["W02"] = {sku: {"on_hand": 0, "allocated": 0, "in_transit": 0}}

    with patch.object(state.rng, "random", return_value=0.5):
        result = transfers.run(state, config, _BIZ_DAY)

    # Should emit exactly 2 events (one transfer)
    assert len(result) == 2
    assert _inv(state, "W01", sku) == 2
    assert _inv(state, "W02", sku) == 1


# ---------------------------------------------------------------------------
# T7: Probability gate suppresses run
# ---------------------------------------------------------------------------


def test_probability_gate_suppresses_run(state, config):
    """Seed 0 produces random()=0.844 >= 0.70 -> gate blocks. No events, no mutation."""
    sku = "FF-GT50-CS-PN16-FL-MN"
    state.inventory["W01"] = {sku: {"on_hand": 100, "allocated": 0, "in_transit": 0}}
    state.inventory["W02"] = {sku: {"on_hand": 0, "allocated": 0, "in_transit": 0}}
    _force_gate_block(state)

    result = transfers.run(state, config, _BIZ_DAY)

    assert result == []
    # Inventory unchanged
    assert _inv(state, "W01", sku) == 100
    assert _inv(state, "W02", sku) == 0


# ---------------------------------------------------------------------------
# T8: MAX_SKUS_PER_DAY cap
# ---------------------------------------------------------------------------


def test_max_skus_per_day_cap(state, config):
    """With 10 eligible SKUs, at most 5 are processed (5 * 2 = 10 events)."""
    state.inventory["W01"] = {}
    state.inventory["W02"] = {}

    for i in range(10):
        sku = f"FF-GT{50 + i}-CS-PN16-FL-MN"
        state.inventory["W01"][sku] = {"on_hand": 100, "allocated": 0, "in_transit": 0}
        state.inventory["W02"][sku] = {"on_hand": 0, "allocated": 0, "in_transit": 0}

    with patch.object(state.rng, "random", return_value=0.5):
        result = transfers.run(state, config, _BIZ_DAY)

    # At most 5 SKUs transferred = at most 10 events
    assert len(result) <= 10
    # At least 1 transfer happened (gate passed, eligible SKUs exist)
    assert len(result) > 0
    # Must be pairs
    assert len(result) % 2 == 0


# ---------------------------------------------------------------------------
# T9: Movement counter increments
# ---------------------------------------------------------------------------


def test_movement_counter_increments(state, config):
    """After emitting N events, counters["movement"] should have increased by N."""
    sku = "FF-GT50-CS-PN16-FL-MN"
    state.inventory["W01"] = {sku: {"on_hand": 100, "allocated": 0, "in_transit": 0}}
    state.inventory["W02"] = {sku: {"on_hand": 10, "allocated": 0, "in_transit": 0}}

    before = state.counters.get("movement", 1000)

    with patch.object(state.rng, "random", return_value=0.5):
        result = transfers.run(state, config, _BIZ_DAY)

    n = len(result)
    assert n > 0
    assert state.counters["movement"] == before + n


# ---------------------------------------------------------------------------
# T10: No crash for W02-only SKU (not in W01)
# ---------------------------------------------------------------------------


def test_w02_only_sku_no_crash(state, config):
    """A SKU present only in W02 (not in W01) must be silently skipped."""
    sku_w2_only = "FF-GT50-CS-PN16-FL-MN"
    state.inventory["W01"] = {}
    state.inventory["W02"] = {sku_w2_only: {"on_hand": 5, "allocated": 0, "in_transit": 0}}

    with patch.object(state.rng, "random", return_value=0.5):
        result = transfers.run(state, config, _BIZ_DAY)

    # No crash, no transfer for this SKU
    assert all(e.sku != sku_w2_only for e in result)


# ---------------------------------------------------------------------------
# T11: W01-only SKU not transferred (replenishment only for SKUs W02 already carries)
# ---------------------------------------------------------------------------


def test_w01_only_sku_not_transferred(state, config):
    """A SKU only in W01 (absent from W02) must NOT be replenished."""
    sku_w1_only = "FF-GT50-CS-PN16-FL-MN"
    state.inventory["W01"] = {sku_w1_only: {"on_hand": 100, "allocated": 0, "in_transit": 0}}
    state.inventory["W02"] = {}

    with patch.object(state.rng, "random", return_value=0.5):
        result = transfers.run(state, config, _BIZ_DAY)

    assert all(e.sku != sku_w1_only for e in result)
    # W01 stock unchanged
    assert _inv(state, "W01", sku_w1_only) == 100


# ---------------------------------------------------------------------------
# T12: Determinism — same seed + same inventory produces same selected SKUs
# ---------------------------------------------------------------------------


def test_determinism_same_seed(config, tmp_path):
    """Two runs with the same RNG seed and identical inventory must select
    the same SKUs (by their sku field in the emitted events)."""
    # Create 10 eligible SKUs
    inv_w01 = {}
    inv_w02 = {}
    for i in range(10):
        sku = f"FF-GT{10 + i}-CS-PN16-FL-MN"
        inv_w01[sku] = {"on_hand": 100, "allocated": 0, "in_transit": 0}
        inv_w02[sku] = {"on_hand": 0, "allocated": 0, "in_transit": 0}

    def _make_state() -> SimulationState:
        db = tmp_path / f"sim_{id(object())}.db"
        s = SimulationState.from_new(config, db_path=db)
        s.inventory.clear()
        s.inventory["W01"] = copy.deepcopy(inv_w01)
        s.inventory["W02"] = copy.deepcopy(inv_w02)
        return s

    state1 = _make_state()
    state2 = _make_state()

    seed = 42
    state1.rng = random.Random(seed)
    state2.rng = random.Random(seed)

    with patch.object(state1.rng, "random", return_value=0.5):
        events1 = transfers.run(state1, config, _BIZ_DAY)

    with patch.object(state2.rng, "random", return_value=0.5):
        events2 = transfers.run(state2, config, _BIZ_DAY)

    skus1 = sorted({e.sku for e in events1})
    skus2 = sorted({e.sku for e in events2})
    assert skus1 == skus2
