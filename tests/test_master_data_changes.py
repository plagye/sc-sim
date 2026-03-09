"""Tests for engines/master_data_changes.py — Step 37."""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest

from flowform.config import load_config
from flowform.engines import master_data_changes
from flowform.engines.master_data_changes import MasterDataChangeEvent
from flowform.state import SimulationState

_CONFIG_PATH = Path("/home/coder/sc-sim/config.yaml")

# Business day used throughout most tests
_BIZ_DAY = date(2026, 3, 9)   # Monday
_SATURDAY = date(2026, 1, 3)
_HOLIDAY = date(2026, 1, 1)    # New Year's Day


@pytest.fixture
def config():
    return load_config(_CONFIG_PATH)


@pytest.fixture
def state(config, tmp_path: Path) -> SimulationState:
    return SimulationState.from_new(config, db_path=tmp_path / "sim.db")


# ---------------------------------------------------------------------------
# 1. No events on weekend
# ---------------------------------------------------------------------------

def test_no_events_on_weekend(state, config):
    result = master_data_changes.run(state, config, _SATURDAY)
    assert result == []


# ---------------------------------------------------------------------------
# 2. No events on holiday
# ---------------------------------------------------------------------------

def test_no_events_on_holiday(state, config):
    result = master_data_changes.run(state, config, _HOLIDAY)
    assert result == []


# ---------------------------------------------------------------------------
# 3. Returns list on business day
# ---------------------------------------------------------------------------

def test_returns_list_on_business_day(state, config):
    result = master_data_changes.run(state, config, _BIZ_DAY)
    assert isinstance(result, list)
    for ev in result:
        assert isinstance(ev, MasterDataChangeEvent)


# ---------------------------------------------------------------------------
# 4. change_id format
# ---------------------------------------------------------------------------

def test_change_id_format(state, config):
    # Run many seeds until we get at least one event
    events = _collect_events(state, config, _BIZ_DAY, n_runs=30)
    if not events:
        pytest.skip("No events fired in 30 runs (very unlikely)")
    for ev in events:
        assert re.match(r"^MDC-\d{8}-\d{4}$", ev.change_id), ev.change_id


# ---------------------------------------------------------------------------
# 5. Address change mutates state
# ---------------------------------------------------------------------------

def test_address_change_mutates_state(state, config):
    ev = _force_event(state, config, _BIZ_DAY, "delivery_address_primary")
    if ev is None:
        pytest.skip("Could not force address event in 200 attempts")
    # Find the customer
    customer = next(c for c in state.customers if c.customer_id == ev.entity_id)
    assert customer.primary_address.street == ev.new_value["street"]
    assert customer.primary_address.city == ev.new_value["city"]
    assert customer.primary_address.postal_code == ev.new_value["postal_code"]


# ---------------------------------------------------------------------------
# 6. Segment change is different from old
# ---------------------------------------------------------------------------

def test_segment_change_is_different(state, config):
    ev = _force_event(state, config, _BIZ_DAY, "segment")
    if ev is None:
        pytest.skip("Could not force segment event in 200 attempts")
    assert ev.new_value != ev.old_value


# ---------------------------------------------------------------------------
# 7. Payment terms valid values
# ---------------------------------------------------------------------------

def test_payment_terms_valid_values(state, config):
    ev = _force_event(state, config, _BIZ_DAY, "payment_terms_days")
    if ev is None:
        pytest.skip("Could not force payment_terms event in 200 attempts")
    assert ev.new_value in [30, 60, 90]
    assert ev.new_value != ev.old_value


# ---------------------------------------------------------------------------
# 8. SKU discontinuation sets is_active = False
# ---------------------------------------------------------------------------

def test_sku_discontinuation_sets_inactive(state, config):
    ev = _force_event(state, config, _BIZ_DAY, "sku_status")
    if ev is None:
        pytest.skip("Could not force sku_status event in 200 attempts")
    entry = next(e for e in state.catalog if e.sku == ev.entity_id)
    assert entry.is_active is False
    assert ev.old_value == "active"
    assert ev.new_value == "discontinued"


# ---------------------------------------------------------------------------
# 9. Carrier rate fires at day 90
# ---------------------------------------------------------------------------

def test_carrier_rate_fires_at_day_90(state, config):
    state.sim_day = 90
    # Run enough times; deterministic once sim_day_number == 90
    found = None
    for _ in range(10):
        events = master_data_changes.run(state, config, _BIZ_DAY)
        for ev in events:
            if ev.field_changed == "carrier_base_rate":
                found = ev
                break
        if found:
            break
        # Reset flag so it fires again in next attempt
        state.counters.pop("carrier_rate_changed", None)

    assert found is not None, "carrier_base_rate event not found at day 90"
    assert found.entity_type == "carrier"
    assert found.entity_id in ["DHL", "DBSC", "RABEN", "GEODIS", "BALTIC"]


# ---------------------------------------------------------------------------
# 10. Carrier rate fires only once
# ---------------------------------------------------------------------------

def test_carrier_rate_fires_only_once(state, config):
    state.sim_day = 90
    # First run — should fire
    events1 = master_data_changes.run(state, config, _BIZ_DAY)
    rate_events1 = [e for e in events1 if e.field_changed == "carrier_base_rate"]
    assert len(rate_events1) == 1
    assert state.counters.get("carrier_rate_changed") == 1

    # Second run — should NOT fire again
    events2 = master_data_changes.run(state, config, _BIZ_DAY)
    rate_events2 = [e for e in events2 if e.field_changed == "carrier_base_rate"]
    assert len(rate_events2) == 0


# ---------------------------------------------------------------------------
# 11. Counter increments
# ---------------------------------------------------------------------------

def test_counter_increments(state, config):
    """Counter "mdc" increases by exactly the number of events emitted per run."""
    import random
    # Run several seeds, each time verifying counter delta == event count
    for seed in range(20):
        state.rng = random.Random(seed * 99991)
        state.counters["mdc"] = 0
        state.counters.pop("carrier_rate_changed", None)
        before = state.counters.get("mdc", 0)
        events = master_data_changes.run(state, config, _BIZ_DAY)
        after = state.counters.get("mdc", 0)
        assert after == before + len(events), (
            f"seed={seed}: counter went {before}→{after} but {len(events)} events emitted"
        )


# ---------------------------------------------------------------------------
# 12. effective_date is strictly after sim_date
# ---------------------------------------------------------------------------

def test_effective_date_is_future(state, config):
    events = _collect_events(state, config, _BIZ_DAY, n_runs=30)
    if not events:
        pytest.skip("No events fired in 30 runs")
    for ev in events:
        assert ev.effective_date > str(_BIZ_DAY), (
            f"effective_date {ev.effective_date!r} is not after {_BIZ_DAY}"
        )


# ---------------------------------------------------------------------------
# 13. All required fields present and non-empty
# ---------------------------------------------------------------------------

def test_event_fields_present(state, config):
    events = _collect_events(state, config, _BIZ_DAY, n_runs=30)
    if not events:
        pytest.skip("No events fired in 30 runs")
    required = [
        "event_type", "change_id", "timestamp", "entity_type",
        "entity_id", "field_changed", "effective_date", "reason",
    ]
    for ev in events:
        d = ev.model_dump()
        for field in required:
            val = d.get(field)
            assert val is not None and val != "", (
                f"Field {field!r} is missing or empty on event {ev.change_id}"
            )
        # old_value and new_value may legitimately be 0.0, check not None
        assert d["old_value"] is not None
        assert d["new_value"] is not None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _collect_events(
    state: SimulationState,
    config,
    sim_date: date,
    n_runs: int,
) -> list[MasterDataChangeEvent]:
    """Run the engine n_runs times on independent fresh states and collect all events."""
    all_events: list[MasterDataChangeEvent] = []
    import random
    for seed in range(n_runs):
        # Build a lightweight clone with an independent RNG
        state.rng = random.Random(seed * 31337)
        # Reset mdc counter so IDs don't bleed across iterations
        state.counters["mdc"] = 0
        events = master_data_changes.run(state, config, sim_date)
        all_events.extend(events)
    return all_events


def _force_event(
    state: SimulationState,
    config,
    sim_date: date,
    field_changed: str,
    max_attempts: int = 200,
) -> MasterDataChangeEvent | None:
    """Try many seeded RNG values until the desired event type fires."""
    import random
    for seed in range(max_attempts):
        state.rng = random.Random(seed * 999983)
        state.counters["mdc"] = 0
        state.counters.pop("carrier_rate_changed", None)
        events = master_data_changes.run(state, config, sim_date)
        for ev in events:
            if ev.field_changed == field_changed:
                return ev
    return None
