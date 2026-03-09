"""Tests for the demand signals engine (Phase 4, Step 33)."""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest

from flowform.config import load_config
from flowform.engines.demand_signals import (
    SIGNAL_SOURCES,
    SIGNAL_TYPES,
    DemandSignalEvent,
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
# Date constants
# ---------------------------------------------------------------------------

_BUSINESS_DAY = date(2026, 1, 5)   # Monday
_SATURDAY = date(2026, 1, 3)
_HOLIDAY = date(2026, 1, 1)        # New Year's Day (Polish holiday)


# ---------------------------------------------------------------------------
# Helper: force probability=1.0 so we always get signals
# ---------------------------------------------------------------------------


def _force_signals(config, state: SimulationState) -> list[DemandSignalEvent]:
    """Run the engine with probability forced to 1.0 so all 10 slots fire."""
    config.demand.signal_probability_daily = 1.0
    return run(state, config, _BUSINESS_DAY)


# ---------------------------------------------------------------------------
# Test 1: no events on weekend
# ---------------------------------------------------------------------------


def test_no_events_on_weekend(state, config):
    """Engine must return [] on Saturday."""
    result = run(state, config, _SATURDAY)
    assert result == []


# ---------------------------------------------------------------------------
# Test 2: no events on holiday
# ---------------------------------------------------------------------------


def test_no_events_on_holiday(state, config):
    """Engine must return [] on Polish public holiday."""
    result = run(state, config, _HOLIDAY)
    assert result == []


# ---------------------------------------------------------------------------
# Test 3: returns DemandSignalEvent instances
# ---------------------------------------------------------------------------


def test_returns_demand_signal_events(state, config):
    """With probability=1.0, all returned items must be DemandSignalEvent instances."""
    signals = _force_signals(config, state)
    assert len(signals) == 10
    for sig in signals:
        assert isinstance(sig, DemandSignalEvent)


# ---------------------------------------------------------------------------
# Test 4: event_type field
# ---------------------------------------------------------------------------


def test_event_type_field(state, config):
    """Every signal must have event_type == 'demand_signal'."""
    config.demand.signal_probability_daily = 1.0
    signals = run(state, config, _BUSINESS_DAY)
    assert len(signals) > 0
    for sig in signals:
        assert sig.event_type == "demand_signal"


# ---------------------------------------------------------------------------
# Test 5: signal_id format
# ---------------------------------------------------------------------------


def test_signal_id_format(state, config):
    """signal_id must match SIG-YYYYMMDD-NNNN."""
    config.demand.signal_probability_daily = 1.0
    signals = run(state, config, _BUSINESS_DAY)
    pattern = re.compile(r"SIG-\d{8}-\d{4}")
    for sig in signals:
        assert pattern.match(sig.signal_id), f"Bad signal_id: {sig.signal_id!r}"


# ---------------------------------------------------------------------------
# Test 6: signal_source valid
# ---------------------------------------------------------------------------


def test_signal_source_valid(state, config):
    """Every signal_source must be one of the six defined values."""
    config.demand.signal_probability_daily = 1.0
    signals = run(state, config, _BUSINESS_DAY)
    for sig in signals:
        assert sig.signal_source in SIGNAL_SOURCES, (
            f"Unexpected signal_source: {sig.signal_source!r}"
        )


# ---------------------------------------------------------------------------
# Test 7: signal_type valid
# ---------------------------------------------------------------------------


def test_signal_type_valid(state, config):
    """Every signal_type must be one of the five defined values."""
    config.demand.signal_probability_daily = 1.0
    signals = run(state, config, _BUSINESS_DAY)
    for sig in signals:
        assert sig.signal_type in SIGNAL_TYPES, (
            f"Unexpected signal_type: {sig.signal_type!r}"
        )


# ---------------------------------------------------------------------------
# Test 8: magnitude valid
# ---------------------------------------------------------------------------


def test_magnitude_valid(state, config):
    """Every magnitude must be one of 'low', 'medium', 'high'."""
    config.demand.signal_probability_daily = 1.0
    signals = run(state, config, _BUSINESS_DAY)
    valid = {"low", "medium", "high"}
    for sig in signals:
        assert sig.magnitude in valid, f"Unexpected magnitude: {sig.magnitude!r}"


# ---------------------------------------------------------------------------
# Test 9: confidence in range
# ---------------------------------------------------------------------------


def test_confidence_in_range(state, config):
    """Confidence must be in [0.1, 0.9]."""
    config.demand.signal_probability_daily = 1.0
    signals = run(state, config, _BUSINESS_DAY)
    for sig in signals:
        assert 0.1 <= sig.confidence <= 0.9, (
            f"Confidence out of range: {sig.confidence}"
        )


# ---------------------------------------------------------------------------
# Test 10: horizon_weeks in range
# ---------------------------------------------------------------------------


def test_horizon_weeks_in_range(state, config):
    """horizon_weeks must be between 2 and 26 inclusive."""
    config.demand.signal_probability_daily = 1.0
    signals = run(state, config, _BUSINESS_DAY)
    for sig in signals:
        assert 2 <= sig.horizon_weeks <= 26, (
            f"horizon_weeks out of range: {sig.horizon_weeks}"
        )


# ---------------------------------------------------------------------------
# Test 11: customer_id is None for market_intelligence and competitor_event
# ---------------------------------------------------------------------------


def test_customer_id_none_for_market_signals(state, config, tmp_path: Path):
    """market_intelligence and competitor_event signals must have customer_id=None."""
    import random

    # Run many trials to collect signals of all source types
    all_signals: list[DemandSignalEvent] = []
    config.demand.signal_probability_daily = 1.0
    # Use a fixed seed to get coverage of both sourceless types
    state.rng = random.Random(99)
    for day_offset in range(20):
        sim_date = date(2026, 1, 5 + day_offset)
        # Only run on weekdays (the engine self-guards, but advance day counter)
        if sim_date.weekday() < 5:
            all_signals.extend(run(state, config, sim_date))

    sourceless = {"market_intelligence", "competitor_event"}
    for sig in all_signals:
        if sig.signal_source in sourceless:
            assert sig.customer_id is None, (
                f"customer_id should be None for source={sig.signal_source!r}, "
                f"got {sig.customer_id!r}"
            )


# ---------------------------------------------------------------------------
# Test 12: product_group format
# ---------------------------------------------------------------------------


def test_product_group_format(state, config):
    """product_group must match the pattern XX-DNNNN-MAT."""
    config.demand.signal_probability_daily = 1.0
    signals = run(state, config, _BUSINESS_DAY)
    # e.g. BL-DN100-S316 or GA-DN15-CS
    pattern = re.compile(r"[A-Z]{2}-DN\d+-[A-Z0-9]+")
    for sig in signals:
        assert pattern.match(sig.product_group), (
            f"Bad product_group format: {sig.product_group!r}"
        )


# ---------------------------------------------------------------------------
# Test 13: counter increments by the number of signals generated
# ---------------------------------------------------------------------------


def test_counter_increments(state, config):
    """state.counters['signal'] must increase by exactly the number of signals."""
    config.demand.signal_probability_daily = 1.0
    before = state.counters.get("signal", 0)
    signals = run(state, config, _BUSINESS_DAY)
    after = state.counters.get("signal", 0)
    assert after == before + len(signals), (
        f"Counter mismatch: before={before}, after={after}, signals={len(signals)}"
    )


# ---------------------------------------------------------------------------
# Test 14: reproducibility
# ---------------------------------------------------------------------------


def test_reproducibility(state, config, tmp_path: Path):
    """Same RNG seed must produce identical signal output twice."""
    import random

    config.demand.signal_probability_daily = 1.0

    def run_once() -> list[dict]:
        # Fresh state with same seed each time
        fresh_state = SimulationState.from_new(
            config, db_path=tmp_path / f"sim_repro_{run_once.call_count}.db"
        )
        run_once.call_count += 1
        fresh_state.rng = random.Random(12345)
        fresh_state.counters["signal"] = 0
        return [s.model_dump() for s in run(fresh_state, config, _BUSINESS_DAY)]

    run_once.call_count = 0

    first = run_once()
    second = run_once()

    assert first == second, (
        "Reproducibility failed: identical seeds produced different output"
    )
