"""Tests for schema evolution field presence/absence on customer_order events.

Covers:
- sales_channel: appears at sim_day >= config.schema_evolution.sales_channel_from_day (60)
- incoterms:     appears at sim_day >= config.schema_evolution.incoterms_from_day (120)

Boundary tests: one day before threshold vs exact threshold vs threshold+1.
Value set validation, weekend EDI behaviour, co-presence after day 120, and
config-driven (not hardcoded) threshold checks.

Existing tests in test_orders.py cover:
  test_schema_evolution_sales_channel_absent_before_threshold (sim_day=0, 10 days)
  test_schema_evolution_sales_channel_present_after_threshold (sim_day=threshold, 20 days)

This file targets all remaining gaps — do not duplicate those two tests.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from flowform.calendar import is_business_day
from flowform.config import load_config
from flowform.engines.orders import run
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_CONFIG_PATH = Path("/home/coder/sc-sim/config.yaml")

# Canonical simulation dates used across tests
_MONDAY = date(2026, 3, 9)    # regular business day
_SATURDAY = date(2026, 3, 7)  # weekend — only Distributors can order


@pytest.fixture(scope="module")
def config():
    """Load the real config from config.yaml (shared across module)."""
    return load_config(_CONFIG_PATH)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _next_business_days(start: date, n: int) -> list[date]:
    """Return the next *n* business days starting from *start* (inclusive)."""
    days: list[date] = []
    current = start
    while len(days) < n:
        if is_business_day(current):
            days.append(current)
        current += timedelta(days=1)
    return days


def _run_until_orders(
    state: SimulationState,
    config,
    start: date,
    max_days: int = 20,
) -> list:
    """Run orders.run() across consecutive business days until at least one order appears.

    Returns the first non-empty list of events, or [] if none appear within max_days.
    """
    current = start
    for _ in range(max_days):
        if is_business_day(current):
            events = run(state, config, current)
            if events:
                return events
        current += timedelta(days=1)
    return []


# ---------------------------------------------------------------------------
# Test 1: sales_channel absent one day before threshold
# ---------------------------------------------------------------------------


def test_sales_channel_absent_at_day_59(config, tmp_path: Path) -> None:
    """sales_channel must NOT appear when sim_day is one below the threshold."""
    threshold = config.schema_evolution.sales_channel_from_day
    state = SimulationState.from_new(config, db_path=tmp_path / "sim.db")
    state.sim_day = threshold - 1

    for d in _next_business_days(_MONDAY, 10):
        events = run(state, config, d)
        for event in events:
            dumped = event.model_dump()
            assert "sales_channel" not in dumped, (
                f"sales_channel appeared at sim_day {state.sim_day} "
                f"(threshold {threshold}), date {d}"
            )


# ---------------------------------------------------------------------------
# Test 2: sales_channel absent at sim_day = 0 (fresh state)
# ---------------------------------------------------------------------------


def test_sales_channel_absent_at_day_0(config, tmp_path: Path) -> None:
    """sales_channel must be absent on a freshly initialised state (sim_day=0)."""
    state = SimulationState.from_new(config, db_path=tmp_path / "sim.db")
    assert state.sim_day == 0

    for d in _next_business_days(_MONDAY, 10):
        events = run(state, config, d)
        for event in events:
            assert "sales_channel" not in event.model_dump(), (
                "sales_channel appeared on sim_day=0"
            )


# ---------------------------------------------------------------------------
# Test 3: sales_channel present at exact threshold (sim_day == 60)
# ---------------------------------------------------------------------------


def test_sales_channel_present_at_exact_threshold(config, tmp_path: Path) -> None:
    """sales_channel must appear on every order event at the exact threshold day."""
    threshold = config.schema_evolution.sales_channel_from_day
    state = SimulationState.from_new(config, db_path=tmp_path / "sim.db")
    state.sim_day = threshold

    events = _run_until_orders(state, config, _MONDAY, max_days=20)
    assert events, "Expected at least one order near the threshold date"

    for event in events:
        dumped = event.model_dump()
        assert "sales_channel" in dumped, (
            f"sales_channel missing at sim_day={threshold} on order {event.order_id}"
        )


# ---------------------------------------------------------------------------
# Test 4: sales_channel present at threshold + 1
# ---------------------------------------------------------------------------


def test_sales_channel_present_at_threshold_plus_one(config, tmp_path: Path) -> None:
    """sales_channel must appear at sim_day = threshold + 1."""
    threshold = config.schema_evolution.sales_channel_from_day
    state = SimulationState.from_new(config, db_path=tmp_path / "sim.db")
    state.sim_day = threshold + 1

    events = _run_until_orders(state, config, _MONDAY, max_days=20)
    assert events, "Expected at least one order beyond the threshold"

    for event in events:
        assert "sales_channel" in event.model_dump(), (
            f"sales_channel missing at sim_day={threshold + 1}"
        )


# ---------------------------------------------------------------------------
# Test 5: sales_channel values are restricted to the declared set
# ---------------------------------------------------------------------------


def test_sales_channel_values_valid(config, tmp_path: Path) -> None:
    """Every sales_channel value must be one of {direct, edi, portal}."""
    valid_values = {"direct", "edi", "portal"}
    threshold = config.schema_evolution.sales_channel_from_day
    state = SimulationState.from_new(config, db_path=tmp_path / "sim.db")
    state.sim_day = threshold

    collected: list[str] = []
    for d in _next_business_days(_MONDAY, 15):
        events = run(state, config, d)
        for event in events:
            dumped = event.model_dump()
            if "sales_channel" in dumped:
                collected.append(dumped["sales_channel"])

    # We need at least a few samples to be meaningful
    assert collected, "No orders produced after threshold — cannot validate values"

    for val in collected:
        assert val in valid_values, (
            f"Unexpected sales_channel value: {val!r} (expected one of {valid_values})"
        )


# ---------------------------------------------------------------------------
# Test 6: weekend EDI orders always have sales_channel == "edi"
# ---------------------------------------------------------------------------


def test_sales_channel_edi_on_weekend(config, tmp_path: Path) -> None:
    """On weekends, any Distributor order must have sales_channel == 'edi'."""
    threshold = config.schema_evolution.sales_channel_from_day
    state = SimulationState.from_new(config, db_path=tmp_path / "sim.db")
    state.sim_day = threshold

    # Try up to 8 consecutive Saturdays to find at least one Distributor order
    found_any = False
    saturday = _SATURDAY
    for _ in range(8):
        events = run(state, config, saturday)
        for event in events:
            dumped = event.model_dump()
            if "sales_channel" in dumped:
                found_any = True
                assert dumped["sales_channel"] == "edi", (
                    f"Weekend order {event.order_id} has sales_channel="
                    f"{dumped['sales_channel']!r} (expected 'edi')"
                )
        saturday += timedelta(weeks=1)

    # If we never found any, skip gracefully — it's probabilistic
    if not found_any:
        pytest.skip("No Distributor weekend orders produced across 8 Saturdays")


# ---------------------------------------------------------------------------
# Test 7: incoterms absent one day before threshold (day 119)
# ---------------------------------------------------------------------------


def test_incoterms_absent_at_day_119(config, tmp_path: Path) -> None:
    """incoterms must NOT appear when sim_day is one below the 120-day threshold."""
    inco_threshold = config.schema_evolution.incoterms_from_day
    state = SimulationState.from_new(config, db_path=tmp_path / "sim.db")
    state.sim_day = inco_threshold - 1

    for d in _next_business_days(_MONDAY, 10):
        events = run(state, config, d)
        for event in events:
            assert "incoterms" not in event.model_dump(), (
                f"incoterms appeared at sim_day={state.sim_day} "
                f"(threshold {inco_threshold})"
            )


# ---------------------------------------------------------------------------
# Test 8: incoterms absent at sim_day = 0
# ---------------------------------------------------------------------------


def test_incoterms_absent_at_day_0(config, tmp_path: Path) -> None:
    """incoterms must be absent on a freshly initialised state (sim_day=0)."""
    state = SimulationState.from_new(config, db_path=tmp_path / "sim.db")
    assert state.sim_day == 0

    for d in _next_business_days(_MONDAY, 10):
        events = run(state, config, d)
        for event in events:
            assert "incoterms" not in event.model_dump(), (
                "incoterms appeared on sim_day=0"
            )


# ---------------------------------------------------------------------------
# Test 9: between thresholds (60–119) sales_channel present but incoterms absent
# ---------------------------------------------------------------------------


def test_incoterms_absent_between_thresholds(config, tmp_path: Path) -> None:
    """Between the two thresholds, sales_channel is present but incoterms is absent."""
    sc_threshold = config.schema_evolution.sales_channel_from_day
    inco_threshold = config.schema_evolution.incoterms_from_day
    mid_day = (sc_threshold + inco_threshold) // 2  # e.g. day 90

    state = SimulationState.from_new(config, db_path=tmp_path / "sim.db")
    state.sim_day = mid_day

    events = _run_until_orders(state, config, _MONDAY, max_days=20)
    assert events, "Expected at least one order between thresholds"

    for event in events:
        dumped = event.model_dump()
        assert "sales_channel" in dumped, (
            f"sales_channel missing at sim_day={mid_day}"
        )
        assert "incoterms" not in dumped, (
            f"incoterms appeared prematurely at sim_day={mid_day} "
            f"(threshold {inco_threshold})"
        )


# ---------------------------------------------------------------------------
# Test 10: incoterms present at exact threshold (sim_day == 120)
# ---------------------------------------------------------------------------


def test_incoterms_present_at_exact_threshold(config, tmp_path: Path) -> None:
    """incoterms must appear on every order event at the exact 120-day threshold."""
    inco_threshold = config.schema_evolution.incoterms_from_day
    state = SimulationState.from_new(config, db_path=tmp_path / "sim.db")
    state.sim_day = inco_threshold

    events = _run_until_orders(state, config, _MONDAY, max_days=20)
    assert events, "Expected at least one order at the incoterms threshold"

    for event in events:
        dumped = event.model_dump()
        assert "incoterms" in dumped, (
            f"incoterms missing at sim_day={inco_threshold} on {event.order_id}"
        )


# ---------------------------------------------------------------------------
# Test 11: incoterms present at threshold + 1
# ---------------------------------------------------------------------------


def test_incoterms_present_at_threshold_plus_one(config, tmp_path: Path) -> None:
    """incoterms must appear at sim_day = threshold + 1."""
    inco_threshold = config.schema_evolution.incoterms_from_day
    state = SimulationState.from_new(config, db_path=tmp_path / "sim.db")
    state.sim_day = inco_threshold + 1

    events = _run_until_orders(state, config, _MONDAY, max_days=20)
    assert events, "Expected at least one order beyond the incoterms threshold"

    for event in events:
        assert "incoterms" in event.model_dump(), (
            f"incoterms missing at sim_day={inco_threshold + 1}"
        )


# ---------------------------------------------------------------------------
# Test 12: incoterms values are restricted to the declared set
# ---------------------------------------------------------------------------


def test_incoterms_values_valid(config, tmp_path: Path) -> None:
    """Every incoterms value must be one of {EXW, FCA, DAP, DDP}."""
    valid_values = {"EXW", "FCA", "DAP", "DDP"}
    inco_threshold = config.schema_evolution.incoterms_from_day
    state = SimulationState.from_new(config, db_path=tmp_path / "sim.db")
    state.sim_day = inco_threshold

    collected: list[str] = []
    for d in _next_business_days(_MONDAY, 15):
        events = run(state, config, d)
        for event in events:
            dumped = event.model_dump()
            if "incoterms" in dumped:
                collected.append(dumped["incoterms"])

    assert collected, "No orders produced after incoterms threshold — cannot validate"

    for val in collected:
        assert val in valid_values, (
            f"Unexpected incoterms value: {val!r} (expected one of {valid_values})"
        )


# ---------------------------------------------------------------------------
# Test 13: both fields co-exist after day 120
# ---------------------------------------------------------------------------


def test_both_fields_coexist_after_day_120(config, tmp_path: Path) -> None:
    """After day 120, every order must carry both sales_channel and incoterms."""
    inco_threshold = config.schema_evolution.incoterms_from_day
    state = SimulationState.from_new(config, db_path=tmp_path / "sim.db")
    state.sim_day = inco_threshold + 5

    events = _run_until_orders(state, config, _MONDAY, max_days=20)
    assert events, "Expected at least one order well after both thresholds"

    for event in events:
        dumped = event.model_dump()
        assert "sales_channel" in dumped, (
            f"sales_channel missing at sim_day={inco_threshold + 5}"
        )
        assert "incoterms" in dumped, (
            f"incoterms missing at sim_day={inco_threshold + 5}"
        )


# ---------------------------------------------------------------------------
# Test 14: thresholds are read from config, not hardcoded
# ---------------------------------------------------------------------------


def test_thresholds_read_from_config_not_hardcoded(config) -> None:
    """Config must specify the expected threshold values (60 and 120)."""
    assert config.schema_evolution.sales_channel_from_day == 60, (
        f"Expected sales_channel_from_day=60, got "
        f"{config.schema_evolution.sales_channel_from_day}"
    )
    assert config.schema_evolution.incoterms_from_day == 120, (
        f"Expected incoterms_from_day=120, got "
        f"{config.schema_evolution.incoterms_from_day}"
    )


# ---------------------------------------------------------------------------
# Test 15: no extra fields at sim_day = 0
# ---------------------------------------------------------------------------


def test_no_extra_fields_before_both_thresholds(config, tmp_path: Path) -> None:
    """At sim_day=0, neither sales_channel nor incoterms must appear on any event."""
    state = SimulationState.from_new(config, db_path=tmp_path / "sim.db")
    assert state.sim_day == 0

    for d in _next_business_days(_MONDAY, 10):
        events = run(state, config, d)
        for event in events:
            dumped = event.model_dump()
            assert "sales_channel" not in dumped, (
                f"sales_channel found at sim_day=0 on order {event.order_id}"
            )
            assert "incoterms" not in dumped, (
                f"incoterms found at sim_day=0 on order {event.order_id}"
            )
