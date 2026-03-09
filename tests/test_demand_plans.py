"""Tests for the demand plan generator (Phase 4, Step 34)."""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest

from flowform.config import load_config
from flowform.engines.planning import (
    DemandPlanEvent,
    _is_first_business_day_of_month,
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

# 2026-03-02 is the 1st business day of March 2026 (Monday)
_FIRST_BIZ_MARCH = date(2026, 3, 2)

# 2026-03-03 is the 2nd business day of March 2026 (Tuesday)
_SECOND_BIZ_MARCH = date(2026, 3, 3)

# 2026-01-02 is the 1st business day of January 2026 (Friday; Jan 1 is holiday)
_FIRST_BIZ_JAN = date(2026, 1, 2)

_SATURDAY = date(2026, 3, 7)

# 2026-01-01 is New Year's Day (Polish holiday)
_HOLIDAY = date(2026, 1, 1)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _run_first_biz(state: SimulationState, config, sim_date: date) -> list:
    return run(state, config, sim_date)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_no_output_on_non_first_business_day(state, config):
    """Returns [] on the 2nd business day of the month."""
    result = run(state, config, _SECOND_BIZ_MARCH)
    assert result == []


def test_no_output_on_weekend(state, config):
    """Returns [] on a Saturday."""
    result = run(state, config, _SATURDAY)
    assert result == []


def test_no_output_on_holiday(state, config):
    """Returns [] on New Year's Day (Polish public holiday)."""
    result = run(state, config, _HOLIDAY)
    assert result == []


def test_fires_on_first_business_day_of_march(state, config):
    """Returns exactly one event on 2026-03-02."""
    result = run(state, config, _FIRST_BIZ_MARCH)
    assert len(result) == 1


def test_fires_on_first_business_day_of_january(state, config):
    """Returns exactly one event on 2026-01-02 (Jan 1 is holiday)."""
    result = run(state, config, _FIRST_BIZ_JAN)
    assert len(result) == 1


def test_event_type(state, config):
    """event_type must be 'demand_plan'."""
    (event,) = run(state, config, _FIRST_BIZ_MARCH)
    assert event.event_type == "demand_plan"


def test_plan_id_format(state, config):
    """plan_id must match DP-YYYY-MM."""
    (event,) = run(state, config, _FIRST_BIZ_MARCH)
    assert re.match(r"^DP-\d{4}-\d{2}$", event.plan_id)
    assert event.plan_id == "DP-2026-03"


def test_planning_horizon_months(state, config):
    """planning_horizon_months must be 6."""
    (event,) = run(state, config, _FIRST_BIZ_MARCH)
    assert event.planning_horizon_months == 6


def test_plan_lines_count(state, config):
    """Total plan lines must be between 48 (8×6) and 90 (15×6)."""
    (event,) = run(state, config, _FIRST_BIZ_MARCH)
    assert 48 <= len(event.plan_lines) <= 90


def test_plan_lines_have_six_periods(state, config):
    """Every PlanLine must have exactly 6 periods."""
    (event,) = run(state, config, _FIRST_BIZ_MARCH)
    for line in event.plan_lines:
        assert len(line.periods) == 6, (
            f"Line {line.product_group}/{line.customer_segment} has "
            f"{len(line.periods)} periods"
        )


def test_period_month_format(state, config):
    """Each period.month is YYYY-MM and periods are consecutive from sim month."""
    (event,) = run(state, config, _FIRST_BIZ_MARCH)
    pattern = re.compile(r"^\d{4}-\d{2}$")
    expected_months = []
    y, m = _FIRST_BIZ_MARCH.year, _FIRST_BIZ_MARCH.month
    for _ in range(6):
        expected_months.append(f"{y}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1

    for line in event.plan_lines:
        actual_months = [p.month for p in line.periods]
        for month_str in actual_months:
            assert pattern.match(month_str), f"Bad month format: {month_str}"
        assert actual_months == expected_months, (
            f"Period months mismatch: {actual_months}"
        )


def test_confidence_bounds_ordering(state, config):
    """confidence_low <= planned_quantity <= confidence_high for all periods."""
    (event,) = run(state, config, _FIRST_BIZ_MARCH)
    for line in event.plan_lines:
        for period in line.periods:
            assert period.confidence_low <= period.planned_quantity, (
                f"low {period.confidence_low} > planned {period.planned_quantity}"
            )
            assert period.planned_quantity <= period.confidence_high, (
                f"planned {period.planned_quantity} > high {period.confidence_high}"
            )


def test_all_six_segments_present(state, config):
    """All 6 canonical segment strings must appear in plan_lines."""
    expected = {
        "oil_gas",
        "water_utilities",
        "chemical_plants",
        "industrial_distributors",
        "hvac_contractors",
        "shipyards",
    }
    (event,) = run(state, config, _FIRST_BIZ_MARCH)
    found = {line.customer_segment for line in event.plan_lines}
    assert found == expected


def test_reproducibility(config, tmp_path: Path):
    """Two independent runs with the same seed produce identical plan_lines."""
    state_a = SimulationState.from_new(config, db_path=tmp_path / "a.db")
    state_b = SimulationState.from_new(config, db_path=tmp_path / "b.db")

    (event_a,) = run(state_a, config, _FIRST_BIZ_MARCH)
    (event_b,) = run(state_b, config, _FIRST_BIZ_MARCH)

    assert len(event_a.plan_lines) == len(event_b.plan_lines)
    for la, lb in zip(event_a.plan_lines, event_b.plan_lines):
        assert la.product_group == lb.product_group
        assert la.customer_segment == lb.customer_segment
        for pa, pb in zip(la.periods, lb.periods):
            assert pa.planned_quantity == pb.planned_quantity
            assert pa.confidence_low == pb.confidence_low
            assert pa.confidence_high == pb.confidence_high
