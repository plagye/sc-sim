"""Tests for the demand forecast generator (Phase 4, Step 35)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from flowform.config import load_config
from flowform.engines.forecasts import (
    DemandForecastEvent,
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

_SATURDAY = date(2026, 3, 7)

# 2026-01-01 is New Year's Day (Polish holiday)
_HOLIDAY = date(2026, 1, 1)


# ---------------------------------------------------------------------------
# Tests: guard conditions
# ---------------------------------------------------------------------------


def test_no_output_on_non_first_biz_day(state, config):
    """2nd business day of month returns []."""
    result = run(state, config, _SECOND_BIZ_MARCH)
    assert result == []


def test_no_output_on_weekend(state, config):
    """Saturday returns []."""
    result = run(state, config, _SATURDAY)
    assert result == []


def test_no_output_on_holiday(state, config):
    """Polish public holiday on 1st of month returns []."""
    # Jan 1 is New Year's Day — even though it's the 1st of the month,
    # it's a holiday so the 1st biz day is Jan 2.
    result = run(state, config, _HOLIDAY)
    assert result == []


# ---------------------------------------------------------------------------
# Tests: event shape
# ---------------------------------------------------------------------------


def test_fires_on_first_business_day_of_month(state, config):
    """Returns exactly 1 DemandForecastEvent on the 1st business day."""
    result = run(state, config, _FIRST_BIZ_MARCH)
    assert len(result) == 1
    assert isinstance(result[0], DemandForecastEvent)


def test_event_type_and_forecast_id(state, config):
    """event_type == 'demand_forecast' and forecast_id matches 'FCST-2026-03'."""
    result = run(state, config, _FIRST_BIZ_MARCH)
    event = result[0]
    assert event.event_type == "demand_forecast"
    assert event.forecast_id == "FCST-2026-03"


def test_timestamp_format(state, config):
    """timestamp ends with 'T06:00:00Z' and starts with the sim date."""
    result = run(state, config, _FIRST_BIZ_MARCH)
    ts = result[0].timestamp
    assert ts.startswith("2026-03-02")
    assert ts.endswith("T06:00:00Z")


def test_horizon_months(state, config):
    """horizon_months == 6 and each line has exactly 6 periods."""
    result = run(state, config, _FIRST_BIZ_MARCH)
    event = result[0]
    assert event.horizon_months == 6
    for line in event.lines:
        assert len(line.periods) == 6


# ---------------------------------------------------------------------------
# Tests: model version
# ---------------------------------------------------------------------------


def test_model_version_before_day_90(state, config):
    """sim_day=89 → model_version == 'v2.3'."""
    state.sim_day = 89
    result = run(state, config, _FIRST_BIZ_MARCH)
    assert result[0].model_version == "v2.3"


def test_model_version_at_day_90(state, config):
    """sim_day=90 → model_version == 'v3.0'."""
    state.sim_day = 90
    result = run(state, config, _FIRST_BIZ_MARCH)
    assert result[0].model_version == "v3.0"


def test_model_version_after_day_90(state, config):
    """sim_day=120 → model_version == 'v3.0'."""
    state.sim_day = 120
    result = run(state, config, _FIRST_BIZ_MARCH)
    assert result[0].model_version == "v3.0"


# ---------------------------------------------------------------------------
# Tests: CI width comparison v2.3 vs v3.0
# ---------------------------------------------------------------------------


def test_v30_wider_intervals(config, tmp_path: Path):
    """v3.0 lower_bound is lower and upper_bound is higher than v2.3 for the
    same product group / month (same rng seed)."""
    # Two fresh states from the same seed so rng sequences align up to version branch
    state_v23 = SimulationState.from_new(config, db_path=tmp_path / "v23.db")
    state_v30 = SimulationState.from_new(config, db_path=tmp_path / "v30.db")

    state_v23.sim_day = 1    # before day 90 → v2.3
    state_v30.sim_day = 90   # at day 90 → v3.0

    result_v23 = run(state_v23, config, _FIRST_BIZ_MARCH)
    result_v30 = run(state_v30, config, _FIRST_BIZ_MARCH)

    assert result_v23[0].model_version == "v2.3"
    assert result_v30[0].model_version == "v3.0"

    # Both runs have the same catalog, same rng seed — product groups and
    # forecast_quantity values will be identical; only CI bounds differ.
    # Check the first period of the first shared product group.
    groups_v23 = {line.sku_group: line for line in result_v23[0].lines}
    groups_v30 = {line.sku_group: line for line in result_v30[0].lines}
    shared = set(groups_v23) & set(groups_v30)
    assert shared, "No shared product groups between v2.3 and v3.0 runs"

    found_wider = False
    for grp in shared:
        for p23, p30 in zip(groups_v23[grp].periods, groups_v30[grp].periods):
            if p23.forecast_quantity == p30.forecast_quantity:
                assert p30.lower_bound <= p23.lower_bound, (
                    f"v3.0 lower_bound {p30.lower_bound} should be <= v2.3 {p23.lower_bound}"
                )
                assert p30.upper_bound >= p23.upper_bound, (
                    f"v3.0 upper_bound {p30.upper_bound} should be >= v2.3 {p23.upper_bound}"
                )
                found_wider = True
    assert found_wider, "Could not find a matching period to compare CI widths"


# ---------------------------------------------------------------------------
# Tests: period months
# ---------------------------------------------------------------------------


def test_period_months_are_sequential(state, config):
    """periods[0].month == sim date month, periods[5].month == +5 months forward."""
    result = run(state, config, _FIRST_BIZ_MARCH)
    event = result[0]
    # Check first line's periods
    line = event.lines[0]
    assert line.periods[0].month == "2026-03"
    assert line.periods[1].month == "2026-04"
    assert line.periods[2].month == "2026-05"
    assert line.periods[3].month == "2026-06"
    assert line.periods[4].month == "2026-07"
    assert line.periods[5].month == "2026-08"


# ---------------------------------------------------------------------------
# Tests: data integrity
# ---------------------------------------------------------------------------


def test_quantities_are_positive_integers(state, config):
    """All forecast_quantity, lower_bound, upper_bound > 0 and are ints."""
    result = run(state, config, _FIRST_BIZ_MARCH)
    event = result[0]
    for line in event.lines:
        for period in line.periods:
            assert isinstance(period.forecast_quantity, int)
            assert isinstance(period.lower_bound, int)
            assert isinstance(period.upper_bound, int)
            assert period.forecast_quantity > 0
            assert period.lower_bound > 0
            assert period.upper_bound > 0


def test_reproducibility(config, tmp_path: Path):
    """Two runs with identical state produce identical events (rng determinism)."""
    state_a = SimulationState.from_new(config, db_path=tmp_path / "a.db")
    state_b = SimulationState.from_new(config, db_path=tmp_path / "b.db")

    result_a = run(state_a, config, _FIRST_BIZ_MARCH)
    result_b = run(state_b, config, _FIRST_BIZ_MARCH)

    assert len(result_a) == len(result_b) == 1
    dump_a = result_a[0].model_dump()
    dump_b = result_b[0].model_dump()
    assert dump_a == dump_b
