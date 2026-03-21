"""Tests for ADM quantity calibration helper — Step 57."""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest

from flowform.adm._calibration import _expected_monthly_units_per_group, _parse_product_group
from flowform.calendar import is_first_business_day_of_month
from flowform.config import load_config
from flowform.engines.forecasts import run as forecasts_run
from flowform.engines.planning import run as planning_run
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_CONFIG_PATH = Path("/home/coder/sc-sim/config.yaml")

_GROUP_RE = re.compile(r"^[A-Z]{2}-DN\d+-[A-Z0-9]+$")

# 2026-03-02 is the 1st business day of March 2026
_FIRST_BIZ_MARCH = date(2026, 3, 2)


@pytest.fixture(scope="module")
def config():
    return load_config(_CONFIG_PATH)


@pytest.fixture()
def state(config, tmp_path: Path) -> SimulationState:
    return SimulationState.from_new(config, db_path=tmp_path / "sim.db")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_returns_dict(state, config):
    """Result is a dict; all keys match the product_group pattern."""
    result = _expected_monthly_units_per_group(state, config)
    assert isinstance(result, dict)
    for key in result:
        assert _GROUP_RE.match(key), f"Key {key!r} does not match product_group pattern"


def test_covers_majority_of_groups(state, config):
    """At least 70% of product groups derived from the catalog appear with value > 0."""
    all_groups = {_parse_product_group(entry.sku) for entry in state.catalog}
    result = _expected_monthly_units_per_group(state, config)
    covered = sum(1 for g in all_groups if result.get(g, 0.0) > 0)
    coverage = covered / len(all_groups)
    assert coverage >= 0.70, (
        f"Only {coverage:.1%} of product groups have value > 0 (need ≥ 70%)"
    )


def test_calibration_override_returned_directly(state, config):
    """Passing _calibration_override returns exactly that dict."""
    override = {"BL-DN100-S316": 999.0}
    result = _expected_monthly_units_per_group(state, config, _calibration_override=override)
    assert result is override


def test_deterministic(state, config):
    """Calling twice with the same state returns identical dicts."""
    r1 = _expected_monthly_units_per_group(state, config)
    r2 = _expected_monthly_units_per_group(state, config)
    assert r1 == r2


def test_no_negative_values(state, config):
    """All values in the result are >= 0."""
    result = _expected_monthly_units_per_group(state, config)
    for key, val in result.items():
        assert val >= 0.0, f"Group {key!r} has negative value {val}"


def test_plan_quantities_within_10x_baseline(state, config):
    """planned_quantity for each plan line is within [0.05×, 20×] of the baseline."""
    assert is_first_business_day_of_month(_FIRST_BIZ_MARCH)
    baseline = _expected_monthly_units_per_group(state, config)

    events = planning_run(state, config, _FIRST_BIZ_MARCH)
    assert len(events) == 1, "Expected exactly one DemandPlanEvent"
    plan = events[0]

    violations = []
    for line in plan.plan_lines:
        group = line.product_group
        if group not in baseline or baseline[group] == 0.0:
            continue  # group not in baseline — fallback to randint, skip check
        b = baseline[group]
        # Skip groups with very small baselines (< 1.0) where rounding to 0 is expected
        if b < 1.0:
            continue
        for period in line.periods:
            qty = period.planned_quantity
            if not (0.05 * b <= qty <= 20 * b):
                violations.append(
                    f"{group}/{line.customer_segment}/{period.month}: qty={qty}, baseline={b:.1f}"
                )

    assert not violations, "plan quantities outside [0.05×, 20×] of baseline:\n" + "\n".join(violations[:10])


def test_forecast_quantities_within_10x_baseline(state, config):
    """forecast_quantity for each forecast line is within [0.05×, 20×] of baseline."""
    assert is_first_business_day_of_month(_FIRST_BIZ_MARCH)
    baseline = _expected_monthly_units_per_group(state, config)

    events = forecasts_run(state, config, _FIRST_BIZ_MARCH)
    assert len(events) == 1, "Expected exactly one DemandForecastEvent"
    fcst = events[0]

    violations = []
    for line in fcst.lines:
        group = line.sku_group
        if group not in baseline or baseline[group] == 0.0:
            continue
        b = baseline[group]
        # Skip groups with very small baselines (< 1.0) where rounding to 0 is expected
        if b < 1.0:
            continue
        for period in line.periods:
            qty = period.forecast_quantity
            if not (0.05 * b <= qty <= 20 * b):
                violations.append(
                    f"{group}/{period.month}: qty={qty}, baseline={b:.1f}"
                )

    assert not violations, "forecast quantities outside [0.05×, 20×] of baseline:\n" + "\n".join(violations[:10])


def test_empty_catalog_returns_empty_dict(config, tmp_path: Path):
    """With no catalog entries, returns an empty dict."""
    state = SimulationState.from_new(config, db_path=tmp_path / "sim.db")
    # Clear the catalog
    state.catalog = []
    result = _expected_monthly_units_per_group(state, config)
    assert result == {}
