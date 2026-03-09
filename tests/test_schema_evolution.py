"""Tests for the schema_evolution engine (Step 39).

Six tests covering trigger days, non-trigger days, weekend suppression,
event schema fields, and ID format.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from flowform.schema_evolution import SchemaEvolutionEvent, run


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).parent.parent
_CONFIG_SRC = _PROJECT_ROOT / "config.yaml"


def _make_state(sim_day: int) -> MagicMock:
    """Return a minimal mock SimulationState for the given sim_day."""
    state = MagicMock()
    state.sim_day = sim_day
    state.counters = {}
    state.schema_flags = {}
    return state


def _make_config() -> object:
    """Return the real config so thresholds match config.yaml."""
    from flowform.config import load_config
    return load_config(_CONFIG_SRC)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestNonTriggerDays:

    def test_no_event_on_non_trigger_day(self) -> None:
        """sim_day=59, business day → no events emitted."""
        config = _make_config()
        state = _make_state(sim_day=59)
        # 2026-04-06 is a Monday (business day)
        result = run(state, config, date(2026, 4, 7))
        assert result == []

    def test_no_event_on_weekend(self) -> None:
        """sim_day=60 but the calendar date is a Saturday → no events (business days only)."""
        config = _make_config()
        state = _make_state(sim_day=60)
        # 2026-04-04 is a Saturday
        result = run(state, config, date(2026, 4, 4))
        assert result == []


class TestTriggerDays:

    def test_sales_channel_event_at_day_60(self) -> None:
        """sim_day=60, business day → exactly 1 event with field_name='sales_channel'."""
        config = _make_config()
        assert config.schema_evolution.sales_channel_from_day == 60
        state = _make_state(sim_day=60)
        result = run(state, config, date(2026, 4, 7))  # Monday
        assert len(result) == 1
        ev = result[0]
        assert isinstance(ev, SchemaEvolutionEvent)
        assert ev.field_name == "sales_channel"
        assert ev.change_type == "field_activation"
        assert ev.affected_system == "erp"
        assert ev.affected_event_type == "customer_order"
        assert ev.effective_from_day == 60

    def test_forecast_upgrade_event_at_day_90(self) -> None:
        """sim_day=90, business day → exactly 1 event with field_name='model_version'.

        sales_channel already fired at day 60, so pre-set that flag.
        """
        config = _make_config()
        assert config.schema_evolution.forecast_model_upgrade_day == 90
        state = _make_state(sim_day=90)
        # Simulate that sales_channel event already fired at day 60
        state.schema_flags["sevt_sales_channel"] = True
        result = run(state, config, date(2026, 4, 7))  # Tuesday
        assert len(result) == 1
        ev = result[0]
        assert ev.field_name == "model_version"
        assert ev.change_type == "model_upgrade"
        assert ev.affected_system == "forecast"
        assert ev.affected_event_type == "demand_forecast"
        assert ev.effective_from_day == 90

    def test_incoterms_event_at_day_120(self) -> None:
        """sim_day=120, business day → exactly 1 event with field_name='incoterms'.

        sales_channel and model_version already fired, so pre-set those flags.
        """
        config = _make_config()
        assert config.schema_evolution.incoterms_from_day == 120
        state = _make_state(sim_day=120)
        # Simulate that earlier events already fired
        state.schema_flags["sevt_sales_channel"] = True
        state.schema_flags["sevt_model_version"] = True
        result = run(state, config, date(2026, 4, 7))  # Tuesday
        assert len(result) == 1
        ev = result[0]
        assert ev.field_name == "incoterms"
        assert ev.change_type == "field_activation"
        assert ev.affected_system == "erp"
        assert ev.affected_event_type == "customer_order"
        assert ev.effective_from_day == 120


class TestEventFormat:

    def test_evolution_id_format(self) -> None:
        """evolution_id must match SEVT-YYYYMMDD-NNNN pattern."""
        config = _make_config()
        state = _make_state(sim_day=60)
        result = run(state, config, date(2026, 4, 7))
        assert len(result) == 1
        pattern = re.compile(r"^SEVT-\d{8}-\d{4}$")
        assert pattern.match(result[0].evolution_id), (
            f"evolution_id '{result[0].evolution_id}' does not match SEVT-YYYYMMDD-NNNN"
        )
