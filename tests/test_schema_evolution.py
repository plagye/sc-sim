"""Tests for the schema_evolution engine (C1 replacement — 9 tests).

Tests cover: below-threshold suppression, exact threshold firing, double-fire
prevention, flag persistence across save/reload, weekend suppression, holiday-
shifted threshold, and a 180-day end-to-end check that exactly 3 events fire.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
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


class TestBelowThreshold:

    def test_no_event_below_threshold(self) -> None:
        """sim_day=59, business day → assert [] returned."""
        config = _make_config()
        state = _make_state(sim_day=59)
        # 2026-04-07 is a Tuesday (business day)
        result = run(state, config, date(2026, 4, 7))
        assert result == []


class TestThresholdFiring:

    def test_sales_channel_fires_at_day_60(self) -> None:
        """sim_day=60, business day → exactly 1 event with field_name=='sales_channel'."""
        config = _make_config()
        state = _make_state(sim_day=60)
        result = run(state, config, date(2026, 4, 7))  # Tuesday
        assert len(result) == 1
        ev = result[0]
        assert isinstance(ev, SchemaEvolutionEvent)
        assert ev.field_name == "sales_channel"

    def test_forecast_model_fires_at_day_90(self) -> None:
        """sim_day=90 → assert field_name=='model_version', change_type=='model_upgrade'."""
        config = _make_config()
        state = _make_state(sim_day=90)
        state.schema_flags["sevt_sales_channel"] = True
        result = run(state, config, date(2026, 4, 7))
        assert len(result) == 1
        ev = result[0]
        assert ev.field_name == "model_version"
        assert ev.change_type == "model_upgrade"

    def test_incoterms_fires_at_day_120(self) -> None:
        """sim_day=120 → assert field_name=='incoterms'."""
        config = _make_config()
        state = _make_state(sim_day=120)
        state.schema_flags["sevt_sales_channel"] = True
        state.schema_flags["sevt_model_version"] = True
        result = run(state, config, date(2026, 4, 7))
        assert len(result) == 1
        ev = result[0]
        assert ev.field_name == "incoterms"


class TestNoDoubleFire:

    def test_no_double_fire(self) -> None:
        """First call at day 60 returns 1 event; second call returns 0 (flag set)."""
        config = _make_config()
        state = _make_state(sim_day=60)
        result1 = run(state, config, date(2026, 4, 7))
        assert len(result1) == 1
        # Second call on same state — flag is now True
        result2 = run(state, config, date(2026, 4, 8))
        assert len(result2) == 0

    def test_flag_persists_across_save_reload(self, tmp_path: Path) -> None:
        """Fire at day 60, save(), from_db(), run again → 0 events."""
        from flowform.config import load_config
        from flowform.state import SimulationState

        config = load_config(_CONFIG_SRC)
        db_path = tmp_path / "sim.db"
        state = SimulationState.from_new(config, db_path=db_path)

        # Advance to sim_day 60 (need 60 business days)
        state.sim_day = 60  # type: ignore[assignment]
        state.schema_flags = {}

        # Fire on a business day
        result1 = run(state, config, date(2026, 4, 7))
        assert len(result1) == 1
        assert state.schema_flags.get("sevt_sales_channel") is True

        # Save and reload
        state.save()
        state2 = SimulationState.from_db(config, db_path=db_path)

        # schema_flags should survive serialisation
        result2 = run(state2, config, date(2026, 4, 8))
        assert len(result2) == 0, (
            f"Expected 0 events after reload, got {len(result2)} — flag not persisted"
        )


class TestWeekendSuppression:

    def test_no_event_on_weekend_above_threshold(self) -> None:
        """sim_day=65, Saturday → assert [] (business days only)."""
        config = _make_config()
        state = _make_state(sim_day=65)
        state.schema_flags["sevt_sales_channel"] = True  # already fired at 60
        # 2026-04-04 is a Saturday
        result = run(state, config, date(2026, 4, 4))
        assert result == []


class TestHolidayShift:

    def test_holiday_shifted_threshold(self) -> None:
        """When threshold day falls on a holiday, event fires on the next business day."""
        config = _make_config()

        # Find a threshold day that is a holiday or weekend, then confirm the event
        # fires on the next business day. We simulate day-by-day at the threshold.
        # Easter Monday 2026 is 2026-04-06. If sim_day==60 on that date, event
        # should NOT fire. It should fire on 2026-04-07 (Tuesday).
        state_holiday = _make_state(sim_day=60)
        result_holiday = run(state_holiday, config, date(2026, 4, 6))  # Easter Monday
        assert result_holiday == [], "Event should not fire on Easter Monday"

        # Same state, next business day (sim_day stays at 60 because it was a holiday)
        result_next = run(state_holiday, config, date(2026, 4, 7))
        assert len(result_next) == 1, (
            "Event should fire on the first business day at/after the threshold"
        )
        assert result_next[0].field_name == "sales_channel"


class TestEndToEnd180Days:

    def test_all_three_fire_across_180_day_run(
        self, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """180-day run: exactly 3 schema_evolution_event records across all ERP files."""
        from flowform.cli import _run_day_loop
        from flowform.config import load_config
        from flowform.state import SimulationState

        config = load_config(_CONFIG_SRC)
        tmp = tmp_path_factory.mktemp("sevt_180")
        db_path = tmp / "state" / "simulation.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        output_dir = tmp / "output"

        state = SimulationState.from_new(config, db_path=db_path)

        for _ in range(180):
            next_date = state.current_date + timedelta(days=1)
            state.advance_day(next_date)
            _run_day_loop(state, config, next_date, output_dir=output_dir)
            state.save()

        # Collect all records from erp/*/schema_evolution_events.json
        sevt_records: list[dict] = []
        for path in output_dir.glob("erp/*/schema_evolution_events.json"):
            data = json.loads(path.read_text())
            if isinstance(data, list):
                sevt_records.extend(data)

        field_names = {r["field_name"] for r in sevt_records if isinstance(r, dict)}
        assert len(sevt_records) == 3, (
            f"Expected exactly 3 schema_evolution_events across 180 days, "
            f"got {len(sevt_records)}: {sevt_records}"
        )
        assert field_names == {"sales_channel", "model_version", "incoterms"}, (
            f"Unexpected field_names: {field_names}"
        )
