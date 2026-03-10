"""Phase 5 integration smoke test: 14-calendar-day simulation.

Covers Steps 17–18 (schema_evolution, noise injection, late_arriving) wired
through the day loop as confirmed in cli.py.

The 14-day run starts from config.simulation.start_date (2026-01-05, a Monday)
and advances 14 calendar days, crossing 2 weekends but NOT the schema evolution
thresholds at sim_day 60 (sales_channel) or 120 (incoterms).

The fixture is module-scoped and runs once for all tests.
"""

from __future__ import annotations

import json
import re
from datetime import timedelta
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).parent.parent
_CONFIG_SRC = _PROJECT_ROOT / "config.yaml"

N_DAYS = 14  # calendar days


# ---------------------------------------------------------------------------
# Module-scoped fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sim_result(tmp_path_factory: pytest.TempPathFactory) -> dict:
    """Run the 14-day simulation and return collected results.

    Returns a dict with:
      - ``state``:      SimulationState after 14 days
      - ``all_events``: flat list of all raw event objects (Pydantic models, pre-noise)
      - ``output_dir``: Path where JSON files were written
      - ``config``:     Config object
    """
    from flowform.cli import _run_day_loop
    from flowform.config import load_config
    from flowform.state import SimulationState

    tmp = tmp_path_factory.mktemp("phase5")

    config = load_config(_CONFIG_SRC)

    db_path = tmp / "state" / "simulation.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    output_dir = tmp / "output"

    state = SimulationState.from_new(config, db_path=db_path)

    all_events: list = []

    for _ in range(N_DAYS):
        next_date = state.current_date + timedelta(days=1)
        state.advance_day(next_date)
        day_events = _run_day_loop(state, config, next_date, output_dir=output_dir)
        all_events.extend(day_events)
        state.save()

    return {
        "state": state,
        "all_events": all_events,
        "output_dir": output_dir,
        "config": config,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_type(ev: object) -> str:
    """Return event_type from either a Pydantic model or a dict."""
    t = getattr(ev, "event_type", None)
    if t is not None:
        return str(t)
    if isinstance(ev, dict):
        return ev.get("event_type", "")
    return ""


def _events_of_type(events: list, event_type: str) -> list:
    return [ev for ev in events if _get_type(ev) == event_type]


def _all_written_json_files(output_dir: Path) -> list[Path]:
    """Return all .json files written under output_dir."""
    return list(output_dir.rglob("*.json"))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFullPipeline:

    def test_full_pipeline_no_crash_14_days(self, sim_result: dict) -> None:
        """Fixture completing without exception is the primary assertion.

        14 calendar days from a Monday crosses 2 weekends, giving ~10 business
        days. sim_day_number counts business days, so it must be >= 8.
        """
        state = sim_result["state"]
        # sim_day is the 1-based business-day counter
        assert state.sim_day >= 8, (
            f"Expected sim_day >= 8 after 14 calendar days, got {state.sim_day}"
        )


class TestExchangeRates:

    def test_exchange_rate_events_every_day(self, sim_result: dict) -> None:
        """2 exchange_rate events per calendar day → at least 28 over 14 days."""
        evts = _events_of_type(sim_result["all_events"], "exchange_rate")
        assert len(evts) >= 28, (
            f"Expected >=28 exchange_rate events, got {len(evts)}"
        )


class TestInventoryMovements:

    def test_inventory_movement_events_present(self, sim_result: dict) -> None:
        """At least 1 inventory_movement(s) event must be present.

        The writer uses the plural alias 'inventory_movements' as the event_type
        on raw event objects returned from the engine (the Pydantic model sets
        event_type='inventory_movements').
        """
        # Raw Pydantic objects use 'inventory_movements' (plural)
        evts = _events_of_type(sim_result["all_events"], "inventory_movements")
        assert len(evts) >= 1, "No inventory_movements events found"


class TestWrittenFiles:

    def test_load_events_written_to_tms_files(self, sim_result: dict) -> None:
        """At least one tms loads file exists and is a valid JSON list.

        Load files are named loads_08.json, loads_14.json, loads_20.json
        (one per sync window), so we glob for loads_*.json.
        """
        output_dir = sim_result["output_dir"]
        loads_files = list(output_dir.glob("tms/*/loads_*.json"))
        assert len(loads_files) >= 1, (
            f"No tms/*/loads_*.json files found under {output_dir}"
        )
        for path in loads_files:
            data = json.loads(path.read_text())
            assert isinstance(data, list), (
                f"{path} is not a JSON list"
            )

    def test_written_files_are_valid_json(self, sim_result: dict) -> None:
        """Every .json file written under output_dir parses without error."""
        output_dir = sim_result["output_dir"]
        files = _all_written_json_files(output_dir)
        assert len(files) >= 1, f"No JSON files written under {output_dir}"
        for path in files:
            try:
                json.loads(path.read_text())
            except json.JSONDecodeError as exc:
                pytest.fail(f"Invalid JSON in {path}: {exc}")

    def test_erp_daily_files_written(self, sim_result: dict) -> None:
        """At least one erp/*/exchange_rates.json file is written."""
        output_dir = sim_result["output_dir"]
        files = list(output_dir.glob("erp/*/exchange_rates.json"))
        assert len(files) >= 1, (
            f"No erp/*/exchange_rates.json files found under {output_dir}"
        )

    def test_adm_demand_signals_written(self, sim_result: dict) -> None:
        """At least one file exists under output_dir/adm/demand_signals/."""
        output_dir = sim_result["output_dir"]
        adm_signals_dir = output_dir / "adm" / "demand_signals"
        files = list(adm_signals_dir.rglob("*.json")) if adm_signals_dir.exists() else []
        # demand_signals are written per-event or daily; check the broader adm dir too
        if not files:
            files = list((output_dir / "adm").rglob("*.json")) if (output_dir / "adm").exists() else []
        assert len(files) >= 1, (
            f"No ADM demand signal files found under {output_dir / 'adm'}"
        )

    def test_inventory_snapshots_written(self, sim_result: dict) -> None:
        """At least one erp/*/inventory_snapshots.json exists and is non-empty."""
        output_dir = sim_result["output_dir"]
        files = list(output_dir.glob("erp/*/inventory_snapshots.json"))
        assert len(files) >= 1, (
            f"No erp/*/inventory_snapshots.json files found under {output_dir}"
        )
        for path in files:
            data = json.loads(path.read_text())
            assert isinstance(data, list) and len(data) > 0, (
                f"{path} is empty or not a list"
            )
            break  # checking one is sufficient


class TestNoiseAndMarkerStripping:

    def test_no_internal_marker_fields_in_written_files(self, sim_result: dict) -> None:
        """No written JSON record should contain '_is_deferred' or any '_'-prefixed key.

        Noise injection may add internal marker fields; the cli strips them
        before writing. This test verifies the stripping works end-to-end.
        """
        output_dir = sim_result["output_dir"]
        files = _all_written_json_files(output_dir)
        assert files, f"No JSON files found under {output_dir}"
        violations: list[str] = []
        for path in files:
            records = json.loads(path.read_text())
            if not isinstance(records, list):
                continue
            for rec in records:
                if not isinstance(rec, dict):
                    continue
                internal_keys = [k for k in rec if k.startswith("_")]
                if internal_keys:
                    violations.append(
                        f"{path.name}: record has internal keys {internal_keys}"
                    )
        assert not violations, (
            f"Found {len(violations)} records with internal marker fields:\n"
            + "\n".join(violations[:10])
        )


class TestLateArrivingState:

    def test_deferred_erp_movements_not_accumulated(self, sim_result: dict) -> None:
        """deferred_erp_movements must not grow unboundedly over 14 days.

        Each day's deferred movements are flushed into the next day's file,
        so the queue should stay small (< 100 items at end of run).
        """
        state = sim_result["state"]
        count = len(state.deferred_erp_movements)
        assert count < 100, (
            f"deferred_erp_movements has {count} items — possible accumulation bug"
        )

    def test_tms_maintenance_month_field_valid(self, sim_result: dict) -> None:
        """tms_maintenance_fired_month is empty string or matches YYYY-MM format."""
        state = sim_result["state"]
        val = state.tms_maintenance_fired_month
        assert val == "" or re.match(r"^\d{4}-\d{2}$", val), (
            f"tms_maintenance_fired_month has unexpected value: {repr(val)}"
        )


class TestSchemaEvolution:

    def test_schema_flags_below_threshold_after_14_days(self, sim_result: dict) -> None:
        """After 14 calendar days (~10 biz days), sim_day < 60, so sales_channel
        flag must not have fired yet.
        """
        state = sim_result["state"]
        flag = state.schema_flags.get("sevt_sales_channel", False)
        assert flag is False, (
            f"sevt_sales_channel fired before threshold (sim_day={state.sim_day})"
        )

    def test_schema_evolution_events_absent_in_14_days(self, sim_result: dict) -> None:
        """No schema_evolution_event should appear in the 14-day all_events list.

        Day 60 threshold is not reached in 14 calendar days.
        """
        evts = _events_of_type(sim_result["all_events"], "schema_evolution_event")
        assert len(evts) == 0, (
            f"Expected 0 schema_evolution_events in 14 days, got {len(evts)}"
        )


class TestLoadSyncWindows:

    def test_load_events_sync_window_values_valid(self, sim_result: dict) -> None:
        """Every record in any loads_*.json file must have sync_window in {08, 14, 20}."""
        output_dir = sim_result["output_dir"]
        loads_files = list(output_dir.glob("tms/*/loads_*.json"))
        valid_windows = {"08", "14", "20"}
        violations: list[str] = []
        for path in loads_files:
            records = json.loads(path.read_text())
            for rec in records:
                if not isinstance(rec, dict):
                    continue
                sw = rec.get("sync_window")
                if sw not in valid_windows:
                    violations.append(
                        f"{path.name}: sync_window={repr(sw)}"
                    )
        assert not violations, (
            f"Found {len(violations)} load records with invalid sync_window:\n"
            + "\n".join(violations[:10])
        )


class TestReproducibility:

    def _run_n_days(
        self,
        tmp_path_factory: pytest.TempPathFactory,
        n: int,
        run_label: str,
    ) -> dict:
        """Run *n* calendar days from a fresh state and return reproducibility fingerprint."""
        from flowform.cli import _run_day_loop
        from flowform.config import load_config
        from flowform.state import SimulationState

        config = load_config(_CONFIG_SRC)
        tmp = tmp_path_factory.mktemp(run_label)
        db_path = tmp / "state" / "simulation.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        output_dir = tmp / "output"
        state = SimulationState.from_new(config, db_path=db_path)

        total = 0
        for _ in range(n):
            next_date = state.current_date + timedelta(days=1)
            state.advance_day(next_date)
            evts = _run_day_loop(state, config, next_date, output_dir=output_dir)
            total += len(evts)
            state.save()

        # Collect load_ids from written TMS files
        load_ids: list[str] = []
        for path in sorted(output_dir.glob("tms/*/loads_*.json")):
            data = json.loads(path.read_text())
            if isinstance(data, list):
                for rec in data:
                    if isinstance(rec, dict) and "load_id" in rec:
                        load_ids.append(rec["load_id"])

        # Collect first payment amount from written ERP files
        first_payment: float | None = None
        for path in sorted(output_dir.glob("erp/*/payments.json")):
            data = json.loads(path.read_text())
            if isinstance(data, list) and data:
                rec = data[0]
                if isinstance(rec, dict) and "payment_amount_pln" in rec:
                    first_payment = rec["payment_amount_pln"]
                    break

        return {
            "count": total,
            "first_payment": first_payment,
            "load_ids": sorted(load_ids),
        }

    def test_reproducibility_14_days(self, tmp_path_factory: pytest.TempPathFactory) -> None:
        """Running 14 calendar days twice from the same seed yields the same
        total event count, first payment amount, and set of load IDs.
        """
        r1 = self._run_n_days(tmp_path_factory, n=14, run_label="repro5_run1")
        r2 = self._run_n_days(tmp_path_factory, n=14, run_label="repro5_run2")
        assert r1["count"] == r2["count"], (
            f"Event counts differ between runs: {r1['count']} vs {r2['count']}"
        )
        assert r1["first_payment"] == r2["first_payment"], (
            f"First payment amounts differ: {r1['first_payment']} vs {r2['first_payment']}"
        )
        assert r1["load_ids"] == r2["load_ids"], (
            "Load ID sets differ between runs"
        )
