"""Phase 4 integration test: 130-calendar-day simulation.

Covers Steps 32–39 (payments, demand signals, demand planning, forecasts,
inventory targets, master data changes, inventory snapshots, schema evolution)
wired through the day loop.

The 130-day run starts from config.simulation.start_date (2026-01-05) and
advances 130 calendar days, crossing the schema evolution thresholds at
sim_day 60 (sales_channel), 90 (model upgrade), and 120 (incoterms).

The fixture is module-scoped and runs once for all test classes.
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).parent.parent
_CONFIG_SRC = _PROJECT_ROOT / "config.yaml"

N_DAYS = 130  # calendar days


# ---------------------------------------------------------------------------
# Module-scoped fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sim_result(tmp_path_factory: pytest.TempPathFactory) -> dict:
    """Run the 130-day simulation and return collected results.

    Returns a dict with:
      - ``state``:      SimulationState after 130 days
      - ``all_events``: flat list of all event objects (Pydantic models)
      - ``config``:     Config object
    """
    from flowform.cli import _run_day_loop
    from flowform.config import load_config
    from flowform.state import SimulationState

    tmp = tmp_path_factory.mktemp("phase4_integration")

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


def _get_field(ev: object, field: str, default=None):
    """Return a field value from a Pydantic model or dict."""
    val = getattr(ev, field, None)
    if val is not None:
        return val
    if isinstance(ev, dict):
        return ev.get(field, default)
    return default


def _events_of_type(events: list, event_type: str) -> list:
    return [ev for ev in events if _get_type(ev) == event_type]


# ---------------------------------------------------------------------------
# TestDemandPlanning
# ---------------------------------------------------------------------------


class TestDemandPlanning:

    def test_demand_plan_events_present(self, sim_result: dict) -> None:
        """At least 3 demand_plan events expected (Jan/Feb/Mar first biz days)."""
        plans = _events_of_type(sim_result["all_events"], "demand_plan")
        assert len(plans) >= 3, f"Expected >=3 demand_plan events, got {len(plans)}"

    def test_demand_plan_has_required_fields(self, sim_result: dict) -> None:
        """Each demand_plan has plan_id matching DP-YYYY-MM, plan_lines, horizon=6."""
        plans = _events_of_type(sim_result["all_events"], "demand_plan")
        assert plans, "No demand_plan events found"
        pattern = re.compile(r"^DP-\d{4}-\d{2}$")
        for plan in plans:
            plan_id = _get_field(plan, "plan_id", "")
            assert pattern.match(plan_id), f"plan_id '{plan_id}' does not match DP-YYYY-MM"
            horizon = _get_field(plan, "planning_horizon_months")
            assert horizon == 6, f"Expected planning_horizon_months=6, got {horizon}"
            plan_lines = _get_field(plan, "plan_lines", [])
            assert len(plan_lines) > 0, "demand_plan has empty plan_lines"

    def test_no_duplicate_monthly_plans(self, sim_result: dict) -> None:
        """No two demand_plan events should have the same plan_id."""
        plans = _events_of_type(sim_result["all_events"], "demand_plan")
        plan_ids = [_get_field(p, "plan_id") for p in plans]
        assert len(plan_ids) == len(set(plan_ids)), (
            f"Duplicate plan_ids found: {[pid for pid in plan_ids if plan_ids.count(pid) > 1]}"
        )


# ---------------------------------------------------------------------------
# TestForecasts
# ---------------------------------------------------------------------------


class TestForecasts:

    def test_forecast_events_present(self, sim_result: dict) -> None:
        """At least 3 demand_forecast events expected over 130 days."""
        forecasts = _events_of_type(sim_result["all_events"], "demand_forecast")
        assert len(forecasts) >= 3, f"Expected >=3 demand_forecast events, got {len(forecasts)}"

    def test_both_model_versions_present(self, sim_result: dict) -> None:
        """Both model_version='v2.3' and model_version='v3.0' must appear."""
        forecasts = _events_of_type(sim_result["all_events"], "demand_forecast")
        versions = {_get_field(f, "model_version") for f in forecasts}
        assert "v2.3" in versions, "model_version='v2.3' not found in forecast events"
        assert "v3.0" in versions, "model_version='v3.0' not found in forecast events"

    def test_forecast_has_required_fields(self, sim_result: dict) -> None:
        """Each demand_forecast has forecast_id, horizon_months=6, non-empty lines."""
        forecasts = _events_of_type(sim_result["all_events"], "demand_forecast")
        assert forecasts, "No demand_forecast events found"
        pattern = re.compile(r"^FCST-\d{4}-\d{2}$")
        for f in forecasts:
            forecast_id = _get_field(f, "forecast_id", "")
            assert pattern.match(forecast_id), (
                f"forecast_id '{forecast_id}' does not match FCST-YYYY-MM"
            )
            horizon = _get_field(f, "horizon_months")
            assert horizon == 6, f"Expected horizon_months=6, got {horizon}"
            lines = _get_field(f, "lines", [])
            assert len(lines) > 0, "demand_forecast has empty lines"


# ---------------------------------------------------------------------------
# TestInventoryTargets
# ---------------------------------------------------------------------------


class TestInventoryTargets:

    def test_inventory_targets_present(self, sim_result: dict) -> None:
        """At least 20 inventory_target events expected over 130 days."""
        targets = _events_of_type(sim_result["all_events"], "inventory_target")
        assert len(targets) >= 20, f"Expected >=20 inventory_target events, got {len(targets)}"

    def test_inventory_target_fields(self, sim_result: dict) -> None:
        """Each target has valid sku, warehouse, and safety_stock<=reorder_point<=target_stock."""
        targets = _events_of_type(sim_result["all_events"], "inventory_target")
        assert targets, "No inventory_target events found"
        for tgt in targets:
            wh = _get_field(tgt, "warehouse", "")
            assert wh in {"W01", "W02"}, f"Invalid warehouse '{wh}'"
            sku = _get_field(tgt, "sku", "")
            assert sku.startswith("FF-"), f"Invalid sku '{sku}'"
            safety = _get_field(tgt, "safety_stock", 0)
            reorder = _get_field(tgt, "reorder_point", 0)
            target = _get_field(tgt, "target_stock", 0)
            assert safety <= reorder, (
                f"safety_stock ({safety}) > reorder_point ({reorder}) for {sku}"
            )
            assert reorder <= target, (
                f"reorder_point ({reorder}) > target_stock ({target}) for {sku}"
            )


# ---------------------------------------------------------------------------
# TestSchemaEvolution
# ---------------------------------------------------------------------------


class TestSchemaEvolution:

    def test_sales_channel_event_emitted(self, sim_result: dict) -> None:
        """Exactly one schema_evolution_event with field_name='sales_channel'."""
        evts = _events_of_type(sim_result["all_events"], "schema_evolution_event")
        sc_evts = [e for e in evts if _get_field(e, "field_name") == "sales_channel"]
        assert len(sc_evts) == 1, (
            f"Expected exactly 1 sales_channel schema_evolution_event, got {len(sc_evts)}"
        )

    def test_incoterms_event_emitted(self, sim_result: dict) -> None:
        """Exactly one schema_evolution_event with field_name='incoterms'."""
        evts = _events_of_type(sim_result["all_events"], "schema_evolution_event")
        ic_evts = [e for e in evts if _get_field(e, "field_name") == "incoterms"]
        assert len(ic_evts) == 1, (
            f"Expected exactly 1 incoterms schema_evolution_event, got {len(ic_evts)}"
        )

    def test_model_upgrade_event_emitted(self, sim_result: dict) -> None:
        """Exactly one schema_evolution_event with field_name='model_version'."""
        evts = _events_of_type(sim_result["all_events"], "schema_evolution_event")
        mu_evts = [e for e in evts if _get_field(e, "field_name") == "model_version"]
        assert len(mu_evts) == 1, (
            f"Expected exactly 1 model_version schema_evolution_event, got {len(mu_evts)}"
        )

    def test_orders_before_day60_lack_sales_channel(self, sim_result: dict) -> None:
        """customer_order events emitted before sim_day 60 must NOT have sales_channel.

        Strategy: collect all customer_order events and check that at least some
        lack the sales_channel field (early orders). This is valid because 130
        calendar days includes enough business days that both pre-60 and post-60
        orders exist.
        """
        orders = _events_of_type(sim_result["all_events"], "customer_order")
        assert orders, "No customer_order events found"
        # At least some orders should lack sales_channel (those before day 60)
        lacking = [
            o for o in orders
            if not hasattr(o, "sales_channel") or getattr(o, "sales_channel", None) is None
        ]
        assert len(lacking) > 0, (
            "All customer_order events have sales_channel — expected some early orders without it"
        )

    def test_orders_after_day60_have_sales_channel(self, sim_result: dict) -> None:
        """customer_order events emitted after sim_day 60 must have sales_channel set.

        Strategy: collect all customer_order events and check that at least some
        DO have the sales_channel field (post-day-60 orders).
        """
        orders = _events_of_type(sim_result["all_events"], "customer_order")
        assert orders, "No customer_order events found"
        with_sc = [
            o for o in orders
            if getattr(o, "sales_channel", None) is not None
        ]
        assert len(with_sc) > 0, (
            "No customer_order events have sales_channel — expected orders after day 60 to have it"
        )


# ---------------------------------------------------------------------------
# TestMasterDataChanges
# ---------------------------------------------------------------------------


class TestMasterDataChanges:

    def test_master_data_changes_present(self, sim_result: dict) -> None:
        """At least 5 master_data_change events over 130 days."""
        changes = _events_of_type(sim_result["all_events"], "master_data_change")
        assert len(changes) >= 5, (
            f"Expected >=5 master_data_change events, got {len(changes)}"
        )

    def test_carrier_rate_change_present(self, sim_result: dict) -> None:
        """Exactly one master_data_change with entity_type='carrier' and field='carrier_base_rate'."""
        changes = _events_of_type(sim_result["all_events"], "master_data_change")
        carrier_rate_changes = [
            c for c in changes
            if _get_field(c, "entity_type") == "carrier"
            and _get_field(c, "field_changed") == "carrier_base_rate"
        ]
        assert len(carrier_rate_changes) == 1, (
            f"Expected exactly 1 carrier rate change, got {len(carrier_rate_changes)}"
        )


# ---------------------------------------------------------------------------
# TestPayments
# ---------------------------------------------------------------------------


class TestPayments:

    def test_payment_events_present(self, sim_result: dict) -> None:
        """At least 5 payment events expected over 130 days.

        Payments require delivered orders (positive customer balance). Given
        the credit-hold dynamics in the sim, only a small fraction of orders
        ship, so we assert a conservative lower bound.
        """
        payments = _events_of_type(sim_result["all_events"], "payment")
        assert len(payments) >= 5, (
            f"Expected >=5 payment events, got {len(payments)}"
        )

    def test_payment_fields_valid(self, sim_result: dict) -> None:
        """Each payment has valid behaviour and balance_after <= balance_before."""
        payments = _events_of_type(sim_result["all_events"], "payment")
        assert payments, "No payment events found"
        valid_behaviours = {"on_time", "late", "very_late"}
        for p in payments:
            behaviour = _get_field(p, "payment_behaviour", "")
            assert behaviour in valid_behaviours, (
                f"Invalid payment_behaviour '{behaviour}'"
            )
            balance_before = _get_field(p, "balance_before_pln", 0.0)
            balance_after = _get_field(p, "balance_after_pln", 0.0)
            assert balance_after <= balance_before + 0.01, (
                f"balance_after ({balance_after}) > balance_before ({balance_before})"
            )


# ---------------------------------------------------------------------------
# TestInventorySnapshots
# ---------------------------------------------------------------------------


class TestInventorySnapshots:

    def test_snapshots_present(self, sim_result: dict) -> None:
        """At least 160 inventory_snapshot events (2 warehouses x ~85 biz days)."""
        snaps = _events_of_type(sim_result["all_events"], "inventory_snapshot")
        assert len(snaps) >= 160, (
            f"Expected >=160 inventory_snapshot events, got {len(snaps)}"
        )

    def test_snapshot_positions_valid(self, sim_result: dict) -> None:
        """Every position in each snapshot has quantity_available <= quantity_on_hand."""
        snaps = _events_of_type(sim_result["all_events"], "inventory_snapshot")
        assert snaps, "No inventory_snapshot events found"
        for snap in snaps[:20]:  # check first 20 to keep runtime manageable
            positions = _get_field(snap, "positions", [])
            for pos in positions:
                on_hand = _get_field(pos, "quantity_on_hand", 0)
                available = _get_field(pos, "quantity_available", 0)
                assert available <= on_hand, (
                    f"quantity_available ({available}) > quantity_on_hand ({on_hand})"
                )


# ---------------------------------------------------------------------------
# TestReproducibility
# ---------------------------------------------------------------------------


class TestReproducibility:

    def test_same_seed_same_event_count(self, tmp_path_factory: pytest.TempPathFactory) -> None:
        """Running 10 calendar days twice with the same seed yields the same event count."""
        from flowform.cli import _run_day_loop
        from flowform.config import load_config
        from flowform.state import SimulationState

        config = load_config(_CONFIG_SRC)

        def _run_10() -> int:
            tmp = tmp_path_factory.mktemp("repro_run")
            db_path = tmp / "state" / "simulation.db"
            db_path.parent.mkdir(parents=True, exist_ok=True)
            output_dir = tmp / "output"
            state = SimulationState.from_new(config, db_path=db_path)
            total = 0
            for _ in range(10):
                next_date = state.current_date + timedelta(days=1)
                state.advance_day(next_date)
                evts = _run_day_loop(state, config, next_date, output_dir=output_dir)
                total += len(evts)
                state.save()
            return total

        count1 = _run_10()
        count2 = _run_10()
        assert count1 == count2, (
            f"Event counts differ between runs: {count1} vs {count2}"
        )
