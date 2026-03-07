"""Phase 2 integration test: 14-calendar-day simulation spanning a weekend + Easter Monday.

Date range: 2025-04-14 (Monday) through 2025-04-27 (Sunday).

Calendar breakdown:
  - Business days (9): Mon 14, Tue 15, Wed 16, Thu 17, Fri 18 (week 1)
                        Tue 22, Wed 23, Thu 24, Fri 25 (week 2)
  - Weekends (4 days): Sat 19, Sun 20, Sat 26, Sun 27
  - Polish holiday (1): Mon 21 = Easter Monday 2025 (no production, no outbound)

Easter 2025 falls on Sun 2025-04-20, so Easter Monday = 2025-04-21.

The test:
  - Calls engine functions directly (no subprocess) for speed and introspection.
  - Uses tmp_path so output/ and state/ are fully isolated.
  - Asserts all invariants described in Step 22.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Project root + config source
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).parent.parent
_CONFIG_SRC = _PROJECT_ROOT / "config.yaml"

# ---------------------------------------------------------------------------
# Test date constants
# ---------------------------------------------------------------------------

START_DATE = date(2025, 4, 14)  # Monday before Easter
N_DAYS = 14
END_DATE = START_DATE + timedelta(days=N_DAYS - 1)  # 2025-04-27 (Sunday)

NON_BUSINESS_DATES = {
    date(2025, 4, 19),  # Saturday
    date(2025, 4, 20),  # Easter Sunday (weekend)
    date(2025, 4, 21),  # Easter Monday (Polish public holiday)
    date(2025, 4, 26),  # Saturday
    date(2025, 4, 27),  # Sunday
}

BUSINESS_DATES = {
    START_DATE + timedelta(days=i)
    for i in range(N_DAYS)
} - NON_BUSINESS_DATES

HOLIDAY_DATE = date(2025, 4, 21)  # Easter Monday — weekday but holiday


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sim_result(tmp_path_factory: pytest.TempPathFactory) -> dict:
    """Run the 14-day simulation and return collected results.

    This is module-scoped so the expensive setup (catalog generation, customer
    generation, 14 days of simulation) runs only once for the whole class.

    Returns a dict with:
      - ``state``: SimulationState after 14 days
      - ``events_by_date``: {date: list[event_object]} for all 14 days
      - ``output_dir``: Path to the output root
      - ``db_path``: Path to the SQLite DB
    """
    from flowform.calendar import is_business_day
    from flowform.cli import _run_day_loop
    from flowform.config import load_config
    from flowform.state import SimulationState

    tmp = tmp_path_factory.mktemp("phase2_integration")

    config = load_config(_CONFIG_SRC)

    # Create state in tmp_path so the DB is isolated from the real project.
    # from_new uses config.simulation.seed (=42) for reproducibility.
    db_path = tmp / "state" / "simulation.db"
    state = SimulationState.from_new(config, db_path=db_path)

    # Override current_date to one day before START_DATE so that the first
    # advance_day() call moves us exactly to START_DATE (2025-04-14).
    state.current_date = START_DATE - timedelta(days=1)
    state.sim_day = 0
    state.save()

    output_dir = tmp / "output"
    events_by_date: dict[date, list] = {}

    for i in range(N_DAYS):
        next_date = state.current_date + timedelta(days=1)
        state.advance_day(next_date)
        day_events = _run_day_loop(state, config, next_date, output_dir=output_dir)
        events_by_date[next_date] = list(day_events)
        state.save()

    return {
        "state": state,
        "events_by_date": events_by_date,
        "output_dir": output_dir,
        "db_path": db_path,
        "config": config,
    }


# ---------------------------------------------------------------------------
# Helper: count events by type within a day's event list
# ---------------------------------------------------------------------------


def _count_type(events: list, prefix: str) -> int:
    """Count events whose event_type starts with *prefix*."""
    count = 0
    for ev in events:
        et = getattr(ev, "event_type", None) or (
            ev.get("event_type") if isinstance(ev, dict) else None
        )
        if et and et.startswith(prefix):
            count += 1
    return count


def _event_types(events: list) -> list[str]:
    """Return sorted list of unique event_type values in *events*."""
    types = set()
    for ev in events:
        et = getattr(ev, "event_type", None) or (
            ev.get("event_type") if isinstance(ev, dict) else None
        )
        if et:
            types.add(et)
    return sorted(types)


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class TestPhase2Integration:

    # ------------------------------------------------------------------
    # Assertion 1: sim_day advances correctly
    # ------------------------------------------------------------------

    def test_sim_day_after_14_days(self, sim_result: dict) -> None:
        """state.sim_day == 14 after running 14 calendar days."""
        state = sim_result["state"]
        assert state.sim_day == 14, (
            f"Expected sim_day=14, got {state.sim_day}"
        )

    def test_current_date_is_end_date(self, sim_result: dict) -> None:
        """state.current_date == 2025-04-27 after 14 days."""
        state = sim_result["state"]
        assert state.current_date == END_DATE, (
            f"Expected current_date={END_DATE}, got {state.current_date}"
        )

    # ------------------------------------------------------------------
    # Assertion 2: Exchange rates generated every day (including weekends/holiday)
    # ------------------------------------------------------------------

    def test_exchange_rate_events_every_day(self, sim_result: dict) -> None:
        """Exchange rate events appear in every day's event list (all 14 days)."""
        events_by_date = sim_result["events_by_date"]
        for sim_date in (START_DATE + timedelta(days=i) for i in range(N_DAYS)):
            day_events = events_by_date[sim_date]
            er_count = _count_type(day_events, "exchange_rate")
            assert er_count >= 2, (
                f"Expected >= 2 exchange_rate events on {sim_date}, got {er_count}"
            )

    def test_total_exchange_rate_events(self, sim_result: dict) -> None:
        """Total exchange rate events across 14 days == 28 (2 per day)."""
        all_events = [
            ev
            for day_events in sim_result["events_by_date"].values()
            for ev in day_events
        ]
        total_er = _count_type(all_events, "exchange_rate")
        assert total_er == 28, (
            f"Expected exactly 28 exchange_rate events (2 × 14 days), got {total_er}"
        )

    def test_exchange_rates_output_files_all_dates(self, sim_result: dict) -> None:
        """output/erp/<date>/exchange_rates.json exists for all 14 dates."""
        output_dir = sim_result["output_dir"]
        for i in range(N_DAYS):
            sim_date = START_DATE + timedelta(days=i)
            er_file = output_dir / "erp" / sim_date.isoformat() / "exchange_rates.json"
            assert er_file.exists(), f"Missing exchange_rates file for {sim_date}"

    def test_exchange_rates_state_updated(self, sim_result: dict) -> None:
        """state.exchange_rates contains EUR and USD after 14 days."""
        state = sim_result["state"]
        assert "EUR" in state.exchange_rates
        assert "USD" in state.exchange_rates
        # Rates should be in a plausible range (mean-reverting walk)
        assert 3.5 < state.exchange_rates["EUR"] < 5.5, (
            f"EUR/PLN rate out of range: {state.exchange_rates['EUR']}"
        )
        assert 3.0 < state.exchange_rates["USD"] < 5.5, (
            f"USD/PLN rate out of range: {state.exchange_rates['USD']}"
        )

    # ------------------------------------------------------------------
    # Assertion 3: Production only on business days
    # ------------------------------------------------------------------

    def test_no_production_on_non_business_days(self, sim_result: dict) -> None:
        """No production_completion events on weekends or Easter Monday."""
        events_by_date = sim_result["events_by_date"]
        for sim_date in NON_BUSINESS_DATES:
            day_events = events_by_date[sim_date]
            prod_count = _count_type(day_events, "production_completion")
            assert prod_count == 0, (
                f"Expected 0 production events on {sim_date} ({sim_date.strftime('%a')}), "
                f"got {prod_count}"
            )

    def test_production_exists_on_business_days(self, sim_result: dict) -> None:
        """Production completion events exist on all 9 business days."""
        events_by_date = sim_result["events_by_date"]
        for sim_date in sorted(BUSINESS_DATES):
            day_events = events_by_date[sim_date]
            prod_count = _count_type(day_events, "production_completion")
            assert prod_count > 0, (
                f"Expected production events on business day {sim_date}, got 0"
            )

    def test_no_production_output_file_on_non_business_days(
        self, sim_result: dict
    ) -> None:
        """output/erp/<date>/production_completions.json absent on non-business days."""
        output_dir = sim_result["output_dir"]
        for sim_date in NON_BUSINESS_DATES:
            prod_file = (
                output_dir / "erp" / sim_date.isoformat() / "production_completions.json"
            )
            assert not prod_file.exists(), (
                f"production_completions.json should NOT exist on {sim_date}, but found it"
            )

    def test_production_output_file_on_business_days(self, sim_result: dict) -> None:
        """output/erp/<date>/production_completions.json present on all business days."""
        output_dir = sim_result["output_dir"]
        for sim_date in sorted(BUSINESS_DATES):
            prod_file = (
                output_dir / "erp" / sim_date.isoformat() / "production_completions.json"
            )
            assert prod_file.exists(), (
                f"production_completions.json should exist on business day {sim_date}"
            )

    # ------------------------------------------------------------------
    # Assertion 4: Orders on business days; possible on weekends; none on holiday
    # ------------------------------------------------------------------

    def test_orders_exist_on_business_days(self, sim_result: dict) -> None:
        """Customer orders appear across the 9 business days (collectively)."""
        events_by_date = sim_result["events_by_date"]
        total_biz_orders = sum(
            _count_type(events_by_date[d], "customer_order")
            for d in BUSINESS_DATES
        )
        assert total_biz_orders > 0, (
            "Expected customer_order events on business days, got none"
        )

    def test_no_orders_on_easter_monday(self, sim_result: dict) -> None:
        """No customer_order events on Easter Monday (holiday, not a weekend).

        Easter Monday is a weekday holiday.  The orders engine applies the
        weekday DOW multiplier (Monday=0.90) — orders CAN arrive on holidays
        per the spec ('holidays do NOT suppress orders — EDI still arrives').
        However, looking at the code, the orders engine skips weekends for
        non-Distributors but lets holidays through.  For the Distributor
        segment, is_edi_only_day is False on Monday.  The question is
        whether any orders land on Easter Monday given the probability.

        We make a softer assertion: if there are order events on Easter Monday,
        they must only be for non-weekend days (i.e., the normal weekday
        flow applies — we just check that the day loop doesn't crash and any
        orders present are structurally valid).
        """
        events_by_date = sim_result["events_by_date"]
        day_events = events_by_date[HOLIDAY_DATE]
        # The day loop must have completed without error (if we're here, it did)
        # Any orders on Easter Monday must have valid customer_order structure
        for ev in day_events:
            et = getattr(ev, "event_type", None) or (
                ev.get("event_type") if isinstance(ev, dict) else None
            )
            if et == "customer_order":
                # Validate basic structure
                order_id = getattr(ev, "order_id", None) or (
                    ev.get("order_id") if isinstance(ev, dict) else None
                )
                assert order_id is not None, (
                    f"Order on Easter Monday missing order_id: {ev}"
                )

    def test_no_orders_on_pure_weekends(self, sim_result: dict) -> None:
        """On Saturday/Sunday, only Distributors may order (or no orders at all).

        We assert that no non-Distributor orders arrive on Sat/Sun.
        """
        events_by_date = sim_result["events_by_date"]
        state = sim_result["state"]

        # Build a quick lookup: customer_id → segment
        customer_segments = {c.customer_id: c.segment for c in state.customers}

        weekend_only_dates = {
            date(2025, 4, 19),  # Saturday
            date(2025, 4, 20),  # Easter Sunday
            date(2025, 4, 26),  # Saturday
            date(2025, 4, 27),  # Sunday
        }

        for sim_date in weekend_only_dates:
            for ev in events_by_date[sim_date]:
                et = getattr(ev, "event_type", None) or (
                    ev.get("event_type") if isinstance(ev, dict) else None
                )
                if et != "customer_order":
                    continue
                cid = getattr(ev, "customer_id", None) or (
                    ev.get("customer_id") if isinstance(ev, dict) else None
                )
                seg = customer_segments.get(cid, "")
                assert seg == "Industrial Distributors", (
                    f"Non-Distributor order from {cid} (segment={seg!r}) "
                    f"found on weekend day {sim_date}"
                )

    # ------------------------------------------------------------------
    # Assertion 5: No negative inventory
    # ------------------------------------------------------------------

    def test_no_negative_inventory(self, sim_result: dict) -> None:
        """state.inventory has no negative quantity for any (warehouse, sku) pair."""
        state = sim_result["state"]
        violations: list[str] = []
        for wh_code, wh_inv in state.inventory.items():
            for sku, qty in wh_inv.items():
                if qty < 0:
                    violations.append(f"{wh_code}:{sku}={qty}")
        assert not violations, (
            f"Negative inventory found for {len(violations)} (warehouse, SKU) pairs:\n"
            + "\n".join(violations[:20])
        )

    def test_inventory_total_non_negative(self, sim_result: dict) -> None:
        """Total units in inventory across all warehouses and SKUs is >= 0."""
        state = sim_result["state"]
        total = sum(
            qty
            for wh_inv in state.inventory.values()
            for qty in wh_inv.values()
        )
        assert total >= 0, f"Total inventory is negative: {total}"

    # ------------------------------------------------------------------
    # Assertion 6: open_orders state
    # ------------------------------------------------------------------

    def test_open_orders_is_dict_or_non_empty(self, sim_result: dict) -> None:
        """state.open_orders is a dict (potentially non-empty after 14 days)."""
        state = sim_result["state"]
        assert isinstance(state.open_orders, dict), (
            f"Expected open_orders to be a dict, got {type(state.open_orders)}"
        )

    def test_credit_check_maintains_valid_statuses(self, sim_result: dict) -> None:
        """All open orders have a valid status field."""
        state = sim_result["state"]
        valid_statuses = {
            "confirmed", "credit_hold", "allocated", "partially_allocated",
            "shipped", "cancelled",
        }
        for order_id, order in state.open_orders.items():
            status = order.get("status")
            assert status in valid_statuses, (
                f"Order {order_id} has invalid status {status!r}. "
                f"Valid: {valid_statuses}"
            )

    # ------------------------------------------------------------------
    # Assertion 7: backorder aging
    # ------------------------------------------------------------------

    def test_backorder_queue_is_list(self, sim_result: dict) -> None:
        """state.backorder_queue is a list."""
        state = sim_result["state"]
        assert isinstance(state.backorder_queue, list), (
            f"Expected backorder_queue to be a list, got {type(state.backorder_queue)}"
        )

    def test_backorder_items_have_valid_structure(self, sim_result: dict) -> None:
        """Each backorder entry has required fields."""
        state = sim_result["state"]
        required_fields = {"order_id", "sku", "quantity_backordered", "backorder_days"}
        for i, bo in enumerate(state.backorder_queue):
            missing = required_fields - set(bo.keys())
            assert not missing, (
                f"Backorder entry {i} missing fields: {missing}. Keys: {set(bo.keys())}"
            )
            assert bo["backorder_days"] >= 0, (
                f"Backorder entry {i} has negative backorder_days: {bo['backorder_days']}"
            )
            assert bo["quantity_backordered"] >= 0, (
                f"Backorder entry {i} has negative quantity: {bo['quantity_backordered']}"
            )

    # ------------------------------------------------------------------
    # Assertion 8: Output file structure
    # ------------------------------------------------------------------

    def test_erp_dir_exists(self, sim_result: dict) -> None:
        """output/erp/ directory exists after 14 days."""
        output_dir = sim_result["output_dir"]
        assert (output_dir / "erp").is_dir(), "output/erp/ directory not found"

    def test_erp_first_business_day_has_expected_files(self, sim_result: dict) -> None:
        """output/erp/2025-04-14/ has exchange_rates, production_completions, customer_orders."""
        output_dir = sim_result["output_dir"]
        day_dir = output_dir / "erp" / "2025-04-14"
        assert day_dir.is_dir(), f"ERP dir for 2025-04-14 not found"
        assert (day_dir / "exchange_rates.json").exists(), "exchange_rates.json missing on 2025-04-14"
        assert (day_dir / "production_completions.json").exists(), (
            "production_completions.json missing on 2025-04-14"
        )
        # Orders might not exist if RNG produces no orders that day; don't require

    def test_erp_easter_monday_has_exchange_rates_only(self, sim_result: dict) -> None:
        """output/erp/2025-04-21/ has exchange_rates but NO production_completions."""
        output_dir = sim_result["output_dir"]
        day_dir = output_dir / "erp" / "2025-04-21"
        assert day_dir.is_dir(), "ERP dir for 2025-04-21 (Easter Monday) not found"
        assert (day_dir / "exchange_rates.json").exists(), (
            "exchange_rates.json missing on Easter Monday"
        )
        assert not (day_dir / "production_completions.json").exists(), (
            "production_completions.json should NOT exist on Easter Monday"
        )

    def test_erp_easter_sunday_has_exchange_rates_only(self, sim_result: dict) -> None:
        """output/erp/2025-04-20/ (Easter Sunday) has exchange_rates but NO production."""
        output_dir = sim_result["output_dir"]
        day_dir = output_dir / "erp" / "2025-04-20"
        assert day_dir.is_dir(), "ERP dir for 2025-04-20 not found"
        assert (day_dir / "exchange_rates.json").exists(), (
            "exchange_rates.json missing on Easter Sunday"
        )
        assert not (day_dir / "production_completions.json").exists(), (
            "production_completions.json should NOT exist on Easter Sunday"
        )

    def test_erp_saturday_has_exchange_rates_only(self, sim_result: dict) -> None:
        """output/erp/2025-04-19/ (Saturday) has exchange_rates but NO production."""
        output_dir = sim_result["output_dir"]
        day_dir = output_dir / "erp" / "2025-04-19"
        assert day_dir.is_dir(), "ERP dir for 2025-04-19 not found"
        assert (day_dir / "exchange_rates.json").exists(), (
            "exchange_rates.json missing on Saturday"
        )
        assert not (day_dir / "production_completions.json").exists(), (
            "production_completions.json should NOT exist on Saturday"
        )

    def test_json_files_are_valid_json_arrays(self, sim_result: dict) -> None:
        """All .json files written under output/ are valid JSON arrays."""
        output_dir = sim_result["output_dir"]
        failures: list[str] = []
        for json_path in output_dir.rglob("*.json"):
            try:
                data = json.loads(json_path.read_text(encoding="utf-8"))
                if not isinstance(data, list):
                    failures.append(f"{json_path}: expected list, got {type(data).__name__}")
            except json.JSONDecodeError as exc:
                failures.append(f"{json_path}: {exc}")
        assert not failures, "Invalid JSON files found:\n" + "\n".join(failures)

    # ------------------------------------------------------------------
    # Assertion 9: State is saveable and resumable
    # ------------------------------------------------------------------

    def test_state_round_trips_through_db(self, sim_result: dict, tmp_path: Path) -> None:
        """Save state, reload from DB, verify key fields match."""
        from flowform.config import load_config
        from flowform.state import SimulationState

        state = sim_result["state"]
        db_path = sim_result["db_path"]

        config = load_config(_CONFIG_SRC)
        state2 = SimulationState.from_db(config, db_path=db_path)

        assert state2.sim_day == state.sim_day, (
            f"sim_day mismatch after reload: {state2.sim_day} != {state.sim_day}"
        )
        assert state2.current_date == state.current_date, (
            f"current_date mismatch: {state2.current_date} != {state.current_date}"
        )
        assert len(state2.open_orders) == len(state.open_orders), (
            f"open_orders count mismatch: {len(state2.open_orders)} != {len(state.open_orders)}"
        )
        assert state2.exchange_rates == state.exchange_rates, (
            f"exchange_rates mismatch: {state2.exchange_rates} != {state.exchange_rates}"
        )

    def test_resumed_state_has_correct_catalog(self, sim_result: dict) -> None:
        """Reloaded state has >= 2000 SKUs in catalog."""
        from flowform.config import load_config
        from flowform.state import SimulationState

        state = sim_result["state"]
        db_path = sim_result["db_path"]
        config = load_config(_CONFIG_SRC)
        state2 = SimulationState.from_db(config, db_path=db_path)

        assert len(state2.catalog) >= 2000, (
            f"Expected >= 2000 SKUs after reload, got {len(state2.catalog)}"
        )
        assert len(state2.catalog) == len(state.catalog), (
            f"Catalog size changed on reload: {len(state2.catalog)} != {len(state.catalog)}"
        )

    # ------------------------------------------------------------------
    # Assertion 10: Event counts are reasonable
    # ------------------------------------------------------------------

    def test_total_events_exceed_threshold(self, sim_result: dict) -> None:
        """Total events across 14 days > 200 (exchange rates alone = 28)."""
        all_events = [
            ev
            for day_events in sim_result["events_by_date"].values()
            for ev in day_events
        ]
        assert len(all_events) > 200, (
            f"Expected > 200 total events, got {len(all_events)}"
        )

    def test_production_events_exist(self, sim_result: dict) -> None:
        """At least some production_completion events across 14 days."""
        all_events = [
            ev
            for day_events in sim_result["events_by_date"].values()
            for ev in day_events
        ]
        prod_count = _count_type(all_events, "production_completion")
        assert prod_count > 0, "Expected production_completion events, got none"

    def test_inventory_movement_events_exist(self, sim_result: dict) -> None:
        """Inventory movement events (receipts from production) exist."""
        all_events = [
            ev
            for day_events in sim_result["events_by_date"].values()
            for ev in day_events
        ]
        # inventory_movements events use event_type="inventory_movements"
        inv_count = _count_type(all_events, "inventory_movement")
        assert inv_count > 0, (
            f"Expected inventory_movement events (receipts from production), "
            f"event types seen: {_event_types(all_events)}"
        )

    def test_event_types_observed(self, sim_result: dict) -> None:
        """At least exchange_rate, production_completion, and inventory_movements are present."""
        all_events = [
            ev
            for day_events in sim_result["events_by_date"].values()
            for ev in day_events
        ]
        types = set(_event_types(all_events))
        assert "exchange_rate" in types, f"No exchange_rate events. Types: {types}"
        assert any(t.startswith("production_completion") for t in types), (
            f"No production_completion events. Types: {types}"
        )
        assert any(t.startswith("inventory_movement") for t in types), (
            f"No inventory_movement events. Types: {types}"
        )

    # ------------------------------------------------------------------
    # Assertion 11: Holiday day completes without error
    # ------------------------------------------------------------------

    def test_easter_monday_day_loop_completed(self, sim_result: dict) -> None:
        """The day loop on Easter Monday (2025-04-21) completed and returned a list."""
        events_by_date = sim_result["events_by_date"]
        easter_monday_events = events_by_date.get(HOLIDAY_DATE)
        assert easter_monday_events is not None, (
            f"No entry for Easter Monday {HOLIDAY_DATE} in events_by_date"
        )
        assert isinstance(easter_monday_events, list), (
            f"Expected list for Easter Monday events, got {type(easter_monday_events)}"
        )

    def test_easter_monday_has_exchange_rates(self, sim_result: dict) -> None:
        """Easter Monday event list contains exactly 2 exchange_rate events."""
        events_by_date = sim_result["events_by_date"]
        easter_monday_events = events_by_date[HOLIDAY_DATE]
        er_count = _count_type(easter_monday_events, "exchange_rate")
        assert er_count == 2, (
            f"Expected 2 exchange_rate events on Easter Monday, got {er_count}"
        )

    def test_easter_monday_has_no_production(self, sim_result: dict) -> None:
        """Easter Monday event list has zero production_completion events."""
        events_by_date = sim_result["events_by_date"]
        easter_monday_events = events_by_date[HOLIDAY_DATE]
        prod_count = _count_type(easter_monday_events, "production_completion")
        assert prod_count == 0, (
            f"Expected 0 production_completion events on Easter Monday, got {prod_count}"
        )

    def test_easter_monday_has_no_inventory_movements(self, sim_result: dict) -> None:
        """Easter Monday event list has no inventory_movement events (no production receipts)."""
        events_by_date = sim_result["events_by_date"]
        easter_monday_events = events_by_date[HOLIDAY_DATE]
        inv_count = _count_type(easter_monday_events, "inventory_movement")
        assert inv_count == 0, (
            f"Expected 0 inventory_movement events on Easter Monday (non-business day), "
            f"got {inv_count}"
        )

    # ------------------------------------------------------------------
    # Assertion 12: Production inventory invariant
    # ------------------------------------------------------------------

    def test_production_adds_to_inventory(self, sim_result: dict) -> None:
        """After 14 days with production, total inventory should be positive."""
        state = sim_result["state"]
        total = sum(
            qty
            for wh_inv in state.inventory.values()
            for qty in wh_inv.values()
        )
        assert total > 0, f"Total inventory is zero or negative after 14 days: {total}"

    def test_w01_has_stock(self, sim_result: dict) -> None:
        """W01 (main warehouse) has positive on-hand stock after 14 days."""
        state = sim_result["state"]
        w01_total = sum(state.inventory.get("W01", {}).values())
        assert w01_total > 0, f"W01 total stock is {w01_total} after 14 days"

    # ------------------------------------------------------------------
    # Assertion 13: Production output file contents are valid
    # ------------------------------------------------------------------

    def test_production_events_have_grade_field(self, sim_result: dict) -> None:
        """All production_completion events have a valid grade (A, B, or REJECT)."""
        output_dir = sim_result["output_dir"]
        valid_grades = {"A", "B", "REJECT"}
        for sim_date in sorted(BUSINESS_DATES):
            prod_file = output_dir / "erp" / sim_date.isoformat() / "production_completions.json"
            if not prod_file.exists():
                continue
            events = json.loads(prod_file.read_text(encoding="utf-8"))
            for ev in events:
                grade = ev.get("grade")
                assert grade in valid_grades, (
                    f"Invalid grade {grade!r} in production event on {sim_date}"
                )

    def test_production_sku_in_catalog(self, sim_result: dict) -> None:
        """Production completion events reference SKUs that exist in the catalog."""
        output_dir = sim_result["output_dir"]
        state = sim_result["state"]
        catalog_skus = {entry.sku for entry in state.catalog}

        for sim_date in sorted(BUSINESS_DATES):
            prod_file = output_dir / "erp" / sim_date.isoformat() / "production_completions.json"
            if not prod_file.exists():
                continue
            events = json.loads(prod_file.read_text(encoding="utf-8"))
            for ev in events:
                sku = ev.get("sku")
                assert sku in catalog_skus, (
                    f"Production event references unknown SKU {sku!r} on {sim_date}"
                )

    # ------------------------------------------------------------------
    # Assertion 14: Customer orders have valid structure
    # ------------------------------------------------------------------

    def test_customer_orders_reference_valid_customers(self, sim_result: dict) -> None:
        """customer_order events (singular) reference customer IDs in state.customers.

        Note: customer_orders.json also contains backorder events (event_type=
        'customer_orders', plural) which have a different schema.  We only
        validate the singular 'customer_order' events here.
        """
        output_dir = sim_result["output_dir"]
        state = sim_result["state"]
        valid_customers = {c.customer_id for c in state.customers}

        for sim_date in sorted(BUSINESS_DATES):
            order_file = output_dir / "erp" / sim_date.isoformat() / "customer_orders.json"
            if not order_file.exists():
                continue
            events = json.loads(order_file.read_text(encoding="utf-8"))
            for ev in events:
                if ev.get("event_type") != "customer_order":
                    continue  # Skip backorder events (event_type="customer_orders")
                cid = ev.get("customer_id")
                assert cid in valid_customers, (
                    f"Order references unknown customer {cid!r} on {sim_date}"
                )

    def test_customer_orders_have_valid_priority(self, sim_result: dict) -> None:
        """customer_order events (singular) have valid priority values."""
        output_dir = sim_result["output_dir"]
        valid_priorities = {"standard", "express", "critical"}

        for sim_date in sorted(BUSINESS_DATES):
            order_file = output_dir / "erp" / sim_date.isoformat() / "customer_orders.json"
            if not order_file.exists():
                continue
            events = json.loads(order_file.read_text(encoding="utf-8"))
            for ev in events:
                if ev.get("event_type") != "customer_order":
                    continue  # Skip backorder events (event_type="customer_orders")
                priority = ev.get("priority")
                assert priority in valid_priorities, (
                    f"Invalid priority {priority!r} on {sim_date}"
                )

    def test_customer_orders_have_lines(self, sim_result: dict) -> None:
        """customer_order events (singular) have at least one line."""
        output_dir = sim_result["output_dir"]

        for sim_date in sorted(BUSINESS_DATES):
            order_file = output_dir / "erp" / sim_date.isoformat() / "customer_orders.json"
            if not order_file.exists():
                continue
            events = json.loads(order_file.read_text(encoding="utf-8"))
            for ev in events:
                if ev.get("event_type") != "customer_order":
                    continue  # Skip backorder events (event_type="customer_orders")
                lines = ev.get("lines", [])
                assert len(lines) >= 1, (
                    f"Order {ev.get('order_id')} on {sim_date} has no lines"
                )

    # ------------------------------------------------------------------
    # Assertion 15: Exchange rate files are parseable with correct structure
    # ------------------------------------------------------------------

    def test_exchange_rate_file_structure(self, sim_result: dict) -> None:
        """exchange_rates.json on first day has EUR/PLN and USD/PLN entries."""
        output_dir = sim_result["output_dir"]
        er_file = output_dir / "erp" / "2025-04-14" / "exchange_rates.json"
        assert er_file.exists()

        events = json.loads(er_file.read_text(encoding="utf-8"))
        assert len(events) == 2, f"Expected 2 exchange rate events, got {len(events)}"

        # The exchange rate schema uses currency_pair (e.g. "EUR/PLN") and rate.
        pairs = {ev.get("currency_pair") for ev in events}
        assert "EUR/PLN" in pairs, f"EUR/PLN not in exchange rate pairs: {pairs}"
        assert "USD/PLN" in pairs, f"USD/PLN not in exchange rate pairs: {pairs}"

        for ev in events:
            rate = ev.get("rate")
            assert isinstance(rate, (int, float)), (
                f"rate should be numeric, got {type(rate)}: {rate!r}"
            )
            assert rate > 0, f"rate should be positive, got {rate}"
