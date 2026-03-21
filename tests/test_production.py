"""Tests for the production engine (Phase 2, Step 15 + Step 54)."""

from __future__ import annotations

import copy
from datetime import date, timedelta
from pathlib import Path

import pytest

from flowform.config import load_config
from flowform.engines.production import (
    ProductionCompletionEvent,
    ProductionReclassificationEvent,
    _production_sku_weights,
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


# Known business day: Monday, 2026-01-05 (not a Polish holiday)
_BUSINESS_DAY = date(2026, 1, 5)

# Known Saturday
_SATURDAY = date(2026, 1, 3)

# Known Polish holiday: New Year's Day (Thursday)
_HOLIDAY = date(2026, 1, 1)


# ---------------------------------------------------------------------------
# Test 1: No output on a weekend (Saturday)
# ---------------------------------------------------------------------------


def test_no_output_on_weekend(state, config):
    result = run(state, config, _SATURDAY)
    assert result == [], f"Expected no events on Saturday, got {len(result)}"


# ---------------------------------------------------------------------------
# Test 2: No output on a Polish public holiday
# ---------------------------------------------------------------------------


def test_no_output_on_holiday(state, config):
    result = run(state, config, _HOLIDAY)
    assert result == [], f"Expected no events on Jan 1 (holiday), got {len(result)}"


# ---------------------------------------------------------------------------
# Test 3: At least one ProductionCompletionEvent on a business day
# ---------------------------------------------------------------------------


def test_completions_on_business_day(config, tmp_path: Path):
    """Over multiple seeds / days, we should get completions on business days.

    We run 20 independent fresh states on the same day and assert that the
    overwhelming majority produce completions (stoppage probability is only
    2.5%).
    """
    completion_days = 0
    trials = 20
    for i in range(trials):
        import random
        cfg = load_config(Path("/home/coder/sc-sim/config.yaml"))
        # Override seed to vary outcomes
        cfg.simulation.seed = 42 + i * 7
        st = SimulationState.from_new(cfg, db_path=tmp_path / f"sim_{i}.db")
        events = run(st, cfg, _BUSINESS_DAY)
        completions = [e for e in events if isinstance(e, ProductionCompletionEvent)]
        if completions:
            completion_days += 1

    # With stoppage probability 2.5%, at least 15 of 20 should have completions
    assert completion_days >= 15, (
        f"Only {completion_days}/{trials} business days had completions — "
        "stoppage rate seems too high or engine is not running."
    )


# ---------------------------------------------------------------------------
# Test 4: Grade distribution over 50 business days
# ---------------------------------------------------------------------------


def test_grade_distribution(config, tmp_path: Path):
    """Over 50 business days, grades should be roughly 90% A, 8% B, 2% REJECT."""
    state = SimulationState.from_new(config, db_path=tmp_path / "sim_grades.db")

    grades: dict[str, int] = {"A": 0, "B": 0, "REJECT": 0}
    day = _BUSINESS_DAY

    # Advance through 50 business days
    days_run = 0
    current = day
    while days_run < 50:
        events = run(state, config, current)
        for e in events:
            if isinstance(e, ProductionCompletionEvent):
                grades[e.grade] += 1
        current += timedelta(days=1)
        from flowform.calendar import is_business_day
        while not is_business_day(current):
            current += timedelta(days=1)
        days_run += 1

    total = sum(grades.values())
    assert total > 0, "No completion events generated in 50 business days"

    grade_a_pct = grades["A"] / total
    grade_b_pct = grades["B"] / total
    reject_pct = grades["REJECT"] / total

    # Loose bounds to avoid flakiness
    assert grade_a_pct >= 0.80, f"Grade A pct too low: {grade_a_pct:.2%}"
    assert grade_b_pct >= 0.02, f"Grade B pct too low: {grade_b_pct:.2%}"
    assert reject_pct >= 0.005, f"REJECT pct too low: {reject_pct:.2%}"

    # Grade A should dominate
    assert grades["A"] > grades["B"], "More Grade B than Grade A — unexpected"
    assert grades["A"] > grades["REJECT"], "More REJECT than Grade A — unexpected"


# ---------------------------------------------------------------------------
# Test 5: Inventory is updated after a business day run
# ---------------------------------------------------------------------------


def test_inventory_updated(config, tmp_path: Path):
    """After run(), W01 inventory total should be >= before (Grade A + B added).

    We run until we get a non-stoppage day (at most 40 attempts).
    """
    state = SimulationState.from_new(config, db_path=tmp_path / "sim_inv.db")

    # Snapshot inventory before (sum of on_hand values)
    inv_before = sum(pos["on_hand"] for pos in state.inventory["W01"].values())

    # Run until a day that has completions (guard against back-to-back stoppages)
    current = _BUSINESS_DAY
    got_completions = False
    for _ in range(40):
        events_before_state = copy.deepcopy(state.inventory["W01"])
        events = run(state, config, current)
        completions = [e for e in events if isinstance(e, ProductionCompletionEvent)]
        if completions:
            got_completions = True
            break
        current += timedelta(days=1)
        from flowform.calendar import is_business_day
        while not is_business_day(current):
            current += timedelta(days=1)

    assert got_completions, "Could not produce any completions in 40 business days"

    inv_after = sum(pos["on_hand"] for pos in state.inventory["W01"].values())

    # At least some Grade A or B units should have been added
    assert inv_after >= inv_before, (
        f"Inventory decreased after production: before={inv_before}, after={inv_after}"
    )


# ---------------------------------------------------------------------------
# Test 6: Batch IDs are strictly increasing across two consecutive days
# ---------------------------------------------------------------------------


def test_batch_ids_sequential(config, tmp_path: Path):
    """Batch IDs across consecutive days must be strictly increasing."""
    state = SimulationState.from_new(config, db_path=tmp_path / "sim_ids.db")

    from flowform.calendar import is_business_day

    all_batch_ids: list[int] = []
    current = _BUSINESS_DAY
    days_collected = 0

    while days_collected < 2:
        events = run(state, config, current)
        completions = [e for e in events if isinstance(e, ProductionCompletionEvent)]
        if completions:
            for e in completions:
                # Parse numeric portion from "BATCH-NNNN"
                num = int(e.batch_id.split("-")[1])
                all_batch_ids.append(num)
            days_collected += 1
        current += timedelta(days=1)
        while not is_business_day(current):
            current += timedelta(days=1)

    assert len(all_batch_ids) >= 2, "Need at least 2 batches to check ordering"
    assert all_batch_ids == sorted(all_batch_ids), (
        f"Batch IDs are not in ascending order: {all_batch_ids}"
    )
    # All IDs must be strictly increasing (no duplicates)
    assert len(set(all_batch_ids)) == len(all_batch_ids), (
        "Duplicate batch IDs detected"
    )


# ---------------------------------------------------------------------------
# Test 7: Reclassification appears after delay when due date <= today
# ---------------------------------------------------------------------------


def test_reclassification_appears_after_delay(config, tmp_path: Path):
    """Manually inject a pipeline entry with reclass_due_date = today.

    Expect exactly one ProductionReclassificationEvent in the output.
    """
    state = SimulationState.from_new(config, db_path=tmp_path / "sim_reclass.db")

    today = _BUSINESS_DAY
    state.production_pipeline.append(
        {
            "batch_id": "BATCH-9999",
            "sku": state.catalog[0].sku,
            "quantity": 50,
            "reclass_due_date": today.isoformat(),
            "original_completion_date": (today - timedelta(days=3)).isoformat(),
        }
    )

    events = run(state, config, today)

    reclass_events = [
        e for e in events if isinstance(e, ProductionReclassificationEvent)
    ]
    assert len(reclass_events) >= 1, "Expected at least one reclassification event"

    found = next(e for e in reclass_events if e.batch_id == "BATCH-9999")
    assert found.original_grade == "A"
    assert found.new_grade == "B"
    assert found.reclassification_reason == "quality_review"
    assert found.simulation_date == today.isoformat()

    # Entry must be removed from the pipeline
    remaining_ids = [e["batch_id"] for e in state.production_pipeline]
    assert "BATCH-9999" not in remaining_ids, (
        "Processed pipeline entry was not removed from production_pipeline"
    )


# ---------------------------------------------------------------------------
# Test 8: Reclassification NOT emitted when due date is in the future
# ---------------------------------------------------------------------------


def test_reclassification_not_early(config, tmp_path: Path):
    """Pipeline entry with future reclass_due_date must not trigger today."""
    state = SimulationState.from_new(config, db_path=tmp_path / "sim_reclass_early.db")

    today = _BUSINESS_DAY
    future_due = (today + timedelta(days=3)).isoformat()
    state.production_pipeline.append(
        {
            "batch_id": "BATCH-8888",
            "sku": state.catalog[0].sku,
            "quantity": 25,
            "reclass_due_date": future_due,
            "original_completion_date": (today - timedelta(days=1)).isoformat(),
        }
    )

    events = run(state, config, today)

    reclass_events = [
        e for e in events if isinstance(e, ProductionReclassificationEvent)
    ]
    batch_ids = [e.batch_id for e in reclass_events]
    assert "BATCH-8888" not in batch_ids, (
        "Reclassification emitted too early (before reclass_due_date)"
    )

    # Entry must still be in the pipeline
    remaining_ids = [e["batch_id"] for e in state.production_pipeline]
    assert "BATCH-8888" in remaining_ids, (
        "Future pipeline entry was removed prematurely"
    )


# ---------------------------------------------------------------------------
# Test 9: Daily units per day are in a reasonable range
# ---------------------------------------------------------------------------


def test_daily_units_in_range(config, tmp_path: Path):
    """Over 20 business days, total units per day should be within [50, 1500].

    Multipliers applied: DOW (0.80-1.10) x monthly (0.70-1.25) x base (200-800).
    Worst case low: 200 * 0.80 * 0.70 = 112. Worst case high: 800 * 1.10 * 1.25 = 1100.
    We use [50, 1500] to be safe with rounding and year-end override.
    """
    from flowform.calendar import is_business_day

    state = SimulationState.from_new(config, db_path=tmp_path / "sim_units.db")

    current = _BUSINESS_DAY
    days_checked = 0
    while days_checked < 20:
        events = run(state, config, current)
        completions = [e for e in events if isinstance(e, ProductionCompletionEvent)]
        if completions:
            total_units = sum(e.quantity for e in completions)
            assert 50 <= total_units <= 1500, (
                f"On {current}, total units {total_units} is outside [50, 1500]"
            )
            days_checked += 1
        current += timedelta(days=1)
        while not is_business_day(current):
            current += timedelta(days=1)


# ---------------------------------------------------------------------------
# Test 10: Reproducibility — same seed + same date -> identical events
# ---------------------------------------------------------------------------


def test_reproducible(config, tmp_path: Path):
    """Two fresh states with the same seed on the same date produce identical events."""
    state_a = SimulationState.from_new(config, db_path=tmp_path / "sim_rep_a.db")
    state_b = SimulationState.from_new(config, db_path=tmp_path / "sim_rep_b.db")

    events_a = run(state_a, config, _BUSINESS_DAY)
    events_b = run(state_b, config, _BUSINESS_DAY)

    # Compare serialised form (model_dump) for full field equality
    dump_a = [e.model_dump() for e in events_a]
    dump_b = [e.model_dump() for e in events_b]

    # event_id is a UUID4 and will differ — strip it before comparison
    for d in dump_a + dump_b:
        d.pop("event_id", None)

    assert dump_a == dump_b, (
        "Production events differed between two states with identical seeds. "
        "Randomness is not flowing deterministically through state.rng."
    )


# ---------------------------------------------------------------------------
# Step 54: Demand-weighted production tests (Tests 11–20)
# ---------------------------------------------------------------------------


def test_weight_function_prefers_high_pressure_sku(config, tmp_path: Path):
    """A starved SKU (on_hand=0, target=150) must get a higher weight than a
    fully-stocked SKU (on_hand=200, target=150)."""
    state = SimulationState.from_new(config, db_path=tmp_path / "sim_w1.db")

    # Use only first two catalog entries
    sku_starved = state.catalog[0].sku
    sku_full = state.catalog[1].sku

    # Set inventory so starved has 0 and full has 200
    state.inventory["W01"][sku_starved] = {"on_hand": 0, "allocated": 0, "in_transit": 0}
    state.inventory["W01"][sku_full] = {"on_hand": 200, "allocated": 0, "in_transit": 0}
    state.inventory_target_stock[sku_starved] = 150
    state.inventory_target_stock[sku_full] = 150

    weights = _production_sku_weights(state, config)
    idx_starved = next(i for i, e in enumerate(state.catalog) if e.sku == sku_starved)
    idx_full = next(i for i, e in enumerate(state.catalog) if e.sku == sku_full)

    assert weights[idx_starved] > weights[idx_full], (
        f"Starved SKU weight {weights[idx_starved]:.4f} should exceed "
        f"full SKU weight {weights[idx_full]:.4f}"
    )


def test_weight_function_skus_above_target_get_zero_stock_pressure(config, tmp_path: Path):
    """A SKU with on_hand >= target_stock must have stock_pressure == 0."""
    state = SimulationState.from_new(config, db_path=tmp_path / "sim_w2.db")
    sku = state.catalog[0].sku

    state.inventory["W01"][sku] = {"on_hand": 200, "allocated": 0, "in_transit": 0}
    state.inventory_target_stock[sku] = 100  # on_hand > target

    weights = _production_sku_weights(state, config)
    idx = next(i for i, e in enumerate(state.catalog) if e.sku == sku)

    # Stock pressure for this SKU must be 0; only days_since contributes.
    # Since sim_day == 0 and SKU was never produced, days_since = 0 - 0 = 0.
    # Both components are 0, so the weight is 0 (fallback uniform applies if ALL are 0).
    # Assert weight <= pure days_since contribution (0.4 * 1.0 max).
    assert weights[idx] <= 0.4 + 1e-9, (
        f"SKU above target should have zero stock_pressure; weight={weights[idx]:.4f}"
    )


def test_weight_function_never_produced_sku_gets_max_days_since(config, tmp_path: Path):
    """A SKU absent from last_produced gets days_since = sim_day (maximum urgency).

    With sim_day > 0, its normalised days_since component should be the highest
    among all SKUs that were produced at sim_day - 1 or earlier.
    """
    state = SimulationState.from_new(config, db_path=tmp_path / "sim_w3.db")
    state.sim_day = 10  # advance sim_day without actually running days

    sku_never = state.catalog[0].sku
    sku_recent = state.catalog[1].sku

    # Mark recent as produced today
    state.last_produced[sku_recent] = 10
    # sku_never is absent from last_produced → days_since = 10

    # Both have identical stock pressure (default target = 150, on_hand = 0)
    state.inventory["W01"][sku_never] = {"on_hand": 0, "allocated": 0, "in_transit": 0}
    state.inventory["W01"][sku_recent] = {"on_hand": 0, "allocated": 0, "in_transit": 0}

    weights = _production_sku_weights(state, config)
    idx_never = next(i for i, e in enumerate(state.catalog) if e.sku == sku_never)
    idx_recent = next(i for i, e in enumerate(state.catalog) if e.sku == sku_recent)

    assert weights[idx_never] > weights[idx_recent], (
        f"Never-produced SKU weight {weights[idx_never]:.4f} should exceed "
        f"recently-produced SKU weight {weights[idx_recent]:.4f}"
    )


def test_weight_function_recently_produced_sku_gets_lower_weight_than_stale(config, tmp_path: Path):
    """Two SKUs with equal stock_pressure: the stale one must have higher weight."""
    state = SimulationState.from_new(config, db_path=tmp_path / "sim_w4.db")
    state.sim_day = 20

    sku_stale = state.catalog[0].sku
    sku_fresh = state.catalog[1].sku

    # Both at 0 inventory (equal stock pressure)
    state.inventory["W01"][sku_stale] = {"on_hand": 0, "allocated": 0, "in_transit": 0}
    state.inventory["W01"][sku_fresh] = {"on_hand": 0, "allocated": 0, "in_transit": 0}
    state.inventory_target_stock[sku_stale] = 150
    state.inventory_target_stock[sku_fresh] = 150

    # Stale: never produced (days_since = 20); fresh: produced today (days_since = 0)
    state.last_produced[sku_fresh] = 20  # produced this exact sim_day

    weights = _production_sku_weights(state, config)
    idx_stale = next(i for i, e in enumerate(state.catalog) if e.sku == sku_stale)
    idx_fresh = next(i for i, e in enumerate(state.catalog) if e.sku == sku_fresh)

    assert weights[idx_stale] > weights[idx_fresh], (
        f"Stale weight {weights[idx_stale]:.4f} should exceed "
        f"fresh weight {weights[idx_fresh]:.4f}"
    )


def test_weight_function_all_at_target_falls_back_to_uniform(config, tmp_path: Path):
    """When all SKUs are at/above target AND produced this sim_day, weights should be uniform."""
    state = SimulationState.from_new(config, db_path=tmp_path / "sim_w5.db")
    state.sim_day = 5

    # Set every catalog SKU: on_hand >= target, produced this day
    for entry in state.catalog:
        state.inventory["W01"][entry.sku] = {"on_hand": 200, "allocated": 0, "in_transit": 0}
        state.inventory_target_stock[entry.sku] = 100
        state.last_produced[entry.sku] = 5  # produced today

    weights = _production_sku_weights(state, config)

    # All weights should be equal (fallback uniform = 1.0)
    assert all(w == weights[0] for w in weights), (
        "Expected all weights equal when every SKU is above target and produced today"
    )
    assert weights[0] == 1.0, (
        f"Fallback uniform should be 1.0, got {weights[0]}"
    )


def test_weight_function_deterministic(config, tmp_path: Path):
    """Same state inputs must always produce the same weights list."""
    state = SimulationState.from_new(config, db_path=tmp_path / "sim_w6.db")
    state.sim_day = 7
    # Set some non-trivial inventory
    for i, entry in enumerate(state.catalog[:10]):
        state.inventory["W01"][entry.sku] = {"on_hand": i * 15, "allocated": 0, "in_transit": 0}
        state.inventory_target_stock[entry.sku] = 100

    weights_1 = _production_sku_weights(state, config)
    weights_2 = _production_sku_weights(state, config)

    assert weights_1 == weights_2, "Weight function produced different results on identical state"


def test_last_produced_updated_after_run(config, tmp_path: Path):
    """After run() on a business day, state.last_produced must contain all
    produced SKUs with value == state.sim_day."""
    state = SimulationState.from_new(config, db_path=tmp_path / "sim_lp1.db")

    from flowform.calendar import is_business_day as _is_biz

    # Run until we get completions
    current = _BUSINESS_DAY
    for _ in range(30):
        events = run(state, config, current)
        completions = [e for e in events if isinstance(e, ProductionCompletionEvent)]
        if completions:
            produced_skus = {e.sku for e in completions}
            for sku in produced_skus:
                assert sku in state.last_produced, f"{sku} missing from last_produced"
                assert state.last_produced[sku] == state.sim_day, (
                    f"last_produced[{sku}]={state.last_produced[sku]} != sim_day={state.sim_day}"
                )
            return
        current += timedelta(days=1)
        while not _is_biz(current):
            current += timedelta(days=1)

    pytest.fail("Could not get any completions in 30 business days")


def test_last_produced_roundtrips_sqlite(config, tmp_path: Path):
    """state.last_produced must survive a save/reload cycle."""
    db = tmp_path / "sim_lp2.db"
    state = SimulationState.from_new(config, db_path=db)

    # Manually write some entries
    sku1 = state.catalog[0].sku
    sku2 = state.catalog[1].sku
    state.last_produced[sku1] = 5
    state.last_produced[sku2] = 12
    state.save()

    # Reload
    state2 = SimulationState.from_db(config, db_path=db)
    assert state2.last_produced.get(sku1) == 5, "sku1 last_produced not persisted"
    assert state2.last_produced.get(sku2) == 12, "sku2 last_produced not persisted"


def test_inventory_target_stock_roundtrips_sqlite(config, tmp_path: Path):
    """state.inventory_target_stock must survive a save/reload cycle."""
    db = tmp_path / "sim_its.db"
    state = SimulationState.from_new(config, db_path=db)

    sku = state.catalog[3].sku
    state.inventory_target_stock[sku] = 275
    state.save()

    state2 = SimulationState.from_db(config, db_path=db)
    assert state2.inventory_target_stock.get(sku) == 275, (
        "inventory_target_stock not persisted through SQLite"
    )


def test_popular_sku_produced_more_than_obscure_sku(config, tmp_path: Path):
    """Integration: one chronically starved SKU (on_hand=0, target=150) vs
    2499 SKUs consistently above target (on_hand=500, target=150).

    After each production run we reset the starved SKU's inventory back to 0
    so it remains perpetually below target throughout all 50 days.

    Under demand-weighted selection the starved SKU must appear in completions
    on at least 15 of 50 business days (>=30%).  Under uniform random it would
    appear on ~2% of days on average.
    """
    from flowform.calendar import is_business_day as _is_biz

    state = SimulationState.from_new(config, db_path=tmp_path / "sim_popular.db")

    starved_sku = state.catalog[0].sku

    # Set all SKUs to on_hand=500, target=150 (all above target → 0 stock pressure)
    for entry in state.catalog:
        state.inventory["W01"][entry.sku] = {"on_hand": 500, "allocated": 0, "in_transit": 0}
        state.inventory_target_stock[entry.sku] = 150

    # Starve only the first SKU — stock_pressure = 1.0
    state.inventory["W01"][starved_sku] = {"on_hand": 0, "allocated": 0, "in_transit": 0}

    days_with_starved = 0
    current = _BUSINESS_DAY
    biz_days_run = 0

    while biz_days_run < 50:
        events = run(state, config, current)
        completions = [e for e in events if isinstance(e, ProductionCompletionEvent)]
        if completions:
            skus_today = {e.sku for e in completions}
            if starved_sku in skus_today:
                days_with_starved += 1
            biz_days_run += 1
        # Keep the starved SKU starved so stock_pressure stays at 1.0 every day.
        state.inventory["W01"][starved_sku] = {"on_hand": 0, "allocated": 0, "in_transit": 0}
        current += timedelta(days=1)
        while not _is_biz(current):
            current += timedelta(days=1)

    assert days_with_starved >= 15, (
        f"Starved SKU appeared on only {days_with_starved}/50 days — "
        "demand-weighted selection not working (expected >=15)"
    )
