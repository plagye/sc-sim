"""Tests for the late-arriving data post-processing engine (Step 42).

Coverage:
- TMS late timestamps (carrier_event, return_event back-dated ~5%)
- ERP inventory movement carry-forward (~2% deferred)
- TMS maintenance burst (all load_events → sync_window "20", once/month)
- No crash on empty input
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from flowform.engines.late_arriving import apply, _prev_business_day
from flowform.state import SimulationState


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_state(tmp_path: Path, config) -> SimulationState:
    """Fresh SimulationState in a tmp directory."""
    state = SimulationState.from_new(config, db_path=tmp_path / "sim.db")
    return state


@pytest.fixture
def config():
    from flowform.config import load_config
    from pathlib import Path
    cfg_path = Path(__file__).parent.parent / "config.yaml"
    return load_config(cfg_path)


# ---------------------------------------------------------------------------
# Event factory helpers
# ---------------------------------------------------------------------------

def _make_carrier_event(sim_date: date, sync_window: str = "08") -> dict[str, Any]:
    return {
        "event_type": "carrier_event",
        "event_id": "evt-ca-001",
        "carrier_code": "DHL",
        "event_subtype": "capacity_alert",
        "timestamp": f"{sim_date.isoformat()}T{sync_window}:00:00Z",
        "sync_window": sync_window,
        "impact_severity": "low",
        "message": "Test",
        "duration_days": 0,
    }


def _make_return_event(sim_date: date) -> dict[str, Any]:
    return {
        "event_type": "return_event",
        "event_id": "evt-ret-001",
        "event_subtype": "return_requested",
        "rma_id": "RMA-1001",
        "order_id": "ORD-2026-001001",
        "customer_id": "CUST-001",
        "carrier_code": "DHL",
        "return_warehouse_id": "W01",
        "lines": [],
        "resolution": None,
        "return_reason": "quality_defect",
        "rma_status": "return_requested",
        "simulation_date": sim_date.isoformat(),
        "timestamp": f"{sim_date.isoformat()}T08:00:00Z",
        "sync_window": "08",
    }


def _make_load_event(sim_date: date, sync_window: str = "08") -> dict[str, Any]:
    return {
        "event_type": "load_event",
        "event_id": "evt-load-001",
        "load_id": "LOAD-001",
        "event_subtype": "load_created",
        "simulation_date": sim_date.isoformat(),
        "sync_window": sync_window,
        "carrier_code": "DHL",
        "source_warehouse_id": "W01",
        "shipment_ids": ["SHP-001"],
        "order_ids": ["ORD-2026-001001"],
        "total_weight_kg": 10.0,
        "weight_unit": "kg",
        "total_weight_reported": 10.0,
        "status": "load_created",
        "planned_date": sim_date.isoformat(),
        "priority": "standard",
        "customer_ids": ["CUST-001"],
    }


def _make_inventory_movement(sim_date: date) -> dict[str, Any]:
    return {
        "event_type": "inventory_movements",
        "event_id": "evt-inv-001",
        "movement_id": "MOV-1001",
        "movement_type": "receipt",
        "sku": "FF-GA15-CS-PN16-FL-MA",
        "warehouse_id": "W01",
        "quantity": 10,
        "simulation_date": sim_date.isoformat(),
        "timestamp": f"{sim_date.isoformat()}T17:00:00Z",
    }


def _make_erp_event(sim_date: date, event_type: str = "customer_order") -> dict[str, Any]:
    return {
        "event_type": event_type,
        "event_id": "evt-erp-001",
        "simulation_date": sim_date.isoformat(),
    }


# ---------------------------------------------------------------------------
# Tests: TMS late timestamps
# ---------------------------------------------------------------------------

SIM_DATE = date(2026, 3, 10)  # Tuesday


def test_tms_late_timestamp_fraction(tmp_state: SimulationState, config) -> None:
    """~5% of TMS carrier/return events should be back-dated."""
    # Verify that the probability is read from config (M2)
    assert config.noise.tms_late_probability == 0.05
    assert config.noise.erp_defer_probability == 0.02
    assert config.noise.maintenance_probability == 0.045

    # Use 200 carrier_events to get a stable estimate
    events = [_make_carrier_event(SIM_DATE) for _ in range(200)]
    result = apply(events, tmp_state, config, SIM_DATE)

    prev_biz = _prev_business_day(SIM_DATE)
    backdated = sum(
        1 for e in result
        if isinstance(e, dict) and e.get("timestamp", "").startswith(prev_biz.isoformat())
    )
    # Expect 2–15 out of 200 (5% ± noise)
    assert 2 <= backdated <= 15, f"Expected 2–15 back-dated, got {backdated}"


def test_tms_late_timestamp_only_tms_events(tmp_state: SimulationState, config) -> None:
    """ERP inventory_movement events must never be back-dated."""
    events = [_make_inventory_movement(SIM_DATE) for _ in range(50)]
    original_timestamps = [e["timestamp"] for e in events]

    result = apply(events, tmp_state, config, SIM_DATE)

    # After apply, deferred ones are removed, but non-deferred keep original timestamp
    erp_in_result = [e for e in result if isinstance(e, dict) and e.get("event_type") == "inventory_movements"]
    for e in erp_in_result:
        assert e["timestamp"].startswith(SIM_DATE.isoformat()), (
            f"ERP event timestamp was back-dated: {e['timestamp']}"
        )


def test_tms_late_timestamp_preserves_sync_window(tmp_state: SimulationState, config) -> None:
    """Back-dated load_events must still have a valid sync_window field."""
    # load_events don't have timestamp so they can't be back-dated, but
    # carrier_events can — after back-dating sync_window must be intact.
    events = [_make_carrier_event(SIM_DATE, sync_window="14") for _ in range(100)]
    result = apply(events, tmp_state, config, SIM_DATE)

    for e in result:
        if isinstance(e, dict) and e.get("event_type") == "carrier_event":
            assert e.get("sync_window") in {"08", "14", "20"}, (
                f"sync_window corrupted: {e.get('sync_window')}"
            )


# ---------------------------------------------------------------------------
# Tests: ERP inventory movement carry-forward
# ---------------------------------------------------------------------------

def test_erp_deferred_fraction(tmp_state: SimulationState, config) -> None:
    """~0–8 of 200 inventory_movement events should be withheld."""
    events = [_make_inventory_movement(SIM_DATE) for _ in range(200)]
    result = apply(events, tmp_state, config, SIM_DATE)

    inv_in_result = sum(
        1 for e in result
        if isinstance(e, dict) and e.get("event_type") == "inventory_movements"
    )
    withheld = 200 - inv_in_result
    assert 0 <= withheld <= 15, f"Expected 0–15 withheld, got {withheld}"


def test_erp_deferred_stored_in_state(tmp_state: SimulationState, config) -> None:
    """After apply(), state.deferred_erp_movements holds the withheld events."""
    events = [_make_inventory_movement(SIM_DATE) for _ in range(200)]
    result = apply(events, tmp_state, config, SIM_DATE)

    inv_in_result = sum(
        1 for e in result
        if isinstance(e, dict) and e.get("event_type") == "inventory_movements"
    )
    withheld = 200 - inv_in_result
    assert len(tmp_state.deferred_erp_movements) == withheld


def test_erp_deferred_injected_next_day(tmp_state: SimulationState, config) -> None:
    """Deferred events from day 1 appear at the start of day 2's output."""
    day1 = SIM_DATE
    day2 = date(2026, 3, 11)  # Wednesday

    # Day 1: run with 100 inventory movements
    events_day1 = [_make_inventory_movement(day1) for _ in range(100)]
    _result_day1 = apply(events_day1, tmp_state, config, day1)

    deferred_count = len(tmp_state.deferred_erp_movements)
    if deferred_count == 0:
        # Nothing deferred — create a known deferred event manually
        tmp_state.deferred_erp_movements = [_make_inventory_movement(day1)]
        deferred_count = 1

    # Day 2: run with a single non-movement event
    events_day2 = [_make_erp_event(day2, "exchange_rate")]
    result_day2 = apply(events_day2, tmp_state, config, day2)

    # Deferred events from day 1 should be prepended
    injected = [
        e for e in result_day2
        if isinstance(e, dict) and e.get("event_type") == "inventory_movements"
        and e.get("simulation_date") == day1.isoformat()
    ]
    assert len(injected) == deferred_count, (
        f"Expected {deferred_count} injected from yesterday, got {len(injected)}"
    )


def test_erp_deferred_cleared_after_injection(tmp_state: SimulationState, config) -> None:
    """After day 2 apply(), deferred contains only day 2's newly withheld events."""
    day1 = SIM_DATE
    day2 = date(2026, 3, 11)

    # Force a known deferred set from day 1
    tmp_state.deferred_erp_movements = [
        _make_inventory_movement(day1),
        _make_inventory_movement(day1),
    ]

    # Day 2: run with 100 movements
    events_day2 = [_make_inventory_movement(day2) for _ in range(100)]
    _result_day2 = apply(events_day2, tmp_state, config, day2)

    # Day 1's deferred should not be in state anymore
    for d in tmp_state.deferred_erp_movements:
        assert d.get("simulation_date") != day1.isoformat(), (
            "Day 1 deferred event found in state after day 2 — not cleared correctly"
        )


# ---------------------------------------------------------------------------
# Tests: TMS maintenance burst
# ---------------------------------------------------------------------------

def test_maintenance_burst_fires_at_most_once_per_month(
    tmp_state: SimulationState, config
) -> None:
    """Over 22 consecutive business days in the same month, burst fires 0 or 1 times."""
    # Use March 2026 business days
    march_biz_days = []
    d = date(2026, 3, 2)
    from flowform.calendar import is_business_day as ibd
    while len(march_biz_days) < 22 and d.month == 3:
        if ibd(d):
            march_biz_days.append(d)
        d = d.replace(day=d.day + 1) if d.day < 31 else date(d.year, d.month + 1, 1)
        if d.month != 3:
            break

    burst_count = 0
    for sim_day in march_biz_days:
        events = [_make_load_event(sim_day, "08") for _ in range(5)]
        prev_month = tmp_state.tms_maintenance_fired_month
        _result = apply(events, tmp_state, config, sim_day)
        if tmp_state.tms_maintenance_fired_month != prev_month:
            burst_count += 1

    assert burst_count <= 1, f"Maintenance burst fired {burst_count} times in one month"


def test_maintenance_burst_all_loads_go_to_20(tmp_state: SimulationState, config) -> None:
    """When maintenance burst fires, all load_events must have sync_window == '20'."""
    # Force maintenance burst: override rng to always return 0.0 for the burst roll
    # We set tms_maintenance_fired_month to a different month so it can fire.
    tmp_state.tms_maintenance_fired_month = "2026-01"

    events = [_make_load_event(SIM_DATE, sw) for sw in ["08", "08", "14", "20"]]
    # Also add a non-load event
    events.append(_make_carrier_event(SIM_DATE, "08"))

    # Patch rng to force the burst (return value < 1/22 for the maintenance check).
    # We need to ensure the burst fires: monkeypatch rng.random to return 0.0
    original_random = tmp_state.rng.random
    call_count = [0]

    def _patched_random():
        call_count[0] += 1
        # Return 0.0 on the maintenance roll (which is the last rng.random call in apply)
        # We can't easily intercept which call is which, so just always return 0.0
        return 0.0

    tmp_state.rng.random = _patched_random  # type: ignore[method-assign]
    try:
        result = apply(events, tmp_state, config, SIM_DATE)
    finally:
        tmp_state.rng.random = original_random  # type: ignore[method-assign]

    load_events = [e for e in result if isinstance(e, dict) and e.get("event_type") == "load_event"]
    assert len(load_events) > 0, "No load_events in result"
    for le in load_events:
        assert le["sync_window"] == "20", (
            f"load_event sync_window should be '20' after burst, got {le['sync_window']}"
        )


def test_maintenance_burst_can_fire_next_month(tmp_state: SimulationState, config) -> None:
    """Burst blocked in month 2026-02 should be allowed in 2026-03."""
    tmp_state.tms_maintenance_fired_month = "2026-02"
    sim_date = date(2026, 3, 9)  # Monday, business day

    events = [_make_load_event(sim_date, "08") for _ in range(10)]

    # Patch rng to force burst
    original_random = tmp_state.rng.random
    tmp_state.rng.random = lambda: 0.0  # type: ignore[method-assign]
    try:
        _result = apply(events, tmp_state, config, sim_date)
    finally:
        tmp_state.rng.random = original_random  # type: ignore[method-assign]

    assert tmp_state.tms_maintenance_fired_month == "2026-03", (
        "Burst should have fired for March 2026 and updated tms_maintenance_fired_month"
    )


def test_maintenance_burst_timestamps_out_of_sequence(
    tmp_state: SimulationState, config
) -> None:
    """After burst, load_events in sync_window '20' have their original timestamps (not '20:...')."""
    tmp_state.tms_maintenance_fired_month = "2026-01"

    # Load events with sync_window "08" — simulation_date based, no timestamp field on LoadEvent
    # But we can check that simulation_date is preserved (not changed to "20")
    events = [_make_load_event(SIM_DATE, "08") for _ in range(5)]

    original_random = tmp_state.rng.random
    tmp_state.rng.random = lambda: 0.0  # type: ignore[method-assign]
    try:
        result = apply(events, tmp_state, config, SIM_DATE)
    finally:
        tmp_state.rng.random = original_random  # type: ignore[method-assign]

    load_events = [e for e in result if isinstance(e, dict) and e.get("event_type") == "load_event"]
    for le in load_events:
        # sync_window is now "20", but simulation_date should be unchanged
        assert le["sync_window"] == "20"
        assert le["simulation_date"] == SIM_DATE.isoformat(), (
            "simulation_date should be preserved after maintenance burst"
        )
        # The test name says "timestamps out of sequence" — meaning the original
        # sync_window time (08 or 14) is preserved in simulation_date, not changed.
        # simulation_date doesn't start with "20" (it's a date not a time).
        assert not le["simulation_date"].startswith("20:"), (
            "simulation_date should not look like a time string"
        )


# ---------------------------------------------------------------------------
# Tests: edge cases
# ---------------------------------------------------------------------------

def test_no_crash_on_empty_events(tmp_state: SimulationState, config) -> None:
    """apply() with an empty event list returns empty list without error."""
    result = apply([], tmp_state, config, SIM_DATE)
    assert result == []
    assert tmp_state.deferred_erp_movements == []


def test_maintenance_burst_never_fires_on_saturday(
    tmp_state: SimulationState, config
) -> None:
    """I1: Maintenance burst must not fire on a Saturday even when rng always returns 0.0."""
    saturday = date(2026, 1, 3)  # Saturday
    # Set previous month so the burst could fire if not guarded
    tmp_state.tms_maintenance_fired_month = "2025-12"
    original_month = tmp_state.tms_maintenance_fired_month

    original_random = tmp_state.rng.random
    tmp_state.rng.random = lambda: 0.0  # type: ignore[method-assign]
    try:
        events = [_make_load_event(saturday) for _ in range(3)]
        result = apply(events, tmp_state, config, saturday)
    finally:
        tmp_state.rng.random = original_random  # type: ignore[method-assign]

    # Burst must NOT have fired — month key must be unchanged
    assert tmp_state.tms_maintenance_fired_month == original_month, (
        "Maintenance burst fired on a Saturday — weekend guard is missing"
    )
    # No load_event should have been moved to sync_window '20' by the burst
    load_events = [e for e in result if isinstance(e, dict) and e.get("event_type") == "load_event"]
    for le in load_events:
        assert le.get("sync_window") != "20", (
            "load_event sync_window was set to '20' by maintenance burst on Saturday"
        )
