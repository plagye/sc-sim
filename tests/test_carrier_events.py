"""Tests for the carrier events engine (Step 25).

Covers:
1.  No events on weekend
2.  No events on public holiday (New Year's Day)
3.  capacity_alert fields — all required fields present and valid
4.  disruption fields — end_date set, rate_delta_pct None
5.  Disruption added to state.carrier_disruptions with end_date and severity
6.  Active disruption prevents a second disruption for same carrier
7.  Disruption expiry — expired disruptions removed at start of run()
8.  rate_change fields — rate_delta_pct between 5 and 15, sync_window="14"
9.  rate_change sync_window always "14"
10. All emitted events have event_type="carrier_event"
11. (bonus) High-severity disruption degrades load_lifecycle reliability by 0.35
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from flowform.config import load_config
from flowform.engines import carrier_events, load_lifecycle
from flowform.engines.carrier_events import CarrierEvent
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"

_BIZ_DAY = date(2026, 1, 5)     # Monday — business day
_SATURDAY = date(2026, 1, 10)   # Saturday — weekend
_NEW_YEARS = date(2026, 1, 1)   # Thursday — Polish public holiday


@pytest.fixture()
def config():
    return load_config(_CONFIG_PATH)


@pytest.fixture()
def state(config, tmp_path):
    """Fresh simulation state isolated in a temp directory."""
    db = tmp_path / "sim.db"
    return SimulationState.from_new(config, db_path=db)


# ---------------------------------------------------------------------------
# Helper: force a specific event to fire by seeding RNG or patching
# ---------------------------------------------------------------------------


def _get_event_of_subtype(
    state: SimulationState,
    config,
    sim_date: date,
    subtype: str,
    max_attempts: int = 500,
) -> CarrierEvent:
    """Run the engine repeatedly with different RNG seeds until a specific
    event subtype is produced.

    Args:
        state:        Simulation state (rng will be re-seeded).
        config:       Config object.
        sim_date:     Business day to simulate.
        subtype:      "capacity_alert" | "disruption" | "rate_change".
        max_attempts: Safety limit on seed attempts.

    Returns:
        The first matching CarrierEvent found.

    Raises:
        RuntimeError: If no event of the requested subtype appears within
                      *max_attempts* seeds.
    """
    for seed in range(max_attempts):
        # Clear disruption state between attempts so disruption can fire
        state.carrier_disruptions.clear()
        state.rng.seed(seed)
        events = carrier_events.run(state, config, sim_date)
        for ev in events:
            if ev.event_subtype == subtype:
                return ev
    raise RuntimeError(
        f"Could not produce a {subtype!r} event in {max_attempts} RNG seeds."
    )


# ---------------------------------------------------------------------------
# Test 1: No events on a weekend
# ---------------------------------------------------------------------------


def test_no_events_on_weekend(state, config):
    """run() on a Saturday must return an empty list (business-day guard)."""
    events = carrier_events.run(state, config, _SATURDAY)
    assert events == []


# ---------------------------------------------------------------------------
# Test 2: No events on a public holiday
# ---------------------------------------------------------------------------


def test_no_events_on_holiday(state, config):
    """run() on New Year's Day (Polish public holiday) must return []."""
    events = carrier_events.run(state, config, _NEW_YEARS)
    assert events == []


# ---------------------------------------------------------------------------
# Test 3: capacity_alert has all required fields
# ---------------------------------------------------------------------------


def test_capacity_alert_fields(state, config):
    """A capacity_alert event must have all required fields with valid values."""
    ev = _get_event_of_subtype(state, config, _BIZ_DAY, "capacity_alert")

    assert ev.event_type == "carrier_event"
    assert ev.event_subtype == "capacity_alert"
    assert ev.event_id != ""
    assert ev.carrier_id != ""
    assert ev.carrier_name != ""
    assert ev.severity in {"low", "medium", "high"}
    assert ev.start_date == _BIZ_DAY.isoformat()
    assert ev.simulation_date == _BIZ_DAY.isoformat()
    assert ev.timestamp.endswith("Z")
    assert ev.sync_window in {"08", "14", "20"}
    assert ev.affected_region is not None
    # rate_delta_pct and end_date must be absent / None for capacity_alert
    assert ev.rate_delta_pct is None
    assert ev.end_date is None


# ---------------------------------------------------------------------------
# Test 4: disruption has end_date set and rate_delta_pct None
# ---------------------------------------------------------------------------


def test_disruption_fields(state, config):
    """A disruption event must have end_date set and rate_delta_pct as None."""
    ev = _get_event_of_subtype(state, config, _BIZ_DAY, "disruption")

    assert ev.event_subtype == "disruption"
    assert ev.end_date is not None
    # end_date must parse as a valid ISO date after sim_date
    end = date.fromisoformat(ev.end_date)
    assert end > _BIZ_DAY
    assert ev.rate_delta_pct is None


# ---------------------------------------------------------------------------
# Test 5: disruption added to state.carrier_disruptions
# ---------------------------------------------------------------------------


def test_disruption_added_to_state(state, config):
    """After a disruption fires, the carrier must appear in
    state.carrier_disruptions with 'end_date' and 'severity' keys."""
    ev = _get_event_of_subtype(state, config, _BIZ_DAY, "disruption")

    carrier_code = ev.carrier_id
    assert carrier_code in state.carrier_disruptions

    disrupt = state.carrier_disruptions[carrier_code]
    assert "end_date" in disrupt
    assert "severity" in disrupt
    assert disrupt["severity"] in {"low", "medium", "high"}
    # end_date in state must match what was emitted
    assert disrupt["end_date"] == ev.end_date


# ---------------------------------------------------------------------------
# Test 6: active disruption prevents a second disruption for same carrier
# ---------------------------------------------------------------------------


def test_disruption_prevents_second_disruption(state, config):
    """Once a disruption is active for a carrier, no second disruption for
    that carrier is emitted — even if the probability roll would fire."""
    # Force a disruption for the first carrier
    first_carrier = state.carriers[0]
    state.carrier_disruptions[first_carrier.code] = {
        "end_date": (_BIZ_DAY + timedelta(days=3)).isoformat(),
        "severity": "high",
    }

    # Run many seeds and assert no second disruption fires for that carrier
    disruption_count_for_carrier = 0
    for seed in range(200):
        state.rng.seed(seed)
        # Do NOT clear carrier_disruptions — we want the block to stay active
        evts = carrier_events.run(state, config, _BIZ_DAY)
        for ev in evts:
            if ev.event_subtype == "disruption" and ev.carrier_id == first_carrier.code:
                disruption_count_for_carrier += 1

    assert disruption_count_for_carrier == 0, (
        f"Expected 0 second disruptions for {first_carrier.code}, "
        f"got {disruption_count_for_carrier}"
    )


# ---------------------------------------------------------------------------
# Test 7: disruption expiry
# ---------------------------------------------------------------------------


def test_disruption_expiry(state, config):
    """A disruption with end_date = yesterday must be expired at the start of
    run() so that state.carrier_disruptions is empty after the call."""
    first_carrier = state.carriers[0]
    yesterday = _BIZ_DAY - timedelta(days=1)
    state.carrier_disruptions[first_carrier.code] = {
        "end_date": yesterday.isoformat(),
        "severity": "medium",
    }
    assert first_carrier.code in state.carrier_disruptions  # pre-condition

    # Seed so no new disruption fires for this carrier (use a deterministic
    # seed; we only care that the old entry is gone)
    state.rng.seed(0)
    carrier_events.run(state, config, _BIZ_DAY)

    # The stale disruption must be gone
    assert first_carrier.code not in state.carrier_disruptions


# ---------------------------------------------------------------------------
# Test 8: rate_change fields
# ---------------------------------------------------------------------------


def test_rate_change_fields(state, config):
    """A rate_change event must have rate_delta_pct between 5 and 15 in
    absolute value, and sync_window must be '14'."""
    ev = _get_event_of_subtype(state, config, _BIZ_DAY, "rate_change")

    assert ev.event_subtype == "rate_change"
    assert ev.rate_delta_pct is not None
    assert 5.0 <= abs(ev.rate_delta_pct) <= 15.0
    assert ev.sync_window == "14"


# ---------------------------------------------------------------------------
# Test 9: rate_change sync_window always "14"
# ---------------------------------------------------------------------------


def test_rate_change_sync_window(state, config):
    """All rate_change events across multiple seeds must have sync_window='14'."""
    rate_change_events: list[CarrierEvent] = []

    for seed in range(300):
        state.carrier_disruptions.clear()
        state.rng.seed(seed)
        evts = carrier_events.run(state, config, _BIZ_DAY)
        rate_change_events.extend(e for e in evts if e.event_subtype == "rate_change")

    # Must have found at least some rate_change events
    assert len(rate_change_events) > 0, "No rate_change events produced in 300 seeds"

    for ev in rate_change_events:
        assert ev.sync_window == "14", (
            f"Expected sync_window='14' for rate_change, got {ev.sync_window!r}"
        )


# ---------------------------------------------------------------------------
# Test 10: all emitted events have event_type="carrier_event"
# ---------------------------------------------------------------------------


def test_event_type_string(state, config):
    """Every event emitted by the carrier_events engine must have
    event_type='carrier_event'."""
    # Accumulate events across several seeds to cover all subtypes
    all_seen: list[CarrierEvent] = []
    for seed in range(100):
        state.carrier_disruptions.clear()
        state.rng.seed(seed)
        all_seen.extend(carrier_events.run(state, config, _BIZ_DAY))

    assert len(all_seen) > 0, "Expected at least some events across 100 seeds"
    for ev in all_seen:
        assert ev.event_type == "carrier_event", (
            f"Unexpected event_type={ev.event_type!r} on event {ev.event_id}"
        )


# ---------------------------------------------------------------------------
# Test 11 (bonus): high-severity disruption degrades load_lifecycle reliability
# ---------------------------------------------------------------------------


def test_disruption_degrades_reliability(state, config):
    """An active high-severity disruption for a carrier reduces the reliability
    used in load_lifecycle transitions by 0.35.

    We verify this by patching _carrier_reliability to return a known value
    (0.80) and asserting that after a disruption is seeded, the net reliability
    used makes the transition fail when state.rng produces a value above the
    reduced threshold.
    """
    from datetime import timedelta

    from flowform.engines.load_lifecycle import _TRANSIT_DAYS  # noqa: F401

    carrier_code = "DHL"
    # Seed a high-severity disruption that is still active today
    state.carrier_disruptions[carrier_code] = {
        "end_date": (_BIZ_DAY + timedelta(days=2)).isoformat(),
        "severity": "high",
    }

    # Build a minimal load in in_transit state, due for OFD today
    load: dict = {
        "load_id": "LOAD-DISRUPT",
        "status": "in_transit",
        "carrier_code": carrier_code,
        "source_warehouse_id": "W01",
        "planned_date": _BIZ_DAY.isoformat(),
        "estimated_departure": _BIZ_DAY.isoformat(),
        "estimated_arrival": _BIZ_DAY.isoformat(),  # arrival reached
        "actual_departure": None,
        "actual_arrival": None,
        "delay_days": 0,
        "shipment_ids": ["SHP-D001"],
        "order_ids": [],
        "total_weight_kg": 50.0,
        "weight_unit": "kg",
        "total_weight_reported": 50.0,
        "sync_window": "14",
        "priority": "standard",
        "customer_ids": ["CUST-0001"],
        "events": [],
    }
    state.active_loads["LOAD-DISRUPT"] = load

    _RELIABILITY_PATCH = "flowform.engines.load_lifecycle._carrier_reliability"

    # Base reliability = 0.80; disruption penalty = 0.35 → net = 0.45.
    # If we force rng.random() to return 0.60, it's > 0.45, so transition fails.
    base_reliability = 0.80
    expected_net = base_reliability - 0.35  # 0.45

    # Use a real rng value slightly above the expected net — transition should fail
    rng_value_above_net = expected_net + 0.10  # 0.55

    with patch(_RELIABILITY_PATCH, return_value=base_reliability):
        with patch.object(state.rng, "random", return_value=rng_value_above_net):
            events = load_lifecycle.run(state, config, _BIZ_DAY)

    # Transition should be blocked (rng > net reliability)
    assert events == [], (
        f"Expected no transition (reliability reduced by disruption), "
        f"but got {len(events)} event(s)"
    )
    assert state.active_loads["LOAD-DISRUPT"]["status"] == "in_transit"
    assert state.active_loads["LOAD-DISRUPT"]["delay_days"] == 1
