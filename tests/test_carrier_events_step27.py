"""Tests for Step 27 additions to the carrier events engine.

Covers the two new event subtypes:
- maintenance_window: ~1/30 per carrier per business day, sync_window="08", no state
- holiday_schedule: fires for ALL carriers on the business day before a Polish
  public holiday, deterministic, sync_window="14", no state change

Key test dates:
- _BIZ_DAY_PRE_HOLIDAY = date(2026, 4, 30): Thursday before Labour Day (May 1 2026)
- _ORDINARY_BIZ_DAY = date(2026, 3, 9): Monday, no holiday the next day
- _SATURDAY = date(2026, 4, 4): Saturday, used to verify the weekend guard
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from flowform.config import load_config
from flowform.engines import carrier_events
from flowform.engines.carrier_events import (
    CarrierEvent,
    _generate_maintenance_window,
)
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"

# April 30 2026 is a Thursday; May 1 2026 (Labour Day) is a Polish public holiday.
_BIZ_DAY_PRE_HOLIDAY = date(2026, 4, 30)

# March 9 2026 is a Monday; March 10 is a Tuesday — no public holiday.
_ORDINARY_BIZ_DAY = date(2026, 3, 9)

# April 4 2026 is a Saturday — used for the weekend guard test.
_SATURDAY = date(2026, 4, 4)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def config():
    return load_config(_CONFIG_PATH)


@pytest.fixture()
def state(config, tmp_path):
    """Fresh simulation state isolated in a temp directory."""
    db = tmp_path / "sim.db"
    return SimulationState.from_new(config, db_path=db)


# ---------------------------------------------------------------------------
# Helper: iterate seeds until a maintenance_window event appears
# ---------------------------------------------------------------------------


def _get_maintenance_event(
    state: SimulationState,
    config,
    sim_date: date,
    max_attempts: int = 500,
) -> CarrierEvent:
    for seed in range(max_attempts):
        state.carrier_disruptions.clear()
        state.rng.seed(seed)
        events = carrier_events.run(state, config, sim_date)
        for ev in events:
            if ev.event_subtype == "maintenance_window":
                return ev
    raise RuntimeError(
        f"Could not produce a 'maintenance_window' event in {max_attempts} seeds."
    )


# ---------------------------------------------------------------------------
# T1: maintenance_window field values
# ---------------------------------------------------------------------------


def test_maintenance_window_fields(state, config):
    """_generate_maintenance_window() returns an event with correct static fields."""
    carrier = state.carriers[0]
    ev = _generate_maintenance_window(
        state, carrier.code, carrier.name, _ORDINARY_BIZ_DAY
    )

    assert ev.event_type == "carrier_event"
    assert ev.event_subtype == "maintenance_window"
    assert ev.impact_severity == "low"
    assert ev.affected_region is None
    assert ev.sync_window == "08"
    assert ev.duration_days == 0
    assert ev.end_date is None
    assert ev.rate_delta_pct is None
    assert ev.start_date == _ORDINARY_BIZ_DAY.isoformat()
    assert ev.simulation_date == _ORDINARY_BIZ_DAY.isoformat()
    assert ev.timestamp == f"{_ORDINARY_BIZ_DAY.isoformat()}T08:00:00Z"
    # Message must contain carrier name and the date
    assert carrier.name in ev.message
    assert _ORDINARY_BIZ_DAY.isoformat() in ev.message


# ---------------------------------------------------------------------------
# T2: maintenance_window message contains a recognisable time window
# ---------------------------------------------------------------------------


def test_maintenance_window_message_contains_time_window(state, config):
    """The maintenance_window message must reference one of the three time windows."""
    carrier = state.carriers[0]
    possible_markers = ["06:00", "10:00", "14:00", "18:00"]

    # Call several times; at least one invocation must mention a time marker.
    found = False
    for seed in range(20):
        state.rng.seed(seed)
        ev = _generate_maintenance_window(
            state, carrier.code, carrier.name, _ORDINARY_BIZ_DAY
        )
        if any(m in ev.message for m in possible_markers):
            found = True
            break

    assert found, (
        "Expected at least one maintenance_window message to contain a time marker "
        f"from {possible_markers}"
    )


# ---------------------------------------------------------------------------
# T3: maintenance_window does NOT update state.carrier_disruptions
# ---------------------------------------------------------------------------


def test_maintenance_window_does_not_update_disruptions(state, config):
    """Running run() and producing maintenance_window events must not alter
    state.carrier_disruptions."""
    # Run enough seeds to likely produce at least one maintenance_window
    for seed in range(100):
        state.carrier_disruptions.clear()
        state.rng.seed(seed)
        events = carrier_events.run(state, config, _ORDINARY_BIZ_DAY)
        maint_events = [e for e in events if e.event_subtype == "maintenance_window"]
        if maint_events:
            # Only maintenance events happened (disruptions may also have fired,
            # but maintenance itself must not add entries).
            for ev in maint_events:
                # If the carrier is now in disruptions, it must be from a
                # service_disruption event, not from maintenance.
                carrier_code = ev.carrier_code
                if carrier_code in state.carrier_disruptions:
                    disruption_codes_from_today = {
                        e.carrier_code
                        for e in events
                        if e.event_subtype == "service_disruption"
                    }
                    assert carrier_code in disruption_codes_from_today, (
                        f"{carrier_code} found in carrier_disruptions but no "
                        "service_disruption was emitted — maintenance_window must "
                        "have incorrectly added it."
                    )
            break  # tested at least once


# ---------------------------------------------------------------------------
# T4: maintenance_window is deterministic for the same seed
# ---------------------------------------------------------------------------


def test_maintenance_window_uses_seed_for_time_window(state, config):
    """The same RNG seed must produce the same time window in the message."""
    carrier = state.carriers[0]

    state.rng.seed(42)
    ev1 = _generate_maintenance_window(
        state, carrier.code, carrier.name, _ORDINARY_BIZ_DAY
    )

    state.rng.seed(42)
    ev2 = _generate_maintenance_window(
        state, carrier.code, carrier.name, _ORDINARY_BIZ_DAY
    )

    assert ev1.message == ev2.message, (
        "Same RNG seed must produce an identical maintenance_window message."
    )


# ---------------------------------------------------------------------------
# T5: holiday_schedule fires for ALL carriers on the pre-holiday business day
# ---------------------------------------------------------------------------


def test_holiday_schedule_fires_for_all_carriers_on_pre_holiday_day(state, config):
    """run() on April 30 2026 (day before Labour Day) must emit exactly one
    holiday_schedule event per carrier."""
    state.rng.seed(0)
    events = carrier_events.run(state, config, _BIZ_DAY_PRE_HOLIDAY)

    holiday_events = [e for e in events if e.event_subtype == "holiday_schedule"]
    assert len(holiday_events) == len(state.carriers), (
        f"Expected {len(state.carriers)} holiday_schedule events, "
        f"got {len(holiday_events)}"
    )

    # Each carrier must appear exactly once
    carrier_codes_seen = {e.carrier_code for e in holiday_events}
    expected_codes = {c.code for c in state.carriers}
    assert carrier_codes_seen == expected_codes


# ---------------------------------------------------------------------------
# T6: holiday_schedule does NOT fire on an ordinary business day
# ---------------------------------------------------------------------------


def test_holiday_schedule_does_not_fire_on_ordinary_business_day(state, config):
    """run() on March 9 2026 (no holiday the next day) must produce zero
    holiday_schedule events."""
    state.rng.seed(0)
    events = carrier_events.run(state, config, _ORDINARY_BIZ_DAY)

    holiday_events = [e for e in events if e.event_subtype == "holiday_schedule"]
    assert holiday_events == [], (
        f"Expected no holiday_schedule events on {_ORDINARY_BIZ_DAY}, "
        f"got {len(holiday_events)}"
    )


# ---------------------------------------------------------------------------
# T7: holiday_schedule event field values
# ---------------------------------------------------------------------------


def test_holiday_schedule_event_fields(state, config):
    """A holiday_schedule event emitted on April 30 2026 must have the correct
    static field values."""
    state.rng.seed(0)
    events = carrier_events.run(state, config, _BIZ_DAY_PRE_HOLIDAY)

    holiday_events = [e for e in events if e.event_subtype == "holiday_schedule"]
    assert holiday_events, "Expected at least one holiday_schedule event."

    ev = holiday_events[0]
    assert ev.event_type == "carrier_event"
    assert ev.event_subtype == "holiday_schedule"
    assert ev.impact_severity == "medium"
    assert ev.affected_region is None
    assert ev.sync_window == "14"
    assert ev.duration_days == 1
    assert ev.end_date == "2026-05-01"
    assert ev.rate_delta_pct is None
    assert ev.start_date == "2026-04-30"
    assert ev.simulation_date == "2026-04-30"
    assert ev.timestamp == "2026-04-30T14:00:00Z"
    assert "2026-05-01" in ev.message


# ---------------------------------------------------------------------------
# T8: holiday_schedule does NOT update state.carrier_disruptions
# ---------------------------------------------------------------------------


def test_holiday_schedule_does_not_update_disruptions(state, config):
    """run() on April 30 2026 must not populate state.carrier_disruptions via
    holiday_schedule events."""
    state.rng.seed(0)
    events = carrier_events.run(state, config, _BIZ_DAY_PRE_HOLIDAY)

    # Any disruption codes in state must come from service_disruption events only
    disruption_event_codes = {
        e.carrier_code
        for e in events
        if e.event_subtype == "service_disruption"
    }
    for code in state.carrier_disruptions:
        assert code in disruption_event_codes, (
            f"Carrier {code!r} in state.carrier_disruptions but no service_disruption "
            "was emitted — holiday_schedule must not have added it."
        )


# ---------------------------------------------------------------------------
# T9 (bonus): no events on a weekend even if the next day happens to be a holiday
# ---------------------------------------------------------------------------


def test_no_events_on_weekend_even_before_holiday(state, config):
    """run() on a Saturday must return [] even when Sunday is a Polish public
    holiday (e.g. if May 3 Constitution Day falls on a Sunday the sim-date
    Saturday still returns nothing).

    We use April 4 2026 (Saturday).  April 5 is a Sunday, not a holiday, but
    the weekend guard is what matters — the engine must return [] regardless.
    """
    state.rng.seed(0)
    events = carrier_events.run(state, config, _SATURDAY)
    assert events == [], (
        f"Expected no events on Saturday {_SATURDAY}, got {len(events)} event(s)."
    )


# ---------------------------------------------------------------------------
# T10: holiday_schedule fires on Friday before Easter Monday (Monday holiday)
# ---------------------------------------------------------------------------


def test_holiday_schedule_fires_for_monday_holiday(state, config):
    """Friday 2026-04-03 is the last business day before Easter Monday 2026-04-06.
    The engine must emit one holiday_schedule event per carrier with end_date='2026-04-06'.
    """
    friday_before_easter = date(2026, 4, 3)
    events = carrier_events.run(state, config, friday_before_easter)
    holiday_events = [
        e for e in events
        if hasattr(e, "event_subtype") and e.event_subtype == "holiday_schedule"
    ]
    assert len(holiday_events) == len(state.carriers)
    for ev in holiday_events:
        assert ev.end_date == "2026-04-06"
