"""Tests for the supply disruptions engine (Step 46)."""

from __future__ import annotations

import re
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from flowform.engines.supply_disruptions import (
    SupplyDisruptionEvent,
    get_active_production_severity,
    run,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BUSINESS_DAY = date(2026, 1, 5)   # Monday, not a holiday
_SATURDAY = date(2026, 1, 3)
_POLISH_HOLIDAY = date(2026, 1, 1)  # New Year's Day


def _make_config(enabled: bool = True) -> MagicMock:
    cfg = MagicMock()
    cfg.disruptions.enabled = enabled
    return cfg


def _make_state(seed: int = 0) -> MagicMock:
    import random
    state = MagicMock()
    state.rng = random.Random(seed)
    state.counters = {"disruption": 0}
    state.supply_disruptions = {}
    # Make hasattr return True for supply_disruptions
    type(state).supply_disruptions = MagicMock()
    state.supply_disruptions = {}
    return state


def _run_until_events(seed_start: int = 0, max_seeds: int = 200) -> tuple[MagicMock, list]:
    """Find a seed that produces at least one disruption event on the business day."""
    for seed in range(seed_start, seed_start + max_seeds):
        state = _make_state(seed)
        config = _make_config(enabled=True)
        events = run(state, config, _BUSINESS_DAY)
        if events:
            return state, events
    raise RuntimeError(f"No events produced in seeds {seed_start}–{seed_start + max_seeds}")


# Cached for reuse
_SEED_WITH_EVENTS: int | None = None
_EVENTS_FOR_SEED: list | None = None


def _get_events() -> tuple[int, list]:
    global _SEED_WITH_EVENTS, _EVENTS_FOR_SEED
    if _SEED_WITH_EVENTS is None:
        for seed in range(200):
            state = _make_state(seed)
            config = _make_config(enabled=True)
            events = run(state, config, _BUSINESS_DAY)
            if events:
                _SEED_WITH_EVENTS = seed
                _EVENTS_FOR_SEED = events
                break
    assert _SEED_WITH_EVENTS is not None, "No seed produced events"
    return _SEED_WITH_EVENTS, _EVENTS_FOR_SEED  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_returns_empty_when_disabled() -> None:
    """config.disruptions.enabled=False always returns []."""
    state = _make_state(0)
    config = _make_config(enabled=False)
    for day_offset in range(10):
        d = _BUSINESS_DAY + timedelta(days=day_offset)
        assert run(state, config, d) == []


def test_returns_empty_on_weekend() -> None:
    """Enabled but Saturday → []."""
    state = _make_state(0)
    config = _make_config(enabled=True)
    result = run(state, config, _SATURDAY)
    assert result == []


def test_returns_empty_on_holiday() -> None:
    """Enabled but Polish public holiday → []."""
    state = _make_state(0)
    config = _make_config(enabled=True)
    result = run(state, config, _POLISH_HOLIDAY)
    assert result == []


def test_event_schema_valid() -> None:
    """Events produced on a business day pass Pydantic validation."""
    _, events = _get_events()
    for ev in events:
        assert isinstance(ev, SupplyDisruptionEvent)
        # Pydantic validates on construction; just check the type
        dumped = ev.model_dump()
        SupplyDisruptionEvent(**dumped)


def test_event_type_is_supply_disruption() -> None:
    """All events have event_type == 'supply_disruption'."""
    _, events = _get_events()
    for ev in events:
        assert ev.event_type == "supply_disruption"


def test_disruption_id_format() -> None:
    """disruption_id matches ^DSRP-\\d{8}-\\d{4}$."""
    pattern = re.compile(r"^DSRP-\d{8}-\d{4}$")
    _, events = _get_events()
    for ev in events:
        assert pattern.match(ev.event_id), f"Bad event_id: {ev.event_id}"


def test_severity_values() -> None:
    """severity is one of 'low', 'medium', 'high'."""
    valid = {"low", "medium", "high"}
    _, events = _get_events()
    for ev in events:
        assert ev.severity in valid, f"Bad severity: {ev.severity}"


def test_duration_range() -> None:
    """1 <= duration_days <= 5."""
    _, events = _get_events()
    for ev in events:
        assert 1 <= ev.duration_days <= 5, f"Bad duration: {ev.duration_days}"


def test_end_date_consistent() -> None:
    """end_date == start_date + timedelta(duration_days - 1)."""
    _, events = _get_events()
    for ev in events:
        start = date.fromisoformat(ev.start_date)
        end = date.fromisoformat(ev.end_date)
        expected_end = start + timedelta(days=ev.duration_days - 1)
        assert end == expected_end, f"end_date mismatch for {ev.event_id}"


def test_counter_increments() -> None:
    """Counter increments by len(events) after run."""
    state = _make_state(0)
    state.supply_disruptions = {}
    config = _make_config(enabled=True)

    # Run until we get at least one event
    initial_counter = state.counters.get("disruption", 0)
    events = run(state, config, _BUSINESS_DAY)

    assert state.counters["disruption"] == initial_counter + len(events)


def test_state_persisted() -> None:
    """Active disruptions are stored in state.supply_disruptions after run."""
    seed, events = _get_events()
    # Recreate state with same seed to reproduce
    state = _make_state(seed)
    state.supply_disruptions = {}
    config = _make_config(enabled=True)

    produced = run(state, config, _BUSINESS_DAY)
    assert len(produced) > 0
    assert len(state.supply_disruptions) == len(produced)
    for ev in produced:
        assert ev.event_id in state.supply_disruptions


def test_expiry_removes_stale() -> None:
    """Manually add a disruption with end_date in the past; run again; stale entry removed."""
    state = _make_state(99)
    state.supply_disruptions = {}
    config = _make_config(enabled=True)

    # Inject a stale disruption
    stale_id = "DSRP-20250101-0001"
    state.supply_disruptions[stale_id] = {
        "event_id": stale_id,
        "start_date": "2025-01-01",
        "end_date": "2025-01-03",  # well in the past
        "severity": "low",
        "disruption_category": "production",
        "disruption_subtype": "line_stoppage",
        "duration_days": 3,
        "affected_scope": "W01",
        "message": "Old disruption",
    }

    run(state, config, _BUSINESS_DAY)

    assert stale_id not in state.supply_disruptions, "Stale disruption was not removed"


def test_reproducibility() -> None:
    """Same seed → same events on same date."""
    seed = 42
    config = _make_config(enabled=True)

    state_a = _make_state(seed)
    state_a.supply_disruptions = {}
    events_a = run(state_a, config, _BUSINESS_DAY)

    state_b = _make_state(seed)
    state_b.supply_disruptions = {}
    events_b = run(state_b, config, _BUSINESS_DAY)

    assert len(events_a) == len(events_b)
    for ea, eb in zip(events_a, events_b):
        assert ea.model_dump() == eb.model_dump()


def test_get_active_production_severity_no_disruptions() -> None:
    """Returns None when there are no production disruptions."""
    state = _make_state(0)
    state.supply_disruptions = {}
    result = get_active_production_severity(state, _BUSINESS_DAY)
    assert result is None


def test_get_active_production_severity_returns_highest() -> None:
    """Returns the highest severity among active production disruptions."""
    state = _make_state(0)
    state.supply_disruptions = {
        "DSRP-20260105-0001": {
            "disruption_category": "production",
            "severity": "low",
            "start_date": "2026-01-05",
            "end_date": "2026-01-07",
        },
        "DSRP-20260105-0002": {
            "disruption_category": "production",
            "severity": "high",
            "start_date": "2026-01-05",
            "end_date": "2026-01-06",
        },
        "DSRP-20260105-0003": {
            "disruption_category": "warehouse",
            "severity": "high",
            "start_date": "2026-01-05",
            "end_date": "2026-01-06",
        },
    }
    result = get_active_production_severity(state, _BUSINESS_DAY)
    assert result == "high"
