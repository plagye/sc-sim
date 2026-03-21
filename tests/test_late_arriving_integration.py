"""Integration tests for late-arriving data fix (C1 — plural event type).

Two tests:
1. test_erp_filter_matches_plural_event_type — verifies the fixed filter picks up
   inventory_movements (plural) events for deferral.
2. test_erp_deferred_appears_next_day — verifies deferred movements from day 1
   surface in day 2's output.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from flowform.config import load_config
from flowform.engines.late_arriving import apply
from flowform.state import SimulationState


_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"

# Tuesday and Wednesday — both business days, no Polish holidays
_DAY1 = date(2026, 3, 10)
_DAY2 = date(2026, 3, 11)


@pytest.fixture
def config():
    return load_config(_CONFIG_PATH)


@pytest.fixture
def state(config, tmp_path: Path) -> SimulationState:
    return SimulationState.from_new(config, db_path=tmp_path / "sim.db")


def _make_inventory_movements_event(sim_date: date, event_id: str = "evt-inv-001") -> dict[str, Any]:
    """Return a minimal inventory_movements event (plural — the real event_type)."""
    return {
        "event_type": "inventory_movements",
        "event_id": event_id,
        "movement_id": f"MOV-{event_id}",
        "movement_type": "receipt",
        "sku": "FF-GA15-CS-PN16-FL-MA",
        "warehouse_id": "W01",
        "quantity": 10,
        "simulation_date": sim_date.isoformat(),
        "timestamp": f"{sim_date.isoformat()}T17:00:00Z",
    }


def _make_exchange_rate_event(sim_date: date) -> dict[str, Any]:
    return {
        "event_type": "exchange_rate",
        "event_id": "evt-fx-001",
        "simulation_date": sim_date.isoformat(),
        "currency_pair": "EUR/PLN",
        "rate": 4.30,
    }


# ---------------------------------------------------------------------------
# Test 1
# ---------------------------------------------------------------------------

def test_erp_filter_matches_plural_event_type(state: SimulationState, config) -> None:
    """apply() defers inventory_movements (plural) events — the fixed filter.

    We force rng to always return 0.0 so every eligible event is deferred.
    After apply(), state.deferred_erp_movements must be non-empty.
    """
    # Force rng to always defer (return 0.0 < erp_defer_probability)
    original_random = state.rng.random
    state.rng.random = lambda: 0.0  # type: ignore[method-assign]

    try:
        events = [_make_inventory_movements_event(_DAY1, f"evt-inv-{i:03d}") for i in range(5)]
        result = apply(events, state, config, _DAY1)
    finally:
        state.rng.random = original_random  # type: ignore[method-assign]

    # With rng always 0.0, all 5 events should be deferred
    assert len(state.deferred_erp_movements) == 5, (
        f"Expected 5 deferred events, got {len(state.deferred_erp_movements)}. "
        "If 0, the plural filter fix is not working."
    )
    # Result should contain no inventory_movements events (all deferred)
    remaining = [e for e in result if isinstance(e, dict) and e.get("event_type") == "inventory_movements"]
    assert len(remaining) == 0, (
        f"Expected 0 remaining inventory_movements in result, got {len(remaining)}"
    )


# ---------------------------------------------------------------------------
# Test 2
# ---------------------------------------------------------------------------

def test_erp_deferred_appears_next_day(state: SimulationState, config) -> None:
    """Deferred inventory_movements from day 1 appear in day 2's output.

    Day 1: force deferral of one inventory_movements event.
    Day 2: run apply() with only an exchange_rate event.
             The deferred movement from day 1 should be prepended.
    """
    # --- Day 1: force deferral ---
    original_random = state.rng.random
    state.rng.random = lambda: 0.0  # type: ignore[method-assign]

    inv_event = _make_inventory_movements_event(_DAY1, "evt-inv-day1")
    fx_event = _make_exchange_rate_event(_DAY1)

    try:
        _result_day1 = apply([inv_event, fx_event], state, config, _DAY1)
    finally:
        state.rng.random = original_random  # type: ignore[method-assign]

    # Confirm deferral happened
    assert len(state.deferred_erp_movements) >= 1, (
        "No events were deferred on day 1 — cannot test day 2 injection"
    )
    deferred_count = len(state.deferred_erp_movements)

    # --- Day 2: normal rng, feed only an exchange_rate event ---
    result_day2 = apply([_make_exchange_rate_event(_DAY2)], state, config, _DAY2)

    # Deferred inventory_movements from day 1 should appear in day 2's output
    injected = [
        e for e in result_day2
        if isinstance(e, dict)
        and e.get("event_type") == "inventory_movements"
        and e.get("simulation_date") == _DAY1.isoformat()
    ]
    assert len(injected) == deferred_count, (
        f"Expected {deferred_count} deferred movements in day 2 output, got {len(injected)}"
    )

    # Deferred queue should be cleared (day 1 events consumed)
    day1_still_in_deferred = [
        e for e in state.deferred_erp_movements
        if e.get("simulation_date") == _DAY1.isoformat()
    ]
    assert day1_still_in_deferred == [], (
        "Day 1 deferred events were not cleared after day 2 injection"
    )
