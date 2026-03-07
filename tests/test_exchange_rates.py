"""Tests for the exchange rate engine (Phase 2, Step 14)."""

from __future__ import annotations

import random
from datetime import date, timedelta
from pathlib import Path

import pytest

from flowform.config import load_config
from flowform.engines.exchange_rates import ExchangeRateEvent, run
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


# ---------------------------------------------------------------------------
# Test 1: run() returns exactly two events, one per currency pair
# ---------------------------------------------------------------------------


def test_run_returns_two_events(state, config):
    result = run(state, config, date(2026, 1, 5))

    assert len(result) == 2, "Expected exactly 2 events (EUR/PLN and USD/PLN)"

    pairs = {e.currency_pair for e in result}
    assert pairs == {"EUR/PLN", "USD/PLN"}


# ---------------------------------------------------------------------------
# Test 2: event schema fields — correct types and values
# ---------------------------------------------------------------------------


def test_event_schema_fields(state, config):
    sim_date = date(2026, 1, 5)
    events = run(state, config, sim_date)

    for evt in events:
        assert isinstance(evt, ExchangeRateEvent)
        assert evt.event_type == "exchange_rate"

        # event_id must be a non-empty string (UUID4)
        assert isinstance(evt.event_id, str)
        assert len(evt.event_id) == 36  # UUID canonical form: 8-4-4-4-12

        # simulation_date must be the ISO string of the date we passed
        assert evt.simulation_date == sim_date.isoformat()

        # Numeric fields must be float-compatible and positive
        assert isinstance(evt.rate, float)
        assert isinstance(evt.previous_rate, float)
        assert isinstance(evt.change_pct, float)
        assert evt.rate > 0
        assert evt.previous_rate > 0

        # rate should be rounded to 4 decimal places
        assert evt.rate == round(evt.rate, 4)
        assert evt.previous_rate == round(evt.previous_rate, 4)
        assert evt.change_pct == round(evt.change_pct, 4)


# ---------------------------------------------------------------------------
# Test 3: state.exchange_rates is updated after run()
# ---------------------------------------------------------------------------


def test_state_updated(state, config):
    initial_eur = state.exchange_rates["EUR"]
    initial_usd = state.exchange_rates["USD"]

    events = run(state, config, date(2026, 1, 5))

    # State should now hold the values from the events
    eur_event = next(e for e in events if e.currency_pair == "EUR/PLN")
    usd_event = next(e for e in events if e.currency_pair == "USD/PLN")

    assert state.exchange_rates["EUR"] == eur_event.rate
    assert state.exchange_rates["USD"] == usd_event.rate

    # previous_rate on the event must match the pre-call state value
    assert eur_event.previous_rate == round(initial_eur, 4)
    assert usd_event.previous_rate == round(initial_usd, 4)


# ---------------------------------------------------------------------------
# Test 4: mean reversion — start extreme, converge toward anchor after 200 days
# ---------------------------------------------------------------------------


def test_mean_reversion(config, tmp_path: Path):
    """Starting from EUR=5.50, after 200 days the rate should be closer to 4.30."""
    state = SimulationState.from_new(config, db_path=tmp_path / "sim_mr.db")

    # Force an extreme starting rate
    state.exchange_rates["EUR"] = 5.50

    base_date = date(2026, 1, 5)
    for i in range(200):
        run(state, config, base_date + timedelta(days=i))

    final_eur = state.exchange_rates["EUR"]
    anchor = config.exchange_rates.base.EUR_PLN  # 4.30

    distance_from_anchor = abs(final_eur - anchor)
    distance_from_start = abs(5.50 - anchor)

    assert distance_from_anchor < distance_from_start, (
        f"After 200 days, EUR/PLN ({final_eur:.4f}) should be closer to anchor "
        f"({anchor}) than starting value 5.50 was "
        f"(gap now {distance_from_anchor:.4f}, was {distance_from_start:.4f})"
    )


# ---------------------------------------------------------------------------
# Test 5: random walk bounded — daily change never exceeds ±1% over 100 days
# ---------------------------------------------------------------------------


def test_random_walk_bounded(config, tmp_path: Path):
    """Over 100 days, no single daily change should exceed ±1%."""
    state = SimulationState.from_new(config, db_path=tmp_path / "sim_bound.db")

    base_date = date(2026, 1, 5)
    for i in range(100):
        sim_date = base_date + timedelta(days=i)
        events = run(state, config, sim_date)

        for evt in events:
            assert abs(evt.change_pct) <= 1.0, (
                f"Daily change for {evt.currency_pair} on {sim_date} exceeded ±1%: "
                f"{evt.change_pct:.4f}%"
            )


# ---------------------------------------------------------------------------
# Test 6: reproducibility — same seed produces identical rate sequences
# ---------------------------------------------------------------------------


def test_reproducible_with_same_seed(config, tmp_path: Path):
    """Two states initialised with the same seed must produce identical rates."""
    state_a = SimulationState.from_new(config, db_path=tmp_path / "sim_a.db")
    state_b = SimulationState.from_new(config, db_path=tmp_path / "sim_b.db")

    base_date = date(2026, 1, 5)
    rates_a: list[tuple[str, float]] = []
    rates_b: list[tuple[str, float]] = []

    for i in range(30):
        sim_date = base_date + timedelta(days=i)

        events_a = run(state_a, config, sim_date)
        events_b = run(state_b, config, sim_date)

        for evt in events_a:
            rates_a.append((evt.currency_pair, evt.rate))
        for evt in events_b:
            rates_b.append((evt.currency_pair, evt.rate))

    assert rates_a == rates_b, (
        "Rate sequences differed despite identical seeds — "
        "randomness is not flowing through state.rng consistently."
    )
