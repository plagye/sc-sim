"""Exchange rate simulation engine for FlowForm Industries.

Generates daily EUR/PLN and USD/PLN exchange rates using a mean-reverting
random walk model.  Runs every day, including weekends.

Model:
    new_rate = rate + reversion_pull + random_noise
    reversion_pull = 0.10 * (anchor - rate)
    random_noise   = rate * rng.uniform(-0.003, 0.003)
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Literal

from pydantic import BaseModel

from flowform.config import Config
from flowform.state import SimulationState


# ---------------------------------------------------------------------------
# Event schema
# ---------------------------------------------------------------------------


class ExchangeRateEvent(BaseModel):
    """A single currency-pair rate observation for one simulation date."""

    event_type: Literal["exchange_rate"]
    event_id: str
    simulation_date: str
    currency_pair: str
    rate: float
    previous_rate: float
    change_pct: float


# ---------------------------------------------------------------------------
# Engine constants
# ---------------------------------------------------------------------------

#: Ordered list of (state_key, currency_pair_label) so iteration order is
#: deterministic and reproducible.
_CURRENCIES: list[tuple[str, str]] = [
    ("EUR", "EUR/PLN"),
    ("USD", "USD/PLN"),
]

_REVERSION_STRENGTH: float = 0.10
_VOLATILITY: float = 0.003  # ±0.3 % per day


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _step_rate(
    current_rate: float,
    anchor: float,
    rng: object,  # random.Random — typed as object to avoid circular import risk
) -> float:
    """Apply one day of mean-reverting random walk and return the new rate.

    Args:
        current_rate: Today's opening rate.
        anchor:       Long-run equilibrium rate (mean-reversion target).
        rng:          Seeded random.Random instance from SimulationState.

    Returns:
        New rate after applying reversion pull and random noise.
    """
    import random as _random  # local import keeps module-level clean

    assert isinstance(rng, _random.Random)

    reversion_pull = _REVERSION_STRENGTH * (anchor - current_rate)
    random_noise = current_rate * rng.uniform(-_VOLATILITY, _VOLATILITY)
    return current_rate + reversion_pull + random_noise


# ---------------------------------------------------------------------------
# Engine entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[ExchangeRateEvent]:
    """Generate exchange rate events for *sim_date* and update state in place.

    Produces exactly two :class:`ExchangeRateEvent` instances — one for
    EUR/PLN and one for USD/PLN.  Intended to run every calendar day,
    including weekends and public holidays.

    Args:
        state:    Mutable simulation state.  ``state.exchange_rates`` is
                  updated in place with the new rates.
        config:   Validated simulation config; anchors are read from
                  ``config.exchange_rates.base``.
        sim_date: Current simulation date (used as ``simulation_date`` on the
                  emitted events).

    Returns:
        List of two :class:`ExchangeRateEvent` objects.
    """
    anchors: dict[str, float] = {
        "EUR": config.exchange_rates.base.EUR_PLN,
        "USD": config.exchange_rates.base.USD_PLN,
    }

    events: list[ExchangeRateEvent] = []

    for currency_key, pair_label in _CURRENCIES:
        anchor = anchors[currency_key]
        previous_rate = state.exchange_rates[currency_key]

        raw_new_rate = _step_rate(previous_rate, anchor, state.rng)
        new_rate = round(raw_new_rate, 4)
        prev_rounded = round(previous_rate, 4)
        change_pct = round((new_rate - prev_rounded) / prev_rounded * 100, 4)

        # Update state in place
        state.exchange_rates[currency_key] = new_rate

        events.append(
            ExchangeRateEvent(
                event_type="exchange_rate",
                event_id=str(uuid.uuid4()),
                simulation_date=sim_date.isoformat(),
                currency_pair=pair_label,
                rate=new_rate,
                previous_rate=prev_rounded,
                change_pct=change_pct,
            )
        )

    return events
