"""Demand signal generator: external and internal demand indicators.

Generates DemandSignalEvent objects on business days, routing to ADM
output under the demand_signals event type.

Each simulated business day runs 10 independent Bernoulli trials using
config.demand.signal_probability_daily. Signals that fire are fully
populated from weighted/uniform distributions seeded via state.rng.
"""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel

from flowform.calendar import is_business_day
from flowform.config import Config
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SIGNAL_SOURCES: list[str] = [
    "customer_communication",
    "market_intelligence",
    "seasonal_model",
    "consumption_trend",
    "contract_renewal",
    "competitor_event",
]

SOURCE_WEIGHTS: list[int] = [15, 10, 30, 25, 10, 10]

# Sources that have no associated customer
_SOURCELESS_SOURCES: frozenset[str] = frozenset(
    {"market_intelligence", "competitor_event"}
)

SIGNAL_TYPES: list[str] = [
    "upside_risk",
    "downside_risk",
    "timing_shift",
    "mix_change",
    "new_demand",
]

_DESCRIPTIONS: dict[str, str] = {
    "upside_risk": (
        "Positive demand indicator suggests higher-than-planned volumes ahead."
    ),
    "downside_risk": (
        "Market signal indicates potential demand softening in the near term."
    ),
    "timing_shift": (
        "Delivery timing expected to shift based on customer project schedule."
    ),
    "mix_change": "Product mix shift anticipated toward this product family.",
    "new_demand": "New demand source identified for this product group.",
}

# ---------------------------------------------------------------------------
# Event schema
# ---------------------------------------------------------------------------


class DemandSignalEvent(BaseModel):
    """A single demand signal event emitted by the ADM system."""

    event_type: Literal["demand_signal"] = "demand_signal"
    signal_id: str
    timestamp: str
    simulation_date: str
    signal_source: str
    customer_id: str | None
    product_group: str
    signal_type: str
    magnitude: str
    horizon_weeks: int
    description: str
    confidence: float


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_product_group(sku: str) -> str:
    """Derive a product-group string from a catalog SKU.

    SKU format: ``FF-{TYPE}{DN}-{MAT}-{PN}-{CONN}{ACT}``
    E.g. ``FF-BL100-S316-PN16-FLEL`` → ``BL-DN100-S316``

    Args:
        sku: A catalog SKU string.

    Returns:
        Product group string in the form ``{type_code}-DN{dn}-{mat}``.
    """
    parts = sku.split("-")
    # parts[1] is like "BL100" or "GA15"
    type_dn = parts[1]
    type_code = type_dn[:2]
    dn = type_dn[2:]
    mat = parts[2]
    return f"{type_code}-DN{dn}-{mat}"


def _generate_signal(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> DemandSignalEvent:
    """Build one DemandSignalEvent using state.rng for all randomness.

    Args:
        state:    Mutable simulation state (provides rng and catalogs).
        config:   Validated simulation config (not currently needed here).
        sim_date: The current simulated date.

    Returns:
        A fully populated DemandSignalEvent.
    """
    # Signal counter
    if "signal" not in state.counters:
        state.counters["signal"] = 0
    state.counters["signal"] += 1
    signal_id = f"SIG-{sim_date.strftime('%Y%m%d')}-{state.counters['signal']:04d}"

    # Source and customer
    signal_source: str = state.rng.choices(
        SIGNAL_SOURCES, weights=SOURCE_WEIGHTS, k=1
    )[0]
    if signal_source in _SOURCELESS_SOURCES:
        customer_id: str | None = None
    else:
        customer_id = state.rng.choice(state.customers).customer_id

    # Product group from a random catalog SKU
    sku: str = state.rng.choice(state.catalog).sku
    product_group = _parse_product_group(sku)

    # Signal attributes
    signal_type: str = state.rng.choice(SIGNAL_TYPES)
    magnitude: str = state.rng.choice(["low", "medium", "high"])
    confidence: float = round(state.rng.uniform(0.1, 0.9), 1)
    horizon_weeks: int = state.rng.randint(2, 26)

    description = (
        f"{_DESCRIPTIONS[signal_type]} "
        f"Source: {signal_source.replace('_', ' ')}."
    )

    return DemandSignalEvent(
        signal_id=signal_id,
        timestamp=f"{sim_date.isoformat()}T08:00:00Z",
        simulation_date=sim_date.isoformat(),
        signal_source=signal_source,
        customer_id=customer_id,
        product_group=product_group,
        signal_type=signal_type,
        magnitude=magnitude,
        horizon_weeks=horizon_weeks,
        description=description,
        confidence=confidence,
    )


# ---------------------------------------------------------------------------
# Public engine entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[DemandSignalEvent]:
    """Generate demand signals for *sim_date*.

    Business days only. Runs 10 independent Bernoulli trials at
    ``config.demand.signal_probability_daily`` each.

    Args:
        state:    Mutable simulation state.
        config:   Validated simulation config.
        sim_date: The calendar date being simulated.

    Returns:
        A (possibly empty) list of DemandSignalEvent objects.
    """
    if not is_business_day(sim_date):
        return []

    signals: list[DemandSignalEvent] = []
    for _ in range(10):
        if state.rng.random() < config.demand.signal_probability_daily:
            signals.append(_generate_signal(state, config, sim_date))

    return signals
