"""Carrier events engine for FlowForm Industries.

Generates probabilistic fleet-level signals from carriers: capacity alerts,
operational disruptions, and rate changes.  These are NOT tied to individual
loads — they represent network-wide carrier communications.

All output routes to TMS.  Active disruptions are persisted in
``state.carrier_disruptions`` and integrated into load_lifecycle's
reliability check.

Engine entry point: ``run(state, config, sim_date) -> list[CarrierEvent]``
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from typing import Literal

from pydantic import BaseModel

from flowform.calendar import is_business_day
from flowform.config import Config
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Probability constants
# ---------------------------------------------------------------------------

_PROB_CAPACITY_ALERT: float = 0.05   # 5% per carrier per business day
_PROB_DISRUPTION: float = 0.02       # 2% per carrier per business day
_RATE_CHANGE_FREQ: float = 1 / 22    # ~once per month per carrier

# ---------------------------------------------------------------------------
# Polish NUTS3 region codes used for affected_region
# ---------------------------------------------------------------------------

_REGIONS: list[str] = [
    "PL-SL",  # Śląskie
    "PL-MA",  # Małopolskie
    "PL-LO",  # Łódźkie
    "PL-PM",  # Pomorskie
    "PL-DS",  # Dolnośląskie
    "PL-KP",  # Kujawsko-Pomorskie
    "PL-WN",  # Warmińsko-Mazurskie
    "PL-LB",  # Lubuskie
]

# ---------------------------------------------------------------------------
# Severity weight tables (cumulative probability ranges)
# ---------------------------------------------------------------------------

# capacity_alert: low 50%, medium 35%, high 15%
_CAPACITY_ALERT_SEVERITIES: list[tuple[float, str]] = [
    (0.50, "low"),
    (0.85, "medium"),
    (1.00, "high"),
]

# disruption: medium 60%, high 40%
_DISRUPTION_SEVERITIES: list[tuple[float, str]] = [
    (0.60, "medium"),
    (1.00, "high"),
]

# ---------------------------------------------------------------------------
# Disruption severity → reliability penalty mapping (used by load_lifecycle)
# ---------------------------------------------------------------------------

DISRUPTION_SEVERITY_PENALTY: dict[str, float] = {
    "low":    0.10,
    "medium": 0.20,
    "high":   0.35,
}

# ---------------------------------------------------------------------------
# Pydantic schema
# ---------------------------------------------------------------------------


class CarrierEvent(BaseModel):
    """A fleet-level signal from a carrier about network conditions."""

    event_type: Literal["carrier_event"] = "carrier_event"
    event_id: str                        # uuid4 string
    event_subtype: str                   # "capacity_alert" | "disruption" | "rate_change"  # noqa: E501
    carrier_id: str                      # carrier code, e.g. "DHL"
    carrier_name: str                    # human-readable name
    affected_region: str | None          # NUTS3 region code or None
    severity: str                        # "low" | "medium" | "high"
    start_date: str                      # ISO date (sim_date)
    end_date: str | None                 # ISO date for disruptions, None otherwise
    rate_delta_pct: float | None         # rate_change only; +/- 5–15%
    description: str                     # human-readable message
    simulation_date: str                 # ISO date
    timestamp: str                       # ISO datetime with Z suffix
    sync_window: str                     # "08" | "14" | "20"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _pick_severity(rng_val: float, table: list[tuple[float, str]]) -> str:
    """Pick a severity string using a pre-rolled uniform [0,1) value.

    Args:
        rng_val: A float in [0, 1) from ``state.rng.random()``.
        table:   Cumulative probability table: list of (threshold, label).

    Returns:
        The severity label for the first threshold that exceeds *rng_val*.
    """
    for threshold, label in table:
        if rng_val < threshold:
            return label
    return table[-1][1]


def _make_timestamp(sim_date: date, sync_window: str) -> str:
    """Build an ISO 8601 UTC timestamp string for the given sync window.

    Args:
        sim_date:    The simulated calendar date.
        sync_window: "08", "14", or "20".

    Returns:
        Timestamp string like "2026-03-01T14:00:00Z".
    """
    return f"{sim_date.isoformat()}T{sync_window}:00:00Z"


def _expire_disruptions(state: SimulationState, sim_date: date) -> None:
    """Remove any disruptions whose end_date is before *sim_date*.

    Args:
        state:    Mutable simulation state.
        sim_date: The current simulation date.
    """
    expired = [
        code
        for code, disrupt in state.carrier_disruptions.items()
        if date.fromisoformat(disrupt["end_date"]) < sim_date
    ]
    for code in expired:
        del state.carrier_disruptions[code]


def _generate_disruption(
    state: SimulationState,
    carrier_code: str,
    carrier_name: str,
    sim_date: date,
) -> CarrierEvent:
    """Generate a new disruption event and register it in state.

    Args:
        state:        Mutable simulation state (RNG + carrier_disruptions).
        carrier_code: Carrier code string (e.g. "DHL").
        carrier_name: Human-readable carrier name.
        sim_date:     The current simulation date.

    Returns:
        A :class:`CarrierEvent` of subtype "disruption".
    """
    duration = state.rng.randint(1, 5)
    end_date = sim_date + timedelta(days=duration)
    severity = _pick_severity(state.rng.random(), _DISRUPTION_SEVERITIES)
    region = state.rng.choice(_REGIONS)
    sync_window = state.rng.choice(["08", "14", "20"])

    description = (
        f"Strike action affecting {carrier_name} operations in {region} "
        f"for {duration} day{'s' if duration != 1 else ''}"
    )

    # Register in state for lifecycle engine integration
    state.carrier_disruptions[carrier_code] = {
        "end_date": end_date.isoformat(),
        "severity": severity,
        "carrier_name": carrier_name,
        "region": region,
    }

    return CarrierEvent(
        event_id=str(uuid.uuid4()),
        event_subtype="disruption",
        carrier_id=carrier_code,
        carrier_name=carrier_name,
        affected_region=region,
        severity=severity,
        start_date=sim_date.isoformat(),
        end_date=end_date.isoformat(),
        rate_delta_pct=None,
        description=description,
        simulation_date=sim_date.isoformat(),
        timestamp=_make_timestamp(sim_date, sync_window),
        sync_window=sync_window,
    )


def _generate_rate_change(
    state: SimulationState,
    carrier_code: str,
    carrier_name: str,
    sim_date: date,
) -> CarrierEvent:
    """Generate a rate_change event.

    Args:
        state:        Mutable simulation state (RNG).
        carrier_code: Carrier code string.
        carrier_name: Human-readable carrier name.
        sim_date:     The current simulation date.

    Returns:
        A :class:`CarrierEvent` of subtype "rate_change".
    """
    magnitude = state.rng.uniform(5.0, 15.0)
    if state.rng.random() < 0.5:
        magnitude = -magnitude
    sign = "+" if magnitude >= 0 else ""
    description = (
        f"{carrier_name} rate adjustment: {sign}{magnitude:.1f}% effective immediately"
    )

    return CarrierEvent(
        event_id=str(uuid.uuid4()),
        event_subtype="rate_change",
        carrier_id=carrier_code,
        carrier_name=carrier_name,
        affected_region=None,
        severity="low",
        start_date=sim_date.isoformat(),
        end_date=None,
        rate_delta_pct=round(magnitude, 4),
        description=description,
        simulation_date=sim_date.isoformat(),
        timestamp=_make_timestamp(sim_date, "14"),
        sync_window="14",
    )


def _generate_capacity_alert(
    state: SimulationState,
    carrier_code: str,
    carrier_name: str,
    sim_date: date,
) -> CarrierEvent:
    """Generate a capacity_alert event.

    Args:
        state:        Mutable simulation state (RNG).
        carrier_code: Carrier code string.
        carrier_name: Human-readable carrier name.
        sim_date:     The current simulation date.

    Returns:
        A :class:`CarrierEvent` of subtype "capacity_alert".
    """
    severity = _pick_severity(state.rng.random(), _CAPACITY_ALERT_SEVERITIES)
    region = state.rng.choice(_REGIONS)
    sync_window = state.rng.choice(["08", "14", "20"])

    description = (
        f"Reduced capacity in {region}, expect 1-2 day delays"
    )

    return CarrierEvent(
        event_id=str(uuid.uuid4()),
        event_subtype="capacity_alert",
        carrier_id=carrier_code,
        carrier_name=carrier_name,
        affected_region=region,
        severity=severity,
        start_date=sim_date.isoformat(),
        end_date=None,
        rate_delta_pct=None,
        description=description,
        simulation_date=sim_date.isoformat(),
        timestamp=_make_timestamp(sim_date, sync_window),
        sync_window=sync_window,
    )


# ---------------------------------------------------------------------------
# Engine entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[CarrierEvent]:
    """Run the carrier events engine for *sim_date*.

    Business days only — returns an empty list on weekends and public holidays.

    For each carrier, independently rolls:
    1. Disruption (0.02 prob) — gated: only if no active disruption for this carrier.
    2. Rate change (1/22 prob) — always independent.
    3. Capacity alert (0.05 prob) — always independent.

    All events that fire are included in the output.  A carrier may emit a
    capacity_alert and a rate_change on the same day.  Active disruptions are
    stored in ``state.carrier_disruptions`` for load_lifecycle integration.

    Args:
        state:    Mutable simulation state (RNG, carriers, carrier_disruptions).
        config:   Validated simulation config (kept for contract consistency).
        sim_date: The simulated calendar date to process.

    Returns:
        List of :class:`CarrierEvent` instances generated today.
    """
    if not is_business_day(sim_date):
        return []

    # Expire stale disruptions before generating new ones
    _expire_disruptions(state, sim_date)

    events: list[CarrierEvent] = []

    for carrier in state.carriers:
        code = carrier.code
        name = carrier.name

        # ----------------------------------------------------------------
        # Roll 1: Disruption — only if no active disruption for this carrier
        # ----------------------------------------------------------------
        if code not in state.carrier_disruptions:
            if state.rng.random() < _PROB_DISRUPTION:
                events.append(_generate_disruption(state, code, name, sim_date))

        # ----------------------------------------------------------------
        # Roll 2: Rate change (~1/month per carrier)
        # ----------------------------------------------------------------
        if state.rng.random() < _RATE_CHANGE_FREQ:
            events.append(_generate_rate_change(state, code, name, sim_date))

        # ----------------------------------------------------------------
        # Roll 3: Capacity alert
        # ----------------------------------------------------------------
        if state.rng.random() < _PROB_CAPACITY_ALERT:
            events.append(_generate_capacity_alert(state, code, name, sim_date))

    return events
