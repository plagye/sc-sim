"""Supply disruptions engine — Step 46.

Generates stochastic supply disruption events across three categories:
production, warehouse, and supply_chain.  Gated by config.disruptions.enabled.

Active disruptions are persisted in state.supply_disruptions so they survive
day boundaries and can influence other engines via the helper
get_active_production_severity().
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Literal

from pydantic import BaseModel

from flowform.calendar import is_business_day
from flowform.config import Config
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Event schema
# ---------------------------------------------------------------------------


class SupplyDisruptionEvent(BaseModel):
    event_type: Literal["supply_disruption"] = "supply_disruption"
    event_id: str                  # DSRP-YYYYMMDD-NNNN
    timestamp: str                 # ISO 8601 UTC
    disruption_category: str       # "production" | "warehouse" | "supply_chain"
    disruption_subtype: str        # see _CATEGORY_SUBTYPES
    severity: str                  # "low" | "medium" | "high"
    duration_days: int             # 1–5
    start_date: str                # ISO date
    end_date: str                  # ISO date (start_date + duration_days - 1)
    affected_scope: str | None     # warehouse_id or None
    message: str                   # human-readable description


# ---------------------------------------------------------------------------
# Configuration tables
# ---------------------------------------------------------------------------

_CATEGORY_SUBTYPES: dict[str, list[str]] = {
    "production": ["line_stoppage", "raw_material_shortage", "equipment_failure"],
    "warehouse": ["system_outage", "fire_drill_closure", "flooding"],
    "supply_chain": ["supplier_delay", "customs_hold", "port_congestion"],
}

_SEVERITY_WEIGHTS = [("low", 0.50), ("medium", 0.35), ("high", 0.15)]
_SEVERITY_VALUES = [s for s, _ in _SEVERITY_WEIGHTS]
_SEVERITY_CUMULATIVE = [
    sum(w for _, w in _SEVERITY_WEIGHTS[:i + 1])
    for i in range(len(_SEVERITY_WEIGHTS))
]

_TRIGGER_PROBABILITY = 0.03

_WAREHOUSE_IDS = ["W01", "W02"]

_MESSAGES: dict[str, dict[str, str]] = {
    "production": {
        "line_stoppage": "Production line halted due to unplanned stoppage.",
        "raw_material_shortage": "Insufficient raw materials available for scheduled production.",
        "equipment_failure": "Critical equipment failure requires emergency maintenance.",
    },
    "warehouse": {
        "system_outage": "Warehouse management system outage affecting picking operations.",
        "fire_drill_closure": "Facility temporarily closed for mandatory fire drill and inspection.",
        "flooding": "Warehouse partially flooded; access restricted to dry zones.",
    },
    "supply_chain": {
        "supplier_delay": "Key supplier experiencing delays in component delivery.",
        "customs_hold": "Inbound shipment held at customs for additional documentation.",
        "port_congestion": "Port congestion causing significant delay to inbound freight.",
    },
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _pick_severity(rng_val: float) -> str:
    """Return a severity string based on a uniform [0, 1) random draw."""
    for sev, cum in zip(_SEVERITY_VALUES, _SEVERITY_CUMULATIVE):
        if rng_val < cum:
            return sev
    return "high"


def _pick_affected_scope(
    category: str,
    rng: Any,
) -> str | None:
    """Return warehouse_id for production/warehouse categories, None for supply_chain."""
    if category in ("production", "warehouse"):
        return rng.choice(_WAREHOUSE_IDS)
    return None


def _build_event(
    state: SimulationState,
    sim_date: date,
    category: str,
) -> SupplyDisruptionEvent:
    """Build one SupplyDisruptionEvent for the given category."""
    subtype = state.rng.choice(_CATEGORY_SUBTYPES[category])
    severity = _pick_severity(state.rng.random())
    duration_days = state.rng.randint(1, 5)
    affected_scope = _pick_affected_scope(category, state.rng)

    state.counters.setdefault("disruption", 0)
    state.counters["disruption"] += 1
    counter_val = state.counters["disruption"]

    event_id = f"DSRP-{sim_date.strftime('%Y%m%d')}-{counter_val:04d}"
    start_date = sim_date
    end_date = start_date + timedelta(days=duration_days - 1)

    timestamp = f"{sim_date.isoformat()}T06:00:00Z"
    message = _MESSAGES[category][subtype]

    return SupplyDisruptionEvent(
        event_id=event_id,
        timestamp=timestamp,
        disruption_category=category,
        disruption_subtype=subtype,
        severity=severity,
        duration_days=duration_days,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        affected_scope=affected_scope,
        message=message,
    )


def _expire_stale(state: SimulationState, sim_date: date) -> None:
    """Remove disruptions whose end_date is before today."""
    supply_disruptions = getattr(state, "supply_disruptions", {})
    to_remove = [
        dsrp_id
        for dsrp_id, rec in supply_disruptions.items()
        if date.fromisoformat(rec["end_date"]) < sim_date
    ]
    for dsrp_id in to_remove:
        del supply_disruptions[dsrp_id]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_active_production_severity(
    state: SimulationState,
    sim_date: date,
) -> str | None:
    """Return the highest active severity among production-category disruptions.

    Returns None if there are no active production disruptions.
    Severity ranking: high > medium > low.

    Args:
        state:    Current simulation state.
        sim_date: The simulated calendar date.

    Returns:
        ``"high"``, ``"medium"``, ``"low"``, or ``None``.
    """
    supply_disruptions = getattr(state, "supply_disruptions", {})
    severity_rank = {"low": 1, "medium": 2, "high": 3}
    best: str | None = None
    best_rank = 0

    for rec in supply_disruptions.values():
        if rec.get("disruption_category") != "production":
            continue
        end = date.fromisoformat(rec["end_date"])
        start = date.fromisoformat(rec["start_date"])
        if start <= sim_date <= end:
            sev = rec.get("severity", "low")
            rank = severity_rank.get(sev, 0)
            if rank > best_rank:
                best_rank = rank
                best = sev

    return best


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[SupplyDisruptionEvent]:
    """Run the supply disruptions engine for one simulation day.

    Gated by ``config.disruptions.enabled``.  On each business day, rolls
    three independent Bernoulli trials (one per disruption category) with
    probability 0.03 each.

    Args:
        state:    Mutable simulation state.
        config:   Validated simulation config.
        sim_date: The simulated calendar date to process.

    Returns:
        List of :class:`SupplyDisruptionEvent` objects (may be empty).
    """
    if not config.disruptions.enabled:
        return []

    if not is_business_day(sim_date):
        return []

    # Ensure supply_disruptions exists on state
    if not hasattr(state, "supply_disruptions"):
        state.supply_disruptions: dict[str, dict[str, Any]] = {}  # type: ignore[assignment]

    # Expire stale entries from previous days
    _expire_stale(state, sim_date)

    events: list[SupplyDisruptionEvent] = []

    for category in ("production", "warehouse", "supply_chain"):
        if state.rng.random() < _TRIGGER_PROBABILITY:
            event = _build_event(state, sim_date, category)
            # Persist to state
            state.supply_disruptions[event.event_id] = event.model_dump()
            events.append(event)

    return events
