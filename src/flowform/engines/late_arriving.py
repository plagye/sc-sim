"""Late-arriving data post-processing for FlowForm Industries.

Applies three realism mechanisms after all engines have run for a given day:

1. **TMS late timestamps** (~5%): back-date the ``timestamp`` field on a random
   subset of TMS events (``carrier_event``, ``return_event``) to the previous
   business day.  Only events that carry a ``timestamp`` field are eligible;
   ``load_event`` uses ``simulation_date`` instead and is left untouched here.

2. **ERP inventory movement carry-forward** (~2%): withhold a random subset of
   ``inventory_movement`` events from today's output.  They are stored in
   ``state.deferred_erp_movements`` and prepended to the *next* day's event list.

3. **TMS maintenance burst** (~1/22 probability, max once per calendar month):
   when triggered, all ``load_event`` events are moved to sync_window ``"20"``
   (their timestamps / simulation_date fields are preserved).  This simulates a
   4-hour maintenance flush where a backlog of load events arrives together in
   the evening sync.

All randomness uses ``state.rng`` for reproducibility.

Public interface::

    apply(events, state, config, sim_date) -> list
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from flowform.calendar import is_business_day
from flowform.config import Config
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TMS_LATE_PROB: float = 0.05        # 5% of TMS events get back-dated
_ERP_DEFER_PROB: float = 0.02       # 2% of inventory_movement events deferred
_MAINTENANCE_PROB: float = 1 / 22   # ~once per month

# Event types routed to TMS that carry a ``timestamp`` field.
_TMS_TIMESTAMP_TYPES: frozenset[str] = frozenset({"carrier_event", "return_event"})

# All TMS event types (for maintenance burst targeting load_event).
_TMS_LOAD_TYPE: str = "load_event"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _prev_business_day(d: date) -> date:
    """Return the most recent business day strictly before *d*."""
    candidate = d - timedelta(days=1)
    while not is_business_day(candidate):
        candidate -= timedelta(days=1)
    return candidate


def _get_event_type(event: Any) -> str:
    """Extract event_type from a Pydantic model or dict."""
    if isinstance(event, dict):
        return event.get("event_type", "")
    return getattr(event, "event_type", "")


def _get_field(event: Any, field: str) -> Any:
    """Get a field from a Pydantic model or dict. Returns None if absent."""
    if isinstance(event, dict):
        return event.get(field)
    return getattr(event, field, None)


def _set_field(event: Any, field: str, value: Any) -> Any:
    """Return a copy of *event* with *field* set to *value*.

    For dicts: returns a shallow-copied dict with the field updated.
    For Pydantic v2 models: returns a new model instance via model_copy(update=…).
    """
    if isinstance(event, dict):
        result = dict(event)
        result[field] = value
        return result
    # Pydantic v2
    return event.model_copy(update={field: value})


def _backdate_timestamp(timestamp: str, prev_date: date) -> str:
    """Replace the date portion of an ISO 8601 timestamp with *prev_date*.

    Input format: ``YYYY-MM-DDTHH:MM:SSZ``
    Output keeps the same time component.
    """
    if "T" not in timestamp:
        return timestamp
    _date_part, time_part = timestamp.split("T", 1)
    return f"{prev_date.isoformat()}T{time_part}"


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------


def apply(
    events: list[Any],
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[Any]:
    """Apply late-arriving data post-processing to *events*.

    Modifies ``state.deferred_erp_movements`` and
    ``state.tms_maintenance_fired_month`` in-place.

    Args:
        events:   All events generated for *sim_date* (Pydantic models or
                  dicts), in the order produced by the day loop.
        state:    Mutable simulation state (provides ``state.rng``).
        config:   Validated simulation config (currently unused but kept for
                  interface consistency).
        sim_date: The simulated calendar date being processed.

    Returns:
        Processed event list (same objects unless modified; deferred ERP events
        from the previous day are prepended; newly deferred events are removed).
    """
    result: list[Any] = list(events)

    # ------------------------------------------------------------------
    # Step A: Inject deferred ERP movements from the previous day first
    # ------------------------------------------------------------------
    deferred_from_yesterday: list[dict[str, Any]] = list(state.deferred_erp_movements)
    state.deferred_erp_movements = []

    # ------------------------------------------------------------------
    # Step B: Separate today's inventory_movement events to decide which
    #         to defer to tomorrow
    # ------------------------------------------------------------------
    today_movements: list[tuple[int, Any]] = [
        (i, e) for i, e in enumerate(result)
        if _get_event_type(e) == "inventory_movement"
    ]

    deferred_indices: set[int] = set()
    newly_deferred: list[dict[str, Any]] = []

    for idx, event in today_movements:
        if state.rng.random() < _ERP_DEFER_PROB:
            deferred_indices.add(idx)
            # Store as dict for persistence
            if isinstance(event, dict):
                newly_deferred.append(dict(event))
            else:
                newly_deferred.append(event.model_dump())

    # Remove deferred events from result (iterate in reverse to preserve indices)
    result = [e for i, e in enumerate(result) if i not in deferred_indices]

    # Store newly deferred for injection tomorrow
    state.deferred_erp_movements = newly_deferred

    # ------------------------------------------------------------------
    # Step C: TMS late timestamps — back-date ~5% of eligible TMS events
    # ------------------------------------------------------------------
    prev_biz_day = _prev_business_day(sim_date)

    updated_result: list[Any] = []
    for event in result:
        etype = _get_event_type(event)
        if etype in _TMS_TIMESTAMP_TYPES:
            ts = _get_field(event, "timestamp")
            if ts is not None and state.rng.random() < _TMS_LATE_PROB:
                new_ts = _backdate_timestamp(ts, prev_biz_day)
                event = _set_field(event, "timestamp", new_ts)
        updated_result.append(event)
    result = updated_result

    # ------------------------------------------------------------------
    # Step D: TMS maintenance burst — move all load_events to sync "20"
    # ------------------------------------------------------------------
    month_key = sim_date.strftime("%Y-%m")
    if (
        state.tms_maintenance_fired_month != month_key
        and state.rng.random() < _MAINTENANCE_PROB
    ):
        state.tms_maintenance_fired_month = month_key
        burst_result: list[Any] = []
        for event in result:
            if _get_event_type(event) == _TMS_LOAD_TYPE:
                event = _set_field(event, "sync_window", "20")
            burst_result.append(event)
        result = burst_result

    # ------------------------------------------------------------------
    # Step E: Prepend deferred events from yesterday
    # ------------------------------------------------------------------
    result = deferred_from_yesterday + result  # type: ignore[operator]

    return result
