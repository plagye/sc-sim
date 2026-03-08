"""POD (Proof of Delivery) engine for FlowForm Industries.

Checks all loads in ``state.pending_pod`` each business day.  If a load's
``pod_due_date`` has been reached, emit a ``pod_received`` LoadEvent, remove
the load from ``state.pending_pod``, and remove it from ``state.active_loads``.

Business days only.

Engine entry point: ``run(state, config, sim_date) -> list[LoadEvent]``
"""

from __future__ import annotations

import uuid
from datetime import date

from flowform.calendar import is_business_day
from flowform.config import Config
from flowform.engines.load_planning import LoadEvent
from flowform.state import SimulationState


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[LoadEvent]:
    """Run the POD engine for *sim_date*.

    Business days only — returns an empty list on weekends and public holidays.

    For each load in ``state.pending_pod`` whose ``pod_due_date`` is on or before
    *sim_date*, emit a ``pod_received`` LoadEvent, remove the load from
    ``state.pending_pod``, and remove it from ``state.active_loads``.

    Args:
        state:    Mutable simulation state.
        config:   Validated simulation config (kept for contract consistency).
        sim_date: The simulated calendar date to process.

    Returns:
        List of :class:`LoadEvent` instances for PODs that fired today.
    """
    if not is_business_day(sim_date):
        return []

    events: list[LoadEvent] = []

    # Build list of load_ids whose POD is due today (avoid mutating dict mid-loop)
    due_load_ids = [
        load_id
        for load_id, pod_rec in state.pending_pod.items()
        if date.fromisoformat(pod_rec["pod_due_date"]) <= sim_date
    ]

    for load_id in due_load_ids:
        pod_rec = state.pending_pod.pop(load_id)

        # Emit pod_received event using LoadEvent schema
        event = LoadEvent(
            event_id=str(uuid.uuid4()),
            load_id=load_id,
            event_subtype="pod_received",
            simulation_date=sim_date.isoformat(),
            sync_window=pod_rec["sync_window"],
            carrier_code=pod_rec["carrier_code"],
            source_warehouse_id=pod_rec["source_warehouse_id"],
            shipment_ids=pod_rec["shipment_ids"],
            order_ids=pod_rec["order_ids"],
            total_weight_kg=pod_rec["total_weight_kg"],
            weight_unit=pod_rec["weight_unit"],
            total_weight_reported=pod_rec["total_weight_reported"],
            status="pod_received",
            planned_date=pod_rec["planned_date"],
            priority=pod_rec["priority"],
            customer_ids=pod_rec["customer_ids"],
        )
        events.append(event)

        # Remove load from active_loads — load lifecycle is complete
        state.active_loads.pop(load_id, None)

    return events
