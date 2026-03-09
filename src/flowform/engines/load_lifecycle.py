"""Load lifecycle engine for FlowForm Industries.

Advances each active load through its status progression on every calendar day.
Carriers operate 7 days a week, so this engine has **no business-day guard**.

Status sequence (strict, at most one step per load per day)::

    load_created → in_transit → out_for_delivery → delivered

Reliability check: for transitions 2 and 3 a per-carrier, per-date
reliability score is sampled; failure means the load stays in its current
status and ``delay_days`` is incremented without emitting an event.

Terminal statuses ``"delivered"`` and ``"cancelled"`` are skipped entirely.
Delivered loads are kept in ``state.active_loads`` (not removed).

Engine entry point: ``run(state, config, sim_date) -> list[LoadEvent]``
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from typing import Any

from flowform.config import Config
from flowform.engines.carrier_events import DISRUPTION_SEVERITY_PENALTY
from flowform.engines.load_planning import LoadEvent
from flowform.master_data.carriers import reliability_on_date as _carrier_reliability
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Transit duration constants — inclusive [min, max] calendar days
# ---------------------------------------------------------------------------

_TRANSIT_DAYS: dict[str, tuple[int, int]] = {
    "DHL":    (1, 2),
    "DBSC":   (2, 3),
    "RABEN":  (2, 4),
    "GEODIS": (3, 5),
    "BALTIC": (4, 7),
}

_TRANSIT_DAYS_FALLBACK: tuple[int, int] = (2, 4)

# Terminal statuses — loads in these states are never touched
_TERMINAL_STATUSES: frozenset[str] = frozenset({"delivered", "cancelled"})


# ---------------------------------------------------------------------------
# Business day advancement helper
# ---------------------------------------------------------------------------


def _advance_business_days(start: date, n: int) -> date:
    """Return the date that is *n* business days after *start*.

    Args:
        start: The starting calendar date (not included in count).
        n:     Number of business days to advance.

    Returns:
        The calendar date exactly *n* business days after *start*.
    """
    from flowform.calendar import is_business_day
    current = start
    count = 0
    while count < n:
        current += timedelta(days=1)
        if is_business_day(current):
            count += 1
    return current


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------


def _make_load_event(load: dict[str, Any], sim_date: date) -> LoadEvent:
    """Build a :class:`LoadEvent` for the load's *current* (newly set) status.

    Args:
        load:     The load record dict from ``state.active_loads``.
        sim_date: The simulation date on which the transition fires.

    Returns:
        A :class:`LoadEvent` ready to be appended to the results list.
    """
    return LoadEvent(
        event_id=str(uuid.uuid4()),
        load_id=load["load_id"],
        event_subtype=load["status"],
        simulation_date=sim_date.isoformat(),
        sync_window=load["sync_window"],
        carrier_code=load["carrier_code"],
        source_warehouse_id=load["source_warehouse_id"],
        shipment_ids=load["shipment_ids"],
        order_ids=load["order_ids"],
        total_weight_kg=load["total_weight_kg"],
        weight_unit=load["weight_unit"],
        total_weight_reported=load["total_weight_reported"],
        status=load["status"],
        planned_date=load["planned_date"],
        priority=load["priority"],
        customer_ids=load["customer_ids"],
    )


def _update_order_lines_on_delivery(
    state: SimulationState,
    load: dict[str, Any],
) -> None:
    """Mark all allocated lines on orders in this load as shipped.

    Also invoices the shipped value into ``state.customer_balances`` so the
    payment engine has a positive balance to work with.

    Args:
        state: Mutable simulation state (for ``open_orders`` access).
        load:  Delivered load record.
    """
    from flowform.engines._order_utils import update_order_status

    for oid in load.get("order_ids", []):
        order = state.open_orders.get(oid)
        if order is None:
            continue

        # Accumulate the invoiced value of shipped lines into customer balance.
        customer_id: str = order.get("customer_id", "")
        currency: str = order.get("currency", "PLN")
        if currency == "PLN":
            rate = 1.0
        elif currency == "EUR":
            rate = state.exchange_rates.get("EUR", 4.30)
        elif currency == "USD":
            rate = state.exchange_rates.get("USD", 4.05)
        else:
            rate = 1.0

        shipped_value_pln = 0.0
        for line in order.get("lines", []):
            if line.get("line_status") == "allocated":
                line["line_status"] = "shipped"
                shipped_qty: int = line.get("quantity_allocated", 0)
                line["quantity_shipped"] = shipped_qty
                shipped_value_pln += shipped_qty * line.get("unit_price", 0.0) * rate

        if customer_id and shipped_value_pln > 0.0:
            state.customer_balances[customer_id] = (
                state.customer_balances.get(customer_id, 0.0) + shipped_value_pln
            )

        update_order_status(order)


# ---------------------------------------------------------------------------
# Engine entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[LoadEvent]:
    """Run the load lifecycle engine for *sim_date*.

    Runs every calendar day — carriers operate 7 days a week.
    Advances each non-terminal load by at most one status step.

    Args:
        state:    Mutable simulation state.
        config:   Validated simulation config (kept for contract consistency).
        sim_date: The simulated calendar date to process.

    Returns:
        List of :class:`LoadEvent` instances for any transitions that fired today.
    """
    from flowform.calendar import is_business_day

    events: list[LoadEvent] = []

    for load in state.active_loads.values():
        status = load.get("status", "")

        # Skip terminal loads (includes "delivered" which awaits POD in pending_pod).
        # The pod.py engine fires pod_received and removes loads from active_loads.
        # A "delivered" load must never be re-advanced by this engine.
        if status in _TERMINAL_STATUSES:
            continue

        carrier_code: str = load.get("carrier_code", "")

        # Resolve carrier object for reliability lookup
        carrier_obj = next(
            (c for c in state.carriers if c.code == carrier_code), None
        )

        # ----------------------------------------------------------------
        # Transition 1: load_created → in_transit
        # ----------------------------------------------------------------
        if status == "load_created":
            planned_date = date.fromisoformat(load["planned_date"])
            if sim_date >= planned_date and is_business_day(sim_date):
                # Determine transit duration
                min_days, max_days = _TRANSIT_DAYS.get(
                    carrier_code, _TRANSIT_DAYS_FALLBACK
                )
                transit_days = state.rng.randint(min_days, max_days)

                load["actual_departure"] = sim_date.isoformat()
                load["estimated_arrival"] = (
                    sim_date + timedelta(days=transit_days)
                ).isoformat()
                load["status"] = "in_transit"

                events.append(_make_load_event(load, sim_date))
            continue

        # ----------------------------------------------------------------
        # Transition 2: in_transit → out_for_delivery
        # ----------------------------------------------------------------
        if status == "in_transit":
            estimated_arrival_str = load.get("estimated_arrival")
            if estimated_arrival_str is None:
                continue
            estimated_arrival = date.fromisoformat(estimated_arrival_str)
            if sim_date < estimated_arrival:
                continue

            # Reliability check
            reliability = (
                _carrier_reliability(carrier_obj, sim_date)
                if carrier_obj is not None
                else 1.0
            )
            disruptions = getattr(state, "carrier_disruptions", {})
            if carrier_code in disruptions:
                disrupt = disruptions[carrier_code]
                disrupt_end = date.fromisoformat(disrupt["end_date"])
                if sim_date <= disrupt_end:
                    penalty = DISRUPTION_SEVERITY_PENALTY.get(
                        disrupt.get("severity", "medium"), 0.20
                    )
                    reliability = max(0.0, reliability - penalty)
            if state.rng.random() > reliability:
                load.setdefault("delay_days", 0)
                load["delay_days"] += 1
                continue

            # Transition succeeds
            delivery_window = state.rng.randint(1, 2)
            load["status"] = "out_for_delivery"
            load["out_for_delivery_date"] = sim_date.isoformat()
            load["delivery_window"] = delivery_window

            events.append(_make_load_event(load, sim_date))
            continue

        # ----------------------------------------------------------------
        # Transition 3: out_for_delivery → delivered
        # ----------------------------------------------------------------
        if status == "out_for_delivery":
            ofd_date_str = load.get("out_for_delivery_date")
            delivery_window = load.get("delivery_window", 1)
            if ofd_date_str is None:
                continue
            ofd_date = date.fromisoformat(ofd_date_str)
            delivery_due = ofd_date + timedelta(days=delivery_window)
            if sim_date < delivery_due:
                continue

            # Reliability check
            reliability = (
                _carrier_reliability(carrier_obj, sim_date)
                if carrier_obj is not None
                else 1.0
            )
            disruptions = getattr(state, "carrier_disruptions", {})
            if carrier_code in disruptions:
                disrupt = disruptions[carrier_code]
                disrupt_end = date.fromisoformat(disrupt["end_date"])
                if sim_date <= disrupt_end:
                    penalty = DISRUPTION_SEVERITY_PENALTY.get(
                        disrupt.get("severity", "medium"), 0.20
                    )
                    reliability = max(0.0, reliability - penalty)
            if state.rng.random() > reliability:
                load.setdefault("delay_days", 0)
                load["delay_days"] += 1
                continue

            # Transition succeeds
            load["actual_arrival"] = sim_date.isoformat()
            load["status"] = "delivered"

            # Register load for POD tracking (1–3 business days after delivery)
            pod_delay_biz_days = state.rng.randint(1, 3)
            pod_due_date = _advance_business_days(sim_date, pod_delay_biz_days)
            state.pending_pod[load["load_id"]] = {
                "load_id": load["load_id"],
                "carrier_code": load["carrier_code"],
                "source_warehouse_id": load["source_warehouse_id"],
                "shipment_ids": load["shipment_ids"],
                "order_ids": load["order_ids"],
                "total_weight_kg": load["total_weight_kg"],
                "weight_unit": load["weight_unit"],
                "total_weight_reported": load["total_weight_reported"],
                "sync_window": load["sync_window"],
                "priority": load["priority"],
                "customer_ids": load["customer_ids"],
                "planned_date": load["planned_date"],
                "delivery_date": sim_date.isoformat(),
                "pod_due_date": pod_due_date.isoformat(),
            }

            _update_order_lines_on_delivery(state, load)

            events.append(_make_load_event(load, sim_date))
            continue

    return events
