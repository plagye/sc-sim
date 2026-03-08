"""Returns / RMA flow engine for FlowForm Industries.

Processes reverse logistics: customer-initiated returns move through
return_requested → in_transit_return → received_at_warehouse → inspected
→ restocked | scrapped.

Business days only.

Engine entry point: run(state, config, date) -> list[ReturnEvent | InventoryMovementEvent]
"""

from __future__ import annotations

import math
import uuid
from datetime import date, timedelta
from typing import Any, Literal

from pydantic import BaseModel

from flowform.calendar import is_business_day
from flowform.config import Config
from flowform.engines.inventory_movements import InventoryMovementEvent
from flowform.engines.load_lifecycle import _advance_business_days
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_RETURN_RATE: float = 0.03  # 3% per shipped order per business day

# Weighted return reason codes (cumulative probability table)
_RETURN_REASONS: list[tuple[float, str]] = [
    (0.20, "wrong_item_shipped"),
    (0.45, "damaged_in_transit"),
    (0.75, "quality_defect"),
    (0.90, "overstock"),
    (1.00, "specification_mismatch"),
]

# Inspection outcome: 80% restocked, 20% scrapped
_RESTOCK_PROB: float = 0.80


# ---------------------------------------------------------------------------
# Pydantic event schema
# ---------------------------------------------------------------------------


class ReturnEvent(BaseModel):
    """A single RMA lifecycle event in the TMS returns flow."""

    event_type: Literal["return_event"] = "return_event"
    event_id: str                    # uuid4 string
    event_subtype: str               # one of the 5 lifecycle statuses
    rma_id: str                      # "RMA-NNNN"
    order_id: str
    customer_id: str
    carrier_code: str
    return_warehouse_id: str         # always "W01"
    lines: list[dict[str, Any]]      # snapshot of the RMA lines at this status
    resolution: str | None           # "restocked" | "scrapped" | None
    return_reason: str               # the reason code
    rma_status: str                  # the new status after this event
    simulation_date: str             # ISO date
    timestamp: str                   # e.g., "2026-03-09T09:00:00Z"
    sync_window: str                 # always "08" for returns


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _pick_reason(rng_val: float) -> str:
    """Pick a return reason using a pre-rolled uniform [0,1) value.

    Args:
        rng_val: A float in [0, 1) from ``state.rng.random()``.

    Returns:
        One of the 5 return reason code strings.
    """
    for threshold, reason in _RETURN_REASONS:
        if rng_val < threshold:
            return reason
    return _RETURN_REASONS[-1][1]


def _make_return_event(
    rma: dict[str, Any],
    sim_date: date,
    *,
    resolution: str | None = None,
) -> ReturnEvent:
    """Build a :class:`ReturnEvent` reflecting the RMA's current status.

    Args:
        rma:        The RMA state dict (already updated to new status).
        sim_date:   The simulation date on which this event fires.
        resolution: "restocked", "scrapped", or None.

    Returns:
        A :class:`ReturnEvent` ready to append to the results list.
    """
    return ReturnEvent(
        event_id=str(uuid.uuid4()),
        event_subtype=rma["rma_status"],
        rma_id=rma["rma_id"],
        order_id=rma["order_id"],
        customer_id=rma["customer_id"],
        carrier_code=rma["carrier_code"],
        return_warehouse_id=rma["return_warehouse_id"],
        lines=list(rma["lines"]),
        resolution=resolution,
        return_reason=rma["lines"][0]["return_reason"] if rma["lines"] else "",
        rma_status=rma["rma_status"],
        simulation_date=sim_date.isoformat(),
        timestamp=f"{sim_date.isoformat()}T09:00:00Z",
        sync_window="08",
    )


def _make_restock_movement(
    state: SimulationState,
    sim_date: date,
    rma: dict[str, Any],
    line: dict[str, Any],
) -> InventoryMovementEvent:
    """Build a return_restock :class:`InventoryMovementEvent` for one RMA line.

    Also updates ``state.inventory["W01"]`` for the SKU in place.

    Args:
        state:    Mutable simulation state.
        sim_date: The simulation date.
        rma:      The RMA state dict.
        line:     One returned line sub-dict (must have quantity_accepted set).

    Returns:
        An :class:`InventoryMovementEvent` with movement_type="return_restock".
    """
    sku = line["sku"]
    qty = line["quantity_accepted"]

    # Update W01 inventory
    w01 = state.inventory.setdefault("W01", {})
    w01[sku] = w01.get(sku, 0) + qty

    state.counters["movement"] = state.counters.get("movement", 1000) + 1
    movement_id = f"MOV-{state.counters['movement']}"

    return InventoryMovementEvent(
        event_id=str(uuid.uuid4()),
        movement_id=movement_id,
        movement_type="return_restock",
        movement_date=sim_date.isoformat(),
        warehouse_id="W01",
        destination_warehouse_id=None,
        sku=sku,
        quantity=qty,
        quantity_direction="in",
        order_id=rma["order_id"],
        order_line_id=None,
        production_batch_id=None,
        reason_code=None,
        reference_id=rma["rma_id"],
    )


def _has_open_rma(state: SimulationState, order_id: str) -> bool:
    """Return True if *order_id* already has an open (non-terminal) RMA.

    Args:
        state:    Simulation state.
        order_id: The order ID to check.

    Returns:
        True if any in-flight RMA references this order and is not yet resolved.
    """
    terminal = {"restocked", "scrapped"}
    return any(
        rma["order_id"] == order_id and rma["rma_status"] not in terminal
        for rma in state.in_transit_returns
    )


def _carrier_for_customer(state: SimulationState, customer_id: str) -> str:
    """Return the preferred carrier code for a customer, defaulting to "DHL".

    Args:
        state:       Simulation state.
        customer_id: Customer identifier.

    Returns:
        Carrier code string.
    """
    for customer in state.customers:
        if customer.customer_id == customer_id:
            preferred = customer.preferred_carrier
            return preferred if preferred else "DHL"
    return "DHL"


# ---------------------------------------------------------------------------
# RMA lifecycle advancement
# ---------------------------------------------------------------------------


def _advance_rma(
    state: SimulationState,
    rma: dict[str, Any],
    sim_date: date,
) -> list:
    """Advance one RMA by at most one status step, returning any events emitted.

    Transitions (in order):
      return_requested → in_transit_return    (next business day after creation)
      in_transit_return → received_at_warehouse (on transit_due_date)
      received_at_warehouse → inspected        (next business day after receipt)
      inspected → restocked | scrapped         (same day, 80/20 roll)

    Args:
        state:    Mutable simulation state (RNG used for inspection roll).
        rma:      The RMA dict to advance (mutated in place).
        sim_date: Today's simulation date.

    Returns:
        List of events (ReturnEvent, and optionally InventoryMovementEvent for restock).
    """
    events: list = []
    status = rma["rma_status"]

    # ----------------------------------------------------------------
    # return_requested → in_transit_return
    # Next business day after creation (created_date < sim_date)
    # ----------------------------------------------------------------
    if status == "return_requested":
        created = date.fromisoformat(rma["created_date"])
        if sim_date > created:
            # Calculate transit due date
            transit_days = state.rng.randint(2, 5)
            transit_due = sim_date + timedelta(days=transit_days)
            rma["rma_status"] = "in_transit_return"
            rma["transit_due_date"] = transit_due.isoformat()
            rma["in_transit_date"] = sim_date.isoformat()
            events.append(_make_return_event(rma, sim_date))
        return events

    # ----------------------------------------------------------------
    # in_transit_return → received_at_warehouse
    # On or after transit_due_date
    # ----------------------------------------------------------------
    if status == "in_transit_return":
        transit_due_str = rma.get("transit_due_date")
        if transit_due_str is None:
            return events
        transit_due = date.fromisoformat(transit_due_str)
        if sim_date >= transit_due:
            rma["rma_status"] = "received_at_warehouse"
            rma["received_date"] = sim_date.isoformat()
            events.append(_make_return_event(rma, sim_date))
        return events

    # ----------------------------------------------------------------
    # received_at_warehouse → inspected
    # Next business day after receipt
    # ----------------------------------------------------------------
    if status == "received_at_warehouse":
        received_str = rma.get("received_date")
        if received_str is None:
            return events
        received = date.fromisoformat(received_str)
        if sim_date > received:
            # Set quantity_accepted for each line (same as quantity_returned for now)
            for line in rma["lines"]:
                line["quantity_accepted"] = line["quantity_returned"]
            rma["rma_status"] = "inspected"

            # Roll for resolution on the same day
            if state.rng.random() < _RESTOCK_PROB:
                resolution = "restocked"
                rma["resolution"] = resolution
                rma["rma_status"] = "restocked"
                events.append(_make_return_event(rma, sim_date, resolution=resolution))
                # Emit inventory movement per line
                for line in rma["lines"]:
                    events.append(_make_restock_movement(state, sim_date, rma, line))
            else:
                resolution = "scrapped"
                rma["resolution"] = resolution
                rma["rma_status"] = "scrapped"
                events.append(_make_return_event(rma, sim_date, resolution=resolution))
        return events

    # Terminal statuses — nothing to do
    return events


# ---------------------------------------------------------------------------
# New RMA creation
# ---------------------------------------------------------------------------


def _create_rma(
    state: SimulationState,
    order: dict[str, Any],
    sim_date: date,
) -> tuple[dict[str, Any], ReturnEvent]:
    """Create a new RMA dict and its return_requested event.

    Picks random lines (at least 1) and partial quantities per line.

    Args:
        state:    Mutable simulation state (RNG + counters).
        order:    The shipped order dict from state.open_orders.
        sim_date: Today's simulation date.

    Returns:
        Tuple of (rma_dict, ReturnEvent).
    """
    order_id = order["order_id"]
    customer_id = order["customer_id"]

    # Eligible lines: shipped with quantity_shipped > 0
    eligible_lines = [
        line for line in order.get("lines", [])
        if line.get("line_status") == "shipped" and line.get("quantity_shipped", 0) > 0
    ]

    # Select at least 1 line, up to all lines
    n_lines = state.rng.randint(1, max(1, len(eligible_lines)))
    selected = state.rng.sample(eligible_lines, n_lines)

    # Pick a reason for the whole RMA
    reason = _pick_reason(state.rng.random())

    # Build line sub-dicts with partial quantity
    rma_lines: list[dict[str, Any]] = []
    for line in selected:
        qty_shipped = line["quantity_shipped"]
        min_qty = math.ceil(qty_shipped * 0.5)
        qty_returned = state.rng.randint(min_qty, qty_shipped)
        rma_lines.append(
            {
                "line_id": line["line_id"],
                "sku": line["sku"],
                "quantity_returned": qty_returned,
                "quantity_accepted": None,
                "return_reason": reason,
            }
        )

    rma_id = f"RMA-{state.next_id('return')}"
    carrier_code = _carrier_for_customer(state, customer_id)

    rma: dict[str, Any] = {
        "rma_id": rma_id,
        "order_id": order_id,
        "customer_id": customer_id,
        "carrier_code": carrier_code,
        "return_warehouse_id": "W01",
        "rma_status": "return_requested",
        "created_date": sim_date.isoformat(),
        "transit_due_date": None,
        "lines": rma_lines,
        "resolution": None,
    }

    event = _make_return_event(rma, sim_date)
    return rma, event


# ---------------------------------------------------------------------------
# Engine entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list:  # ReturnEvent | InventoryMovementEvent
    """Run the returns / RMA flow engine for *sim_date*.

    Business days only.

    Execution order:
      1. Business day guard.
      2. Advance existing RMAs (at most one step each).
      3. Generate new RMAs from eligible shipped orders (3% roll per order).
      4. Append new RMAs to state.in_transit_returns.
      5. Remove completed RMAs (restocked or scrapped) from state.in_transit_returns.
      6. Return all collected events.

    Args:
        state:    Mutable simulation state.
        config:   Validated simulation config.
        sim_date: The simulated calendar date to process.

    Returns:
        List of :class:`ReturnEvent` and :class:`InventoryMovementEvent` instances.
    """
    if not is_business_day(sim_date):
        return []

    events: list = []

    # Step 2: Advance existing RMAs
    for rma in list(state.in_transit_returns):
        events.extend(_advance_rma(state, rma, sim_date))

    # Step 3: Generate new RMAs from eligible shipped orders
    new_rmas: list[dict[str, Any]] = []

    for order in state.open_orders.values():
        # Skip cancelled orders
        if order.get("status") == "cancelled":
            continue

        # Only consider shipped orders with at least one shipped line
        if order.get("status") != "shipped":
            continue
        has_shipped_line = any(
            line.get("line_status") == "shipped" and line.get("quantity_shipped", 0) > 0
            for line in order.get("lines", [])
        )
        if not has_shipped_line:
            continue

        # Skip if an open RMA already exists for this order
        if _has_open_rma(state, order["order_id"]):
            continue

        # Roll 3% probability
        if state.rng.random() >= _RETURN_RATE:
            continue

        rma, return_event = _create_rma(state, order, sim_date)
        new_rmas.append(rma)
        events.append(return_event)

    # Step 4: Append new RMAs to state
    state.in_transit_returns.extend(new_rmas)

    # Step 5: Remove completed RMAs
    terminal = {"restocked", "scrapped"}
    state.in_transit_returns = [
        rma for rma in state.in_transit_returns
        if rma["rma_status"] not in terminal
    ]

    return events
