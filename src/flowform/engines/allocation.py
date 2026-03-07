"""Allocation engine for FlowForm Industries.

Allocates confirmed open order lines against on-hand inventory.  Handles:

- Priority-based sort: critical > express > standard, then requested_delivery_date, then FIFO
- Full allocation when stock is sufficient
- Partial fill (allocate available, backorder remainder)
- Zero-stock backorders
- Backorder day counting and escalation:
    - >14 days + standard priority → escalate to express
    - >30 days → 15% daily cancellation probability
- Credit-hold orders are skipped entirely (handled by credit engine)
- Weekend / holiday guard: returns [] on non-business days

Emits:
- ``InventoryMovementEvent`` (movement_type="pick") for each unit allocated
- ``BackorderEvent`` for each line (or portion) that cannot be filled
- ``BackorderCancellationEvent`` when a stale backorder is auto-cancelled

Engine entry point: ``run(state, config, date) -> list[Event]``
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Any, Literal

from pydantic import BaseModel

from flowform.calendar import is_business_day
from flowform.config import Config
from flowform.engines._order_utils import update_order_status
from flowform.engines.inventory_movements import InventoryMovementEvent, _make_event
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Priority ordering for sort: lower rank = higher priority
_PRIORITY_RANK: dict[str, int] = {
    "critical": 0,
    "express": 1,
    "standard": 2,
}

# Backorder escalation thresholds (in business days)
_ESCALATE_DAYS = 14
_CANCEL_DAYS = 30
_CANCEL_PROB = 0.15

# Default warehouse when line doesn't specify one
_DEFAULT_WAREHOUSE = "W01"

# Statuses that mean "this line is waiting for stock"
_ALLOCATABLE_LINE_STATUSES: frozenset[str] = frozenset({
    "open",
    "backordered",
    "partially_allocated",
})

# Order-level statuses that should NOT be allocated (skip entirely)
_SKIP_ORDER_STATUSES: frozenset[str] = frozenset({
    "credit_hold",
    "cancelled",
    "shipped",
    "partially_shipped",
})


# ---------------------------------------------------------------------------
# Additional event schemas
# ---------------------------------------------------------------------------


class BackorderEvent(BaseModel):
    """Emitted when a line (or portion of a line) cannot be filled from stock.

    Routes to the ERP customer_orders file via ``event_type="customer_orders"``.
    """

    event_type: Literal["customer_orders"] = "customer_orders"
    event_id: str
    backorder_date: str          # ISO date
    order_id: str
    order_line_id: str           # matches line["line_id"]
    sku: str
    quantity_backordered: int
    quantity_allocated: int
    backorder_reason: Literal["insufficient_stock", "no_stock", "credit_hold"]
    days_backordered: int
    escalated_to_express: bool = False


class BackorderCancellationEvent(BaseModel):
    """Emitted when a backorder line is auto-cancelled after 30+ days waiting.

    Routes to the ERP order_cancellations file.
    """

    event_type: Literal["order_cancellations"] = "order_cancellations"
    event_id: str
    cancellation_date: str       # ISO date
    order_id: str
    order_line_id: str
    sku: str
    quantity_cancelled: int
    reason: str = "backorder_timeout"
    days_backordered: int


# Type alias for the union of event types this engine can emit
AllocationEngineEvent = InventoryMovementEvent | BackorderEvent | BackorderCancellationEvent


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _line_priority(order: dict[str, Any]) -> int:
    """Return the numeric priority rank for sort purposes."""
    return _PRIORITY_RANK.get(order["priority"], 2)


def _unallocated_qty(line: dict[str, Any]) -> int:
    """Quantity still needing allocation for this line.

    For a fresh "open" line: quantity_ordered - quantity_allocated - quantity_shipped - quantity_backordered.
    For a "backordered" or "partially_allocated" line: the backordered quantity still outstanding.

    Args:
        line: Line dict from an order stored in state.open_orders.

    Returns:
        Integer number of units still to be allocated.  Never negative.
    """
    status = line.get("line_status", "open")
    if status in ("backordered", "partially_allocated"):
        return max(0, line.get("quantity_backordered", 0))

    # "open" or "confirmed" — compute from scratch
    ordered = line.get("quantity_ordered", 0)
    allocated = line.get("quantity_allocated", 0)
    shipped = line.get("quantity_shipped", 0)
    backordered = line.get("quantity_backordered", 0)
    return max(0, ordered - allocated - shipped - backordered)


def _warehouse_for_line(line: dict[str, Any]) -> str:
    """Return the warehouse from which this line should be filled.

    Lines generated by the orders engine do not carry a warehouse_id; we
    default to W01 (primary warehouse, ~80% of all stock).

    Args:
        line: Line dict from state.open_orders.

    Returns:
        Warehouse code string.
    """
    return line.get("warehouse_id", _DEFAULT_WAREHOUSE)


# ---------------------------------------------------------------------------
# Escalation and cancellation pass
# ---------------------------------------------------------------------------


def _run_escalation(
    state: SimulationState,
    sim_date: date,
) -> list[BackorderCancellationEvent | BackorderEvent]:
    """Increment backorder_days and apply escalation/cancellation rules.

    Iterates all open orders.  For each line whose ``line_status`` is
    ``"backordered"`` or ``"partially_allocated"`` (i.e. waiting for stock):

    - Increments ``backorder_days`` by 1.
    - If ``backorder_days > 30``: 15% chance of auto-cancellation.
    - If ``backorder_days > 14`` and ``priority == "standard"``: escalate to
      express (both the order and the line get their priority updated).

    Args:
        state:    Mutable simulation state.
        sim_date: Current simulation date (used for event timestamps).

    Returns:
        List of :class:`BackorderCancellationEvent` and/or
        :class:`BackorderEvent` (escalation notifications).
    """
    events: list[BackorderCancellationEvent | BackorderEvent] = []

    for order_id, order in list(state.open_orders.items()):
        order_status: str = order.get("status", "")
        if order_status in _SKIP_ORDER_STATUSES:
            continue

        priority: str = order.get("priority", "standard")

        for line in order.get("lines", []):
            line_status = line.get("line_status", "open")
            if line_status not in ("backordered", "partially_allocated"):
                continue

            # Ensure backorder_days field exists
            if "backorder_days" not in line:
                line["backorder_days"] = 0

            line["backorder_days"] += 1
            days = line["backorder_days"]

            # 30-day stale cancellation (15% daily)
            if days > _CANCEL_DAYS:
                if state.rng.random() < _CANCEL_PROB:
                    qty_cancelled = line.get("quantity_backordered", 0)
                    if qty_cancelled > 0:
                        # Remove from backordered — treat as cancelled
                        line["quantity_backordered"] = 0
                        line["line_status"] = "cancelled"

                        events.append(
                            BackorderCancellationEvent(
                                event_id=str(uuid.uuid4()),
                                cancellation_date=sim_date.isoformat(),
                                order_id=order_id,
                                order_line_id=line["line_id"],
                                sku=line["sku"],
                                quantity_cancelled=qty_cancelled,
                                reason="backorder_timeout",
                                days_backordered=days,
                            )
                        )
                    continue  # skip escalation check for cancelled line

            # 14-day escalation: standard → express
            escalated = False
            if days > _ESCALATE_DAYS and priority == "standard":
                order["priority"] = "express"
                priority = "express"  # update local var for consistency
                escalated = True

            if escalated:
                events.append(
                    BackorderEvent(
                        event_id=str(uuid.uuid4()),
                        backorder_date=sim_date.isoformat(),
                        order_id=order_id,
                        order_line_id=line["line_id"],
                        sku=line["sku"],
                        quantity_backordered=line.get("quantity_backordered", 0),
                        quantity_allocated=line.get("quantity_allocated", 0),
                        backorder_reason="insufficient_stock",
                        days_backordered=days,
                        escalated_to_express=True,
                    )
                )

    return events


# ---------------------------------------------------------------------------
# Allocation pass
# ---------------------------------------------------------------------------


def _collect_allocatable_lines(
    state: SimulationState,
) -> list[dict[str, Any]]:
    """Collect all order lines that are eligible for allocation today.

    A line is allocatable if:
    - Its parent order status is not in ``_SKIP_ORDER_STATUSES``
    - Its ``line_status`` is in ``_ALLOCATABLE_LINE_STATUSES``
    - It has positive unallocated quantity remaining

    Returns:
        List of dicts, each containing:
        - ``order``: reference to the parent order dict
        - ``line``: reference to the line dict
        - ``need``: units still needed
        - ``priority``: parent order priority string
        - ``priority_rank``: int for sorting
        - ``requested_delivery_date``: str (ISO) for sorting
        - ``order_date``: str (ISO) for FIFO tiebreak
    """
    result: list[dict[str, Any]] = []

    for order_id, order in state.open_orders.items():
        order_status = order.get("status", "")
        if order_status in _SKIP_ORDER_STATUSES:
            continue

        priority = order.get("priority", "standard")
        priority_rank = _PRIORITY_RANK.get(priority, 2)
        requested_delivery_date = order.get("requested_delivery_date", "9999-12-31")
        order_date = order.get("simulation_date", "9999-12-31")

        for line in order.get("lines", []):
            line_status = line.get("line_status", "open")
            if line_status not in _ALLOCATABLE_LINE_STATUSES:
                continue

            need = _unallocated_qty(line)
            if need <= 0:
                continue

            result.append({
                "order_id": order_id,
                "order": order,
                "line": line,
                "need": need,
                "priority": priority,
                "priority_rank": priority_rank,
                "requested_delivery_date": requested_delivery_date,
                "order_date": order_date,
            })

    # Sort: critical > express > standard, then earliest delivery, then FIFO
    result.sort(
        key=lambda x: (
            x["priority_rank"],
            x["requested_delivery_date"],
            x["order_date"],
        )
    )
    return result


def _run_allocation(
    state: SimulationState,
    sim_date: date,
    allocatable: list[dict[str, Any]],
) -> list[InventoryMovementEvent | BackorderEvent]:
    """Perform the actual allocation pass against on-hand inventory.

    For each line in priority order:
    - Look up available inventory in the line's warehouse.
    - Full fill, partial fill, or full backorder.
    - Mutate line quantities and line_status in place.
    - Mutate ``state.inventory`` in place.
    - Emit pick events (InventoryMovementEvent) and/or BackorderEvent.

    After processing all lines in an order, recalculates the order-level
    status.

    Args:
        state:       Mutable simulation state.
        sim_date:    Current simulation date.
        allocatable: Pre-sorted list from ``_collect_allocatable_lines``.

    Returns:
        List of :class:`InventoryMovementEvent` and :class:`BackorderEvent`.
    """
    events: list[InventoryMovementEvent | BackorderEvent] = []

    # Track which orders were touched so we can update order-level status
    touched_order_ids: set[str] = set()

    for item in allocatable:
        order = item["order"]
        order_id = item["order_id"]
        line = item["line"]
        need = item["need"]

        warehouse_id = _warehouse_for_line(line)
        sku = line["sku"]

        wh_inv = state.inventory.get(warehouse_id, {})
        available = wh_inv.get(sku, 0)

        if available == 0:
            # No stock at all — backorder the full need
            line["quantity_backordered"] = line.get("quantity_backordered", 0) + need
            line["line_status"] = "backordered"
            if "backorder_days" not in line:
                line["backorder_days"] = 0

            events.append(
                BackorderEvent(
                    event_id=str(uuid.uuid4()),
                    backorder_date=sim_date.isoformat(),
                    order_id=order_id,
                    order_line_id=line["line_id"],
                    sku=sku,
                    quantity_backordered=need,
                    quantity_allocated=line.get("quantity_allocated", 0),
                    backorder_reason="no_stock",
                    days_backordered=line.get("backorder_days", 0),
                    escalated_to_express=False,
                )
            )

        elif available >= need:
            # Full fill
            state.inventory[warehouse_id][sku] = available - need
            line["quantity_allocated"] = line.get("quantity_allocated", 0) + need
            # If this line was previously backordered, clear the backorder qty
            line["quantity_backordered"] = 0
            line["backorder_days"] = 0
            line["line_status"] = "allocated"

            events.append(
                _make_event(
                    state,
                    sim_date,
                    movement_type="pick",
                    warehouse_id=warehouse_id,
                    sku=sku,
                    quantity=need,
                    quantity_direction="out",
                    order_id=order_id,
                    order_line_id=line["line_id"],
                )
            )

        else:
            # Partial fill: allocate what's available, backorder the rest
            remainder = need - available

            state.inventory[warehouse_id][sku] = 0
            line["quantity_allocated"] = line.get("quantity_allocated", 0) + available
            # Replace backordered qty with the new remainder
            line["quantity_backordered"] = remainder
            line["line_status"] = "partially_allocated"
            if "backorder_days" not in line:
                line["backorder_days"] = 0

            events.append(
                _make_event(
                    state,
                    sim_date,
                    movement_type="pick",
                    warehouse_id=warehouse_id,
                    sku=sku,
                    quantity=available,
                    quantity_direction="out",
                    order_id=order_id,
                    order_line_id=line["line_id"],
                )
            )

            events.append(
                BackorderEvent(
                    event_id=str(uuid.uuid4()),
                    backorder_date=sim_date.isoformat(),
                    order_id=order_id,
                    order_line_id=line["line_id"],
                    sku=sku,
                    quantity_backordered=remainder,
                    quantity_allocated=line.get("quantity_allocated", 0),
                    backorder_reason="insufficient_stock",
                    days_backordered=line.get("backorder_days", 0),
                    escalated_to_express=False,
                )
            )

        touched_order_ids.add(order_id)

    # Update order-level status for every touched order
    for order_id in touched_order_ids:
        order = state.open_orders.get(order_id)
        if order is None:
            continue
        _update_order_status(order)

    return events


def _update_order_status(order: dict[str, Any]) -> None:
    """Recalculate and update the order-level status from its lines.

    Delegates to the shared :func:`~flowform.engines._order_utils.update_order_status`
    utility so both the allocation and modifications engines stay in sync.

    Args:
        order: Order dict (mutated in place).
    """
    update_order_status(order)


# ---------------------------------------------------------------------------
# Engine entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[AllocationEngineEvent]:
    """Run the allocation engine for *sim_date*.

    Step 1: Business day guard — returns [] on weekends and Polish holidays.
    Step 2: Escalation pass — increment ``backorder_days`` for waiting lines;
            auto-cancel stale backorders (>30 days, 15% probability);
            escalate >14 day standard backorders to express.
    Step 3: Collect all allocatable lines and sort by priority.
    Step 4: Allocate each line against on-hand inventory; emit pick movements
            and/or backorder events.

    ``state.inventory`` and ``state.open_orders`` are mutated in place.
    The caller is responsible for persisting state after the day loop.

    Args:
        state:    Mutable simulation state.
        config:   Validated simulation config (kept for contract consistency;
                  not directly referenced in this engine).
        sim_date: Current simulation date.

    Returns:
        List of :class:`InventoryMovementEvent`, :class:`BackorderEvent`,
        and :class:`BackorderCancellationEvent` instances (may be empty).
    """
    if not is_business_day(sim_date):
        return []

    events: list[AllocationEngineEvent] = []

    # --- Pass 1: Escalation / cancellation of existing backorders ---
    events.extend(_run_escalation(state, sim_date))

    # --- Pass 2: Allocate open lines ---
    allocatable = _collect_allocatable_lines(state)
    events.extend(_run_allocation(state, sim_date, allocatable))

    return events
