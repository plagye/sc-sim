"""Modification and cancellation engine for FlowForm Industries.

Handles customer-initiated modifications and cancellations of open orders.
Backorder aging escalation/cancellation is handled by the allocation engine
(Step 19), not here.

Runs on business days only.

Engine entry point: ``run(state, config, date) -> list[Event]``
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
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

# Probability of a customer-initiated modification for any eligible order
_MODIFICATION_PROB = 0.10

# Probability of a customer-initiated cancellation for any eligible order
# (applied only to orders that were NOT modified today)
_CANCELLATION_PROB = 0.025

# Modification type weights: quantity_change, date_change, priority_change,
# line_removal, line_addition
_MODIFICATION_TYPES = [
    "quantity_change",
    "date_change",
    "priority_change",
    "line_removal",
    "line_addition",
]
_MODIFICATION_WEIGHTS = [35, 30, 20, 10, 5]

# For full vs. partial cancellation (multi-line orders)
_FULL_CANCEL_PROB = 0.60

# Eligible order statuses for modification / cancellation
_ELIGIBLE_STATUSES: frozenset[str] = frozenset({
    "confirmed",
    "allocated",
    "partially_allocated",
    "backordered",
    "partially_shipped",
})

# Line statuses that are still active (can be touched)
_ACTIVE_LINE_STATUSES: frozenset[str] = frozenset({
    "open",
    "confirmed",
    "allocated",
    "partially_allocated",
    "backordered",
})

# Default warehouse when a line doesn't carry one
_DEFAULT_WAREHOUSE = "W01"

# Quantity change bounds
_QTY_CHANGE_LO = 0.70
_QTY_CHANGE_HI = 1.50

# Date change bounds (days)
_DATE_CHANGE_LO = -5
_DATE_CHANGE_HI = 30

# Maximum quantity for a newly added line
_LINE_ADDITION_MAX_QTY = 20


# ---------------------------------------------------------------------------
# Event schemas
# ---------------------------------------------------------------------------


class OrderModificationEvent(BaseModel):
    """Emitted when a customer modifies an open order."""

    event_type: Literal["order_modifications"] = "order_modifications"
    event_id: str
    modification_date: str          # ISO date
    order_id: str
    modification_type: str          # quantity_change | date_change | line_addition | line_removal | priority_change
    line_id: str | None = None      # which line was modified (None for order-level changes)
    old_value: str | None = None    # string representation of old value
    new_value: str | None = None    # string representation of new value
    reason: str | None = None       # customer request, urgency, etc.


class OrderCancellationEvent(BaseModel):
    """Emitted when a customer cancels an open order (fully or partially)."""

    event_type: Literal["order_cancellations"] = "order_cancellations"
    event_id: str
    cancellation_date: str          # ISO date
    order_id: str
    cancellation_type: str          # "full" | "partial"
    cancelled_lines: list[str]      # list of line_ids cancelled
    reason: str                     # customer_request | duplicate_order | credit_issues | backorder_timeout
    total_quantity_cancelled: int


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _active_lines(order: dict[str, Any]) -> list[dict[str, Any]]:
    """Return lines that have not yet been shipped or cancelled.

    Args:
        order: Order dict from state.open_orders.

    Returns:
        Filtered list of line dicts.
    """
    return [
        line for line in order.get("lines", [])
        if line.get("line_status", "open") in _ACTIVE_LINE_STATUSES
        and line.get("line_status", "open") != "shipped"
    ]


def _warehouse_for_line(line: dict[str, Any]) -> str:
    """Return the warehouse for this line, defaulting to W01."""
    return line.get("warehouse_id", _DEFAULT_WAREHOUSE)


def _return_allocated_to_inventory(
    state: SimulationState,
    line: dict[str, Any],
    sim_date: date,
    order_id: str,
    unpick_events: list[InventoryMovementEvent],
) -> None:
    """Add a line's allocated quantity back to warehouse inventory.

    Also emits an ``InventoryMovementEvent`` with ``movement_type="unpick"``
    so the ERP inventory_movements file records the reversal.

    Args:
        state:         Mutable simulation state (inventory mutated in place).
        line:          Line dict whose allocated qty should be returned.
        sim_date:      Current simulation date (for the movement event).
        order_id:      Parent order ID (for the movement event reference).
        unpick_events: Accumulator list — the new unpick event is appended here.
    """
    allocated = line.get("quantity_allocated", 0)
    if allocated <= 0:
        return
    warehouse_id = _warehouse_for_line(line)
    sku = line["sku"]
    wh_inv = state.inventory.setdefault(warehouse_id, {})
    # Unallocate: the units are still physically in the warehouse (on_hand unchanged);
    # we just reduce the reservation counter.
    pos = wh_inv.get(sku)
    if pos is None:
        pos = {"on_hand": 0, "allocated": 0, "in_transit": 0}
        wh_inv[sku] = pos
    pos["allocated"] = max(0, pos["allocated"] - allocated)

    unpick_events.append(
        _make_event(
            state,
            sim_date,
            movement_type="unpick",
            warehouse_id=warehouse_id,
            sku=sku,
            quantity=allocated,
            quantity_direction="in",
            order_id=order_id,
            order_line_id=line.get("line_id"),
        )
    )


def _update_order_status(order: dict[str, Any]) -> None:
    """Recalculate order-level status from its lines.

    Delegates to the shared utility so both engines stay in sync.
    """
    update_order_status(order)


def _pick_customer_sku(
    state: SimulationState,
    customer_id: str,
) -> str:
    """Pick a random SKU for a new line addition.

    Tries to use the customer's sku_affinity weights; falls back to a
    uniform random pick from the catalog if the customer is not found.

    Args:
        state:       SimulationState with catalog and customers.
        customer_id: Customer whose affinity weights to use.

    Returns:
        SKU string.
    """
    # Find customer
    customer = None
    for c in state.customers:
        if c.customer_id == customer_id:
            customer = c
            break

    if customer is None or not state.catalog:
        # Fallback: random from catalog
        return state.rng.choice(state.catalog).sku

    from flowform.engines.orders import _build_sku_weights

    weights = _build_sku_weights(customer, state.catalog)
    entry = state.rng.choices(state.catalog, weights=weights, k=1)[0]
    return entry.sku


# ---------------------------------------------------------------------------
# Modification handlers
# ---------------------------------------------------------------------------


def _apply_quantity_change(
    state: SimulationState,
    order: dict[str, Any],
    sim_date: date,
    unpick_events: list[InventoryMovementEvent],
) -> OrderModificationEvent | None:
    """Change the quantity on a random non-shipped, non-cancelled line.

    Rules:
    - New qty = old qty * uniform(0.7, 1.5), rounded, min 1.
    - Cannot reduce below quantity_allocated + quantity_shipped.
    - If qty increased, the surplus goes to backordered.
    - If allocated qty is reduced, an unpick movement event is emitted.

    Args:
        state:         Mutable simulation state.
        order:         Order dict (mutated in place).
        sim_date:      Current simulation date.
        unpick_events: Accumulator for unpick movement events.
    """
    candidates = [
        line for line in order.get("lines", [])
        if line.get("line_status", "open") not in ("cancelled", "shipped")
    ]
    if not candidates:
        return None

    line = state.rng.choice(candidates)
    old_qty = line.get("quantity_ordered", 1)
    allocated = line.get("quantity_allocated", 0)
    shipped = line.get("quantity_shipped", 0)
    backordered = line.get("quantity_backordered", 0)
    committed = allocated + shipped

    # Compute new qty with floor at committed
    raw_new = int(round(old_qty * state.rng.uniform(_QTY_CHANGE_LO, _QTY_CHANGE_HI)))
    new_qty = max(1, raw_new)
    if new_qty < committed:
        new_qty = committed  # can't reduce below what's already gone out

    if new_qty == old_qty:
        # No actual change — skip to avoid noise
        return None

    # Adjust line fields
    line["quantity_ordered"] = new_qty
    order_id: str = order.get("order_id", "")

    if new_qty > old_qty:
        # Increase: extra goes to backorder
        extra = new_qty - old_qty
        line["quantity_backordered"] = backordered + extra
        if line.get("line_status") == "allocated":
            line["line_status"] = "partially_allocated"
        elif line.get("line_status") == "open":
            line["line_status"] = "backordered"
    else:
        # Decrease: remove from backordered first, then from allocated if needed
        reduction = old_qty - new_qty
        bo_reduction = min(backordered, reduction)
        line["quantity_backordered"] = backordered - bo_reduction

        remaining_reduction = reduction - bo_reduction
        if remaining_reduction > 0:
            # Need to release some reserved stock (unallocate it — on_hand unchanged)
            alloc_reduction = min(allocated, remaining_reduction)
            line["quantity_allocated"] = allocated - alloc_reduction
            _warehouse_return = _warehouse_for_line(line)
            sku = line["sku"]
            wh_inv = state.inventory.setdefault(_warehouse_return, {})
            pos = wh_inv.get(sku)
            if pos is None:
                pos = {"on_hand": 0, "allocated": 0, "in_transit": 0}
                wh_inv[sku] = pos
            pos["allocated"] = max(0, pos["allocated"] - alloc_reduction)

            if alloc_reduction > 0:
                unpick_events.append(
                    _make_event(
                        state,
                        sim_date,
                        movement_type="unpick",
                        warehouse_id=_warehouse_return,
                        sku=sku,
                        quantity=alloc_reduction,
                        quantity_direction="in",
                        order_id=order_id,
                        order_line_id=line.get("line_id"),
                    )
                )

    return OrderModificationEvent(
        event_id=str(uuid.uuid4()),
        modification_date=sim_date.isoformat(),
        order_id=order_id,
        modification_type="quantity_change",
        line_id=line["line_id"],
        old_value=str(old_qty),
        new_value=str(new_qty),
        reason="customer_request",
    )


def _apply_date_change(
    state: SimulationState,
    order: dict[str, Any],
    sim_date: date,
) -> OrderModificationEvent | None:
    """Change the requested_delivery_date on a random line (or order-level).

    New date = old date + randint(-5, 30), clamped so it's >= sim_date.
    """
    lines = order.get("lines", [])
    candidates = [
        line for line in lines
        if line.get("line_status", "open") not in ("cancelled", "shipped")
    ]
    if not candidates:
        return None

    line = state.rng.choice(candidates)

    # Parse old date — lines may not carry it; fall back to order-level
    old_date_str = (
        line.get("requested_delivery_date")
        or order.get("requested_delivery_date", sim_date.isoformat())
    )
    try:
        old_date = date.fromisoformat(old_date_str)
    except ValueError:
        old_date = sim_date

    delta = state.rng.randint(_DATE_CHANGE_LO, _DATE_CHANGE_HI)
    new_date = old_date + timedelta(days=delta)
    # Clamp: must not be in the past
    if new_date < sim_date:
        new_date = sim_date

    if new_date == old_date:
        return None

    # Persist: update the line if it carries its own date, else update order
    if "requested_delivery_date" in line:
        line["requested_delivery_date"] = new_date.isoformat()
    else:
        order["requested_delivery_date"] = new_date.isoformat()

    return OrderModificationEvent(
        event_id=str(uuid.uuid4()),
        modification_date=sim_date.isoformat(),
        order_id=order["order_id"],
        modification_type="date_change",
        line_id=line.get("line_id"),
        old_value=old_date.isoformat(),
        new_value=new_date.isoformat(),
        reason="customer_request",
    )


def _apply_priority_change(
    state: SimulationState,
    order: dict[str, Any],
    sim_date: date,
) -> OrderModificationEvent | None:
    """Escalate order priority (standard→express or critical; express→critical).

    Cannot escalate beyond critical.
    """
    current_priority: str = order.get("priority", "standard")

    if current_priority == "critical":
        return None  # already at max

    old_priority = current_priority
    if current_priority == "standard":
        # 80% → express, 20% → critical
        new_priority = state.rng.choices(
            ["express", "critical"], weights=[80, 20], k=1
        )[0]
    else:  # "express"
        new_priority = "critical"

    # Update order and all lines
    order["priority"] = new_priority
    for line in order.get("lines", []):
        line["priority"] = new_priority  # propagate to lines (some may carry it)

    return OrderModificationEvent(
        event_id=str(uuid.uuid4()),
        modification_date=sim_date.isoformat(),
        order_id=order["order_id"],
        modification_type="priority_change",
        line_id=None,
        old_value=old_priority,
        new_value=new_priority,
        reason="urgency",
    )


def _apply_line_removal(
    state: SimulationState,
    order: dict[str, Any],
    sim_date: date,
    unpick_events: list[InventoryMovementEvent],
) -> OrderModificationEvent | None:
    """Remove the line with the smallest quantity_ordered (only if >1 active line).

    Returns allocated inventory for the removed line to warehouse stock and
    emits an unpick movement event.

    Args:
        state:         Mutable simulation state.
        order:         Order dict (mutated in place).
        sim_date:      Current simulation date.
        unpick_events: Accumulator for unpick movement events.
    """
    active = _active_lines(order)
    if len(active) <= 1:
        return None  # can't remove last active line this way

    # Pick the line with the smallest quantity_ordered
    target = min(active, key=lambda ln: ln.get("quantity_ordered", 0))

    # Return allocated qty to inventory (and emit unpick event)
    order_id: str = order.get("order_id", "")
    _return_allocated_to_inventory(state, target, sim_date, order_id, unpick_events)

    # Remove from state.allocated tracking
    state.allocated.get(order_id, {}).pop(target.get("line_id", ""), None)

    # Cancel the line
    old_qty = target.get("quantity_ordered", 0)
    target["line_status"] = "cancelled"
    target["quantity_allocated"] = 0
    target["quantity_backordered"] = 0

    # Update order status — if all lines now cancelled, cancel the order
    _update_order_status(order)

    return OrderModificationEvent(
        event_id=str(uuid.uuid4()),
        modification_date=sim_date.isoformat(),
        order_id=order_id,
        modification_type="line_removal",
        line_id=target.get("line_id"),
        old_value=str(old_qty),
        new_value="0",
        reason="customer_request",
    )


def _apply_line_addition(
    state: SimulationState,
    order: dict[str, Any],
    sim_date: date,
    config: Config | None = None,
) -> OrderModificationEvent | None:
    """Add a new line to an open order.

    Picks a random SKU using the customer's affinity weights.  New line
    has quantity 1–20 and starts as "open" (will be allocated next cycle).
    """
    customer_id: str = order.get("customer_id", "")
    sku = _pick_customer_sku(state, customer_id)
    qty = state.rng.randint(1, _LINE_ADDITION_MAX_QTY)

    # Determine next line number
    existing_lines = order.get("lines", [])
    line_num = len(existing_lines) + 1
    line_id = f"{order['order_id']}-L{line_num}"

    # Find a unit_price for this SKU from the catalog, applying discount + conversion
    from flowform.catalog import pricing as _pricing
    from flowform.engines.orders import _make_rates_dict

    # Look up customer for discount + currency
    customer = None
    for c in state.customers:
        if c.customer_id == customer_id:
            customer = c
            break

    discount_pct: float = customer.contract_discount_pct if customer is not None else 0.0
    currency: str = customer.currency if customer is not None else order.get("currency", "PLN")
    rates = _make_rates_dict(state)

    unit_price = 0.0
    for entry in state.catalog:
        if entry.sku == sku:
            max_price = config.catalog.max_unit_price_pln if config is not None else None
            pln_price = _pricing.base_price_pln(entry.spec, max_price)
            discounted = _pricing.apply_customer_discount(pln_price, discount_pct)
            unit_price = _pricing.convert_price(discounted, currency, rates)
            break

    new_line: dict[str, Any] = {
        "line_id": line_id,
        "sku": sku,
        "quantity_ordered": qty,
        "quantity_allocated": 0,
        "quantity_shipped": 0,
        "quantity_backordered": 0,
        "unit_price": unit_price,
        "line_status": "open",
    }

    order["lines"].append(new_line)

    return OrderModificationEvent(
        event_id=str(uuid.uuid4()),
        modification_date=sim_date.isoformat(),
        order_id=order["order_id"],
        modification_type="line_addition",
        line_id=line_id,
        old_value=None,
        new_value=f"{sku}:{qty}",
        reason="customer_request",
    )


# ---------------------------------------------------------------------------
# Cancellation handler
# ---------------------------------------------------------------------------


def _apply_cancellation(
    state: SimulationState,
    order: dict[str, Any],
    order_id: str,
    sim_date: date,
    unpick_events: list[InventoryMovementEvent],
) -> OrderCancellationEvent:
    """Cancel an order (fully or partially).

    Multi-line orders: 60% full cancel, 40% partial (cancel 1 to half the lines).
    Single-line orders: always full cancel.

    Returns allocated inventory for every cancelled line and emits unpick events.

    Args:
        state:         Mutable simulation state.
        order:         Order dict (mutated in place).
        order_id:      ID of the order being cancelled.
        sim_date:      Current simulation date.
        unpick_events: Accumulator for unpick movement events.
    """
    active = _active_lines(order)
    n_active = len(active)

    # Determine cancellation type
    if n_active <= 1 or state.rng.random() < _FULL_CANCEL_PROB:
        # Full cancellation
        lines_to_cancel = active
        cancellation_type = "full"
    else:
        # Partial: cancel 1 to ceil(n_active / 2) lines
        import math
        max_cancel = math.ceil(n_active / 2)
        n_cancel = state.rng.randint(1, max_cancel)
        lines_to_cancel = state.rng.sample(active, n_cancel)
        cancellation_type = "partial"

    cancelled_line_ids: list[str] = []
    total_qty_cancelled = 0

    for line in lines_to_cancel:
        # Return allocated stock (and emit unpick event)
        _return_allocated_to_inventory(state, line, sim_date, order_id, unpick_events)

        qty_remaining = (
            line.get("quantity_ordered", 0) - line.get("quantity_shipped", 0)
        )
        total_qty_cancelled += max(0, qty_remaining)
        cancelled_line_ids.append(line.get("line_id", ""))

        line["line_status"] = "cancelled"
        line["quantity_allocated"] = 0
        line["quantity_backordered"] = 0

    # Recalculate order status
    _update_order_status(order)

    return OrderCancellationEvent(
        event_id=str(uuid.uuid4()),
        cancellation_date=sim_date.isoformat(),
        order_id=order_id,
        cancellation_type=cancellation_type,
        cancelled_lines=cancelled_line_ids,
        reason="customer_request",
        total_quantity_cancelled=total_qty_cancelled,
    )


# ---------------------------------------------------------------------------
# Engine entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[OrderModificationEvent | OrderCancellationEvent | InventoryMovementEvent]:
    """Run the modification and cancellation engine for *sim_date*.

    Step 1: Business day guard — returns [] on weekends and Polish holidays.
    Step 2: Modifications (10% of eligible orders):
        - quantity_change (35%), date_change (30%), priority_change (20%),
          line_removal (10%), line_addition (5%)
    Step 3: Cancellations (2.5% of eligible orders, excluding modified ones).

    ``state.open_orders`` and ``state.inventory`` are mutated in place.
    The caller is responsible for persisting state after the day loop.

    Returns unpick :class:`InventoryMovementEvent` instances alongside the
    modification and cancellation events so the ERP inventory_movements file
    is consistent with order state.

    Args:
        state:    Mutable simulation state.
        config:   Validated simulation config (kept for contract consistency;
                  not directly referenced here).
        sim_date: Current simulation date.

    Returns:
        List of :class:`OrderModificationEvent`,
        :class:`OrderCancellationEvent`, and :class:`InventoryMovementEvent`
        (``movement_type="unpick"``); may be empty.
    """
    if not is_business_day(sim_date):
        return []

    events: list[OrderModificationEvent | OrderCancellationEvent | InventoryMovementEvent] = []
    unpick_events: list[InventoryMovementEvent] = []
    modified_order_ids: set[str] = set()

    # --- Pass 1: Modifications ---
    for order_id, order in state.open_orders.items():
        status: str = order.get("status", "")
        if status not in _ELIGIBLE_STATUSES:
            continue

        if state.rng.random() >= _MODIFICATION_PROB:
            continue

        # Pick modification type
        mod_type = state.rng.choices(
            _MODIFICATION_TYPES, weights=_MODIFICATION_WEIGHTS, k=1
        )[0]

        event: OrderModificationEvent | None = None

        if mod_type == "quantity_change":
            event = _apply_quantity_change(state, order, sim_date, unpick_events)
        elif mod_type == "date_change":
            event = _apply_date_change(state, order, sim_date)
        elif mod_type == "priority_change":
            event = _apply_priority_change(state, order, sim_date)
        elif mod_type == "line_removal":
            event = _apply_line_removal(state, order, sim_date, unpick_events)
        elif mod_type == "line_addition":
            event = _apply_line_addition(state, order, sim_date, config)

        if event is not None:
            events.append(event)
            modified_order_ids.add(order_id)

    # --- Pass 2: Cancellations ---
    for order_id, order in state.open_orders.items():
        if order_id in modified_order_ids:
            continue  # already modified today — skip

        status = order.get("status", "")
        if status not in _ELIGIBLE_STATUSES:
            continue

        if state.rng.random() >= _CANCELLATION_PROB:
            continue

        canc_event = _apply_cancellation(state, order, order_id, sim_date, unpick_events)
        events.append(canc_event)

    # Append all unpick movement events at the end
    events.extend(unpick_events)

    return events
