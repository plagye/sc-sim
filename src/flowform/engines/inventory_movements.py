"""Inventory movement tracker for FlowForm Industries.

Generates ``inventory_movements`` ERP events for the following movement types:

- **receipt**        — mirrors production completions already recorded in
                       ``state.daily_production_receipts`` (populated by the
                       production engine each business day).
- **transfer**       — inter-warehouse transfers between W01 and W02
                       (0–2 per day, probabilistic).
- **adjustment**     — cycle-count adjustments, ±5% of on-hand quantity
                       (~5% chance per day, 1–5 SKUs).
- **scrap**          — disposal of damaged/expired stock
                       (~1% chance per day, 1–3 SKUs).

``pick``, ``unpick``, and ``return_restock`` movements are *not* generated
here.  Picks are emitted by the allocation engine (Step 19); unpicks by the
modification/cancellation engine (Step 20); return restocks by the returns
engine (Step 28).

Runs on **business days only**.  Weekends and Polish public holidays return
an empty list.

Engine entry point: ``run(state, config, date) -> list[InventoryMovementEvent]``
"""

from __future__ import annotations

import uuid
from datetime import date
from typing import Any, Literal

from pydantic import BaseModel

from flowform.calendar import is_business_day
from flowform.config import Config
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Probability that any transfers occur on a given business day
_TRANSFER_PROB = 0.40
# Maximum number of transfers per day when they do occur
_TRANSFER_MAX = 2

# Probability that a cycle-count adjustment run happens today
_ADJUSTMENT_PROB = 0.05
# How many SKUs get adjusted when it does happen
_ADJUSTMENT_SKU_MIN = 1
_ADJUSTMENT_SKU_MAX = 5
# Adjustment size: ±fraction of on-hand quantity
_ADJUSTMENT_MAX_FRACTION = 0.05

# Probability of a scrap event today
_SCRAP_PROB = 0.01
# SKUs scrapped when it does happen
_SCRAP_SKU_MIN = 1
_SCRAP_SKU_MAX = 3
# Maximum fraction of on-hand quantity scrapped per SKU
_SCRAP_MAX_FRACTION = 0.03

# Maximum fraction of W01 on-hand quantity transferred in a single transfer
_TRANSFER_MAX_FRACTION = 0.10

# Reason codes
_ADJUST_REASONS = ["cycle_count", "system_correction", "count_discrepancy"]
_SCRAP_REASONS = ["quality_reject", "expired", "damaged_storage"]


# ---------------------------------------------------------------------------
# Event schema
# ---------------------------------------------------------------------------


class InventoryMovementEvent(BaseModel):
    """A single inventory movement record in the ERP system.

    ``quantity`` is always a positive integer; direction is conveyed by
    ``quantity_direction`` (``"in"`` increases on-hand, ``"out"`` decreases
    it).
    """

    event_type: Literal["inventory_movements"] = "inventory_movements"
    event_id: str
    movement_id: str                         # e.g. "MOV-1001"
    movement_type: Literal[
        "pick",
        "receipt",
        "unpick",
        "transfer",
        "adjustment",
        "scrap",
        "return_restock",
    ]
    movement_date: str                       # ISO date
    warehouse_id: str                        # source warehouse
    destination_warehouse_id: str | None     # transfers only
    sku: str
    quantity: int                            # always positive
    quantity_direction: Literal["in", "out"]
    order_id: str | None                     # picks / unpicks
    order_line_id: str | None               # picks / unpicks
    production_batch_id: str | None         # receipts
    reason_code: str | None                 # adjustments / scrap
    reference_id: str | None                # generic cross-reference


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _make_event(
    state: SimulationState,
    sim_date: date,
    movement_type: str,
    warehouse_id: str,
    sku: str,
    quantity: int,
    quantity_direction: str,
    *,
    destination_warehouse_id: str | None = None,
    order_id: str | None = None,
    order_line_id: str | None = None,
    production_batch_id: str | None = None,
    reason_code: str | None = None,
    reference_id: str | None = None,
) -> InventoryMovementEvent:
    """Build and return a single :class:`InventoryMovementEvent`.

    Allocates a new ``movement_id`` from the state counters.
    """
    if "movement" not in state.counters:
        state.counters["movement"] = 1000
    state.counters["movement"] += 1
    movement_id = f"MOV-{state.counters['movement']}"

    return InventoryMovementEvent(
        event_id=str(uuid.uuid4()),
        movement_id=movement_id,
        movement_type=movement_type,  # type: ignore[arg-type]
        movement_date=sim_date.isoformat(),
        warehouse_id=warehouse_id,
        destination_warehouse_id=destination_warehouse_id,
        sku=sku,
        quantity=quantity,
        quantity_direction=quantity_direction,  # type: ignore[arg-type]
        order_id=order_id,
        order_line_id=order_line_id,
        production_batch_id=production_batch_id,
        reason_code=reason_code,
        reference_id=reference_id,
    )


def _generate_receipts(
    state: SimulationState,
    sim_date: date,
) -> list[InventoryMovementEvent]:
    """Create receipt movement events for today's production completions.

    Reads ``state.daily_production_receipts`` which the production engine
    populates on the same day.

    Args:
        state:    Current simulation state.
        sim_date: Current simulation date.

    Returns:
        One :class:`InventoryMovementEvent` per production receipt.
    """
    events: list[InventoryMovementEvent] = []
    for receipt in state.daily_production_receipts:
        events.append(
            _make_event(
                state,
                sim_date,
                movement_type="receipt",
                warehouse_id=receipt["warehouse_id"],
                sku=receipt["sku"],
                quantity=receipt["quantity"],
                quantity_direction="in",
                production_batch_id=receipt["batch_id"],
                reference_id=f"GRADE-{receipt['grade']}",
            )
        )
    return events


def _generate_transfers(
    state: SimulationState,
    sim_date: date,
) -> list[InventoryMovementEvent]:
    """Randomly generate inter-warehouse transfers (W01 → W02 or W02 → W01).

    Each transfer:
      - Selects a random SKU with on-hand stock in the source warehouse.
      - Transfers up to ``_TRANSFER_MAX_FRACTION`` of source stock.
      - Updates ``state.inventory`` for both warehouses in place.

    Args:
        state:    Current simulation state (mutated in place).
        sim_date: Current simulation date.

    Returns:
        List of :class:`InventoryMovementEvent` (may be empty).
    """
    if state.rng.random() >= _TRANSFER_PROB:
        return []

    n_transfers = state.rng.randint(1, _TRANSFER_MAX)
    events: list[InventoryMovementEvent] = []

    for _ in range(n_transfers):
        # Pick direction: 70% W01→W02, 30% W02→W01
        if state.rng.random() < 0.70:
            src, dst = "W01", "W02"
        else:
            src, dst = "W02", "W01"

        # Find SKUs with positive stock in source
        src_inv = state.inventory.get(src, {})
        stocked_skus = [sku for sku, qty in src_inv.items() if qty > 0]
        if not stocked_skus:
            continue

        sku = state.rng.choice(stocked_skus)
        on_hand = src_inv[sku]
        max_transfer = max(1, int(on_hand * _TRANSFER_MAX_FRACTION))
        qty = state.rng.randint(1, max(1, max_transfer))

        # Guard: never transfer more than available
        qty = min(qty, on_hand)
        if qty <= 0:
            continue

        # Mutate inventory
        state.inventory[src][sku] = on_hand - qty
        dst_inv = state.inventory.setdefault(dst, {})
        dst_inv[sku] = dst_inv.get(sku, 0) + qty

        # Emit outbound leg (source)
        events.append(
            _make_event(
                state,
                sim_date,
                movement_type="transfer",
                warehouse_id=src,
                sku=sku,
                quantity=qty,
                quantity_direction="out",
                destination_warehouse_id=dst,
            )
        )
        # Emit inbound leg (destination)
        events.append(
            _make_event(
                state,
                sim_date,
                movement_type="transfer",
                warehouse_id=dst,
                sku=sku,
                quantity=qty,
                quantity_direction="in",
                destination_warehouse_id=None,
                reference_id=src,
            )
        )

    return events


def _generate_adjustments(
    state: SimulationState,
    sim_date: date,
) -> list[InventoryMovementEvent]:
    """Generate cycle-count adjustment events.

    Runs with probability ``_ADJUSTMENT_PROB`` per business day.  Picks 1–5
    random SKUs across warehouses and applies a small ±quantity correction.
    Inventory is updated in place.

    Args:
        state:    Current simulation state (mutated in place).
        sim_date: Current simulation date.

    Returns:
        List of :class:`InventoryMovementEvent` (may be empty).
    """
    if state.rng.random() >= _ADJUSTMENT_PROB:
        return []

    n_skus = state.rng.randint(_ADJUSTMENT_SKU_MIN, _ADJUSTMENT_SKU_MAX)
    events: list[InventoryMovementEvent] = []

    # Gather all (warehouse, sku) pairs with stock
    candidates: list[tuple[str, str, int]] = []
    for wh_code, wh_inv in state.inventory.items():
        for sku, qty in wh_inv.items():
            if qty > 0:
                candidates.append((wh_code, sku, qty))

    if not candidates:
        return []

    chosen = state.rng.sample(candidates, min(n_skus, len(candidates)))

    for wh_code, sku, on_hand in chosen:
        max_delta = max(1, int(on_hand * _ADJUSTMENT_MAX_FRACTION))
        delta = state.rng.randint(1, max(1, max_delta))
        # Randomly increase or decrease; never go negative
        if state.rng.random() < 0.5:
            direction: Literal["in", "out"] = "in"
            state.inventory[wh_code][sku] = on_hand + delta
        else:
            direction = "out"
            actual_delta = min(delta, on_hand)
            state.inventory[wh_code][sku] = on_hand - actual_delta
            delta = actual_delta

        reason = state.rng.choice(_ADJUST_REASONS)
        events.append(
            _make_event(
                state,
                sim_date,
                movement_type="adjustment",
                warehouse_id=wh_code,
                sku=sku,
                quantity=delta,
                quantity_direction=direction,
                reason_code=reason,
            )
        )

    return events


def _generate_scrap(
    state: SimulationState,
    sim_date: date,
) -> list[InventoryMovementEvent]:
    """Generate scrap events for damaged or expired inventory.

    Runs with probability ``_SCRAP_PROB`` per business day.  Picks 1–3 random
    SKUs from W01 (primary warehouse) and scraps a small fraction.  Inventory
    is updated in place.

    Args:
        state:    Current simulation state (mutated in place).
        sim_date: Current simulation date.

    Returns:
        List of :class:`InventoryMovementEvent` (may be empty).
    """
    if state.rng.random() >= _SCRAP_PROB:
        return []

    n_skus = state.rng.randint(_SCRAP_SKU_MIN, _SCRAP_SKU_MAX)
    events: list[InventoryMovementEvent] = []

    w01_inv = state.inventory.get("W01", {})
    stocked = [(sku, qty) for sku, qty in w01_inv.items() if qty > 0]
    if not stocked:
        return []

    chosen = state.rng.sample(stocked, min(n_skus, len(stocked)))

    for sku, on_hand in chosen:
        max_scrap = max(1, int(on_hand * _SCRAP_MAX_FRACTION))
        qty = state.rng.randint(1, max(1, max_scrap))
        qty = min(qty, on_hand)
        if qty <= 0:
            continue

        state.inventory["W01"][sku] = on_hand - qty
        reason = state.rng.choice(_SCRAP_REASONS)
        events.append(
            _make_event(
                state,
                sim_date,
                movement_type="scrap",
                warehouse_id="W01",
                sku=sku,
                quantity=qty,
                quantity_direction="out",
                reason_code=reason,
            )
        )

    return events


# ---------------------------------------------------------------------------
# Engine entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[InventoryMovementEvent]:
    """Generate inventory movement events for *sim_date*.

    Processes four sub-categories in order:

    1. Receipts from today's production completions
       (reads ``state.daily_production_receipts``).
    2. Inter-warehouse transfers (probabilistic, mutates ``state.inventory``).
    3. Cycle-count adjustments (probabilistic, mutates ``state.inventory``).
    4. Scrap events (probabilistic, mutates ``state.inventory``).

    On non-business days (weekends and Polish public holidays) returns an
    empty list immediately.

    Args:
        state:    Mutable simulation state.  ``state.inventory`` and
                  ``state.counters`` are updated in place.
        config:   Validated simulation config (unused directly; kept for
                  engine contract consistency).
        sim_date: Current simulation date.

    Returns:
        List of :class:`InventoryMovementEvent` instances (may be empty).
    """
    if not is_business_day(sim_date):
        return []

    events: list[InventoryMovementEvent] = []

    events.extend(_generate_receipts(state, sim_date))
    events.extend(_generate_transfers(state, sim_date))
    events.extend(_generate_adjustments(state, sim_date))
    events.extend(_generate_scrap(state, sim_date))

    return events
