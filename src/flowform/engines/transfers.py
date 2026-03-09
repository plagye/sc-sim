"""Inter-warehouse transfer (replenishment) engine for FlowForm Industries.

Implements demand-pull replenishment from W01 (Katowice) to W02 (Gdansk).
W02 is replenished when its on-hand stock drops below 30% of W01's on-hand
quantity for the same SKU. Runs on business days only.

Engine entry point: run(state, config, sim_date) -> list[InventoryMovementEvent]
"""

from __future__ import annotations

import math
import uuid
from datetime import date

from flowform.calendar import is_business_day
from flowform.config import Config
from flowform.engines.inventory_movements import InventoryMovementEvent
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_REPLENISHMENT_PROB: float = 0.70       # probability the engine fires at all
_REPLENISHMENT_THRESHOLD: float = 0.30  # W02 < 30% of W01 triggers eligibility
_REPLENISHMENT_TARGET: float = 0.25     # transfer qty targets 25% of W01
_MAX_SKUS_PER_DAY: int = 5              # cap on distinct SKUs replenished per day


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _make_transfer_event(
    state: SimulationState,
    sim_date: date,
    warehouse_id: str,
    sku: str,
    qty_delta: int,
    direction: str,
    *,
    destination_warehouse_id: str | None,
    reason_code: str,
) -> InventoryMovementEvent:
    """Build one :class:`InventoryMovementEvent` for a replenishment leg.

    Allocates a new movement_id from ``state.counters["movement"]``, using
    the same counter as ``inventory_movements._make_event()``.

    Args:
        state:                    Mutable simulation state (counter incremented).
        sim_date:                 The current simulation date.
        warehouse_id:             Source warehouse for this leg.
        sku:                      SKU being transferred.
        qty_delta:                Absolute transfer quantity (always positive).
        direction:                "in" or "out".
        destination_warehouse_id: "W02" for the outbound leg, None for inbound.
        reason_code:              Always "replenishment".

    Returns:
        A fully populated :class:`InventoryMovementEvent`.
    """
    movement_id = state.next_movement_id()

    return InventoryMovementEvent(
        event_id=str(uuid.uuid4()),
        movement_id=movement_id,
        movement_type="transfer",
        movement_date=sim_date.isoformat(),
        warehouse_id=warehouse_id,
        destination_warehouse_id=destination_warehouse_id,
        sku=sku,
        quantity=qty_delta,
        quantity_direction=direction,  # type: ignore[arg-type]
        order_id=None,
        order_line_id=None,
        production_batch_id=None,
        reason_code=reason_code,
        reference_id="W01" if warehouse_id == "W02" else None,
    )


def _find_eligible_skus(
    state: SimulationState,
    threshold_pct: float = _REPLENISHMENT_THRESHOLD,
) -> list[tuple[str, int]]:
    """Identify SKUs where W02 is below the replenishment threshold.

    Only SKUs that W02 already carries (i.e. present in
    ``state.inventory["W02"]``) are considered.  The source W01 must have
    positive stock.

    Args:
        state:         Current simulation state.
        threshold_pct: Fraction of W01 on-hand below which W02 is eligible
                       for replenishment.  Defaults to the module constant;
                       at runtime this is sourced from config.

    Returns:
        Sorted list of ``(sku, transfer_qty)`` tuples for all eligible SKUs,
        ordered by SKU string for determinism.
    """
    w02_inv = state.inventory.get("W02", {})
    w01_inv = state.inventory.get("W01", {})

    eligible: list[tuple[str, int]] = []

    for sku, w02_qty in w02_inv.items():
        w01_qty = w01_inv.get(sku, 0)
        if w01_qty <= 0:
            continue  # no source stock

        threshold = math.ceil(threshold_pct * w01_qty)
        if w02_qty >= threshold:
            continue  # W02 adequately stocked

        # Compute transfer quantity
        target_qty = math.ceil(_REPLENISHMENT_TARGET * w01_qty)
        transfer_qty = max(1, target_qty - w02_qty)
        transfer_qty = min(transfer_qty, w01_qty)

        eligible.append((sku, transfer_qty))

    # Sort by SKU for deterministic ordering before RNG sampling
    eligible.sort(key=lambda t: t[0])
    return eligible


# ---------------------------------------------------------------------------
# Engine entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[InventoryMovementEvent]:
    """Run the inter-warehouse replenishment engine for *sim_date*.

    Implements demand-pull: W02 is replenished from W01 when its on-hand
    quantity drops below 30% of W01's level for the same SKU.

    Execution order:
      1. Business day guard — returns [] on weekends and Polish public holidays.
      2. Probability gate — 70% chance the engine fires at all.
      3. Find eligible SKUs (W02 below threshold, W01 has stock).
      4. Cap at _MAX_SKUS_PER_DAY; sample if more eligible.
      5. Emit two InventoryMovementEvent per SKU (outbound W01, inbound W02).
      6. Mutate state.inventory for both warehouses.

    Args:
        state:    Mutable simulation state (inventory + counters updated).
        config:   Validated simulation config (kept for engine contract).
        sim_date: The simulated calendar date to process.

    Returns:
        List of :class:`InventoryMovementEvent` instances (may be empty).
    """
    if not is_business_day(sim_date):
        return []

    # Probability gate: 70% chance the replenishment run fires
    if state.rng.random() >= _REPLENISHMENT_PROB:
        return []

    # Read threshold from config; fall back to module constant if not set
    threshold_pct = getattr(
        getattr(config, "company", None), "warehouse_transfer_threshold",
        _REPLENISHMENT_THRESHOLD,
    )

    eligible = _find_eligible_skus(state, threshold_pct=threshold_pct)
    if not eligible:
        return []

    # Cap at _MAX_SKUS_PER_DAY; use RNG sample when over the limit
    if len(eligible) > _MAX_SKUS_PER_DAY:
        selected = state.rng.sample(eligible, _MAX_SKUS_PER_DAY)
    else:
        selected = eligible

    events: list[InventoryMovementEvent] = []

    for sku, transfer_qty in selected:
        # Mutate inventory
        state.inventory["W01"][sku] -= transfer_qty
        w02 = state.inventory.setdefault("W02", {})
        w02[sku] = w02.get(sku, 0) + transfer_qty

        # Outbound leg: W01 → W02
        events.append(
            _make_transfer_event(
                state,
                sim_date,
                warehouse_id="W01",
                sku=sku,
                qty_delta=transfer_qty,
                direction="out",
                destination_warehouse_id="W02",
                reason_code="replenishment",
            )
        )

        # Inbound leg: arrival at W02
        events.append(
            _make_transfer_event(
                state,
                sim_date,
                warehouse_id="W02",
                sku=sku,
                qty_delta=transfer_qty,
                direction="in",
                destination_warehouse_id=None,
                reason_code="replenishment",
            )
        )

    return events
