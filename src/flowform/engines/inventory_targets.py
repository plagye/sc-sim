"""Inventory target calculator — Step 36.

Fires once per month, on the 1st business day of the month.
Samples 10–20 SKUs from the catalog and emits one InventoryTargetEvent per
SKU per warehouse (W01, W02). Stock levels are derived from current on-hand
inventory with random multipliers, giving downstream teams a sense of desired
safety stock, reorder points, and target stock levels.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Literal

from pydantic import BaseModel

from flowform.calendar import is_business_day
from flowform.config import Config
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Event schema
# ---------------------------------------------------------------------------

_WAREHOUSES = ("W01", "W02")


class InventoryTargetEvent(BaseModel):
    event_type: Literal["inventory_target"] = "inventory_target"
    target_id: str       # "TGT-20260301-0001"
    timestamp: str       # "2026-03-01T07:30:00Z"
    sku: str             # "FF-BL100-S316-PN25-FP"
    warehouse: str       # "W01" or "W02"
    safety_stock: int
    reorder_point: int
    target_stock: int
    review_trigger: str  # "demand_plan_update"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _is_first_business_day_of_month(sim_date: date) -> bool:
    """Return True if sim_date is the first business day of its month."""
    if not is_business_day(sim_date):
        return False
    for day_offset in range(1, sim_date.day):
        earlier = sim_date - timedelta(days=day_offset)
        if is_business_day(earlier):
            return False
    return True


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[InventoryTargetEvent]:
    """Generate monthly inventory targets on the 1st business day of the month.

    Args:
        state:    Mutable simulation state (provides catalog, inventory, rng).
        config:   Simulation configuration (unused directly but kept for
                  interface consistency).
        sim_date: The current simulation date.

    Returns:
        A list of InventoryTargetEvent objects (one per SKU per warehouse),
        or an empty list if today is not the first business day of the month.
    """
    if not _is_first_business_day_of_month(sim_date):
        return []

    rng = state.rng
    date_str = sim_date.strftime("%Y%m%d")
    timestamp = f"{sim_date}T07:30:00Z"

    # Initialise counter if not present
    if "target" not in state.counters:
        state.counters["target"] = 0

    # Sample 10–20 distinct SKUs from the full catalog
    n_skus = rng.randint(10, 20)
    n_skus = min(n_skus, len(state.catalog))
    sampled_entries = rng.sample(state.catalog, n_skus)

    events: list[InventoryTargetEvent] = []

    for entry in sampled_entries:
        sku = entry.sku
        for wh in _WAREHOUSES:
            base = state.inventory.get((sku, wh), {}).get("on_hand", 50)

            safety_stock = max(1, round(rng.uniform(0.10, 0.25) * base))
            reorder_point = max(1, round(rng.uniform(1.4, 1.8) * safety_stock))
            target_stock = max(1, round(rng.uniform(2.5, 3.5) * safety_stock))

            state.counters["target"] += 1
            target_id = f"TGT-{date_str}-{state.counters['target']:04d}"

            events.append(
                InventoryTargetEvent(
                    target_id=target_id,
                    timestamp=timestamp,
                    sku=sku,
                    warehouse=wh,
                    safety_stock=safety_stock,
                    reorder_point=reorder_point,
                    target_stock=target_stock,
                    review_trigger="demand_plan_update",
                )
            )

    return events
