"""Inventory snapshot engine: end-of-day full dump per warehouse.

Fires one InventorySnapshotEvent per warehouse at 23:59:00Z on every business
day.  Each event contains a full list of positions (one per SKU with positive
on-hand stock), including:

- quantity_on_hand  — physical units in the warehouse
- quantity_allocated — units picked/reserved for open orders (not yet shipped)
- quantity_available — on_hand minus allocated, clamped to >= 0

Because the allocation engine reduces ``state.inventory`` at pick time,
``state.inventory[wh][sku]`` already holds the *available* quantity.  To
reconstruct ``on_hand`` we add back units that are allocated-but-not-shipped
from open orders.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel

from flowform.calendar import is_business_day
from flowform.config import Config
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class InventoryPosition(BaseModel):
    sku: str
    quantity_on_hand: int
    quantity_allocated: int
    quantity_available: int


class InventorySnapshotEvent(BaseModel):
    event_type: Literal["inventory_snapshot"] = "inventory_snapshot"
    snapshot_id: str  # SNAP-YYYYMMDD-W01
    timestamp: str  # ISO 8601, T23:59:00Z
    warehouse: str  # "W01" or "W02"
    positions: list[InventoryPosition]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_allocated_by_sku(
    open_orders: dict[str, dict[str, Any]],
    warehouse_code: str,
) -> dict[str, int]:
    """Return per-SKU units allocated-but-not-shipped for *warehouse_code*.

    When the allocation engine picks units it:
    1. Decrements ``state.inventory[wh][sku]``.
    2. Increments ``line["quantity_allocated"]``.

    Units remain "allocated" until ``line["quantity_shipped"]`` catches up
    (i.e. line status reaches "shipped").  We only count lines that are
    still in transit — status "allocated", "partially_allocated", or similar
    open statuses.

    We also default to W01 for lines that carry no explicit warehouse_id,
    mirroring the ``_DEFAULT_WAREHOUSE`` constant in allocation.py.
    """
    _SHIPPED_STATUSES = {"shipped", "delivered", "cancelled"}
    _DEFAULT_WH = "W01"

    allocated_by_sku: dict[str, int] = {}

    for order in open_orders.values():
        status = order.get("status", "")
        if status in ("cancelled",):
            continue
        for line in order.get("lines", []):
            line_status = line.get("line_status", "open")
            if line_status in _SHIPPED_STATUSES:
                continue

            line_wh = line.get("warehouse_id", _DEFAULT_WH)
            if line_wh != warehouse_code:
                continue

            qty_allocated = line.get("quantity_allocated", 0)
            qty_shipped = line.get("quantity_shipped", 0)
            # Units still reserved = allocated - already shipped
            pending = max(0, qty_allocated - qty_shipped)
            if pending > 0:
                sku = line["sku"]
                allocated_by_sku[sku] = allocated_by_sku.get(sku, 0) + pending

    return allocated_by_sku


# ---------------------------------------------------------------------------
# Engine entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[InventorySnapshotEvent]:
    """Generate end-of-day inventory snapshots for every warehouse.

    Returns [] on weekends and Polish public holidays.
    """
    if not is_business_day(sim_date):
        return []

    date_str = sim_date.strftime("%Y%m%d")
    timestamp = f"{sim_date.isoformat()}T23:59:00Z"

    events: list[InventorySnapshotEvent] = []

    for warehouse in state.warehouses:
        wh_code = warehouse.code
        wh_inventory = state.inventory.get(wh_code, {})

        # Compute per-SKU allocated-but-not-shipped from open orders
        allocated_by_sku = _build_allocated_by_sku(state.open_orders, wh_code)

        positions: list[InventoryPosition] = []
        for sku, available_qty in wh_inventory.items():
            qty_allocated = allocated_by_sku.get(sku, 0)
            # on_hand = available (state.inventory) + allocated-but-not-shipped
            qty_on_hand = available_qty + qty_allocated
            if qty_on_hand <= 0:
                continue
            qty_available = max(0, qty_on_hand - qty_allocated)
            positions.append(
                InventoryPosition(
                    sku=sku,
                    quantity_on_hand=qty_on_hand,
                    quantity_allocated=qty_allocated,
                    quantity_available=qty_available,
                )
            )

        events.append(
            InventorySnapshotEvent(
                snapshot_id=f"SNAP-{date_str}-{wh_code}",
                timestamp=timestamp,
                warehouse=wh_code,
                positions=positions,
            )
        )

    return events
