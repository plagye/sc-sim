"""Shared order-status utilities used by multiple engines.

Extracted from the allocation and modifications engines to avoid duplication.
"""

from __future__ import annotations

from typing import Any


def update_order_status(order: dict[str, Any]) -> None:
    """Recalculate and update the order-level status from its lines.

    Rules:
    - All lines "allocated"             → "allocated"
    - All lines "cancelled"             → "cancelled"
    - All lines either "allocated" or "cancelled" → "allocated"
    - Any line "backordered" or "partially_allocated" → "partially_allocated"
    - Otherwise: leave status unchanged (e.g. still "open" / "confirmed")

    Args:
        order: Order dict (mutated in place).
    """
    lines = order.get("lines", [])
    if not lines:
        return

    statuses = {line.get("line_status", "open") for line in lines}

    if statuses == {"allocated"}:
        order["status"] = "allocated"
    elif statuses == {"cancelled"}:
        order["status"] = "cancelled"
    elif statuses <= {"allocated", "cancelled"}:
        # All either allocated or cancelled — treat as allocated
        order["status"] = "allocated"
    elif statuses <= {"shipped", "cancelled"}:
        order["status"] = "shipped"
    elif "shipped" in statuses:
        order["status"] = "partially_shipped"
    elif "backordered" in statuses or "partially_allocated" in statuses:
        # {'allocated', 'backordered'} → 'partially_allocated': some lines picked,
        # some still waiting for stock.
        order["status"] = "partially_allocated"
    # else: leave status unchanged
