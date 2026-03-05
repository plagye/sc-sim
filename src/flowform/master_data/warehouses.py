"""Warehouse master data: W01 Katowice (main) and W02 Gdańsk (port)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class Warehouse:
    code: str              # W01, W02
    name: str
    city: str
    country_code: str      # always PL
    role: str              # "main" | "port"
    inventory_share: float # fraction of total inventory held here


# ---------------------------------------------------------------------------
# Static definitions — these never change during a simulation run
# ---------------------------------------------------------------------------

W01 = Warehouse(
    code="W01",
    name="FlowForm Katowice Distribution Centre",
    city="Katowice",
    country_code="PL",
    role="main",
    inventory_share=0.80,
)

W02 = Warehouse(
    code="W02",
    name="FlowForm Gdańsk Port Warehouse",
    city="Gdańsk",
    country_code="PL",
    role="port",
    inventory_share=0.20,
)

ALL_WAREHOUSES: Final[list[Warehouse]] = [W01, W02]

# Lookup by code
WAREHOUSE_BY_CODE: Final[dict[str, Warehouse]] = {w.code: w for w in ALL_WAREHOUSES}


def get_warehouse(code: str) -> Warehouse:
    """Return a Warehouse by its code, raising KeyError if not found."""
    return WAREHOUSE_BY_CODE[code]
