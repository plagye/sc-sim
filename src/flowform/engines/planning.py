"""Demand plan generator — Step 34.

Fires once per month, on the 1st business day of the month.
Emits one DemandPlanEvent covering a 6-month planning horizon.
"""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel

from flowform.adm._calibration import _expected_monthly_units_per_group
from flowform.calendar import is_business_day, is_first_business_day_of_month
from flowform.config import Config
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Segment factors
# ---------------------------------------------------------------------------

_SEGMENT_FACTORS: dict[str, float] = {
    "oil_gas": 1.3,
    "water_utilities": 1.1,
    "chemical_plants": 0.9,
    "industrial_distributors": 1.2,
    "hvac_contractors": 0.8,
    "shipyards": 0.6,
}

# ---------------------------------------------------------------------------
# Event schemas
# ---------------------------------------------------------------------------


class PlanPeriod(BaseModel):
    month: str  # "2026-03"
    planned_quantity: int
    confidence_low: int
    confidence_high: int


class PlanLine(BaseModel):
    product_group: str  # e.g. "BL-DN100-S316"
    customer_segment: str
    periods: list[PlanPeriod]


class DemandPlanEvent(BaseModel):
    event_type: Literal["demand_plan"] = "demand_plan"
    plan_id: str  # "DP-2026-03"
    timestamp: str
    planning_horizon_months: int = 6
    plan_lines: list[PlanLine]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_product_group(sku: str) -> str:
    """Derive product-group from a catalog SKU.

    SKU format: ``FF-{TYPE}{DN}-{MAT}-{PN}-{CONN}{ACT}``
    Returns ``{type_code}-DN{dn}-{mat}`` e.g. ``BL-DN100-S316``.
    """
    parts = sku.split("-")
    type_dn = parts[1]
    type_code = type_dn[:2]
    dn = type_dn[2:]
    mat = parts[2]
    return f"{type_code}-DN{dn}-{mat}"


def _month_str(year: int, month: int) -> str:
    """Return 'YYYY-MM' for given year/month, handling month overflow."""
    while month > 12:
        month -= 12
        year += 1
    return f"{year}-{month:02d}"


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[DemandPlanEvent]:
    """Generate a monthly demand plan on the 1st business day of the month.

    Args:
        state:    Mutable simulation state (provides catalog and rng).
        config:   Simulation configuration.
        sim_date: The current simulation date.

    Returns:
        A list containing exactly one DemandPlanEvent, or an empty list if
        today is not the first business day of the month.
    """
    if not is_first_business_day_of_month(sim_date):
        return []

    rng = state.rng

    # --- Sample 8–15 distinct product groups from the catalog ---
    all_product_groups = list(
        {_parse_product_group(entry.sku) for entry in state.catalog}
    )
    n_groups = rng.randint(8, 15)
    # Clamp in case catalog is tiny in tests
    n_groups = min(n_groups, len(all_product_groups))
    selected_groups: list[str] = rng.sample(all_product_groups, n_groups)
    selected_groups.sort()  # deterministic ordering

    segments = list(_SEGMENT_FACTORS.keys())

    # Compute calibrated baseline once — expected monthly units per product group
    baseline = _expected_monthly_units_per_group(state, config)

    plan_lines: list[PlanLine] = []

    for product_group in selected_groups:
        for segment in segments:
            seg_factor = _SEGMENT_FACTORS[segment]
            periods: list[PlanPeriod] = []

            for offset in range(6):
                target_month = sim_date.month + offset
                target_year = sim_date.year
                while target_month > 12:
                    target_month -= 12
                    target_year += 1

                monthly_mult = config.demand.base_monthly_multipliers[target_month]
                base_qty = round(baseline.get(product_group, rng.randint(80, 600)))
                planned = round(base_qty * monthly_mult * seg_factor)
                # Ensure non-zero plan when baseline is positive (rounding can floor to 0)
                if base_qty > 0 and planned == 0:
                    planned = 1
                conf_low = round(planned * 0.75)
                conf_high = round(planned * 1.30)

                periods.append(
                    PlanPeriod(
                        month=_month_str(target_year, target_month),
                        planned_quantity=planned,
                        confidence_low=conf_low,
                        confidence_high=conf_high,
                    )
                )

            plan_lines.append(
                PlanLine(
                    product_group=product_group,
                    customer_segment=segment,
                    periods=periods,
                )
            )

    plan_id = f"DP-{sim_date.year}-{sim_date.month:02d}"
    timestamp = f"{sim_date}T07:00:00Z"

    return [
        DemandPlanEvent(
            plan_id=plan_id,
            timestamp=timestamp,
            plan_lines=plan_lines,
        )
    ]
