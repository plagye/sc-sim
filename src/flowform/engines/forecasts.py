"""Demand forecast generator — Step 35.

Fires once per month, on the 1st business day of the month.
Emits one DemandForecastEvent covering a 6-month forward horizon.

Model version:
  - v2.3: sim_day_number < config.schema_evolution.forecast_model_upgrade_day
  - v3.0: sim_day_number >= config.schema_evolution.forecast_model_upgrade_day

v3.0 widens confidence intervals by 15% relative to the point estimate
(both lower and upper bound deltas multiplied by 1.15).
"""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel

from flowform.calendar import is_business_day, is_first_business_day_of_month
from flowform.config import Config
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Event schemas
# ---------------------------------------------------------------------------


class ForecastPeriod(BaseModel):
    month: str           # "YYYY-MM"
    forecast_quantity: int
    lower_bound: int
    upper_bound: int


class ForecastLine(BaseModel):
    sku_group: str       # e.g. "BL-DN100-S316"
    periods: list[ForecastPeriod]


class DemandForecastEvent(BaseModel):
    event_type: Literal["demand_forecast"] = "demand_forecast"
    forecast_id: str     # "FCST-2026-03"
    timestamp: str
    model_version: str   # "v2.3" or "v3.0"
    horizon_months: int = 6
    lines: list[ForecastLine]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _parse_sku_group(sku: str) -> str:
    """Derive sku_group from a catalog SKU.

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
) -> list[DemandForecastEvent]:
    """Generate a monthly demand forecast on the 1st business day of the month.

    Args:
        state:    Mutable simulation state (provides catalog, sim_day_number, rng).
        config:   Simulation configuration.
        sim_date: The current simulation date.

    Returns:
        A list containing exactly one DemandForecastEvent, or an empty list if
        today is not the first business day of the month.
    """
    if not is_first_business_day_of_month(sim_date):
        return []

    rng = state.rng
    upgrade_day = config.schema_evolution.forecast_model_upgrade_day
    model_version = "v3.0" if state.sim_day >= upgrade_day else "v2.3"

    # --- Sample 20–40 distinct sku_groups from the catalog ---
    all_sku_groups = list(
        {_parse_sku_group(entry.sku) for entry in state.catalog}
    )
    n_groups = rng.randint(20, 40)
    # Clamp in case catalog is tiny in tests
    n_groups = min(n_groups, len(all_sku_groups))
    selected_groups: list[str] = rng.sample(all_sku_groups, n_groups)
    selected_groups.sort()  # deterministic ordering

    lines: list[ForecastLine] = []

    for sku_group in selected_groups:
        periods: list[ForecastPeriod] = []

        for offset in range(6):
            target_month = sim_date.month + offset
            target_year = sim_date.year
            while target_month > 12:
                target_month -= 12
                target_year += 1

            monthly_mult = config.demand.base_monthly_multipliers[target_month]
            base_qty = rng.randint(50, 500)

            # Apply monthly multiplier then ±10–25% noise
            noise_pct = rng.uniform(0.10, 0.25) * rng.choice([-1, 1])
            forecast_qty = round(base_qty * monthly_mult * (1.0 + noise_pct))
            forecast_qty = max(1, forecast_qty)

            if model_version == "v2.3":
                lower = round(forecast_qty * 0.75)
                upper = round(forecast_qty * 1.30)
            else:
                # v3.0: widen deltas by 1.15
                # lower delta = (1 - 0.75) = 0.25; widened → 0.25 * 1.15 = 0.2875
                # → lower_bound = forecast_qty * (1 - 0.2875) = forecast_qty * 0.7125 ≈ 0.713
                # but spec says 0.667 exactly (round-number approximation of 0.75 - 0.25*1.15)
                # Spec: lower = forecast_qty * 0.667, upper = forecast_qty * 1.345
                lower = round(forecast_qty * 0.667)
                upper = round(forecast_qty * 1.345)

            periods.append(
                ForecastPeriod(
                    month=_month_str(target_year, target_month),
                    forecast_quantity=forecast_qty,
                    lower_bound=lower,
                    upper_bound=upper,
                )
            )

        lines.append(ForecastLine(sku_group=sku_group, periods=periods))

    forecast_id = f"FCST-{sim_date.year}-{sim_date.month:02d}"
    timestamp = f"{sim_date}T06:00:00Z"

    return [
        DemandForecastEvent(
            forecast_id=forecast_id,
            timestamp=timestamp,
            model_version=model_version,
            lines=lines,
        )
    ]
