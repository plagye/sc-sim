"""Schema evolution engine: emits notification events when field thresholds are crossed.

The orders and forecasts engines already apply schema evolution thresholds
directly — this engine only emits SchemaEvolutionEvent notifications so
downstream consumers know when fields appeared or models changed.

Triggers (first business day AT OR AFTER the sim_day threshold):
  - sim_day >= sales_channel_from_day (60): sales_channel field on customer_order
  - sim_day >= forecast_model_upgrade_day (90): forecast model v2.3 → v3.0
  - sim_day >= incoterms_from_day (120): incoterms field on customer_order

Each event fires at most once. A fired flag stored in state.schema_flags
ensures no duplicate notifications even when a threshold day falls on a
weekend or Polish public holiday.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel

from flowform.calendar import is_business_day

if TYPE_CHECKING:
    from flowform.config import Config
    from flowform.state import SimulationState


# ---------------------------------------------------------------------------
# Event schema
# ---------------------------------------------------------------------------


class SchemaEvolutionEvent(BaseModel):
    event_type: Literal["schema_evolution_event"] = "schema_evolution_event"
    evolution_id: str
    timestamp: str
    affected_system: str       # "erp" | "forecast"
    affected_event_type: str   # "customer_order" | "demand_forecast"
    field_name: str            # "sales_channel" | "incoterms" | "model_version"
    change_type: str           # "field_activation" | "model_upgrade"
    effective_from_day: int
    description: str


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


def run(
    state: "SimulationState",
    config: "Config",
    sim_date: date,
) -> list[SchemaEvolutionEvent]:
    """Emit SchemaEvolutionEvent notifications at configured day thresholds.

    Each notification fires once, on the first business day at or after the
    configured threshold day.  The fired state is tracked via ``state.schema_flags``
    using keys ``sevt_sales_channel``, ``sevt_model_version``, and ``sevt_incoterms``.

    Args:
        state:    Mutable simulation state.
        config:   Validated simulation config.
        sim_date: The simulated calendar date being processed.

    Returns:
        A list of 0–3 SchemaEvolutionEvent objects (usually 0 or 1).
    """
    if not is_business_day(sim_date):
        return []

    events: list[SchemaEvolutionEvent] = []
    evo = config.schema_evolution
    iso_ts = f"{sim_date.isoformat()}T09:00:00Z"

    # Ensure sevt counter exists (may not be present in older saved states)
    if "sevt" not in state.counters:
        state.counters["sevt"] = 0

    def _next_evol_id() -> str:
        state.counters["sevt"] += 1
        return f"SEVT-{sim_date.strftime('%Y%m%d')}-{state.counters['sevt']:04d}"

    # --- sales_channel activation ---
    if (
        state.sim_day >= evo.sales_channel_from_day
        and not state.schema_flags.get("sevt_sales_channel", False)
    ):
        state.schema_flags["sevt_sales_channel"] = True
        evol_id = _next_evol_id()
        events.append(SchemaEvolutionEvent(
            evolution_id=evol_id,
            timestamp=iso_ts,
            affected_system="erp",
            affected_event_type="customer_order",
            field_name="sales_channel",
            change_type="field_activation",
            effective_from_day=evo.sales_channel_from_day,
            description=(
                f"Field 'sales_channel' is now active on customer_order events "
                f"from sim day {evo.sales_channel_from_day} onwards."
            ),
        ))

    # --- forecast model upgrade ---
    if (
        state.sim_day >= evo.forecast_model_upgrade_day
        and not state.schema_flags.get("sevt_model_version", False)
    ):
        state.schema_flags["sevt_model_version"] = True
        evol_id = _next_evol_id()
        events.append(SchemaEvolutionEvent(
            evolution_id=evol_id,
            timestamp=iso_ts,
            affected_system="forecast",
            affected_event_type="demand_forecast",
            field_name="model_version",
            change_type="model_upgrade",
            effective_from_day=evo.forecast_model_upgrade_day,
            description=(
                f"Forecast model upgraded from v2.3 to v3.0 at sim day "
                f"{evo.forecast_model_upgrade_day}. Wider confidence intervals apply."
            ),
        ))

    # --- incoterms activation ---
    if (
        state.sim_day >= evo.incoterms_from_day
        and not state.schema_flags.get("sevt_incoterms", False)
    ):
        state.schema_flags["sevt_incoterms"] = True
        evol_id = _next_evol_id()
        events.append(SchemaEvolutionEvent(
            evolution_id=evol_id,
            timestamp=iso_ts,
            affected_system="erp",
            affected_event_type="customer_order",
            field_name="incoterms",
            change_type="field_activation",
            effective_from_day=evo.incoterms_from_day,
            description=(
                f"Field 'incoterms' is now active on customer_order events "
                f"from sim day {evo.incoterms_from_day} onwards."
            ),
        ))

    return events
