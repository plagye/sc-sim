"""Production completion and reclassification engine for FlowForm Industries.

Generates daily production_completion events (batches of finished goods entering
inventory) and production_reclassification events (retroactive Grade A -> B
downgrades 2-5 calendar days after completion).

Runs on business days only.  Weekends and Polish public holidays produce no
completions.  Reclassifications are also checked and emitted only on business
days.

Engine entry point: ``run(state, config, date) -> list[Event]``
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from typing import Any, Literal

from pydantic import BaseModel

from flowform.calendar import (
    PRODUCTION_DOW_MULTIPLIERS,
    is_business_day,
)
from flowform.config import Config
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Monthly seasonality multipliers
# ---------------------------------------------------------------------------

_MONTHLY_SEASONALITY: dict[int, float] = {
    1: 0.70,
    2: 0.85,
    3: 1.15,
    4: 1.25,
    5: 1.05,
    6: 1.00,
    7: 0.75,
    8: 0.80,
    9: 1.15,
    10: 1.10,
    11: 1.15,
    12: 0.55,
}

# Batches per production day
_BATCH_MIN = 3
_BATCH_MAX = 8

# Quality grade probabilities (cumulative thresholds)
_GRADE_A_PROB = 0.90
_GRADE_B_PROB = 0.98  # cumulative (0.90 + 0.08); remainder -> REJECT

# Probability of a Grade A batch being flagged for reclassification
_RECLASS_PROB = 0.01

# Year-end: last week of December, production runs at ~30%
_YEAREND_START_DAY = 25  # Dec 25+
_YEAREND_MULTIPLIER = 0.30


# ---------------------------------------------------------------------------
# Event schemas
# ---------------------------------------------------------------------------


class ProductionCompletionEvent(BaseModel):
    """A single production batch completing into finished-goods inventory."""

    event_type: Literal["production_completion"]
    event_id: str
    simulation_date: str          # ISO date
    batch_id: str                 # e.g. "BATCH-1001"
    sku: str
    quantity: int
    grade: Literal["A", "B", "REJECT"]
    production_line: int          # 1-5
    warehouse: str                # always "W01"
    completion_timestamp: str     # ISO datetime, same day 06:00-18:00


class ProductionReclassificationEvent(BaseModel):
    """Retroactive downgrade of a Grade A batch to Grade B."""

    event_type: Literal["production_reclassification"]
    event_id: str
    simulation_date: str           # ISO date of reclassification
    batch_id: str
    sku: str
    quantity: int
    original_grade: Literal["A"]
    new_grade: Literal["B"]
    original_completion_date: str  # ISO date
    reclassification_reason: str   # always "quality_review"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _pick_grade(rng_value: float) -> Literal["A", "B", "REJECT"]:
    """Convert a uniform [0, 1) draw into a quality grade."""
    if rng_value < _GRADE_A_PROB:
        return "A"
    if rng_value < _GRADE_B_PROB:
        return "B"
    return "REJECT"


def _daily_unit_count(d: date, rng: Any, min_units: int, max_units: int) -> int:
    """Compute effective daily unit target after all multipliers.

    Applies DOW multiplier, monthly seasonality, and year-end override.
    Returns an integer; the caller is responsible for the stoppage check
    (which bypasses this entirely).
    """
    dow_mult = PRODUCTION_DOW_MULTIPLIERS[d.weekday()]
    month_mult = _MONTHLY_SEASONALITY[d.month]

    # Year-end override: last week of December
    yearend_mult = 1.0
    if d.month == 12 and d.day >= _YEAREND_START_DAY:
        yearend_mult = _YEAREND_MULTIPLIER

    base = rng.randint(min_units, max_units)
    total = base * dow_mult * month_mult * yearend_mult
    return max(1, int(total))


def _distribute_units(total: int, n_batches: int, rng: Any) -> list[int]:
    """Split *total* units across *n_batches* with some random variance.

    Each batch gets a random weight in [0.5, 1.5]; quantities are scaled
    proportionally.  The last batch absorbs any integer rounding remainder.
    """
    weights = [rng.uniform(0.5, 1.5) for _ in range(n_batches)]
    weight_sum = sum(weights)
    quantities: list[int] = []
    remaining = total
    for i, w in enumerate(weights):
        if i == n_batches - 1:
            quantities.append(max(1, remaining))
        else:
            q = max(1, round(w / weight_sum * total))
            quantities.append(q)
            remaining -= q
    return quantities


def _random_timestamp(d: date, rng: Any) -> str:
    """Return an ISO datetime string on *d* between 06:00 and 18:00."""
    hour = rng.randint(6, 17)
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    return f"{d.isoformat()}T{hour:02d}:{minute:02d}:{second:02d}"


def _reclass_due_date(completion_date: date, rng: Any) -> date:
    """Return the reclassification due date (2-5 calendar days after completion)."""
    offset = rng.randint(2, 5)
    return completion_date + timedelta(days=offset)


# ---------------------------------------------------------------------------
# Reclassification processing
# ---------------------------------------------------------------------------


def _process_reclassifications(
    state: SimulationState,
    sim_date: date,
) -> list[ProductionReclassificationEvent]:
    """Emit reclassification events for any pipeline entries due today or earlier.

    Removes processed entries from ``state.production_pipeline`` in place.
    """
    events: list[ProductionReclassificationEvent] = []
    remaining: list[dict[str, Any]] = []

    for entry in state.production_pipeline:
        due = date.fromisoformat(entry["reclass_due_date"])
        if due <= sim_date:
            events.append(
                ProductionReclassificationEvent(
                    event_type="production_reclassification",
                    event_id=str(uuid.uuid4()),
                    simulation_date=sim_date.isoformat(),
                    batch_id=entry["batch_id"],
                    sku=entry["sku"],
                    quantity=entry["quantity"],
                    original_grade="A",
                    new_grade="B",
                    original_completion_date=entry["original_completion_date"],
                    reclassification_reason="quality_review",
                )
            )
        else:
            remaining.append(entry)

    state.production_pipeline = remaining
    return events


# ---------------------------------------------------------------------------
# Demand-weighted SKU selection
# ---------------------------------------------------------------------------

_DEFAULT_TARGET_STOCK = 150


_BACKORDER_BOOST = 5.0  # Multiplier for SKUs with active backorders at zero stock


def _skus_with_backorders(state: "SimulationState") -> set[str]:
    """Return the set of SKUs that have at least one backordered order line.

    Used by :func:`_production_sku_weights` to apply the backorder boost.
    """
    skus: set[str] = set()
    for order in state.open_orders.values():
        for line in order.get("lines", []):
            if line.get("line_status") in ("backordered", "partially_allocated"):
                skus.add(line["sku"])
    return skus


def _production_sku_weights(state: "SimulationState", config: "Config") -> list[float]:
    """Compute blended selection weights for every SKU in state.catalog.

    Three signals are blended per SKU:

    1. days_since_norm: how long ago the SKU was last produced (normalised 0–1).
       SKUs never produced get ``state.sim_day`` (maximum urgency).
    2. stock_pressure: how far below the target stock the SKU currently sits,
       normalised to [0, 1].  SKUs at or above target get 0.
    3. backorder_boost: SKUs with active backordered demand that are at zero
       on-hand inventory receive a ``_BACKORDER_BOOST`` (5×) multiplier applied
       after the base blend.  This models the factory urgently scheduling
       production runs to clear customer backorder queues.

    Base blend: ``weight = 0.4 * days_since_norm + 0.6 * stock_pressure``
    Then: if SKU has backorders AND on_hand == 0: weight *= _BACKORDER_BOOST

    If every weight is 0 (all SKUs produced today and all above target), a
    uniform list of 1.0s is returned so ``rng.choices`` keeps working.

    Args:
        state:  Current simulation state.
        config: Simulation config (accepted for interface consistency; unused).

    Returns:
        A list of floats, one per entry in ``state.catalog``, suitable for
        passing as the ``weights`` argument of ``random.choices()``.
    """
    catalog = state.catalog
    n = len(catalog)
    if n == 0:
        return []

    # --- days_since signal ---
    raw_days: list[float] = []
    for entry in catalog:
        last = state.last_produced.get(entry.sku, 0)
        raw_days.append(float(state.sim_day - last))

    max_days = max(raw_days) if raw_days else 1.0
    if max_days == 0.0:
        max_days = 1.0  # guard division-by-zero when sim_day == 0
    days_since_norm = [d / max_days for d in raw_days]

    # --- stock_pressure signal ---
    w01 = state.inventory.get("W01", {})
    stock_pressure: list[float] = []
    for entry in catalog:
        target = float(state.inventory_target_stock.get(entry.sku, _DEFAULT_TARGET_STOCK))
        on_hand = float(w01.get(entry.sku, {}).get("on_hand", 0))
        if target <= 0:
            sp = 0.0
        else:
            sp = max(0.0, (target - on_hand) / target)
        stock_pressure.append(sp)

    # --- base blend ---
    weights = [
        0.4 * ds + 0.6 * sp
        for ds, sp in zip(days_since_norm, stock_pressure)
    ]

    # --- backorder boost ---
    # SKUs with active demand backlogged AND zero inventory get a strong boost.
    # This prevents popular SKUs from sitting at zero while production ignores them.
    backordered_skus = _skus_with_backorders(state)
    if backordered_skus:
        for i, entry in enumerate(catalog):
            if entry.sku in backordered_skus and w01.get(entry.sku, {}).get("on_hand", 0) == 0:
                weights[i] *= _BACKORDER_BOOST

    # Fallback to uniform if every weight is zero
    if all(w == 0.0 for w in weights):
        return [1.0] * n

    return weights


# ---------------------------------------------------------------------------
# Engine entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[ProductionCompletionEvent | ProductionReclassificationEvent]:
    """Run one day of production simulation.

    On non-business days (weekends and Polish public holidays) no completions
    are generated and reclassification processing is also skipped (they will
    be picked up on the next business day).

    Args:
        state:    Mutable simulation state.  ``state.inventory["W01"]`` and
                  ``state.production_pipeline`` are updated in place.
        config:   Validated simulation config (unused directly here; kept for
                  engine contract consistency).
        sim_date: Current simulation date.

    Returns:
        List of :class:`ProductionCompletionEvent` and/or
        :class:`ProductionReclassificationEvent` instances.
    """
    if not is_business_day(sim_date):
        return []

    events: list[ProductionCompletionEvent | ProductionReclassificationEvent] = []

    # --- Reclassifications first (independent of stoppage check) ---
    reclass_events = _process_reclassifications(state, sim_date)
    events.extend(reclass_events)

    # --- Production stoppage check ---
    if state.rng.random() < config.company.production_stoppage_probability:
        # No completions today; reclassifications already emitted above.
        return events

    # --- Determine daily unit target ---
    min_units, max_units = config.company.daily_production_range
    total_units = _daily_unit_count(sim_date, state.rng, min_units, max_units)

    # --- Number of batches ---
    n_batches = state.rng.randint(_BATCH_MIN, _BATCH_MAX)

    # --- Distribute units across batches ---
    batch_quantities = _distribute_units(total_units, n_batches, state.rng)

    # --- Pick SKUs (demand-weighted from active catalog) ---
    sku_weights = _production_sku_weights(state, config)
    skus_chosen = state.rng.choices(state.catalog, weights=sku_weights, k=n_batches)

    # --- Emit one completion event per batch ---
    for i in range(n_batches):
        entry = skus_chosen[i]
        quantity = batch_quantities[i]
        grade_roll = state.rng.random()
        grade = _pick_grade(grade_roll)
        production_line = state.rng.randint(1, config.company.production_lines)
        batch_id = f"BATCH-{state.next_id('batch')}"
        timestamp = _random_timestamp(sim_date, state.rng)

        event = ProductionCompletionEvent(
            event_type="production_completion",
            event_id=str(uuid.uuid4()),
            simulation_date=sim_date.isoformat(),
            batch_id=batch_id,
            sku=entry.sku,
            quantity=quantity,
            grade=grade,
            production_line=production_line,
            warehouse="W01",
            completion_timestamp=timestamp,
        )
        events.append(event)

        # Record that this SKU was produced today (for demand-weighted selection)
        state.last_produced[entry.sku] = state.sim_day

        # Update W01 inventory for Grade A and B (REJECT goes nowhere)
        if grade in ("A", "B"):
            pos = state.inventory["W01"].setdefault(entry.sku, {"on_hand": 0, "allocated": 0, "in_transit": 0})
            pos["on_hand"] += quantity
            # Record receipt so the inventory_movements engine can create a
            # matching receipt movement event without duplicating production logic.
            state.daily_production_receipts.append(
                {
                    "batch_id": batch_id,
                    "sku": entry.sku,
                    "warehouse_id": "W01",
                    "quantity": quantity,
                    "grade": grade,
                    "timestamp": timestamp,
                }
            )

        # Flag Grade A batches for possible reclassification (~1%)
        if grade == "A" and state.rng.random() < _RECLASS_PROB:
            due = _reclass_due_date(sim_date, state.rng)
            state.production_pipeline.append(
                {
                    "batch_id": batch_id,
                    "sku": entry.sku,
                    "quantity": quantity,
                    "reclass_due_date": due.isoformat(),
                    "original_completion_date": sim_date.isoformat(),
                }
            )

    return events
