"""Load planning engine for FlowForm Industries.

Translates allocated order lines into shipments and loads, assigns carriers,
and emits TMS ``load_event`` events for the initial ``load_created`` status.

Conceptual model::

    Allocated order lines → grouped into Shipments → consolidated into Loads

- A **Shipment** is one-per-order: a logical grouping of all non-cancelled
  allocated lines for a given order.
- A **Load** is a physical truck.  Multiple shipments may be consolidated onto
  one load when they share the same source warehouse and carrier.  Loads carry
  1–5 shipments.

Business-day guard: this engine does **not** run on weekends or Polish public
holidays (unlike exchange rates which run every day).

Engine entry point: ``run(state, config, sim_date) -> list[LoadEvent]``
"""

from __future__ import annotations

import uuid
from collections import Counter
from datetime import date
from typing import Any, Literal

from pydantic import BaseModel

from flowform.calendar import is_business_day
from flowform.config import Config
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Order-level statuses that are NOT eligible for load planning
_SKIP_ORDER_STATUSES: frozenset[str] = frozenset({
    "credit_hold",
    "cancelled",
    "shipped",
    "partially_shipped",
})

# Weight conversion: kg → lbs
_KG_TO_LBS: float = 2.20462

# ---------------------------------------------------------------------------
# NUTS2 region lookup — keyed by actual region strings from customers._COUNTRY_REGIONS
# ---------------------------------------------------------------------------

REGION_TO_NUTS2: dict[str, str] = {
    # PL — Śląskie, Mazowieckie, Małopolskie, Łódźkie, Dolnośląskie,
    #       Wielkopolskie, Pomorskie, Kujawsko-Pomorskie, Zachodniopomorskie
    "Śląskie":             "PL-SL",
    "Mazowieckie":         "PL-MZ",
    "Małopolskie":         "PL-MA",
    "Łódźkie":             "PL-LD",
    "Dolnośląskie":        "PL-DS",
    "Wielkopolskie":       "PL-WP",
    "Pomorskie":           "PL-PM",
    "Kujawsko-Pomorskie":  "PL-KP",
    "Zachodniopomorskie":  "PL-ZP",
    # DE — Bavaria, North Rhine-Westphalia, Baden-Württemberg, Hamburg,
    #       Saxony, Brandenburg, Hesse, Lower Saxony, Bremen
    "Bavaria":                  "DE-BY",
    "North Rhine-Westphalia":   "DE-NW",
    "Baden-Württemberg":        "DE-BW",
    "Hamburg":                  "DE-HH",
    "Saxony":                   "DE-SN",
    "Brandenburg":              "DE-BB",
    "Hesse":                    "DE-HE",
    "Lower Saxony":             "DE-NI",
    "Bremen":                   "DE-HB",
    # CZ — Prague, South Moravia, Central Bohemia, Moravia-Silesia, Pilsen
    "Prague":           "CZ-PR",
    "South Moravia":    "CZ-JM",
    "Central Bohemia":  "CZ-ST",
    "Moravia-Silesia":  "CZ-MS",
    "Pilsen":           "CZ-PL",
    # NL — North Holland, South Holland, Zeeland, North Brabant, Gelderland
    "North Holland":   "NL-NH",
    "South Holland":   "NL-ZH",
    "Zeeland":         "NL-ZE",
    "North Brabant":   "NL-NB",
    "Gelderland":      "NL-GE",
    # NO — Vestland, Rogaland, Møre og Romsdal, Viken, Troms og Finnmark
    "Vestland":          "NO-VL",
    "Rogaland":          "NO-RO",
    "Møre og Romsdal":   "NO-MR",
    "Viken":             "NO-VK",
    "Troms og Finnmark": "NO-TF",
    # SE — Västra Götaland, Skåne, Stockholm, Norrland, Östergötland
    "Västra Götaland":  "SE-O",
    "Skåne":            "SE-M",
    "Stockholm":        "SE-AB",
    "Norrland":         "SE-AC",
    "Östergötland":     "SE-E",
    # FR — Île-de-France, Auvergne-Rhône-Alpes, Occitanie, Nouvelle-Aquitaine
    "Île-de-France":         "FR-IDF",
    "Auvergne-Rhône-Alpes":  "FR-ARA",
    "Occitanie":             "FR-OCC",
    "Nouvelle-Aquitaine":    "FR-NAQ",
    # IT — Lombardy, Veneto, Emilia-Romagna, Tuscany, Lazio
    "Lombardy":       "IT-LOM",
    "Veneto":         "IT-VEN",
    "Emilia-Romagna": "IT-EMR",
    "Tuscany":        "IT-TOS",
    "Lazio":          "IT-LAZ",
}

# Carrier selection weights by destination country code
_CARRIER_WEIGHTS_BY_COUNTRY: dict[str, dict[str, int]] = {
    # Nordic
    "NO": {"DHL": 60, "GEODIS": 20, "DBSC": 20},
    "SE": {"DHL": 60, "GEODIS": 20, "DBSC": 20},
    # Western EU
    "FR": {"GEODIS": 50, "DHL": 30, "DBSC": 20},
    "IT": {"GEODIS": 50, "DHL": 30, "DBSC": 20},
    "NL": {"GEODIS": 50, "DHL": 30, "DBSC": 20},
    # CEE
    "PL": {"RABEN": 50, "DBSC": 30, "DHL": 20},
    "CZ": {"RABEN": 50, "DBSC": 30, "DHL": 20},
    # DE
    "DE": {"DBSC": 50, "DHL": 30, "RABEN": 20},
}

# Fallback weights when country is not in the table above
_CARRIER_WEIGHTS_FALLBACK: dict[str, int] = {
    "DHL": 40,
    "DBSC": 30,
    "RABEN": 20,
    "GEODIS": 10,
}

# Weight threshold (kg) above which DHL gets a +20 bump
_LARGE_SHIPMENT_KG: float = 500.0


# ---------------------------------------------------------------------------
# Event schema
# ---------------------------------------------------------------------------


class LoadEvent(BaseModel):
    """Emitted once per load on the day it is created (load_created status).

    Routes to ``tms/<date>/loads_<sync_window>.json``.
    """

    event_type: Literal["load_event"] = "load_event"
    event_id: str                          # uuid4
    load_id: str
    event_subtype: str = "load_created"    # e.g. "load_created", "in_transit", "delivered"
    simulation_date: str                   # ISO date
    sync_window: str                       # "08", "14", or "20"
    carrier_code: str
    source_warehouse_id: str
    shipment_ids: list[str]
    order_ids: list[str]
    total_weight_kg: float
    weight_unit: str                       # "kg" or "lbs" for BALTIC
    total_weight_reported: float           # same as total_weight_kg, or converted if BALTIC
    status: str = "load_created"           # mirrors event_subtype for current load status
    planned_date: str                      # ISO date
    priority: str                          # highest priority across orders in load
    customer_ids: list[str]                # deduplicated
    destination_region: str = ""           # NUTS2 code e.g. "PL-SL", "DE-BY"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _dominant_warehouse(order: dict[str, Any]) -> str:
    """Return the warehouse that appears most frequently across the order's lines.

    Falls back to ``"W01"`` on a tie.

    Args:
        order: Order dict from ``state.open_orders``.

    Returns:
        Warehouse code string.
    """
    counts: Counter[str] = Counter()
    for line in order.get("lines", []):
        if line.get("line_status") == "cancelled":
            continue
        wh = line.get("warehouse_id", "W01")
        counts[wh] += 1

    if not counts:
        return "W01"

    max_count = max(counts.values())
    candidates = [wh for wh, cnt in counts.items() if cnt == max_count]
    # Prefer W01 on tie; otherwise take alphabetically first for determinism
    if "W01" in candidates:
        return "W01"
    return sorted(candidates)[0]


def destination_region(customer: Any) -> str:
    """Map a customer's region to a NUTS2 code.

    Falls back to ``"{country_code}-XX"`` if the region is not found in
    :data:`REGION_TO_NUTS2`.

    Args:
        customer: Customer object with ``region`` and ``country_code`` attributes.

    Returns:
        NUTS2 code string (e.g. ``"PL-SL"``).
    """
    region = getattr(customer, "region", "")
    code = REGION_TO_NUTS2.get(region)
    if code:
        return code
    country = getattr(customer, "country_code", "XX")
    return f"{country}-XX"


def _select_carrier(
    state: SimulationState,
    customer: Any,  # Customer dataclass
    source_warehouse_id: str,
    total_weight_kg: float = 0.0,
) -> str:
    """Determine which carrier to use for a shipment.

    Priority order:
    1. Customer has a ``preferred_carrier`` → apply BALTIC gate.
    2. BALTIC gate: BALTIC is only valid if customer segment is "Shipyards"
       OR source warehouse is W02.  Otherwise override to RABEN.
    3. No preferred carrier (or overridden): use country-based weighted selection.
    4. Large-shipment override: if ``total_weight_kg > 500``, bump DHL by +20
       before weighted draw.

    Args:
        state:               Mutable simulation state (RNG access).
        customer:            Customer object.
        source_warehouse_id: The warehouse the shipment departs from.
        total_weight_kg:     Total shipment weight in kg (for large-shipment logic).

    Returns:
        Carrier code string (e.g. ``"DHL"``).
    """
    preferred = getattr(customer, "preferred_carrier", "") or ""

    if preferred:
        # Apply BALTIC gate: BALTIC only valid for Shipyards or W02
        if preferred == "BALTIC":
            segment = getattr(customer, "segment", "")
            if segment != "Shipyards" and source_warehouse_id != "W02":
                return "RABEN"
        return preferred

    # Country-based weighted selection
    country_code = getattr(customer, "country_code", "")
    weights_map = dict(_CARRIER_WEIGHTS_BY_COUNTRY.get(country_code, _CARRIER_WEIGHTS_FALLBACK))

    # Large-shipment override: bump DHL weight
    if total_weight_kg > _LARGE_SHIPMENT_KG:
        weights_map["DHL"] = weights_map.get("DHL", 0) + 20

    carrier_codes = list(weights_map.keys())
    carrier_weights = list(weights_map.values())
    return state.rng.choices(carrier_codes, weights=carrier_weights, k=1)[0]


def _highest_priority(orders: list[dict[str, Any]]) -> str:
    """Return the highest priority across a list of orders.

    Ranking: critical > express > standard.

    Args:
        orders: List of order dicts.

    Returns:
        Priority string.
    """
    for priority in ("critical", "express", "standard"):
        if any(o.get("priority") == priority for o in orders):
            return priority
    return "standard"


def _compute_total_weight(
    order: dict[str, Any],
    sku_weight: dict[str, float],
) -> float:
    """Sum ``quantity_allocated × weight_kg`` across all non-cancelled lines.

    Args:
        order:      Order dict from ``state.open_orders``.
        sku_weight: Mapping of SKU → weight_kg from the catalog.

    Returns:
        Total weight in kg (may be 0.0 if no weights are found).
    """
    total = 0.0
    for line in order.get("lines", []):
        if line.get("line_status") == "cancelled":
            continue
        sku = line.get("sku", "")
        qty_alloc = line.get("quantity_allocated", 0)
        weight_per_unit = sku_weight.get(sku, 0.0)
        total += qty_alloc * weight_per_unit
    return round(total, 3)


# ---------------------------------------------------------------------------
# Engine entry point
# ---------------------------------------------------------------------------


def run(
    state: SimulationState,
    config: Config,
    sim_date: date,
) -> list[LoadEvent]:
    """Run the load planning engine for *sim_date*.

    Steps:
    1. Business-day guard (skip weekends and Polish public holidays).
    2. Collect eligible orders (status ``"allocated"`` or ``"partially_allocated"``
       with at least one line in ``"allocated"`` line_status, no ``load_id`` set).
    3. For each eligible order, create one shipment.
    4. Consolidate shipments into loads by ``(source_warehouse_id, carrier_code)``.
    5. Assign sync windows across the day's loads.
    6. Mutate ``state.active_loads``, ``state.open_orders`` (set load_id/shipment_id).
    7. Return a list of :class:`LoadEvent` instances.

    Args:
        state:    Mutable simulation state.
        config:   Validated simulation config (kept for contract consistency).
        sim_date: The simulated calendar date to process.

    Returns:
        List of :class:`LoadEvent` instances (may be empty).
    """
    if not is_business_day(sim_date):
        return []

    # Build SKU → weight_kg lookup once
    sku_weight: dict[str, float] = {e.sku: e.weight_kg for e in state.catalog}

    # Build customer lookup once
    customer_by_id: dict[str, Any] = {c.customer_id: c for c in state.customers}

    # --- Step 1: Collect eligible orders ---
    eligible_orders: list[dict[str, Any]] = []
    for order in state.open_orders.values():
        status = order.get("status", "")
        if status in _SKIP_ORDER_STATUSES:
            continue
        if status not in ("allocated", "partially_allocated"):
            continue
        if order.get("load_id"):  # already planned
            continue
        # For partially_allocated orders, only include if at least one line is ready to ship
        has_allocated_lines = any(
            line.get("line_status") == "allocated"
            for line in order.get("lines", [])
        )
        if not has_allocated_lines:
            continue
        eligible_orders.append(order)

    if not eligible_orders:
        return []

    # --- Step 2: Create one shipment per eligible order ---
    # Each shipment is a plain dict (not persisted separately; it's embedded in
    # the load record and referenced via order["shipment_id"]).
    shipments: list[dict[str, Any]] = []

    for order in eligible_orders:
        order_id: str = order["order_id"]
        customer_id: str = order["customer_id"]

        source_wh = _dominant_warehouse(order)

        non_cancelled_line_ids = [
            line["line_id"]
            for line in order.get("lines", [])
            if line.get("line_status") != "cancelled"
        ]

        total_weight_kg = _compute_total_weight(order, sku_weight)

        shipment_id = f"SHP-{state.next_id('shipment')}"

        # Determine carrier for this shipment
        customer = customer_by_id.get(customer_id)
        carrier_code = _select_carrier(state, customer, source_wh, total_weight_kg)

        shipments.append({
            "shipment_id": shipment_id,
            "order_id": order_id,
            "customer_id": customer_id,
            "source_warehouse_id": source_wh,
            "carrier_code": carrier_code,
            "line_ids": non_cancelled_line_ids,
            "total_weight_kg": total_weight_kg,
            "requested_delivery_date": order.get("requested_delivery_date", ""),
        })

        # Write back to order immediately so later steps can read it
        order["shipment_id"] = shipment_id

    # --- Step 3: Consolidate shipments into loads by (warehouse, carrier) ---
    # Group shipments by consolidation key
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for shp in shipments:
        key = (shp["source_warehouse_id"], shp["carrier_code"])
        groups.setdefault(key, []).append(shp)

    # Build loads by packing shipments in groups of 1–5
    loads_data: list[dict[str, Any]] = []  # each entry is a load record

    for (source_wh, carrier_code), group_shipments in groups.items():
        remaining = list(group_shipments)  # copy so we can pop from it

        while remaining:
            # Pick a load size of 1–min(5, len(remaining))
            load_size = state.rng.randint(1, min(5, len(remaining)))
            batch = remaining[:load_size]
            remaining = remaining[load_size:]

            load_id = f"LOAD-{state.next_id('load')}"

            batch_weight_kg = round(sum(s["total_weight_kg"] for s in batch), 3)
            batch_order_ids = [s["order_id"] for s in batch]
            batch_shipment_ids = [s["shipment_id"] for s in batch]
            batch_customer_ids = list(dict.fromkeys(s["customer_id"] for s in batch))

            # Determine weight unit (BALTIC reports in lbs)
            carrier_obj = next(
                (c for c in state.carriers if c.code == carrier_code), None
            )
            if carrier_obj is not None and carrier_obj.weight_unit == "lbs":
                weight_unit = "lbs"
                total_weight_reported = round(batch_weight_kg * _KG_TO_LBS, 3)
            else:
                weight_unit = "kg"
                total_weight_reported = batch_weight_kg

            # Priority: highest across orders in this load
            batch_orders = [state.open_orders[oid] for oid in batch_order_ids]
            priority = _highest_priority(batch_orders)

            # Destination region: take from the first customer in this batch
            first_customer_id = batch_customer_ids[0] if batch_customer_ids else ""
            first_customer = customer_by_id.get(first_customer_id)
            dest_region = destination_region(first_customer) if first_customer else ""

            load_record: dict[str, Any] = {
                "load_id": load_id,
                "status": "load_created",
                "carrier_code": carrier_code,
                "source_warehouse_id": source_wh,
                "planned_date": sim_date.isoformat(),
                "estimated_departure": sim_date.isoformat(),
                "estimated_arrival": None,
                "actual_departure": None,
                "actual_arrival": None,
                "shipment_ids": batch_shipment_ids,
                "order_ids": batch_order_ids,
                "total_weight_kg": batch_weight_kg,
                "weight_unit": weight_unit,
                "total_weight_reported": total_weight_reported,
                "sync_window": "08",  # placeholder; filled in below
                "events": [],
                "priority": priority,
                "customer_ids": batch_customer_ids,
                "destination_region": dest_region,
            }
            loads_data.append(load_record)

            # Stamp load_id onto each order in this load
            for oid in batch_order_ids:
                state.open_orders[oid]["load_id"] = load_id

    if not loads_data:
        return []

    # --- Step 4: Assign sync windows ---
    n = len(loads_data)
    sync_windows = ["08", "14", "20"]
    for i, load_record in enumerate(loads_data):
        sw = sync_windows[i * 3 // n]
        load_record["sync_window"] = sw

    # --- Step 5: Persist loads to state and emit events ---
    events: list[LoadEvent] = []

    for load_record in loads_data:
        load_id = load_record["load_id"]
        state.active_loads[load_id] = load_record

        events.append(
            LoadEvent(
                event_id=str(uuid.uuid4()),
                load_id=load_id,
                simulation_date=sim_date.isoformat(),
                sync_window=load_record["sync_window"],
                carrier_code=load_record["carrier_code"],
                source_warehouse_id=load_record["source_warehouse_id"],
                shipment_ids=load_record["shipment_ids"],
                order_ids=load_record["order_ids"],
                total_weight_kg=load_record["total_weight_kg"],
                weight_unit=load_record["weight_unit"],
                total_weight_reported=load_record["total_weight_reported"],
                planned_date=load_record["planned_date"],
                priority=load_record["priority"],
                customer_ids=load_record["customer_ids"],
                destination_region=load_record["destination_region"],
            )
        )

    return events
