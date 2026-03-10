"""Tests for the load planning engine (Step 23).

Covers:
- Business-day guard (weekend and public holiday)
- Order eligibility filtering (unallocated, credit_hold, already-planned)
- Shipment and load creation mechanics
- State mutations (load_id / shipment_id written to orders, active_loads populated)
- BALTIC lbs conversion
- Sync window distribution
- TMS routing of load_event
"""

from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path

import pytest

from flowform.config import load_config
from flowform.engines import load_planning
from flowform.engines.load_planning import LoadEvent
from flowform.output.writer import _system_and_stem
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


@pytest.fixture()
def config():
    return load_config(_CONFIG_PATH)


@pytest.fixture()
def state(config, tmp_path):
    """Fresh simulation state isolated in a temp directory."""
    db = tmp_path / "sim.db"
    return SimulationState.from_new(config, db_path=db)


# ---------------------------------------------------------------------------
# Helper: create an allocated order in state.open_orders
# ---------------------------------------------------------------------------


def _make_allocated_order(
    state: SimulationState,
    order_id: str,
    customer_id: str,
    sku: str,
    qty: int = 5,
    warehouse_id: str = "W01",
    priority: str = "standard",
    load_id: str | None = None,
) -> None:
    state.open_orders[order_id] = {
        "event_type": "customer_order",
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "customer_id": customer_id,
        "simulation_date": "2026-01-05",
        "timestamp": "2026-01-05T10:00:00",
        "currency": "PLN",
        "priority": priority,
        "status": "allocated",
        "requested_delivery_date": "2026-02-20",
        "load_id": load_id,
        "shipment_id": None,
        "lines": [
            {
                "line_id": f"{order_id}-L1",
                "sku": sku,
                "quantity_ordered": qty,
                "quantity_allocated": qty,
                "quantity_shipped": 0,
                "quantity_backordered": 0,
                "unit_price": 500.0,
                "line_status": "allocated",
                "warehouse_id": warehouse_id,
            }
        ],
    }


def _first_customer_id(state: SimulationState) -> str:
    return state.customers[0].customer_id


def _first_sku(state: SimulationState) -> str:
    return state.catalog[0].sku


# ---------------------------------------------------------------------------
# Test 1: weekend guard
# ---------------------------------------------------------------------------


def test_returns_empty_on_weekend(state, config):
    """No loads are created on a Saturday."""
    saturday = date(2026, 1, 3)  # Saturday

    cid = _first_customer_id(state)
    sku = _first_sku(state)
    _make_allocated_order(state, "ORD-9001", cid, sku)

    events = load_planning.run(state, config, saturday)
    assert events == []
    assert len(state.active_loads) == 0


# ---------------------------------------------------------------------------
# Test 2: public holiday guard
# ---------------------------------------------------------------------------


def test_returns_empty_on_holiday(state, config):
    """No loads are created on a Polish public holiday (New Year's Day)."""
    new_years = date(2026, 1, 1)  # New Year — always a Polish holiday

    cid = _first_customer_id(state)
    sku = _first_sku(state)
    _make_allocated_order(state, "ORD-9002", cid, sku)

    events = load_planning.run(state, config, new_years)
    assert events == []
    assert len(state.active_loads) == 0


# ---------------------------------------------------------------------------
# Test 3: no eligible orders → empty
# ---------------------------------------------------------------------------


def test_no_eligible_orders_returns_empty(state, config):
    """If state has no orders at all, the engine returns an empty list."""
    business_day = date(2026, 1, 5)  # Monday
    events = load_planning.run(state, config, business_day)
    assert events == []


# ---------------------------------------------------------------------------
# Test 4: unallocated order skipped
# ---------------------------------------------------------------------------


def test_unallocated_order_skipped(state, config):
    """An order with status 'confirmed' (not 'allocated') must not be included."""
    business_day = date(2026, 1, 5)

    cid = _first_customer_id(state)
    sku = _first_sku(state)
    _make_allocated_order(state, "ORD-9003", cid, sku)
    # Override status to something that is not "allocated"
    state.open_orders["ORD-9003"]["status"] = "confirmed"

    events = load_planning.run(state, config, business_day)
    assert events == []
    assert len(state.active_loads) == 0


# ---------------------------------------------------------------------------
# Test 5: credit_hold order skipped
# ---------------------------------------------------------------------------


def test_credit_hold_order_skipped(state, config):
    """An order with status 'credit_hold' must not be included."""
    business_day = date(2026, 1, 5)

    cid = _first_customer_id(state)
    sku = _first_sku(state)
    _make_allocated_order(state, "ORD-9004", cid, sku)
    state.open_orders["ORD-9004"]["status"] = "credit_hold"

    events = load_planning.run(state, config, business_day)
    assert events == []
    assert len(state.active_loads) == 0


# ---------------------------------------------------------------------------
# Test 6: already-planned order skipped
# ---------------------------------------------------------------------------


def test_already_planned_order_skipped(state, config):
    """An order that already has a load_id must not be re-planned."""
    business_day = date(2026, 1, 5)

    cid = _first_customer_id(state)
    sku = _first_sku(state)
    _make_allocated_order(state, "ORD-9005", cid, sku, load_id="LOAD-9999")

    events = load_planning.run(state, config, business_day)
    assert events == []


# ---------------------------------------------------------------------------
# Test 7: one allocated order creates one LoadEvent + one active_loads entry
# ---------------------------------------------------------------------------


def test_allocated_order_creates_load(state, config):
    """A single allocated order produces exactly one LoadEvent and one active_load."""
    business_day = date(2026, 1, 5)

    cid = _first_customer_id(state)
    sku = _first_sku(state)
    _make_allocated_order(state, "ORD-9006", cid, sku)

    events = load_planning.run(state, config, business_day)

    assert len(events) == 1
    event = events[0]
    assert isinstance(event, LoadEvent)
    assert event.event_type == "load_event"
    assert event.event_subtype == "load_created"
    assert event.status == "load_created"
    assert len(state.active_loads) == 1


# ---------------------------------------------------------------------------
# Test 8: load_id written to order
# ---------------------------------------------------------------------------


def test_load_id_written_to_order(state, config):
    """After run(), state.open_orders[order_id]['load_id'] must be set."""
    business_day = date(2026, 1, 5)

    cid = _first_customer_id(state)
    sku = _first_sku(state)
    _make_allocated_order(state, "ORD-9007", cid, sku)

    events = load_planning.run(state, config, business_day)
    assert len(events) == 1

    load_id = state.open_orders["ORD-9007"].get("load_id")
    assert load_id is not None
    assert load_id.startswith("LOAD-")


# ---------------------------------------------------------------------------
# Test 9: shipment_id written to order
# ---------------------------------------------------------------------------


def test_shipment_id_written_to_order(state, config):
    """After run(), state.open_orders[order_id]['shipment_id'] must be set."""
    business_day = date(2026, 1, 5)

    cid = _first_customer_id(state)
    sku = _first_sku(state)
    _make_allocated_order(state, "ORD-9008", cid, sku)

    events = load_planning.run(state, config, business_day)
    assert len(events) == 1

    shipment_id = state.open_orders["ORD-9008"].get("shipment_id")
    assert shipment_id is not None
    assert shipment_id.startswith("SHP-")


# ---------------------------------------------------------------------------
# Test 10: BALTIC carrier → weight reported in lbs
# ---------------------------------------------------------------------------


def test_weight_reported_in_lbs_for_baltic(state, config):
    """When BALTIC is the carrier, weight_unit must be 'lbs' and
    total_weight_reported must be greater than total_weight_kg."""
    business_day = date(2026, 1, 5)

    # Force a customer to prefer BALTIC
    customer = state.customers[0]
    # Temporarily patch the preferred_carrier via a minimal workaround:
    # Customer is a frozen dataclass, so we rebuild it minimally by injecting
    # a plain dict-based order with the customer_id and testing via the order.
    cid = customer.customer_id
    sku = _first_sku(state)

    # Use a customer that has no preferred carrier, and force W02 source
    # so BALTIC gets high weight (35 out of 100 for W02), OR just inject
    # a custom order with a customer whose preferred_carrier we set.
    # Because Customer is a frozen dataclass, we'll instead add a synthetic
    # customer-like object by patching state.customers temporarily.

    import dataclasses

    patched_customer = dataclasses.replace(customer, preferred_carrier="BALTIC")
    # Swap out in the list
    original_customers = state.customers
    state.customers = [patched_customer] + state.customers[1:]

    _make_allocated_order(state, "ORD-9009", cid, sku, qty=10)

    events = load_planning.run(state, config, business_day)

    # Restore
    state.customers = original_customers

    assert len(events) == 1
    event = events[0]
    assert event.weight_unit == "lbs"
    assert event.total_weight_reported > event.total_weight_kg


# ---------------------------------------------------------------------------
# Test 11: sync window distribution across multiple loads
# ---------------------------------------------------------------------------


def test_sync_window_assignment(state, config):
    """Three allocated orders produce loads covering at least two distinct sync windows."""
    business_day = date(2026, 1, 5)

    cid = _first_customer_id(state)
    skus = [e.sku for e in state.catalog[:3]]

    # Use three distinct customers to avoid consolidated single load
    # (they go to different customers, each gets own shipment; consolidation
    # by warehouse/carrier may still combine them, but with 3 orders we
    # should exercise sync window assignment logic across indices).
    for i, (sku, c) in enumerate(zip(skus, state.customers[:3])):
        _make_allocated_order(state, f"ORD-SW{i}", c.customer_id, sku)

    events = load_planning.run(state, config, business_day)

    # We must have at least one event
    assert len(events) >= 1

    # Sync windows used
    sync_windows_used = {e.sync_window for e in events}

    if len(events) >= 2:
        # With multiple loads, at least two distinct windows should appear
        # (formula: sync_windows[i * 3 // n] spreads across 08/14/20)
        assert len(sync_windows_used) >= 2, (
            f"Expected ≥2 distinct sync windows with {len(events)} loads, "
            f"got: {sync_windows_used}"
        )
    else:
        # Single load → always "08"
        assert sync_windows_used == {"08"}


# ---------------------------------------------------------------------------
# Test 12: load_event routes correctly through writer
# ---------------------------------------------------------------------------


def test_load_event_type_routes_correctly(state, config):
    """load_event with a valid sync_window must route to ('tms', 'loads_<sw>')."""
    business_day = date(2026, 1, 5)

    cid = _first_customer_id(state)
    sku = _first_sku(state)
    _make_allocated_order(state, "ORD-9010", cid, sku)

    events = load_planning.run(state, config, business_day)
    assert len(events) == 1

    event = events[0]
    assert event.event_type == "load_event"

    event_dict = event.model_dump()
    system, stem = _system_and_stem(event_dict)

    assert system == "tms"
    assert stem == f"loads_{event.sync_window}"
    assert event.sync_window in {"08", "14", "20"}


# ---------------------------------------------------------------------------
# Test 13: partially_allocated order eligible for load planning (Fix 3)
# ---------------------------------------------------------------------------


def test_partially_allocated_order_eligible_for_load(state, config):
    """A partially_allocated order with at least one 'allocated' line must be
    included in load planning so the ready portion ships immediately.

    Previously only 'allocated' orders were picked up, leaving partial orders
    stuck indefinitely even when some lines were ready to ship.
    """
    business_day = date(2026, 1, 5)
    cid = _first_customer_id(state)
    sku = _first_sku(state)
    order_id = "ORD-PARTIAL-9999"

    # Seed a partially_allocated order: one line allocated, one backordered
    state.open_orders[order_id] = {
        "event_type": "customer_order",
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "customer_id": cid,
        "simulation_date": "2026-01-05",
        "timestamp": "2026-01-05T10:00:00",
        "currency": "PLN",
        "priority": "standard",
        "status": "partially_allocated",
        "requested_delivery_date": "2026-02-20",
        "load_id": None,
        "shipment_id": None,
        "lines": [
            {
                "line_id": f"{order_id}-L1",
                "sku": sku,
                "quantity_ordered": 10,
                "quantity_allocated": 5,
                "quantity_shipped": 0,
                "quantity_backordered": 5,
                "unit_price": 500.0,
                "line_status": "allocated",   # this line is ready
                "warehouse_id": "W01",
            },
            {
                "line_id": f"{order_id}-L2",
                "sku": sku,
                "quantity_ordered": 10,
                "quantity_allocated": 0,
                "quantity_shipped": 0,
                "quantity_backordered": 10,
                "unit_price": 500.0,
                "line_status": "backordered",  # this line is waiting for stock
                "warehouse_id": "W01",
            },
        ],
    }

    events = load_planning.run(state, config, business_day)

    # The partially_allocated order must produce a load
    load_ids = [e.load_id for e in events]
    order_in_load = any(
        order_id in e.order_ids for e in events
    )
    assert order_in_load, (
        "partially_allocated order with an 'allocated' line must appear in a load"
    )

    # The order must have a load_id stamped on it
    assert state.open_orders[order_id].get("load_id"), (
        "load_id must be written back to the order"
    )

    # Cleanup
    del state.open_orders[order_id]
