"""Tests for Step 28: Returns / RMA flow engine.

Covers:
- Weekend guard (business days only)
- return_requested event fields
- RMA persisted in state.in_transit_returns
- No second RMA for the same order
- Lifecycle transitions: in_transit_return, received_at_warehouse, inspected+restocked
- inspected+scrapped
- RMA removed from state after resolution
- Return reason code validity
- Cancelled order exclusion
- Partial line return quantity range
"""

from __future__ import annotations

import math
import random
from datetime import date, timedelta
from pathlib import Path

import pytest

from flowform.config import load_config
from flowform.engines import returns
from flowform.engines.returns import ReturnEvent, _RETURN_REASONS
from flowform.engines.inventory_movements import InventoryMovementEvent
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"

# A business day with no Polish public holiday complications
_BIZ_DAY = date(2026, 3, 9)   # Monday

# A Saturday
_SATURDAY = date(2026, 4, 4)

_VALID_REASON_CODES = {reason for _, reason in _RETURN_REASONS}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def config():
    return load_config(_CONFIG_PATH)


@pytest.fixture()
def state(config, tmp_path):
    """Fresh simulation state isolated in a temp directory."""
    db = tmp_path / "sim.db"
    return SimulationState.from_new(config, db_path=db)


# ---------------------------------------------------------------------------
# Helper: create a minimal shipped order dict
# ---------------------------------------------------------------------------


def _make_shipped_order(
    order_id: str,
    customer_id: str,
    sku: str,
    quantity_shipped: int = 10,
) -> dict:
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "status": "shipped",
        "lines": [
            {
                "line_id": f"{order_id}-L1",
                "sku": sku,
                "line_status": "shipped",
                "quantity_ordered": quantity_shipped,
                "quantity_allocated": quantity_shipped,
                "quantity_shipped": quantity_shipped,
                "quantity_backordered": 0,
            }
        ],
    }


def _any_sku(state: SimulationState) -> str:
    """Return any SKU present in the catalog."""
    return state.catalog[0].sku


def _inject_shipped_order(state: SimulationState, order_id: str = "ORD-9999") -> dict:
    """Add a shipped order to state and return it."""
    sku = _any_sku(state)
    cust_id = state.customers[0].customer_id
    order = _make_shipped_order(order_id, cust_id, sku, quantity_shipped=10)
    state.open_orders[order_id] = order
    return order


def _force_return_roll(state: SimulationState) -> None:
    """Seed RNG so that the very first random() call is < 0.03.

    We seed with a value and verify; cycle through seeds until one works.
    """
    for seed in range(10000):
        state.rng = random.Random(seed)
        # Simulate the first random call that the engine will make for the
        # already-existing RMA advancement (none here) and then the order roll.
        val = state.rng.random()
        if val < 0.03:
            # Reset so engine starts from the same position
            state.rng = random.Random(seed)
            return
    raise RuntimeError("Could not find a seed that forces the 3% roll to fire.")


def _force_no_return_roll(state: SimulationState) -> None:
    """Seed RNG so that the first random() call is >= 0.03."""
    for seed in range(10000):
        state.rng = random.Random(seed)
        val = state.rng.random()
        if val >= 0.03:
            state.rng = random.Random(seed)
            return
    raise RuntimeError("Could not find a seed that suppresses the 3% roll.")


# ---------------------------------------------------------------------------
# T1: Weekend guard
# ---------------------------------------------------------------------------


def test_weekend_guard_returns_empty(state, config):
    """run() on a Saturday must return an empty list."""
    result = returns.run(state, config, _SATURDAY)
    assert result == []


# ---------------------------------------------------------------------------
# T2: return_requested event fields
# ---------------------------------------------------------------------------


def test_return_requested_event_fields(state, config):
    """A forced 3% roll must produce exactly one return_requested ReturnEvent
    with all required fields set correctly."""
    _inject_shipped_order(state)
    _force_return_roll(state)

    events = returns.run(state, config, _BIZ_DAY)

    req_events = [e for e in events if isinstance(e, ReturnEvent) and e.event_subtype == "return_requested"]
    assert len(req_events) == 1, f"Expected 1 return_requested, got {len(req_events)}"

    ev = req_events[0]
    assert ev.event_type == "return_event"
    assert ev.event_subtype == "return_requested"
    assert ev.rma_status == "return_requested"
    assert ev.sync_window == "08"
    assert ev.resolution is None
    assert ev.rma_id.startswith("RMA-")
    assert ev.simulation_date == _BIZ_DAY.isoformat()
    assert ev.timestamp == f"{_BIZ_DAY.isoformat()}T09:00:00Z"
    assert ev.return_warehouse_id == "W01"
    assert ev.order_id == "ORD-9999"
    assert len(ev.lines) >= 1


# ---------------------------------------------------------------------------
# T3: RMA persisted in state
# ---------------------------------------------------------------------------


def test_rma_persisted_in_state(state, config):
    """After a return_requested fires, state.in_transit_returns must contain 1 RMA
    with all required keys."""
    _inject_shipped_order(state)
    _force_return_roll(state)

    returns.run(state, config, _BIZ_DAY)

    assert len(state.in_transit_returns) == 1
    rma = state.in_transit_returns[0]
    for key in ("rma_id", "order_id", "customer_id", "rma_status", "lines", "created_date"):
        assert key in rma, f"Missing key in RMA dict: {key!r}"
    assert rma["rma_status"] == "return_requested"
    assert rma["rma_id"].startswith("RMA-")


# ---------------------------------------------------------------------------
# T4: No second RMA for same order
# ---------------------------------------------------------------------------


def test_no_second_rma_for_same_order(state, config):
    """Calling run() twice with the same shipped order must not generate a
    second return_requested event; the first call creates an open RMA that
    blocks the second."""
    _inject_shipped_order(state)

    # First call — force the roll to fire
    _force_return_roll(state)
    events1 = returns.run(state, config, _BIZ_DAY)
    req_events1 = [
        e for e in events1
        if isinstance(e, ReturnEvent) and e.event_subtype == "return_requested"
    ]
    assert len(req_events1) == 1, "Expected exactly 1 return_requested on first call"

    # Second call on the next business day — the RMA will advance, but no new
    # return_requested should be generated for the same order.
    next_biz = _BIZ_DAY + timedelta(days=1)
    events2 = returns.run(state, config, next_biz)
    new_req_events = [
        e for e in events2
        if isinstance(e, ReturnEvent)
        and e.event_subtype == "return_requested"
        and e.order_id == "ORD-9999"
    ]
    assert len(new_req_events) == 0, (
        "A second return_requested must not be generated for an order with an open RMA"
    )


# ---------------------------------------------------------------------------
# T5: in_transit_return advancement
# ---------------------------------------------------------------------------


def test_in_transit_return_advancement(state, config):
    """An RMA in return_requested status created yesterday must advance to
    return_authorized today (first step of the new lifecycle)."""
    yesterday = _BIZ_DAY - timedelta(days=1)
    rma = {
        "rma_id": "RMA-5000",
        "order_id": "ORD-8888",
        "customer_id": state.customers[0].customer_id,
        "carrier_code": "DHL",
        "return_warehouse_id": "W01",
        "rma_status": "return_requested",
        "created_date": yesterday.isoformat(),
        "transit_due_date": None,
        "lines": [
            {
                "line_number": 1,
                "sku": _any_sku(state),
                "quantity_returned": 5,
                "quantity_accepted": None,
                "return_reason": "quality_defect",
            }
        ],
        "resolution": None,
    }
    state.in_transit_returns.append(rma)

    events = returns.run(state, config, _BIZ_DAY)

    # After pre-fix 10, return_requested → return_authorized (not directly in_transit_return)
    authorized_events = [
        e for e in events
        if isinstance(e, ReturnEvent) and e.event_subtype == "return_authorized"
    ]
    assert len(authorized_events) == 1, (
        f"Expected 1 return_authorized event, got {len(authorized_events)}"
    )

    # RMA dict must be updated to return_authorized
    assert rma["rma_status"] == "return_authorized"
    assert rma.get("authorized_date") is not None


# ---------------------------------------------------------------------------
# Pre-Fix 10: return_authorized → pickup_scheduled stage test
# ---------------------------------------------------------------------------


def test_return_authorized_advances_to_pickup_scheduled(state, config):
    """An RMA in return_authorized status authorized yesterday must advance to
    pickup_scheduled today."""
    yesterday = _BIZ_DAY - timedelta(days=1)
    rma = {
        "rma_id": "RMA-AUTH-001",
        "order_id": "ORD-AUTH-001",
        "customer_id": state.customers[0].customer_id,
        "carrier_code": "DHL",
        "return_warehouse_id": "W01",
        "rma_status": "return_authorized",
        "created_date": (yesterday - timedelta(days=1)).isoformat(),
        "authorized_date": yesterday.isoformat(),
        "transit_due_date": None,
        "lines": [
            {
                "line_number": 1,
                "sku": _any_sku(state),
                "quantity_returned": 3,
                "quantity_accepted": None,
                "return_reason": "wrong_item_shipped",
            }
        ],
        "resolution": None,
    }
    state.in_transit_returns.append(rma)

    events = returns.run(state, config, _BIZ_DAY)

    pickup_events = [
        e for e in events
        if isinstance(e, ReturnEvent) and e.event_subtype == "pickup_scheduled"
    ]
    assert len(pickup_events) == 1, (
        f"Expected 1 pickup_scheduled event, got {len(pickup_events)}"
    )
    assert rma["rma_status"] == "pickup_scheduled"
    assert rma.get("pickup_scheduled_date") is not None


# ---------------------------------------------------------------------------
# Pre-Fix 10: pickup_scheduled → in_transit_return stage test
# ---------------------------------------------------------------------------


def test_pickup_scheduled_advances_to_in_transit(state, config):
    """An RMA in pickup_scheduled status scheduled yesterday must advance to
    in_transit_return today with a transit_due_date set."""
    yesterday = _BIZ_DAY - timedelta(days=1)
    rma = {
        "rma_id": "RMA-PICKUP-001",
        "order_id": "ORD-PICKUP-001",
        "customer_id": state.customers[0].customer_id,
        "carrier_code": "DHL",
        "return_warehouse_id": "W01",
        "rma_status": "pickup_scheduled",
        "created_date": (yesterday - timedelta(days=2)).isoformat(),
        "authorized_date": (yesterday - timedelta(days=1)).isoformat(),
        "pickup_scheduled_date": yesterday.isoformat(),
        "transit_due_date": None,
        "lines": [
            {
                "line_number": 1,
                "sku": _any_sku(state),
                "quantity_returned": 2,
                "quantity_accepted": None,
                "return_reason": "damaged_in_transit",
            }
        ],
        "resolution": None,
    }
    state.in_transit_returns.append(rma)

    events = returns.run(state, config, _BIZ_DAY)

    transit_events = [
        e for e in events
        if isinstance(e, ReturnEvent) and e.event_subtype == "in_transit_return"
    ]
    assert len(transit_events) == 1, (
        f"Expected 1 in_transit_return event, got {len(transit_events)}"
    )
    assert rma["rma_status"] == "in_transit_return"
    assert rma.get("transit_due_date") is not None
    transit_due = date.fromisoformat(rma["transit_due_date"])
    assert transit_due >= _BIZ_DAY + timedelta(days=2)


# ---------------------------------------------------------------------------
# T6: received_at_warehouse on transit due date
# ---------------------------------------------------------------------------


def test_received_at_warehouse_on_transit_due_date(state, config):
    """An in_transit_return RMA with transit_due_date == sim_date must advance
    to received_at_warehouse."""
    rma = {
        "rma_id": "RMA-5001",
        "order_id": "ORD-7777",
        "customer_id": state.customers[0].customer_id,
        "carrier_code": "DHL",
        "return_warehouse_id": "W01",
        "rma_status": "in_transit_return",
        "created_date": (_BIZ_DAY - timedelta(days=3)).isoformat(),
        "transit_due_date": _BIZ_DAY.isoformat(),
        "in_transit_date": (_BIZ_DAY - timedelta(days=2)).isoformat(),
        "lines": [
            {
                "line_number": 1,
                "sku": _any_sku(state),
                "quantity_returned": 4,
                "quantity_accepted": None,
                "return_reason": "overstock",
            }
        ],
        "resolution": None,
    }
    state.in_transit_returns.append(rma)

    events = returns.run(state, config, _BIZ_DAY)

    recv_events = [
        e for e in events
        if isinstance(e, ReturnEvent) and e.event_subtype == "received_at_warehouse"
    ]
    assert len(recv_events) == 1, (
        f"Expected 1 received_at_warehouse event, got {len(recv_events)}"
    )
    assert rma["rma_status"] == "received_at_warehouse"


# ---------------------------------------------------------------------------
# T7: inspected and restocked
# ---------------------------------------------------------------------------


def test_inspected_and_restocked(state, config):
    """A received_at_warehouse RMA inspected today with 80% restock roll must
    emit a ReturnEvent(resolution='restocked') and inventory movement(s)."""
    sku = _any_sku(state)
    initial_qty = state.inventory.get("W01", {}).get(sku, 0)
    qty_returned = 6

    yesterday = _BIZ_DAY - timedelta(days=1)
    rma = {
        "rma_id": "RMA-5002",
        "order_id": "ORD-6666",
        "customer_id": state.customers[0].customer_id,
        "carrier_code": "DHL",
        "return_warehouse_id": "W01",
        "rma_status": "received_at_warehouse",
        "created_date": (yesterday - timedelta(days=2)).isoformat(),
        "transit_due_date": yesterday.isoformat(),
        "received_date": yesterday.isoformat(),
        "lines": [
            {
                "line_number": 1,
                "sku": sku,
                "quantity_returned": qty_returned,
                "quantity_accepted": None,
                "return_reason": "quality_defect",
            }
        ],
        "resolution": None,
    }
    state.in_transit_returns.append(rma)

    # Seed RNG so the inspection roll lands in the Grade-A range (< 0.60 → W01 restock)
    for seed in range(10000):
        state.rng = random.Random(seed)
        if state.rng.random() < 0.60:
            state.rng = random.Random(seed)
            break

    events = returns.run(state, config, _BIZ_DAY)

    return_events = [e for e in events if isinstance(e, ReturnEvent)]
    restock_events = [e for e in events if isinstance(e, InventoryMovementEvent) and e.movement_type == "return_restock"]

    assert any(e.resolution == "restocked" for e in return_events), (
        "Expected at least one ReturnEvent with resolution='restocked'"
    )
    assert len(restock_events) >= 1, (
        "Expected at least one return_restock InventoryMovementEvent"
    )
    # Grade-A restock goes to W01; reference_id encodes the grade
    assert any(e.reference_id == "GRADE-A" for e in restock_events), (
        "Expected GRADE-A reference on restock movement when roll < 0.60"
    )

    # Inventory must have increased in W01 (Grade-A)
    new_qty = state.inventory.get("W01", {}).get(sku, 0)
    assert new_qty == initial_qty + qty_returned, (
        f"Expected W01[{sku}] = {initial_qty + qty_returned}, got {new_qty}"
    )


# ---------------------------------------------------------------------------
# T8: inspected and scrapped
# ---------------------------------------------------------------------------


def test_inspected_and_scrapped(state, config):
    """A received_at_warehouse RMA inspected today with 20% scrap roll must
    emit a ReturnEvent(resolution='scrapped') and NOT update inventory."""
    sku = _any_sku(state)
    initial_qty = state.inventory.get("W01", {}).get(sku, 0)

    yesterday = _BIZ_DAY - timedelta(days=1)
    rma = {
        "rma_id": "RMA-5003",
        "order_id": "ORD-5555",
        "customer_id": state.customers[0].customer_id,
        "carrier_code": "DHL",
        "return_warehouse_id": "W01",
        "rma_status": "received_at_warehouse",
        "created_date": (yesterday - timedelta(days=2)).isoformat(),
        "transit_due_date": yesterday.isoformat(),
        "received_date": yesterday.isoformat(),
        "lines": [
            {
                "line_number": 1,
                "sku": sku,
                "quantity_returned": 3,
                "quantity_accepted": None,
                "return_reason": "damaged_in_transit",
            }
        ],
        "resolution": None,
    }
    state.in_transit_returns.append(rma)

    # Seed RNG so the inspection roll falls in the scrap range (random() >= 0.85 → scrap)
    for seed in range(10000):
        state.rng = random.Random(seed)
        if state.rng.random() >= 0.85:
            state.rng = random.Random(seed)
            break

    events = returns.run(state, config, _BIZ_DAY)

    return_events = [e for e in events if isinstance(e, ReturnEvent)]
    restock_events = [
        e for e in events
        if isinstance(e, InventoryMovementEvent) and e.movement_type == "return_restock"
    ]

    assert any(e.resolution == "scrapped" for e in return_events), (
        "Expected at least one ReturnEvent with resolution='scrapped'"
    )
    assert restock_events == [], "No return_restock events expected when scrapped"

    # Inventory must be unchanged
    new_qty = state.inventory.get("W01", {}).get(sku, 0)
    assert new_qty == initial_qty, (
        f"Expected W01[{sku}] to remain {initial_qty}, got {new_qty}"
    )


# ---------------------------------------------------------------------------
# T9: RMA removed from state after resolution
# ---------------------------------------------------------------------------


def test_rma_removed_after_resolution(state, config):
    """After an RMA resolves (restocked or scrapped), it must be absent from
    state.in_transit_returns."""
    sku = _any_sku(state)
    yesterday = _BIZ_DAY - timedelta(days=1)
    rma = {
        "rma_id": "RMA-5004",
        "order_id": "ORD-4444",
        "customer_id": state.customers[0].customer_id,
        "carrier_code": "DHL",
        "return_warehouse_id": "W01",
        "rma_status": "received_at_warehouse",
        "created_date": (yesterday - timedelta(days=3)).isoformat(),
        "transit_due_date": yesterday.isoformat(),
        "received_date": yesterday.isoformat(),
        "lines": [
            {
                "line_number": 1,
                "sku": sku,
                "quantity_returned": 2,
                "quantity_accepted": None,
                "return_reason": "overstock",
            }
        ],
        "resolution": None,
    }
    state.in_transit_returns.append(rma)

    # Use any seed; the RMA will resolve one way or the other
    state.rng.seed(7)
    returns.run(state, config, _BIZ_DAY)

    # The RMA must have been removed regardless of outcome
    remaining_ids = {r["rma_id"] for r in state.in_transit_returns}
    assert "RMA-5004" not in remaining_ids, (
        "Resolved RMA must be removed from state.in_transit_returns"
    )


# ---------------------------------------------------------------------------
# T10: Return reason code is valid
# ---------------------------------------------------------------------------


def test_return_reason_code_is_valid(state, config):
    """The return_reason in an RMA line must be one of the 5 valid codes."""
    _inject_shipped_order(state)
    _force_return_roll(state)

    events = returns.run(state, config, _BIZ_DAY)

    req_events = [e for e in events if isinstance(e, ReturnEvent) and e.event_subtype == "return_requested"]
    assert req_events, "Expected at least one return_requested event"

    rma = state.in_transit_returns[0]
    for line in rma["lines"]:
        assert line["return_reason"] in _VALID_REASON_CODES, (
            f"Invalid return_reason: {line['return_reason']!r}. "
            f"Must be one of {_VALID_REASON_CODES}"
        )


# ---------------------------------------------------------------------------
# T11: No return from cancelled order
# ---------------------------------------------------------------------------


def test_no_return_from_cancelled_order(state, config):
    """An order with status='cancelled' must not generate a return_requested."""
    sku = _any_sku(state)
    cust_id = state.customers[0].customer_id
    order = _make_shipped_order("ORD-CANCELLED", cust_id, sku, quantity_shipped=10)
    order["status"] = "cancelled"
    state.open_orders["ORD-CANCELLED"] = order

    # Force RNG to fire if it were eligible
    _force_return_roll(state)

    events = returns.run(state, config, _BIZ_DAY)

    req_events = [
        e for e in events
        if isinstance(e, ReturnEvent)
        and e.event_subtype == "return_requested"
        and e.order_id == "ORD-CANCELLED"
    ]
    assert req_events == [], (
        "Cancelled orders must not generate return_requested events"
    )


# ---------------------------------------------------------------------------
# Pre-Fix 9: Grade-B restock goes to W02
# ---------------------------------------------------------------------------


def test_inspected_and_restocked_grade_b(state, config):
    """A received_at_warehouse RMA with inspection roll in [0.60, 0.85) must
    emit resolution='restocked' with GRADE-B reference and update W02 inventory."""
    sku = _any_sku(state)
    initial_w02_qty = state.inventory.get("W02", {}).get(sku, 0)
    qty_returned = 4

    yesterday = _BIZ_DAY - timedelta(days=1)
    rma = {
        "rma_id": "RMA-GRADEB-001",
        "order_id": "ORD-GRADEB-001",
        "customer_id": state.customers[0].customer_id,
        "carrier_code": "DHL",
        "return_warehouse_id": "W01",
        "rma_status": "received_at_warehouse",
        "created_date": (yesterday - timedelta(days=2)).isoformat(),
        "transit_due_date": yesterday.isoformat(),
        "received_date": yesterday.isoformat(),
        "lines": [
            {
                "line_number": 1,
                "sku": sku,
                "quantity_returned": qty_returned,
                "quantity_accepted": None,
                "return_reason": "overstock",
            }
        ],
        "resolution": None,
    }
    state.in_transit_returns.append(rma)

    # Seed RNG so the inspection roll lands in the Grade-B range [0.60, 0.85)
    for seed in range(10000):
        state.rng = random.Random(seed)
        roll = state.rng.random()
        if 0.60 <= roll < 0.85:
            state.rng = random.Random(seed)
            break

    events = returns.run(state, config, _BIZ_DAY)

    restock_events = [e for e in events if isinstance(e, InventoryMovementEvent) and e.movement_type == "return_restock"]

    assert len(restock_events) >= 1, "Expected at least one return_restock event for Grade-B"
    assert any(e.reference_id == "GRADE-B" for e in restock_events), (
        "Expected GRADE-B reference on restock movement when 0.60 <= roll < 0.85"
    )
    # Grade-B restocked to W02
    assert any(e.warehouse_id == "W02" for e in restock_events), (
        "Expected Grade-B restock to go to W02"
    )

    # W02 inventory must have increased
    new_w02_qty = state.inventory.get("W02", {}).get(sku, 0)
    assert new_w02_qty == initial_w02_qty + qty_returned, (
        f"Expected W02[{sku}] = {initial_w02_qty + qty_returned}, got {new_w02_qty}"
    )


# ---------------------------------------------------------------------------
# T12: Partial line return quantity range
# ---------------------------------------------------------------------------


def test_partial_line_return_quantity_range(state, config):
    """quantity_returned per line must be in [ceil(qty_shipped * 0.5), qty_shipped]
    and at least 1."""
    qty_shipped = 20
    sku = _any_sku(state)
    cust_id = state.customers[0].customer_id
    order = _make_shipped_order("ORD-QTY-TEST", cust_id, sku, quantity_shipped=qty_shipped)
    state.open_orders["ORD-QTY-TEST"] = order

    _force_return_roll(state)
    returns.run(state, config, _BIZ_DAY)

    assert state.in_transit_returns, "Expected at least one RMA in state"
    rma = state.in_transit_returns[0]
    assert rma["order_id"] == "ORD-QTY-TEST"

    min_qty = math.ceil(qty_shipped * 0.5)
    for line in rma["lines"]:
        qty = line["quantity_returned"]
        assert qty >= 1, f"quantity_returned must be at least 1, got {qty}"
        assert qty >= min_qty, (
            f"quantity_returned {qty} is below min {min_qty} (50% of {qty_shipped})"
        )
        assert qty <= qty_shipped, (
            f"quantity_returned {qty} exceeds quantity_shipped {qty_shipped}"
        )
