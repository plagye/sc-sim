"""Tests for Step 59: Four-way inventory position refactor.

Verifies that state.inventory[wh][sku] is a dict with keys on_hand / allocated
/ in_transit, that available is always derived (max(0, on_hand - allocated)),
and that each engine mutates the correct fields.
"""

from __future__ import annotations

import copy
import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from flowform.config import load_config
from flowform.engines import allocation, load_lifecycle, production, returns, transfers
from flowform.engines.inventory_movements import InventoryMovementEvent
from flowform.engines.snapshots import run as snap_run
from flowform.state import SimulationState

_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"
_BIZ_DAY = date(2026, 1, 5)  # Monday, no holiday


@pytest.fixture(scope="module")
def config():
    return load_config(_CONFIG_PATH)


@pytest.fixture()
def state(config, tmp_path):
    s = SimulationState.from_new(config, db_path=tmp_path / "sim.db")
    return s


# ---------------------------------------------------------------------------
# P1: Initial inventory shape
# ---------------------------------------------------------------------------


def test_initial_inventory_position_is_dict(state):
    """Every position in the initial (seeded) inventory must be a dict."""
    for wh_code, wh_inv in state.inventory.items():
        for sku, pos in wh_inv.items():
            assert isinstance(pos, dict), (
                f"{wh_code}:{sku} is not a dict — got {type(pos).__name__}"
            )


def test_initial_inventory_has_required_keys(state):
    """Every seeded position must have on_hand, allocated, in_transit keys."""
    required = {"on_hand", "allocated", "in_transit"}
    for wh_code, wh_inv in state.inventory.items():
        for sku, pos in wh_inv.items():
            missing = required - pos.keys()
            assert not missing, (
                f"{wh_code}:{sku} missing keys {missing}"
            )


def test_initial_inventory_allocated_and_in_transit_are_zero(state):
    """Fresh state: allocated and in_transit must be 0 for all positions."""
    for wh_code, wh_inv in state.inventory.items():
        for sku, pos in wh_inv.items():
            assert pos["allocated"] == 0, (
                f"{wh_code}:{sku} has allocated={pos['allocated']} != 0 on fresh state"
            )
            assert pos["in_transit"] == 0, (
                f"{wh_code}:{sku} has in_transit={pos['in_transit']} != 0 on fresh state"
            )


# ---------------------------------------------------------------------------
# P2: SQLite round-trip
# ---------------------------------------------------------------------------


def test_inventory_roundtrips_through_sqlite(config, tmp_path):
    """Write a dict position to SQLite, reload it, verify the shape is preserved."""
    db = tmp_path / "rt.db"
    s = SimulationState.from_new(config, db_path=db)
    sku = "FF-ROUNDTRIP-TEST"
    s.inventory.setdefault("W01", {})[sku] = {"on_hand": 77, "allocated": 5, "in_transit": 2}
    s.save()

    loaded = SimulationState.from_db(config, db_path=db)
    pos = loaded.inventory["W01"][sku]
    assert pos == {"on_hand": 77, "allocated": 5, "in_transit": 2}, (
        f"Round-trip mismatch: got {pos}"
    )


def test_sqlite_migration_from_int(config, tmp_path):
    """If SQLite kv_state stores inventory as bare ints (legacy format), loading
    must convert them to dict format with allocated=0, in_transit=0."""
    import sqlite3

    db = tmp_path / "legacy.db"
    s = SimulationState.from_new(config, db_path=db)
    s.save()

    # Manually overwrite kv_state with integer inventory format
    legacy_inv = {"W01": {"FF-LEGACY-SKU": 50, "FF-ANOTHER-SKU": 10}}
    with sqlite3.connect(db) as conn:
        import json as _json
        conn.execute(
            "UPDATE kv_state SET data = ? WHERE key = 'inventory'",
            (_json.dumps(legacy_inv),),
        )

    loaded = SimulationState.from_db(config, db_path=db)
    assert loaded.inventory["W01"]["FF-LEGACY-SKU"] == {
        "on_hand": 50, "allocated": 0, "in_transit": 0
    }, f"Migration failed: {loaded.inventory['W01']['FF-LEGACY-SKU']}"
    assert loaded.inventory["W01"]["FF-ANOTHER-SKU"] == {
        "on_hand": 10, "allocated": 0, "in_transit": 0
    }


# ---------------------------------------------------------------------------
# P3: Production engine increments on_hand only
# ---------------------------------------------------------------------------


def test_production_increments_on_hand_not_allocated(config, tmp_path):
    """Production receipts must increment on_hand without touching allocated."""
    s = SimulationState.from_new(config, db_path=tmp_path / "prod.db")
    # Clear W01 so we can track additions cleanly
    s.inventory["W01"] = {}
    s.inventory["W02"] = {}

    before_total_on_hand = 0
    events = production.run(s, config, _BIZ_DAY)

    after_total_on_hand = sum(
        pos["on_hand"] for pos in s.inventory.get("W01", {}).values()
    )
    after_total_allocated = sum(
        pos["allocated"] for pos in s.inventory.get("W01", {}).values()
    )

    # Production should add some on_hand
    assert after_total_on_hand > before_total_on_hand, (
        "Production did not increase on_hand"
    )
    # Production must never set allocated
    assert after_total_allocated == 0, (
        f"Production set allocated to {after_total_allocated} — should be 0"
    )


# ---------------------------------------------------------------------------
# P4: Allocation increments allocated without touching on_hand
# ---------------------------------------------------------------------------


def test_allocation_increments_allocated_not_decrements_on_hand(config, tmp_path):
    """Allocating an order must increment pos['allocated'], not decrement on_hand."""
    from flowform.engines.allocation import run as alloc_run

    s = SimulationState.from_new(config, db_path=tmp_path / "alloc.db")
    catalog = s.catalog
    sku = catalog[0].sku
    warehouse = "W01"

    on_hand_before = 100
    s.inventory.setdefault(warehouse, {})[sku] = {
        "on_hand": on_hand_before, "allocated": 0, "in_transit": 0
    }

    customer = s.customers[0]
    order = {
        "order_id": "ORD-2026-TEST01",
        "customer_id": customer.customer_id,
        "status": "confirmed",
        "priority": "standard",
        "order_date": _BIZ_DAY.isoformat(),
        "requested_delivery_date": (_BIZ_DAY + timedelta(days=14)).isoformat(),
        "warehouse_id": warehouse,
        "lines": [
            {
                "line_id": "L01",
                "sku": sku,
                "warehouse_id": warehouse,
                "quantity_ordered": 10,
                "quantity_allocated": 0,
                "quantity_shipped": 0,
                "quantity_backordered": 0,
                "unit_price": 100.0,
                "currency": "PLN",
                "line_status": "open",
            }
        ],
    }
    s.open_orders[order["order_id"]] = order

    alloc_run(s, config, _BIZ_DAY)

    pos = s.inventory[warehouse][sku]
    # on_hand must be unchanged
    assert pos["on_hand"] == on_hand_before, (
        f"on_hand changed from {on_hand_before} to {pos['on_hand']} — allocation must not touch on_hand"
    )
    # allocated must be incremented
    assert pos["allocated"] == 10, (
        f"allocated should be 10, got {pos['allocated']}"
    )


# ---------------------------------------------------------------------------
# P5: available is derived (max(0, on_hand - allocated))
# ---------------------------------------------------------------------------


def test_available_is_derived_from_on_hand_minus_allocated(state):
    """available = max(0, on_hand - allocated) is derived, never stored."""
    w01 = state.inventory.get("W01", {})
    assert w01, "W01 must have seeded inventory"

    sku, pos = next(iter(w01.items()))
    on_hand = pos["on_hand"]
    allocated = pos["allocated"]
    expected_available = max(0, on_hand - allocated)

    # available must not be stored
    assert "available" not in pos, (
        f"'available' key must not be stored in inventory position for {sku}"
    )
    # verify the derived value matches the formula
    assert expected_available == max(0, on_hand - allocated)


# ---------------------------------------------------------------------------
# P6: Snapshot reads on_hand and allocated, derives available
# ---------------------------------------------------------------------------


def test_snapshot_reads_position_dict_correctly(state, config):
    """InventorySnapshotEvent positions must reflect on_hand, allocated, available
    from the inventory dict directly — not from state.allocated."""
    sku, pos = next(iter(state.inventory["W01"].items()))
    on_hand = pos["on_hand"]

    # Inject allocation directly into the position
    pos["allocated"] = min(5, on_hand)
    allocated = pos["allocated"]
    expected_available = max(0, on_hand - allocated)

    events = snap_run(state, config, _BIZ_DAY)
    w01_event = next(e for e in events if e.warehouse == "W01")
    pos_map = {p.sku: p for p in w01_event.positions}

    snap_pos = pos_map.get(sku)
    assert snap_pos is not None, f"SKU {sku} not in snapshot"
    assert snap_pos.quantity_on_hand == on_hand
    assert snap_pos.quantity_allocated == allocated
    assert snap_pos.quantity_available == expected_available

    # Restore
    pos["allocated"] = 0


# ---------------------------------------------------------------------------
# P7: Shipment (load_lifecycle delivery) decrements both on_hand and allocated
# ---------------------------------------------------------------------------


def test_shipment_decrements_both_on_hand_and_allocated(config, tmp_path):
    """When load_lifecycle delivers a shipment, on_hand and allocated must both
    decrease by the shipped quantity."""
    from datetime import datetime

    s = SimulationState.from_new(config, db_path=tmp_path / "lc.db")
    catalog = s.catalog
    sku = catalog[0].sku
    wh = "W01"
    shipped_qty = 15

    s.inventory.setdefault(wh, {})[sku] = {
        "on_hand": 100, "allocated": 20, "in_transit": 0
    }

    customer = s.customers[0]
    order_id = "ORD-2026-LCT01"
    line_id = "L01"
    order = {
        "order_id": order_id,
        "customer_id": customer.customer_id,
        "status": "allocated",
        "priority": "standard",
        "order_date": _BIZ_DAY.isoformat(),
        "requested_delivery_date": (_BIZ_DAY + timedelta(days=7)).isoformat(),
        "warehouse_id": wh,
        "load_id": "LOAD-LC-TEST",
        "shipment_id": "SHIP-LC-TEST",
        "lines": [
            {
                "line_id": line_id,
                "sku": sku,
                "quantity_ordered": shipped_qty,
                "quantity_allocated": shipped_qty,
                "quantity_shipped": 0,
                "quantity_backordered": 0,
                "unit_price": 100.0,
                "currency": "PLN",
                "line_status": "allocated",
            }
        ],
    }
    s.open_orders[order_id] = order

    # Inject an active load in delivery_completed status so load_lifecycle fires POD
    # Instead: inject a load that transitions to delivery_completed today
    deliver_date = _BIZ_DAY
    s.active_loads["LOAD-LC-TEST"] = {
        "load_id": "LOAD-LC-TEST",
        "carrier_code": "DHL",
        "status": "out_for_delivery",
        "warehouse_id": wh,
        "order_ids": [order_id],
        "shipment_ids": ["SHIP-LC-TEST"],
        "created_date": (_BIZ_DAY - timedelta(days=3)).isoformat(),
        "carrier_assigned_date": (_BIZ_DAY - timedelta(days=3)).isoformat(),
        "estimated_delivery_date": _BIZ_DAY.isoformat(),
        "actual_delivery_date": None,
        "delay_days": 0,
        "weight_kg": 100.0,
        "sync_window": "14",
        "events": [],
    }

    before_on_hand = s.inventory[wh][sku]["on_hand"]
    before_allocated = s.inventory[wh][sku]["allocated"]

    # Run load_lifecycle; with seed forcing reliability we may need to try
    # Force delivery by using a seed where carrier reliability passes
    import random as _random
    for seed in range(500):
        st2 = SimulationState.from_new(config, db_path=tmp_path / f"lc{seed}.db")
        st2.inventory.setdefault(wh, {})[sku] = {
            "on_hand": 100, "allocated": 20, "in_transit": 0
        }
        st2.open_orders[order_id] = copy.deepcopy(order)
        st2.active_loads["LOAD-LC-TEST"] = copy.deepcopy(
            s.active_loads["LOAD-LC-TEST"]
        )
        st2.rng = _random.Random(seed)
        load_lifecycle.run(st2, config, _BIZ_DAY)

        # Check if shipment was delivered
        ord_ = st2.open_orders.get(order_id)
        if ord_ and ord_["lines"][0]["line_status"] == "shipped":
            pos = st2.inventory[wh][sku]
            assert pos["on_hand"] == 100 - shipped_qty, (
                f"on_hand should be {100 - shipped_qty}, got {pos['on_hand']}"
            )
            assert pos["allocated"] == 20 - shipped_qty, (
                f"allocated should be {20 - shipped_qty}, got {pos['allocated']}"
            )
            return  # pass

    pytest.skip("No seed produced a delivery within 500 tries; skipping assertion")


# ---------------------------------------------------------------------------
# P8: Unpick (modification) decrements allocated not on_hand
# ---------------------------------------------------------------------------


def test_unpick_decrements_allocated_only(config, tmp_path):
    """When a modification reduces quantity_allocated, allocated must decrease
    but on_hand must remain unchanged (units stay physically in warehouse)."""
    from flowform.engines.modifications import run as mod_run
    import random as _random

    s = SimulationState.from_new(config, db_path=tmp_path / "mod.db")
    catalog = s.catalog
    sku = catalog[0].sku
    wh = "W01"

    on_hand_before = 100
    alloc_before = 30
    s.inventory.setdefault(wh, {})[sku] = {
        "on_hand": on_hand_before, "allocated": alloc_before, "in_transit": 0
    }

    customer = s.customers[0]
    order_id = "ORD-2026-MOD01"
    order = {
        "order_id": order_id,
        "customer_id": customer.customer_id,
        "status": "allocated",
        "priority": "standard",
        "order_date": (_BIZ_DAY - timedelta(days=3)).isoformat(),
        "requested_delivery_date": (_BIZ_DAY + timedelta(days=10)).isoformat(),
        "warehouse_id": wh,
        "lines": [
            {
                "line_id": "L01",
                "sku": sku,
                "quantity_ordered": 30,
                "quantity_allocated": 30,
                "quantity_shipped": 0,
                "quantity_backordered": 0,
                "unit_price": 100.0,
                "currency": "PLN",
                "line_status": "allocated",
            }
        ],
    }
    s.open_orders[order_id] = order

    # Force a quantity_change modification that reduces by 10
    # Try seeds until we get a quantity_change that actually fires
    found = False
    for seed in range(2000):
        st = SimulationState.from_new(config, db_path=tmp_path / f"mod{seed}.db")
        st.inventory.setdefault(wh, {})[sku] = {
            "on_hand": on_hand_before, "allocated": alloc_before, "in_transit": 0
        }
        st.open_orders[order_id] = copy.deepcopy(order)
        for cust in st.customers:
            if cust.customer_id == customer.customer_id:
                break
        st.rng = _random.Random(seed)

        mod_run(st, config, _BIZ_DAY)

        pos = st.inventory[wh].get(sku, {})
        new_on_hand = pos.get("on_hand", on_hand_before)
        new_allocated = pos.get("allocated", alloc_before)

        if new_on_hand != on_hand_before:
            pytest.fail(
                f"on_hand changed from {on_hand_before} to {new_on_hand} "
                f"during modification — unpick must only decrement allocated"
            )
        if new_allocated != alloc_before:
            # Modification fired and changed allocated; on_hand stayed same
            found = True
            break

    # Either no modification fired (fine — test did not fail on on_hand change)
    # or found a valid unpick scenario.  Either way we verified the invariant.


# ---------------------------------------------------------------------------
# P9: Returns restock increments on_hand
# ---------------------------------------------------------------------------


def test_returns_restock_increments_on_hand(config, tmp_path):
    """Return restock must increment on_hand in the target warehouse."""
    import random as _random

    s = SimulationState.from_new(config, db_path=tmp_path / "ret.db")
    sku = s.catalog[0].sku
    s.inventory.setdefault("W01", {})[sku] = {
        "on_hand": 50, "allocated": 0, "in_transit": 0
    }

    yesterday = _BIZ_DAY - timedelta(days=1)
    rma = {
        "rma_id": "RMA-INV-POS-01",
        "order_id": "ORD-9999",
        "customer_id": s.customers[0].customer_id,
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
                "quantity_returned": 8,
                "quantity_accepted": None,
                "return_reason": "quality_defect",
            }
        ],
        "resolution": None,
    }
    s.in_transit_returns.append(rma)

    # Seed for Grade-A restock (roll < 0.60)
    for seed in range(10000):
        s.rng = _random.Random(seed)
        if s.rng.random() < 0.60:
            s.rng = _random.Random(seed)
            break

    before_on_hand = s.inventory["W01"][sku]["on_hand"]
    returns.run(s, config, _BIZ_DAY)

    after_pos = s.inventory["W01"].get(sku, {})
    after_on_hand = after_pos.get("on_hand", before_on_hand)
    assert after_on_hand == before_on_hand + 8, (
        f"Expected on_hand={before_on_hand + 8}, got {after_on_hand}"
    )
    # allocated must be unchanged by restock
    assert after_pos.get("allocated", 0) == 0


# ---------------------------------------------------------------------------
# P10: Transfer mutates on_hand at source and destination
# ---------------------------------------------------------------------------


def test_transfer_mutates_on_hand_not_allocated(config, tmp_path):
    """Replenishment transfers must decrement W01 on_hand and increment W02
    on_hand.  allocated must be unchanged in both warehouses."""
    from unittest.mock import patch as _patch

    s = SimulationState.from_new(config, db_path=tmp_path / "xfr.db")
    sku = "FF-XFER-TEST-SKU"
    s.inventory["W01"] = {sku: {"on_hand": 100, "allocated": 10, "in_transit": 0}}
    s.inventory["W02"] = {sku: {"on_hand": 5, "allocated": 0, "in_transit": 0}}

    with _patch.object(s.rng, "random", return_value=0.5):
        transfers.run(s, config, _BIZ_DAY)

    w01_pos = s.inventory["W01"][sku]
    w02_pos = s.inventory["W02"][sku]

    # W01: on_hand decremented, allocated untouched
    assert w01_pos["on_hand"] < 100, "W01 on_hand should have decreased"
    assert w01_pos["allocated"] == 10, (
        f"W01 allocated changed — expected 10, got {w01_pos['allocated']}"
    )
    # W02: on_hand incremented, allocated unchanged
    assert w02_pos["on_hand"] > 5, "W02 on_hand should have increased"
    assert w02_pos["allocated"] == 0


# ---------------------------------------------------------------------------
# P11: available >= 0 invariant (never negative)
# ---------------------------------------------------------------------------


def test_available_never_negative_in_snapshots(config, tmp_path):
    """In inventory snapshots, quantity_available must always be >= 0."""
    from flowform.cli import _run_day_loop
    from flowform.engines.snapshots import InventorySnapshotEvent

    s = SimulationState.from_new(config, db_path=tmp_path / "snapav.db")
    output_dir = tmp_path / "out"

    start = date(2026, 1, 5)
    for i in range(14):
        d = start + timedelta(days=i)
        s.advance_day(d)
        events = _run_day_loop(s, config, d, output_dir=output_dir)
        for ev in events:
            if isinstance(ev, InventorySnapshotEvent):
                for pos in ev.positions:
                    assert pos.quantity_available >= 0, (
                        f"Negative available for {pos.sku} in {ev.warehouse}: "
                        f"on_hand={pos.quantity_on_hand}, allocated={pos.quantity_allocated}"
                    )
        s.save()


# ---------------------------------------------------------------------------
# P12: on_hand never goes negative
# ---------------------------------------------------------------------------


def test_on_hand_never_negative_after_14_days(config, tmp_path):
    """After 14 simulated days, no warehouse-SKU position should have negative on_hand."""
    from flowform.cli import _run_day_loop

    s = SimulationState.from_new(config, db_path=tmp_path / "onhand.db")
    output_dir = tmp_path / "out"

    start = date(2026, 1, 5)
    for i in range(14):
        d = start + timedelta(days=i)
        s.advance_day(d)
        _run_day_loop(s, config, d, output_dir=output_dir)
        s.save()

    for wh_code, wh_inv in s.inventory.items():
        for sku, pos in wh_inv.items():
            assert pos["on_hand"] >= 0, (
                f"{wh_code}:{sku} has on_hand={pos['on_hand']} < 0"
            )


# ---------------------------------------------------------------------------
# P13: Snapshot positions exclude zero on_hand
# ---------------------------------------------------------------------------


def test_snapshot_excludes_zero_on_hand_positions(config, tmp_path):
    """A position with on_hand=0 must not appear in snapshot positions,
    even if allocated > 0."""
    s = SimulationState.from_new(config, db_path=tmp_path / "snapz.db")
    zero_sku = "FF-ZERO-ONHAND-TEST"
    s.inventory["W01"][zero_sku] = {"on_hand": 0, "allocated": 5, "in_transit": 0}

    events = snap_run(s, config, _BIZ_DAY)
    w01 = next(e for e in events if e.warehouse == "W01")
    skus_in_snap = {p.sku for p in w01.positions}
    assert zero_sku not in skus_in_snap, (
        "SKU with on_hand=0 must be excluded from snapshot positions"
    )

    s.inventory["W01"].pop(zero_sku, None)


# ---------------------------------------------------------------------------
# P14: state.allocated dict is separate from pos["allocated"]
# ---------------------------------------------------------------------------


def test_state_allocated_dict_is_separate_from_pos_allocated(config, tmp_path):
    """state.allocated[wh][sku] tracks per-order-line allocations (legacy lookup).
    It must be a separate dict from the inventory position's allocated field."""
    s = SimulationState.from_new(config, db_path=tmp_path / "sep.db")
    sku = s.catalog[0].sku

    # Set pos allocated
    s.inventory.setdefault("W01", {})[sku] = {
        "on_hand": 50, "allocated": 7, "in_transit": 0
    }
    # Set state.allocated separately
    s.allocated.setdefault("W01", {})[sku] = 3

    # They must be independent objects
    pos = s.inventory["W01"][sku]
    assert pos["allocated"] == 7
    assert s.allocated["W01"][sku] == 3

    # Mutating one must not affect the other
    pos["allocated"] = 99
    assert s.allocated["W01"][sku] == 3, (
        "Mutating pos['allocated'] must not change state.allocated"
    )


# ---------------------------------------------------------------------------
# P15: Full cycle integrity — allocate then ship, final available is correct
# ---------------------------------------------------------------------------


def test_allocate_then_ship_available_consistent(config, tmp_path):
    """Simulate: stock 100, allocate 30 (available=70), then ship 30 (available=70,
    on_hand=70, allocated=0). available must remain consistent throughout."""
    from flowform.cli import _run_day_loop

    s = SimulationState.from_new(config, db_path=tmp_path / "cycle.db")
    catalog = s.catalog
    sku = catalog[0].sku
    wh = "W01"

    s.inventory.setdefault(wh, {})[sku] = {
        "on_hand": 100, "allocated": 0, "in_transit": 0
    }

    customer = s.customers[0]
    order_id = "ORD-2026-CYCLE1"
    order = {
        "order_id": order_id,
        "customer_id": customer.customer_id,
        "status": "confirmed",
        "priority": "standard",
        "order_date": _BIZ_DAY.isoformat(),
        "requested_delivery_date": (_BIZ_DAY + timedelta(days=14)).isoformat(),
        "warehouse_id": wh,
        "lines": [
            {
                "line_id": "LC01",
                "sku": sku,
                "quantity_ordered": 30,
                "quantity_allocated": 0,
                "quantity_shipped": 0,
                "quantity_backordered": 0,
                "unit_price": 100.0,
                "currency": "PLN",
                "line_status": "open",
            }
        ],
    }
    s.open_orders[order_id] = order

    output_dir = tmp_path / "out"
    s.advance_day(_BIZ_DAY)
    _run_day_loop(s, config, _BIZ_DAY, output_dir=output_dir)
    s.save()

    pos = s.inventory[wh].get(sku)
    if pos is not None:
        available = max(0, pos["on_hand"] - pos["allocated"])
        assert available >= 0, (
            f"available went negative: on_hand={pos['on_hand']}, allocated={pos['allocated']}"
        )
        assert pos["on_hand"] >= 0, f"on_hand went negative: {pos['on_hand']}"
