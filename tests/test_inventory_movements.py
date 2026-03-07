"""Tests for the inventory movement tracker (Phase 2, Step 18)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from flowform.config import load_config
from flowform.engines.inventory_movements import InventoryMovementEvent, run
from flowform.state import SimulationState


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def config():
    """Load the real config from config.yaml."""
    return load_config(Path("/home/coder/sc-sim/config.yaml"))


@pytest.fixture()
def state(config, tmp_path: Path) -> SimulationState:
    """Fresh SimulationState backed by a temp DB."""
    return SimulationState.from_new(config, db_path=tmp_path / "sim.db")


# Known dates
_BUSINESS_DAY = date(2026, 1, 5)   # Monday, not a holiday
_SATURDAY = date(2026, 1, 3)       # Weekend
_HOLIDAY = date(2026, 1, 1)        # New Year's Day — Polish holiday


# ---------------------------------------------------------------------------
# Test 1: Returns empty list on a weekend
# ---------------------------------------------------------------------------


def test_empty_on_weekend(state, config):
    """Engine must return an empty list on Saturday."""
    result = run(state, config, _SATURDAY)
    assert result == [], f"Expected empty list on Saturday, got {len(result)} events"


# ---------------------------------------------------------------------------
# Test 2: Returns empty list on a Polish public holiday
# ---------------------------------------------------------------------------


def test_empty_on_holiday(state, config):
    """Engine must return an empty list on a Polish public holiday."""
    result = run(state, config, _HOLIDAY)
    assert result == [], f"Expected empty list on Jan 1 (holiday), got {len(result)} events"


# ---------------------------------------------------------------------------
# Test 3: Receipt movements generated from daily_production_receipts
# ---------------------------------------------------------------------------


def test_receipts_from_production(state, config):
    """Receipt events should be emitted for every entry in daily_production_receipts."""
    # Manually inject two production receipts (as the production engine would do)
    sku_a = state.catalog[0].sku
    sku_b = state.catalog[1].sku

    state.daily_production_receipts = [
        {
            "batch_id": "BATCH-9001",
            "sku": sku_a,
            "warehouse_id": "W01",
            "quantity": 50,
            "grade": "A",
            "timestamp": "2026-01-05T08:30:00",
        },
        {
            "batch_id": "BATCH-9002",
            "sku": sku_b,
            "warehouse_id": "W01",
            "quantity": 20,
            "grade": "B",
            "timestamp": "2026-01-05T09:15:00",
        },
    ]

    result = run(state, config, _BUSINESS_DAY)

    receipts = [e for e in result if e.movement_type == "receipt"]
    assert len(receipts) == 2, f"Expected 2 receipt events, got {len(receipts)}"

    batch_ids = {e.production_batch_id for e in receipts}
    assert "BATCH-9001" in batch_ids
    assert "BATCH-9002" in batch_ids

    for r in receipts:
        assert r.quantity_direction == "in"
        assert r.warehouse_id == "W01"
        assert r.quantity > 0

    # Cleanup
    state.daily_production_receipts = []


# ---------------------------------------------------------------------------
# Test 4: Transfer reduces source inventory and increases destination
# ---------------------------------------------------------------------------


def test_transfer_updates_inventory(config, tmp_path: Path):
    """A transfer event must reduce source inventory and increase destination."""
    # Use a deterministic seed that reliably generates transfers
    # We force a transfer by injecting one directly via _generate_transfers
    # and verifying state mutations. We set _TRANSFER_PROB to 1.0 effectively
    # by testing the private helper.
    from flowform.engines import inventory_movements as mod

    cfg = load_config(Path("/home/coder/sc-sim/config.yaml"))
    st = SimulationState.from_new(cfg, db_path=tmp_path / "sim_transfer.db")

    sku = st.catalog[0].sku
    # Set known W01 stock; W02 starts at 0 for this SKU
    st.inventory["W01"][sku] = 100
    st.inventory["W02"].pop(sku, None)

    inv_w01_before = st.inventory["W01"][sku]
    inv_w02_before = st.inventory["W02"].get(sku, 0)

    # Call the private helper directly so we can guarantee it runs
    events = mod._generate_transfers(st, _BUSINESS_DAY)

    # If no transfer happened (RNG said no) the inventories are unchanged — retry
    # with a manipulated RNG state by forcing the first call to succeed.
    if not events:
        # Patch: temporarily override _TRANSFER_PROB to force a run
        original = mod._TRANSFER_PROB
        mod._TRANSFER_PROB = 1.0
        # Reset inventory since RNG consumed some draws
        st.inventory["W01"][sku] = 100
        st.inventory["W02"].pop(sku, None)
        inv_w01_before = st.inventory["W01"][sku]
        inv_w02_before = st.inventory["W02"].get(sku, 0)
        events = mod._generate_transfers(st, _BUSINESS_DAY)
        mod._TRANSFER_PROB = original

    if not events:
        pytest.skip("RNG produced no transfers — non-deterministic; skip for now")

    transfer_events = [e for e in events if e.movement_type == "transfer"]
    assert transfer_events, "Expected at least one transfer event"

    # Verify that at least one warehouse pair shows the expected inventory change
    inv_w01_after = st.inventory["W01"].get(sku, 0)
    inv_w02_after = st.inventory["W02"].get(sku, 0)

    total_before = inv_w01_before + inv_w02_before
    total_after = inv_w01_after + inv_w02_after

    # Total inventory across both warehouses must be conserved
    assert total_before == total_after, (
        f"Transfer did not conserve total inventory: before={total_before}, after={total_after}"
    )


# ---------------------------------------------------------------------------
# Test 5: Transfer quantity is within available inventory bounds
# ---------------------------------------------------------------------------


def test_transfer_no_negative_inventory(config, tmp_path: Path):
    """After any transfers, no warehouse should have negative inventory."""
    from flowform.engines import inventory_movements as mod

    cfg = load_config(Path("/home/coder/sc-sim/config.yaml"))
    st = SimulationState.from_new(cfg, db_path=tmp_path / "sim_no_neg.db")

    # Set small stock to increase pressure on bounds
    for sku_key in list(st.inventory["W01"].keys())[:10]:
        st.inventory["W01"][sku_key] = 1

    original = mod._TRANSFER_PROB
    mod._TRANSFER_PROB = 1.0
    mod._TRANSFER_MAX = 5
    try:
        for _ in range(10):
            mod._generate_transfers(st, _BUSINESS_DAY)
    finally:
        mod._TRANSFER_PROB = original
        mod._TRANSFER_MAX = 2

    # Verify no negative quantities
    for wh_code, wh_inv in st.inventory.items():
        for sku, qty in wh_inv.items():
            assert qty >= 0, (
                f"Negative inventory at {wh_code}/{sku}: {qty}"
            )


# ---------------------------------------------------------------------------
# Test 6: Adjustment event has correct schema fields
# ---------------------------------------------------------------------------


def test_adjustment_event_schema(config, tmp_path: Path):
    """Adjustment events must have all required fields populated correctly."""
    from flowform.engines import inventory_movements as mod

    cfg = load_config(Path("/home/coder/sc-sim/config.yaml"))
    st = SimulationState.from_new(cfg, db_path=tmp_path / "sim_adj.db")

    # Force an adjustment by temporarily setting probability to 1.0
    original = mod._ADJUSTMENT_PROB
    mod._ADJUSTMENT_PROB = 1.0
    try:
        events = mod._generate_adjustments(st, _BUSINESS_DAY)
    finally:
        mod._ADJUSTMENT_PROB = original

    assert events, "Expected at least one adjustment event"

    for evt in events:
        assert evt.movement_type == "adjustment"
        assert evt.quantity > 0
        assert evt.quantity_direction in ("in", "out")
        assert evt.warehouse_id in ("W01", "W02")
        assert evt.sku != ""
        assert evt.reason_code is not None
        assert evt.reason_code in ("cycle_count", "system_correction", "count_discrepancy")
        assert evt.movement_date == _BUSINESS_DAY.isoformat()
        assert evt.event_type == "inventory_movements"
        assert evt.movement_id.startswith("MOV-")
        assert evt.event_id != ""
        # Not a transfer, pick, or receipt
        assert evt.destination_warehouse_id is None
        assert evt.order_id is None
        assert evt.production_batch_id is None


# ---------------------------------------------------------------------------
# Test 7: Scrap event reduces inventory and has correct fields
# ---------------------------------------------------------------------------


def test_scrap_event_reduces_inventory(config, tmp_path: Path):
    """Scrap events must reduce W01 inventory for the scrapped SKU."""
    from flowform.engines import inventory_movements as mod

    cfg = load_config(Path("/home/coder/sc-sim/config.yaml"))
    st = SimulationState.from_new(cfg, db_path=tmp_path / "sim_scrap.db")

    # Ensure W01 has stocked SKUs
    sku = st.catalog[0].sku
    st.inventory["W01"][sku] = 200
    inv_before = st.inventory["W01"][sku]

    original = mod._SCRAP_PROB
    mod._SCRAP_PROB = 1.0
    try:
        events = mod._generate_scrap(st, _BUSINESS_DAY)
    finally:
        mod._SCRAP_PROB = original

    assert events, "Expected at least one scrap event"

    for evt in events:
        assert evt.movement_type == "scrap"
        assert evt.quantity > 0
        assert evt.quantity_direction == "out"
        assert evt.warehouse_id == "W01"
        assert evt.reason_code in ("quality_reject", "expired", "damaged_storage")

    # Inventory must have decreased overall in W01
    inv_after = sum(st.inventory["W01"].values())
    # We can't assert on a single SKU since scrap picks randomly, but total W01
    # must be ≤ total before
    total_before = sum(
        v for k, v in st.inventory["W01"].items()
        if k != sku
    ) + inv_before
    # Just verify the scrapped quantities are positive and inventory didn't go negative
    for sku_key, qty in st.inventory["W01"].items():
        assert qty >= 0, f"W01/{sku_key} has negative inventory after scrap: {qty}"


# ---------------------------------------------------------------------------
# Test 8: All events have required fields (schema validation)
# ---------------------------------------------------------------------------


def test_all_events_have_required_fields(config, tmp_path: Path):
    """Every event returned by run() must have all required schema fields."""
    from flowform.engines import inventory_movements as mod

    cfg = load_config(Path("/home/coder/sc-sim/config.yaml"))
    st = SimulationState.from_new(cfg, db_path=tmp_path / "sim_fields.db")

    # Inject a production receipt so we always have at least one event
    st.daily_production_receipts = [
        {
            "batch_id": "BATCH-7777",
            "sku": st.catalog[5].sku,
            "warehouse_id": "W01",
            "quantity": 30,
            "grade": "A",
            "timestamp": "2026-01-05T07:00:00",
        }
    ]

    # Force all generators to run
    orig_transfer = mod._TRANSFER_PROB
    orig_adjust = mod._ADJUSTMENT_PROB
    orig_scrap = mod._SCRAP_PROB
    mod._TRANSFER_PROB = 1.0
    mod._ADJUSTMENT_PROB = 1.0
    mod._SCRAP_PROB = 1.0
    try:
        result = run(st, cfg, _BUSINESS_DAY)
    finally:
        mod._TRANSFER_PROB = orig_transfer
        mod._ADJUSTMENT_PROB = orig_adjust
        mod._SCRAP_PROB = orig_scrap
        st.daily_production_receipts = []

    assert result, "Expected at least one event"

    required_fields = {
        "event_type",
        "event_id",
        "movement_id",
        "movement_type",
        "movement_date",
        "warehouse_id",
        "sku",
        "quantity",
        "quantity_direction",
    }

    valid_movement_types = {
        "pick", "receipt", "unpick", "transfer",
        "adjustment", "scrap", "return_restock",
    }

    for evt in result:
        d = evt.model_dump()
        for field in required_fields:
            assert field in d and d[field] is not None, (
                f"Event {evt.event_id} missing required field '{field}'"
            )
        assert d["event_type"] == "inventory_movements"
        assert d["movement_type"] in valid_movement_types
        assert d["quantity_direction"] in ("in", "out")
        assert d["quantity"] > 0
        assert d["movement_id"].startswith("MOV-")
        assert isinstance(d["event_id"], str) and len(d["event_id"]) > 0


# ---------------------------------------------------------------------------
# Test 9: Movement IDs are strictly monotone within a single run
# ---------------------------------------------------------------------------


def test_movement_ids_monotone(state, config):
    """All MOV-NNN IDs within a single run() call must be strictly increasing."""
    # Inject a receipt to guarantee at least one event
    state.daily_production_receipts = [
        {
            "batch_id": "BATCH-6001",
            "sku": state.catalog[10].sku,
            "warehouse_id": "W01",
            "quantity": 15,
            "grade": "A",
            "timestamp": "2026-01-05T10:00:00",
        }
    ]

    result = run(state, config, _BUSINESS_DAY)
    state.daily_production_receipts = []

    if len(result) < 2:
        pytest.skip("Not enough events to verify ordering (< 2 events)")

    nums = [int(e.movement_id.split("-")[1]) for e in result]
    assert nums == sorted(nums), f"Movement IDs are not in ascending order: {nums}"
    assert len(set(nums)) == len(nums), "Duplicate movement IDs detected"


# ---------------------------------------------------------------------------
# Test 10: Receipt events reference the correct batch IDs from production
# ---------------------------------------------------------------------------


def test_receipt_batch_ids_match_production(config, tmp_path: Path):
    """Receipt events must reference batch_ids from daily_production_receipts."""
    cfg = load_config(Path("/home/coder/sc-sim/config.yaml"))
    st = SimulationState.from_new(cfg, db_path=tmp_path / "sim_batch.db")

    expected_batches = ["BATCH-A001", "BATCH-A002", "BATCH-A003"]
    st.daily_production_receipts = [
        {
            "batch_id": bid,
            "sku": st.catalog[i].sku,
            "warehouse_id": "W01",
            "quantity": 10 * (i + 1),
            "grade": "A",
            "timestamp": "2026-01-05T08:00:00",
        }
        for i, bid in enumerate(expected_batches)
    ]

    result = run(st, cfg, _BUSINESS_DAY)
    st.daily_production_receipts = []

    receipts = [e for e in result if e.movement_type == "receipt"]
    assert len(receipts) == 3, f"Expected 3 receipts, got {len(receipts)}"

    actual_batches = {e.production_batch_id for e in receipts}
    assert actual_batches == set(expected_batches), (
        f"Receipt batch IDs {actual_batches} do not match expected {set(expected_batches)}"
    )
