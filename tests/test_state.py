"""Tests for SimulationState + SQLite persistence (Step 10)."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from flowform.config import load_config
from flowform.state import SimulationState


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def config():
    """Load the real config from config.yaml."""
    return load_config(Path("/home/coder/sc-sim/config.yaml"))


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    """Return a temporary DB path that does not yet exist."""
    return tmp_path / "simulation.db"


# ---------------------------------------------------------------------------
# Test 1: from_new() creates DB file and returns a valid state object
# ---------------------------------------------------------------------------


def test_from_new_creates_db_and_returns_state(config, db_path):
    state = SimulationState.from_new(config, db_path=db_path)

    assert db_path.exists(), "DB file should be created by from_new()"
    assert state.sim_day == 0
    assert state.current_date == config.simulation.start_date
    assert state.start_date == config.simulation.start_date
    assert isinstance(state.rng, __import__("random").Random)
    assert len(state.catalog) == 2500
    assert len(state.customers) == 60
    assert len(state.carriers) == 5
    assert len(state.warehouses) == 2

    # Operational defaults
    assert state.exchange_rates == {"EUR": 4.30, "USD": 4.05}
    assert state.schema_flags == {"sales_channel": False, "incoterms": False}
    assert state.open_orders == {}
    assert state.backorder_queue == []
    assert state.active_loads == {}
    assert state.in_transit_returns == []

    # All customer balances start at zero
    assert all(v == 0.0 for v in state.customer_balances.values())
    assert len(state.customer_balances) == 60


# ---------------------------------------------------------------------------
# Test 2: save() + from_db() round-trips scalars and operational tables
# ---------------------------------------------------------------------------


def test_round_trip_scalars_and_operational(config, db_path):
    state = SimulationState.from_new(config, db_path=db_path)

    # Mutate some operational state
    state.sim_day = 7
    state.current_date = config.simulation.start_date + timedelta(days=10)
    state.inventory["W01"]["FF-BL50-CS-PN16-FMAN"] = 42
    state.allocated["W02"]["FF-BL50-CS-PN16-FMAN"] = 5
    state.customer_balances["CUST-0001"] = 12345.67
    state.exchange_rates["EUR"] = 4.35
    state.exchange_rates["USD"] = 4.10
    state.schema_flags["sales_channel"] = True

    state.save()

    loaded = SimulationState.from_db(config, db_path=db_path)

    assert loaded.sim_day == 7
    assert loaded.current_date == state.current_date
    assert loaded.start_date == state.start_date
    assert loaded.inventory["W01"]["FF-BL50-CS-PN16-FMAN"] == 42
    assert loaded.allocated["W02"]["FF-BL50-CS-PN16-FMAN"] == 5
    assert loaded.customer_balances["CUST-0001"] == pytest.approx(12345.67)
    assert loaded.exchange_rates["EUR"] == pytest.approx(4.35)
    assert loaded.exchange_rates["USD"] == pytest.approx(4.10)
    assert loaded.schema_flags["sales_channel"] is True
    assert loaded.schema_flags["incoterms"] is False


# ---------------------------------------------------------------------------
# Test 3: advance_day() increments sim_day and updates current_date
# ---------------------------------------------------------------------------


def test_advance_day(config, db_path):
    state = SimulationState.from_new(config, db_path=db_path)
    initial_date = state.current_date
    initial_day = state.sim_day

    next_date = initial_date + timedelta(days=1)
    state.advance_day(next_date)

    assert state.sim_day == initial_day + 1
    assert state.current_date == next_date


def test_advance_day_multiple_times(config, db_path):
    state = SimulationState.from_new(config, db_path=db_path)
    d = state.current_date
    for i in range(5):
        d = d + timedelta(days=1)
        state.advance_day(d)

    assert state.sim_day == 5
    assert state.current_date == config.simulation.start_date + timedelta(days=5)


# ---------------------------------------------------------------------------
# Test 4: RNG state survives round-trip
# ---------------------------------------------------------------------------


def test_rng_state_round_trip(config, db_path):
    state = SimulationState.from_new(config, db_path=db_path)

    # Draw some numbers to advance the RNG past its initial state
    for _ in range(50):
        state.rng.random()

    state.save()

    # Capture the sequence produced *after* save
    expected = [state.rng.random() for _ in range(20)]

    # Reload and produce the same sequence from the same RNG position
    loaded = SimulationState.from_db(config, db_path=db_path)
    actual = [loaded.rng.random() for _ in range(20)]

    assert actual == expected, "RNG must produce identical sequence after round-trip"


# ---------------------------------------------------------------------------
# Test 5: from_db() raises FileNotFoundError if DB does not exist
# ---------------------------------------------------------------------------


def test_from_db_raises_if_no_db(config, tmp_path):
    missing = tmp_path / "nonexistent.db"
    with pytest.raises(FileNotFoundError):
        SimulationState.from_db(config, db_path=missing)


# ---------------------------------------------------------------------------
# Test 6: Master data survives round-trip: customer count and catalog size
# ---------------------------------------------------------------------------


def test_master_data_round_trip(config, db_path):
    state = SimulationState.from_new(config, db_path=db_path)
    original_catalog_skus = [e.sku for e in state.catalog]
    original_customer_ids = [c.customer_id for c in state.customers]

    state.save()
    loaded = SimulationState.from_db(config, db_path=db_path)

    assert len(loaded.catalog) == len(state.catalog)
    assert len(loaded.customers) == len(state.customers)
    assert [e.sku for e in loaded.catalog] == original_catalog_skus
    assert [c.customer_id for c in loaded.customers] == original_customer_ids

    # Spot-check a customer's fields
    orig_c = state.customers[0]
    loaded_c = loaded.customers[0]
    assert loaded_c.customer_id == orig_c.customer_id
    assert loaded_c.segment == orig_c.segment
    assert loaded_c.credit_limit == pytest.approx(orig_c.credit_limit)
    assert loaded_c.seasonal_profile == orig_c.seasonal_profile
    assert loaded_c.ordering_profile.order_size_range == orig_c.ordering_profile.order_size_range

    # Carriers and warehouses
    assert len(loaded.carriers) == 5
    assert len(loaded.warehouses) == 2
    assert {c.code for c in loaded.carriers} == {c.code for c in state.carriers}
    assert {w.code for w in loaded.warehouses} == {w.code for w in state.warehouses}


# ---------------------------------------------------------------------------
# Test 7: Backorder queue round-trip
# ---------------------------------------------------------------------------


def test_backorder_queue_round_trip(config, db_path):
    state = SimulationState.from_new(config, db_path=db_path)

    entries = [
        {
            "order_id": "ORD-001",
            "line_id": "L1",
            "sku": "FF-BL100-CS-PN16-FMAN",
            "warehouse": "W01",
            "quantity": 10,
            "days_waiting": 3,
            "priority": "standard",
        },
        {
            "order_id": "ORD-002",
            "line_id": "L2",
            "sku": "FF-GA150-SS316-PN40-FPNE",
            "warehouse": "W02",
            "quantity": 5,
            "days_waiting": 15,
            "priority": "express",
        },
    ]
    state.backorder_queue.extend(entries)
    state.save()

    loaded = SimulationState.from_db(config, db_path=db_path)

    assert len(loaded.backorder_queue) == 2
    assert loaded.backorder_queue[0]["order_id"] == "ORD-001"
    assert loaded.backorder_queue[1]["priority"] == "express"
    assert loaded.backorder_queue[1]["days_waiting"] == 15


# ---------------------------------------------------------------------------
# Test 8: from_new() twice (reset scenario) overwrites the first
# ---------------------------------------------------------------------------


def test_from_new_reset_overwrites(config, db_path):
    state1 = SimulationState.from_new(config, db_path=db_path)
    state1.sim_day = 42
    state1.customer_balances["CUST-0001"] = 99999.0
    state1.save()

    # Second reset should start fresh
    state2 = SimulationState.from_new(config, db_path=db_path)

    assert state2.sim_day == 0
    assert state2.customer_balances["CUST-0001"] == 0.0

    # Loading from DB after reset should give fresh state, not the old one
    loaded = SimulationState.from_db(config, db_path=db_path)
    assert loaded.sim_day == 0
    assert loaded.customer_balances["CUST-0001"] == 0.0


# ---------------------------------------------------------------------------
# Test 9: open_orders and active_loads round-trip
# ---------------------------------------------------------------------------


def test_open_orders_and_active_loads_round_trip(config, db_path):
    state = SimulationState.from_new(config, db_path=db_path)

    state.open_orders["ORD-999"] = {
        "order_id": "ORD-999",
        "customer_id": "CUST-0001",
        "status": "open",
        "lines": [{"line_id": "L1", "sku": "FF-BL50-CS-PN16-FMAN", "quantity": 20}],
    }
    state.active_loads["LOAD-001"] = {
        "load_id": "LOAD-001",
        "carrier_code": "DHL",
        "status": "in_transit",
        "weight_kg": 500.0,
    }
    state.save()

    loaded = SimulationState.from_db(config, db_path=db_path)

    assert "ORD-999" in loaded.open_orders
    assert loaded.open_orders["ORD-999"]["customer_id"] == "CUST-0001"
    assert "LOAD-001" in loaded.active_loads
    assert loaded.active_loads["LOAD-001"]["carrier_code"] == "DHL"


# ---------------------------------------------------------------------------
# Test 10: Catalog weight_kg survives round-trip
# ---------------------------------------------------------------------------


def test_catalog_weight_round_trip(config, db_path):
    state = SimulationState.from_new(config, db_path=db_path)
    orig_weights = {e.sku: e.weight_kg for e in state.catalog}

    state.save()
    loaded = SimulationState.from_db(config, db_path=db_path)

    loaded_weights = {e.sku: e.weight_kg for e in loaded.catalog}
    for sku, weight in orig_weights.items():
        assert loaded_weights[sku] == pytest.approx(weight, rel=1e-6), (
            f"weight mismatch for {sku}"
        )


# ---------------------------------------------------------------------------
# Test 11: counters round-trip
# ---------------------------------------------------------------------------


def test_counters_roundtrip(config, db_path):
    state = SimulationState.from_new(config, db_path=db_path)

    assert set(state.counters.keys()) == {"order", "batch", "shipment", "load", "return", "signal"}
    # All counters start at 1000 except signal which starts at 0
    assert state.counters["signal"] == 0
    assert all(v == 1000 for k, v in state.counters.items() if k != "signal")

    state.counters["order"] = 1042
    state.counters["load"] = 1007
    state.save()

    loaded = SimulationState.from_db(config, db_path=db_path)
    assert loaded.counters["order"] == 1042
    assert loaded.counters["load"] == 1007
    assert loaded.counters["batch"] == 1000


# ---------------------------------------------------------------------------
# Test 12: next_id() increments sequentially
# ---------------------------------------------------------------------------


def test_next_id_increments(config, db_path):
    state = SimulationState.from_new(config, db_path=db_path)

    first = state.next_id("order")
    second = state.next_id("order")

    assert first == 1001
    assert second == 1002
    assert state.counters["order"] == 1002


# ---------------------------------------------------------------------------
# Test 13: production_pipeline round-trip
# ---------------------------------------------------------------------------


def test_next_movement_id_increments(config, db_path):
    """next_movement_id() must return strictly increasing MOV-NNN strings."""
    state = SimulationState.from_new(config, db_path=db_path)

    first = state.next_movement_id()
    second = state.next_movement_id()

    # Both must start with "MOV-"
    assert first.startswith("MOV-")
    assert second.startswith("MOV-")

    first_num = int(first.split("-")[1])
    second_num = int(second.split("-")[1])

    # Second call must be exactly 1 higher than first
    assert second_num == first_num + 1, (
        f"Expected second movement ID to be {first_num + 1}, got {second_num}"
    )


def test_production_pipeline_roundtrip(config, db_path):
    state = SimulationState.from_new(config, db_path=db_path)

    assert state.production_pipeline == []

    entry = {
        "batch_id": "BATCH-1001",
        "sku": "FF-BL50-CS-PN16-FMAN",
        "quantity": 50,
        "production_line": 2,
        "start_date": "2026-01-06",
        "completion_date": "2026-01-09",
    }
    state.production_pipeline.append(entry)
    state.save()

    loaded = SimulationState.from_db(config, db_path=db_path)
    assert len(loaded.production_pipeline) == 1
    assert loaded.production_pipeline[0]["batch_id"] == "BATCH-1001"
    assert loaded.production_pipeline[0]["quantity"] == 50
    assert loaded.production_pipeline[0]["production_line"] == 2


# ---------------------------------------------------------------------------
# Test 14: initial inventory is seeded (not empty)
# ---------------------------------------------------------------------------


def test_initial_inventory_seeded(config, db_path):
    state = SimulationState.from_new(config, db_path=db_path)

    w01 = state.inventory["W01"]
    w02 = state.inventory["W02"]
    catalog_skus = {e.sku for e in state.catalog}

    # At least 50% of catalog SKUs should have W01 stock
    stocked_fraction = len(w01) / len(catalog_skus)
    assert stocked_fraction >= 0.50, f"Only {stocked_fraction:.1%} of SKUs stocked in W01"

    # Total units across W01 must be positive
    assert sum(w01.values()) > 0

    # All stocked SKUs must be valid catalog SKUs
    assert set(w01.keys()).issubset(catalog_skus)
    assert set(w02.keys()).issubset(catalog_skus)

    # W02 should be a fraction of W01 (not exceed it)
    assert sum(w02.values()) < sum(w01.values())
