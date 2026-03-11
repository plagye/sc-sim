"""Tests for master data JSON export (Feature 1).

All 13 tests use a module-level fixture that calls SimulationState.from_new()
and write_master_data() once, then inspects the written files.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from flowform.config import load_config
from flowform.master_data.export import write_master_data
from flowform.state import SimulationState


# ---------------------------------------------------------------------------
# Module-level fixture — runs once, shared by all tests
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def master_output(tmp_path_factory):
    """Run from_new() + write_master_data(), return (state, master_dir)."""
    tmp = tmp_path_factory.mktemp("master_export")

    # Config resolution: find config.yaml two levels up from tests/
    config_path = Path(__file__).parent.parent / "config.yaml"
    config = load_config(config_path)

    state = SimulationState.from_new(config, db_path=tmp / "state" / "simulation.db")
    master_dir = tmp / "master"
    write_master_data(state, tmp)  # writes to tmp/master/

    return state, master_dir


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _load(master_dir: Path, filename: str):
    return json.loads((master_dir / filename).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_master_dir_exists_after_reset(master_output):
    _, master_dir = master_output
    assert master_dir.is_dir()
    expected = {
        "catalog.json",
        "customers.json",
        "carriers.json",
        "warehouses.json",
        "initial_inventory.json",
        "initial_customer_balances.json",
    }
    actual = {p.name for p in master_dir.iterdir()}
    assert expected == actual


def test_catalog_json_is_valid_array(master_output):
    _, master_dir = master_output
    catalog = _load(master_dir, "catalog.json")
    assert isinstance(catalog, list)
    assert len(catalog) > 0


def test_catalog_all_skus_present(master_output):
    state, master_dir = master_output
    catalog = _load(master_dir, "catalog.json")
    assert len(catalog) == len(state.catalog)


def test_catalog_entries_have_required_fields(master_output):
    _, master_dir = master_output
    catalog = _load(master_dir, "catalog.json")
    for entry in catalog:
        assert "sku" in entry, f"Missing 'sku' in {entry}"
        assert "dn" in entry, f"Missing 'dn' in {entry}"
        assert "weight_kg" in entry, f"Missing 'weight_kg' in {entry}"
        assert "base_price_pln" in entry, f"Missing 'base_price_pln' in {entry}"
        assert "base_price_eur" in entry, f"Missing 'base_price_eur' in {entry}"
        assert "valve_type" in entry
        assert "material" in entry
        assert entry["base_price_pln"] > 0
        assert entry["base_price_eur"] > 0
        assert entry["weight_kg"] > 0


def test_customers_json_is_valid_array(master_output):
    _, master_dir = master_output
    customers = _load(master_dir, "customers.json")
    assert isinstance(customers, list)
    assert len(customers) > 0


def test_customers_have_customer_id_and_segment(master_output):
    state, master_dir = master_output
    customers = _load(master_dir, "customers.json")
    assert len(customers) == len(state.customers)
    for c in customers:
        assert "customer_id" in c
        assert "segment" in c
        assert c["customer_id"].startswith("CUST-")


def test_carriers_json_has_five_entries(master_output):
    _, master_dir = master_output
    carriers = _load(master_dir, "carriers.json")
    assert len(carriers) == 5
    codes = {c["code"] for c in carriers}
    assert codes == {"DHL", "DBSC", "RABEN", "GEODIS", "BALTIC"}


def test_carriers_weight_unit_field(master_output):
    _, master_dir = master_output
    carriers = _load(master_dir, "carriers.json")
    carrier_by_code = {c["code"]: c for c in carriers}
    # BALTIC is the legacy lbs carrier
    assert carrier_by_code["BALTIC"]["weight_unit"] == "lbs"
    # All others report kg
    for code in ("DHL", "DBSC", "RABEN", "GEODIS"):
        assert carrier_by_code[code]["weight_unit"] == "kg"


def test_warehouses_json_has_w01_and_w02(master_output):
    _, master_dir = master_output
    warehouses = _load(master_dir, "warehouses.json")
    codes = {w["code"] for w in warehouses}
    assert "W01" in codes
    assert "W02" in codes
    assert len(warehouses) == 2


def test_initial_inventory_keys_match_warehouse_codes(master_output):
    _, master_dir = master_output
    inventory = _load(master_dir, "initial_inventory.json")
    assert set(inventory.keys()) == {"W01", "W02"}


def test_initial_inventory_values_are_positive_ints(master_output):
    _, master_dir = master_output
    inventory = _load(master_dir, "initial_inventory.json")
    for wh_code, sku_map in inventory.items():
        assert isinstance(sku_map, dict), f"W{wh_code} should be a dict"
        for sku, qty in sku_map.items():
            assert isinstance(qty, int), f"{wh_code}/{sku} qty should be int, got {type(qty)}"
            assert qty > 0, f"{wh_code}/{sku} qty should be positive, got {qty}"


def test_initial_customer_balances_all_zero(master_output):
    state, master_dir = master_output
    balances = _load(master_dir, "initial_customer_balances.json")
    assert len(balances) == len(state.customers)
    for cust_id, balance in balances.items():
        assert balance == 0.0, f"{cust_id} balance should be 0.0 at reset, got {balance}"


def test_master_files_are_stable_across_resets(tmp_path_factory):
    """Two from_new() calls with the same seed produce identical master files."""
    config_path = Path(__file__).parent.parent / "config.yaml"
    config = load_config(config_path)

    # First reset
    tmp1 = tmp_path_factory.mktemp("stable1")
    state1 = SimulationState.from_new(config, db_path=tmp1 / "sim.db")
    write_master_data(state1, tmp1)

    # Second reset
    tmp2 = tmp_path_factory.mktemp("stable2")
    state2 = SimulationState.from_new(config, db_path=tmp2 / "sim.db")
    write_master_data(state2, tmp2)

    for filename in (
        "catalog.json",
        "customers.json",
        "carriers.json",
        "warehouses.json",
        "initial_inventory.json",
        "initial_customer_balances.json",
    ):
        content1 = (tmp1 / "master" / filename).read_text(encoding="utf-8")
        content2 = (tmp2 / "master" / filename).read_text(encoding="utf-8")
        assert content1 == content2, f"{filename} differs between two resets"
