"""Tests for ShipmentConfirmationEvent emitted by load_lifecycle.py."""

from __future__ import annotations

import copy
import json
import shutil
from datetime import date, timedelta
from pathlib import Path

import pytest

from flowform.engines.load_lifecycle import ShipmentConfirmationEvent, run as lc_run
from flowform.engines.load_planning import LoadEvent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_order(order_id: str, currency: str = "PLN", lines: list | None = None) -> dict:
    if lines is None:
        lines = [
            {
                "line_id": f"{order_id}-L1",
                "sku": "FF-BV50-CS-PN16-FL-MA",
                "quantity_ordered": 10,
                "quantity_allocated": 10,
                "quantity_shipped": 0,
                "unit_price": 100.0,
                "line_status": "allocated",
            }
        ]
    return {
        "order_id": order_id,
        "customer_id": "CUST-0001",
        "currency": currency,
        "lines": lines,
        "status": "allocated",
    }


def _make_load(load_id: str, order_ids: list[str], carrier_code: str = "DHL") -> dict:
    return {
        "load_id": load_id,
        "status": "out_for_delivery",
        "carrier_code": carrier_code,
        "source_warehouse_id": "W01",
        "shipment_ids": [f"SHP-{load_id}"],
        "order_ids": order_ids,
        "total_weight_kg": 50.0,
        "weight_unit": "kg",
        "total_weight_reported": 50.0,
        "sync_window": "14",
        "priority": "standard",
        "customer_ids": ["CUST-0001"],
        "planned_date": "2026-01-06",
        "out_for_delivery_date": "2026-01-07",
        "delivery_window": 1,
    }


def _make_state_with_loads(
    tmp_path: Path,
    loads: list[dict],
    orders: list[dict],
    exchange_rates: dict | None = None,
) -> object:
    """Build a minimal SimulationState with specified loads and orders."""
    import random

    from flowform.config import load_config
    from flowform.state import SimulationState

    cfg_src = Path(__file__).parent.parent / "config.yaml"
    cfg_dst = tmp_path / "config.yaml"
    shutil.copy(cfg_src, cfg_dst)

    config = load_config(cfg_dst)
    db_path = tmp_path / "state" / "sim.db"
    state = SimulationState.from_new(config, db_path=db_path)

    # Inject loads and orders
    for load in loads:
        state.active_loads[load["load_id"]] = load
    for order in orders:
        state.open_orders[order["order_id"]] = order

    if exchange_rates:
        state.exchange_rates.update(exchange_rates)

    return state


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_no_events_on_weekend(tmp_path: Path) -> None:
    """Weekend date: load_lifecycle runs but delivery on weekend should still emit confirmation."""
    # Actually carriers run on weekends — load_lifecycle has NO business day guard.
    # A load delivered on a weekend should still emit confirmation. So we test
    # that a non-delivered load (in_transit) produces no confirmation events.
    order = _make_order("ORD-2026-000001")
    load = _make_load("LOAD-001", ["ORD-2026-000001"])
    load["status"] = "in_transit"
    load["estimated_arrival"] = "2026-01-10"  # future

    state = _make_state_with_loads(tmp_path, [load], [order])

    from flowform.config import load_config
    config = load_config(tmp_path / "config.yaml")

    weekend = date(2026, 1, 10)  # Saturday
    events = lc_run(state, config, weekend)

    confirmations = [e for e in events if isinstance(e, ShipmentConfirmationEvent)]
    assert confirmations == []


def test_no_events_before_delivery(tmp_path: Path) -> None:
    """Load in 'in_transit' status produces no confirmation events."""
    order = _make_order("ORD-2026-000002")
    load = _make_load("LOAD-002", ["ORD-2026-000002"])
    load["status"] = "in_transit"
    load["estimated_arrival"] = "2026-06-01"  # far future

    state = _make_state_with_loads(tmp_path, [load], [order])

    from flowform.config import load_config
    config = load_config(tmp_path / "config.yaml")

    events = lc_run(state, config, date(2026, 1, 7))
    confirmations = [e for e in events if isinstance(e, ShipmentConfirmationEvent)]
    assert confirmations == []


def test_one_event_per_order(tmp_path: Path) -> None:
    """Load with 3 orders produces 3 confirmation events."""
    orders = [_make_order(f"ORD-2026-00000{i}") for i in range(1, 4)]
    order_ids = [o["order_id"] for o in orders]
    load = _make_load("LOAD-003", order_ids)
    load["status"] = "out_for_delivery"
    load["out_for_delivery_date"] = "2026-01-07"
    load["delivery_window"] = 1

    state = _make_state_with_loads(tmp_path, [load], orders)

    from flowform.config import load_config
    config = load_config(tmp_path / "config.yaml")

    # Force delivery on 2026-01-08 by making rng always succeed reliability check
    state.rng.random = lambda: 0.0  # type: ignore[method-assign]

    events = lc_run(state, config, date(2026, 1, 8))
    confirmations = [e for e in events if isinstance(e, ShipmentConfirmationEvent)]
    assert len(confirmations) == 3
    confirmed_order_ids = {e.order_id for e in confirmations}
    assert confirmed_order_ids == set(order_ids)


def test_event_id_format(tmp_path: Path) -> None:
    """event_id matches SHIP-YYYYMMDD-NNNN."""
    import re

    order = _make_order("ORD-2026-000010")
    load = _make_load("LOAD-010", ["ORD-2026-000010"])
    load["out_for_delivery_date"] = "2026-01-07"
    load["delivery_window"] = 1

    state = _make_state_with_loads(tmp_path, [load], [order])
    state.rng.random = lambda: 0.0  # type: ignore[method-assign]

    from flowform.config import load_config
    config = load_config(tmp_path / "config.yaml")

    events = lc_run(state, config, date(2026, 1, 8))
    confirmations = [e for e in events if isinstance(e, ShipmentConfirmationEvent)]
    assert len(confirmations) == 1
    assert re.match(r"^SHIP-\d{8}-\d{4}$", confirmations[0].event_id)
    assert "20260108" in confirmations[0].event_id


def test_lines_list_populated(tmp_path: Path) -> None:
    """Each confirmation event has lines list with correct fields."""
    order = _make_order("ORD-2026-000020")
    load = _make_load("LOAD-020", ["ORD-2026-000020"])
    load["out_for_delivery_date"] = "2026-01-07"
    load["delivery_window"] = 1

    state = _make_state_with_loads(tmp_path, [load], [order])
    state.rng.random = lambda: 0.0  # type: ignore[method-assign]

    from flowform.config import load_config
    config = load_config(tmp_path / "config.yaml")

    events = lc_run(state, config, date(2026, 1, 8))
    confirmations = [e for e in events if isinstance(e, ShipmentConfirmationEvent)]
    assert len(confirmations) == 1
    lines = confirmations[0].lines
    assert len(lines) == 1
    assert lines[0]["line_id"] == "ORD-2026-000020-L1"
    assert lines[0]["sku"] == "FF-BV50-CS-PN16-FL-MA"
    assert "qty_shipped" in lines[0]
    assert "unit_price" in lines[0]


def test_qty_shipped_matches_allocated(tmp_path: Path) -> None:
    """qty_shipped in confirmation equals quantity_allocated from the order line."""
    lines = [
        {
            "line_id": "ORD-2026-000030-L1",
            "sku": "FF-BV50-CS-PN16-FL-MA",
            "quantity_ordered": 7,
            "quantity_allocated": 7,
            "quantity_shipped": 0,
            "unit_price": 200.0,
            "line_status": "allocated",
        }
    ]
    order = _make_order("ORD-2026-000030", lines=lines)
    load = _make_load("LOAD-030", ["ORD-2026-000030"])
    load["out_for_delivery_date"] = "2026-01-07"
    load["delivery_window"] = 1

    state = _make_state_with_loads(tmp_path, [load], [order])
    state.rng.random = lambda: 0.0  # type: ignore[method-assign]

    from flowform.config import load_config
    config = load_config(tmp_path / "config.yaml")

    events = lc_run(state, config, date(2026, 1, 8))
    confirmations = [e for e in events if isinstance(e, ShipmentConfirmationEvent)]
    assert len(confirmations) == 1
    assert confirmations[0].lines[0]["qty_shipped"] == 7


def test_total_qty_correct(tmp_path: Path) -> None:
    """total_qty_shipped equals sum of line qty_shipped values."""
    lines = [
        {
            "line_id": "ORD-2026-000040-L1",
            "sku": "FF-BV50-CS-PN16-FL-MA",
            "quantity_ordered": 5,
            "quantity_allocated": 5,
            "quantity_shipped": 0,
            "unit_price": 100.0,
            "line_status": "allocated",
        },
        {
            "line_id": "ORD-2026-000040-L2",
            "sku": "FF-BV80-CS-PN16-FL-MA",
            "quantity_ordered": 3,
            "quantity_allocated": 3,
            "quantity_shipped": 0,
            "unit_price": 150.0,
            "line_status": "allocated",
        },
    ]
    order = _make_order("ORD-2026-000040", lines=lines)
    load = _make_load("LOAD-040", ["ORD-2026-000040"])
    load["out_for_delivery_date"] = "2026-01-07"
    load["delivery_window"] = 1

    state = _make_state_with_loads(tmp_path, [load], [order])
    state.rng.random = lambda: 0.0  # type: ignore[method-assign]

    from flowform.config import load_config
    config = load_config(tmp_path / "config.yaml")

    events = lc_run(state, config, date(2026, 1, 8))
    confirmations = [e for e in events if isinstance(e, ShipmentConfirmationEvent)]
    assert len(confirmations) == 1
    ev = confirmations[0]
    assert ev.total_qty_shipped == sum(l["qty_shipped"] for l in ev.lines)
    assert ev.total_qty_shipped == 8


def test_total_value_pln_pln_order(tmp_path: Path) -> None:
    """PLN order: total_value_pln = sum(qty × unit_price) with rate 1.0."""
    lines = [
        {
            "line_id": "ORD-2026-000050-L1",
            "sku": "FF-BV50-CS-PN16-FL-MA",
            "quantity_ordered": 4,
            "quantity_allocated": 4,
            "quantity_shipped": 0,
            "unit_price": 500.0,
            "line_status": "allocated",
        }
    ]
    order = _make_order("ORD-2026-000050", currency="PLN", lines=lines)
    load = _make_load("LOAD-050", ["ORD-2026-000050"])
    load["out_for_delivery_date"] = "2026-01-07"
    load["delivery_window"] = 1

    state = _make_state_with_loads(tmp_path, [load], [order])
    state.rng.random = lambda: 0.0  # type: ignore[method-assign]

    from flowform.config import load_config
    config = load_config(tmp_path / "config.yaml")

    events = lc_run(state, config, date(2026, 1, 8))
    confirmations = [e for e in events if isinstance(e, ShipmentConfirmationEvent)]
    assert len(confirmations) == 1
    assert confirmations[0].total_value_pln == pytest.approx(4 * 500.0)


def test_total_value_pln_eur_order(tmp_path: Path) -> None:
    """EUR order: total_value_pln uses EUR exchange rate correctly."""
    lines = [
        {
            "line_id": "ORD-2026-000060-L1",
            "sku": "FF-BV50-CS-PN16-FL-MA",
            "quantity_ordered": 2,
            "quantity_allocated": 2,
            "quantity_shipped": 0,
            "unit_price": 100.0,
            "line_status": "allocated",
        }
    ]
    order = _make_order("ORD-2026-000060", currency="EUR", lines=lines)
    load = _make_load("LOAD-060", ["ORD-2026-000060"])
    load["out_for_delivery_date"] = "2026-01-07"
    load["delivery_window"] = 1

    eur_rate = 4.30
    state = _make_state_with_loads(
        tmp_path, [load], [order], exchange_rates={"EUR": eur_rate}
    )
    state.rng.random = lambda: 0.0  # type: ignore[method-assign]

    from flowform.config import load_config
    config = load_config(tmp_path / "config.yaml")

    events = lc_run(state, config, date(2026, 1, 8))
    confirmations = [e for e in events if isinstance(e, ShipmentConfirmationEvent)]
    assert len(confirmations) == 1
    expected = round(2 * 100.0 * eur_rate, 2)
    assert confirmations[0].total_value_pln == pytest.approx(expected)


def test_counter_increments(tmp_path: Path) -> None:
    """state.counters['ship'] increments by number of orders on load."""
    orders = [_make_order(f"ORD-2026-00010{i}") for i in range(1, 4)]
    order_ids = [o["order_id"] for o in orders]
    load = _make_load("LOAD-100", order_ids)
    load["out_for_delivery_date"] = "2026-01-07"
    load["delivery_window"] = 1

    state = _make_state_with_loads(tmp_path, [load], orders)
    initial_counter = state.counters.get("ship", 0)
    state.rng.random = lambda: 0.0  # type: ignore[method-assign]

    from flowform.config import load_config
    config = load_config(tmp_path / "config.yaml")

    events = lc_run(state, config, date(2026, 1, 8))
    confirmations = [e for e in events if isinstance(e, ShipmentConfirmationEvent)]
    assert len(confirmations) == 3
    assert state.counters["ship"] == initial_counter + 3


def test_written_to_erp_dir(tmp_path: Path) -> None:
    """write_events routes shipment_confirmation to erp/YYYY-MM-DD/shipment_confirmations.json."""
    from flowform.output.writer import write_events

    sim_date = date(2026, 1, 8)
    event = ShipmentConfirmationEvent(
        event_id="SHIP-20260108-0001",
        confirmation_date="2026-01-08",
        load_id="LOAD-001",
        order_id="ORD-2026-000001",
        carrier_code="DHL",
        source_warehouse_id="W01",
        lines=[{"line_id": "L1", "sku": "FF-BV50-CS-PN16-FL-MA", "qty_shipped": 5, "unit_price": 100.0}],
        total_qty_shipped=5,
        total_value_pln=500.0,
    )

    output_dir = tmp_path / "output"
    write_events([event.model_dump()], sim_date, output_dir=output_dir)

    expected = output_dir / "erp" / "2026-01-08" / "shipment_confirmations.json"
    assert expected.exists()
    data = json.loads(expected.read_text())
    assert len(data) == 1
    assert data[0]["event_type"] == "shipment_confirmation"
    assert data[0]["order_id"] == "ORD-2026-000001"


def test_reproducibility(tmp_path: Path) -> None:
    """Two states with same seed produce identical confirmation events for same load."""
    order = _make_order("ORD-2026-000070")
    load = _make_load("LOAD-070", ["ORD-2026-000070"])
    load["out_for_delivery_date"] = "2026-01-07"
    load["delivery_window"] = 1

    cfg_src = Path(__file__).parent.parent / "config.yaml"

    from flowform.config import load_config
    config = load_config(cfg_src)

    results = []
    for i in range(2):
        run_dir = tmp_path / f"run_{i}"
        run_dir.mkdir()
        shutil.copy(cfg_src, run_dir / "config.yaml")
        state = _make_state_with_loads(
            run_dir,
            [copy.deepcopy(load)],
            [copy.deepcopy(order)],
        )
        state.rng.random = lambda: 0.0  # type: ignore[method-assign]
        events = lc_run(state, config, date(2026, 1, 8))
        confirmations = [e for e in events if isinstance(e, ShipmentConfirmationEvent)]
        results.append([e.model_dump() for e in confirmations])

    assert results[0] == results[1]
