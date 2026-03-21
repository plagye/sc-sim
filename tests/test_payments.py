"""Tests for the payment processing engine (Phase 4, Step 32 / Step 58).

Step 58 replaces the daily Bernoulli trigger with explicit scheduled payments
stored in ``state.scheduled_payments`` and fired when ``due_date <= sim_date``.
"""

from __future__ import annotations

import random
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest

from flowform.config import load_config
from flowform.engines.credit import CreditReleaseEvent
from flowform.engines.payments import PaymentEvent, run
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


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BUSINESS_DAY = date(2026, 1, 5)   # Monday
_SATURDAY = date(2026, 1, 3)
_HOLIDAY = date(2026, 1, 1)        # New Year's Day (Polish holiday)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_order(
    order_id: str,
    customer_id: str,
    unit_price: float,
    quantity: int,
    currency: str = "PLN",
    status: str = "confirmed",
) -> dict[str, Any]:
    """Build a minimal order dict matching orders engine shape."""
    return {
        "event_type": "customer_order",
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "customer_id": customer_id,
        "simulation_date": "2026-01-05",
        "timestamp": "2026-01-05T10:00:00",
        "currency": currency,
        "priority": "standard",
        "status": status,
        "requested_delivery_date": "2026-01-20",
        "lines": [
            {
                "line_id": f"{order_id}-L1",
                "sku": "FF-GA15-CS-PN16-FL-MN",
                "quantity_ordered": quantity,
                "quantity_allocated": 0,
                "quantity_shipped": 0,
                "quantity_backordered": 0,
                "unit_price": unit_price,
                "line_status": "open",
            }
        ],
    }


def _add_scheduled_entry(
    state: SimulationState,
    customer_id: str,
    amount_pln: float,
    due_date: date,
    behaviour: str = "on_time",
    invoice_ref: str = "SHIP-20260105-0001",
) -> None:
    """Seed one entry in state.scheduled_payments."""
    state.scheduled_payments.append({
        "customer_id": customer_id,
        "amount_pln": amount_pln,
        "due_date": due_date.isoformat(),
        "behaviour": behaviour,
        "invoice_ref": invoice_ref,
    })


# ---------------------------------------------------------------------------
# Test 1: no payment on weekend
# ---------------------------------------------------------------------------


def test_no_payment_on_weekend(state, config):
    """Engine must return [] on Saturday even if entries are due."""
    customer = state.customers[0]
    state.customer_balances[customer.customer_id] = 5000.0
    _add_scheduled_entry(state, customer.customer_id, 5000.0, _SATURDAY)

    result = run(state, config, _SATURDAY)

    assert result == []
    # Entry must still be present — not fired
    assert any(
        e["customer_id"] == customer.customer_id
        for e in state.scheduled_payments
    )

    # Cleanup
    state.scheduled_payments.clear()
    state.customer_balances[customer.customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 2: no payment on holiday
# ---------------------------------------------------------------------------


def test_no_payment_on_holiday(state, config):
    """Engine must return [] on Polish public holiday."""
    customer = state.customers[0]
    state.customer_balances[customer.customer_id] = 5000.0
    _add_scheduled_entry(state, customer.customer_id, 5000.0, _HOLIDAY)

    result = run(state, config, _HOLIDAY)

    assert result == []

    # Cleanup
    state.scheduled_payments.clear()
    state.customer_balances[customer.customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 3: no payment when no entries are due
# ---------------------------------------------------------------------------


def test_no_payment_when_no_entries_due(state, config):
    """Engine must return [] when no scheduled entry is due today."""
    # Ensure no entries in schedule
    state.scheduled_payments.clear()
    for cid in list(state.customer_balances.keys()):
        state.customer_balances[cid] = 0.0

    result = run(state, config, _BUSINESS_DAY)

    assert result == []


# ---------------------------------------------------------------------------
# Test 4: on_time payment fires on due date
# ---------------------------------------------------------------------------


def test_on_time_payment_fires_on_due_date(state, config):
    """A scheduled on_time entry fires and pays full balance on due_date."""
    customer = state.customers[0]
    customer_id = customer.customer_id
    amount = 12000.0
    state.customer_balances[customer_id] = amount
    _add_scheduled_entry(state, customer_id, amount, _BUSINESS_DAY, behaviour="on_time")

    result = run(state, config, _BUSINESS_DAY)

    payment_events = [e for e in result if isinstance(e, PaymentEvent)]
    assert len(payment_events) == 1
    evt = payment_events[0]
    assert evt.payment_behaviour == "on_time"
    assert evt.balance_after_pln == 0.0
    assert state.customer_balances[customer_id] == 0.0
    # Entry must be removed after firing
    assert not any(e["customer_id"] == customer_id for e in state.scheduled_payments)

    # Cleanup
    state.customer_balances[customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 5: late payment fires on due date
# ---------------------------------------------------------------------------


def test_late_payment_fires_on_due_date(state, config):
    """A scheduled late entry fires on due_date and pays 50–80% of balance."""
    customer = state.customers[0]
    customer_id = customer.customer_id
    amount = 20000.0
    state.customer_balances[customer_id] = amount
    _add_scheduled_entry(state, customer_id, amount, _BUSINESS_DAY, behaviour="late")

    result = run(state, config, _BUSINESS_DAY)

    payment_events = [
        e for e in result
        if isinstance(e, PaymentEvent) and e.customer_id == customer_id
    ]
    assert len(payment_events) == 1
    evt = payment_events[0]
    assert evt.payment_behaviour == "late"
    assert round(0.50 * amount, 2) <= evt.payment_amount_pln <= round(0.80 * amount, 2), (
        f"Late payment {evt.payment_amount_pln} not in [50%, 80%] of {amount}"
    )
    assert evt.balance_after_pln > 0.0

    # Cleanup
    state.scheduled_payments.clear()
    state.customer_balances[customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 6: very_late payment fires on due date
# ---------------------------------------------------------------------------


def test_very_late_payment_fires_on_due_date(state, config):
    """A scheduled very_late entry fires on due_date and pays 10–30% of balance."""
    customer = state.customers[0]
    customer_id = customer.customer_id
    amount = 30000.0
    state.customer_balances[customer_id] = amount
    _add_scheduled_entry(state, customer_id, amount, _BUSINESS_DAY, behaviour="very_late")

    result = run(state, config, _BUSINESS_DAY)

    payment_events = [
        e for e in result
        if isinstance(e, PaymentEvent) and e.customer_id == customer_id
    ]
    assert len(payment_events) == 1
    evt = payment_events[0]
    assert evt.payment_behaviour == "very_late"
    assert round(0.10 * amount, 2) <= evt.payment_amount_pln <= round(0.30 * amount, 2), (
        f"Very late payment {evt.payment_amount_pln} not in [10%, 30%] of {amount}"
    )

    # Cleanup
    state.scheduled_payments.clear()
    state.customer_balances[customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 7: payment not fired before due date
# ---------------------------------------------------------------------------


def test_payment_not_fired_before_due_date(state, config):
    """Entry with due_date tomorrow must not fire today."""
    customer = state.customers[0]
    customer_id = customer.customer_id
    state.customer_balances[customer_id] = 5000.0
    tomorrow = _BUSINESS_DAY + timedelta(days=1)
    _add_scheduled_entry(state, customer_id, 5000.0, tomorrow)

    result = run(state, config, _BUSINESS_DAY)

    assert result == []
    # Entry must still be present
    assert any(e["customer_id"] == customer_id for e in state.scheduled_payments)

    # Cleanup
    state.scheduled_payments.clear()
    state.customer_balances[customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 8: multiple payments same day
# ---------------------------------------------------------------------------


def test_multiple_payments_same_day(state, config):
    """Three entries due today for 3 different customers emit 3 PaymentEvents."""
    customers = state.customers[:3]
    for c in customers:
        state.customer_balances[c.customer_id] = 10000.0
        _add_scheduled_entry(state, c.customer_id, 10000.0, _BUSINESS_DAY)

    result = run(state, config, _BUSINESS_DAY)

    payment_events = [e for e in result if isinstance(e, PaymentEvent)]
    paying_ids = {e.customer_id for e in payment_events}
    for c in customers:
        assert c.customer_id in paying_ids, f"{c.customer_id} did not pay"

    assert len(payment_events) == 3

    # Cleanup
    state.scheduled_payments.clear()
    for c in customers:
        state.customer_balances[c.customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 9: balance clamped to zero
# ---------------------------------------------------------------------------


def test_balance_clamped_to_zero(state, config):
    """Payment that exceeds balance must clamp balance_after to 0.0."""
    customer = state.customers[0]
    customer_id = customer.customer_id
    state.customer_balances[customer_id] = 100.0
    # Schedule payment for a larger amount (on_time = fraction 1.0)
    _add_scheduled_entry(state, customer_id, 50000.0, _BUSINESS_DAY, behaviour="on_time")

    result = run(state, config, _BUSINESS_DAY)

    payment_events = [e for e in result if isinstance(e, PaymentEvent)]
    assert len(payment_events) == 1
    assert payment_events[0].balance_after_pln == 0.0
    assert state.customer_balances[customer_id] == 0.0


# ---------------------------------------------------------------------------
# Test 10: credit release fires after payment
# ---------------------------------------------------------------------------


def test_credit_release_fires_after_payment(state, config):
    """Credit-held orders are released when payment drops exposure below limit."""
    customer = state.customers[0]
    customer_id = customer.customer_id
    credit_limit = customer.credit_limit

    state.customer_balances[customer_id] = credit_limit + 1000.0
    _add_scheduled_entry(
        state, customer_id, credit_limit + 1000.0, _BUSINESS_DAY, behaviour="on_time"
    )

    order_id = "ORD-PAY-HOLD-CRS-001"
    order = _make_order(order_id, customer_id, 100.0, 1, status="credit_hold")
    state.open_orders[order_id] = order

    result = run(state, config, _BUSINESS_DAY)

    release_events = [e for e in result if isinstance(e, CreditReleaseEvent)]
    assert len(release_events) >= 1
    assert any(e.order_id == order_id for e in release_events)
    assert state.open_orders[order_id]["status"] == "confirmed"

    # Cleanup
    del state.open_orders[order_id]
    state.customer_balances[customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 11: invoice_ref in event
# ---------------------------------------------------------------------------


def test_invoice_ref_in_event(state, config):
    """PaymentEvent must include the invoice_ref from the scheduled entry."""
    customer = state.customers[0]
    customer_id = customer.customer_id
    ref = "SHIP-20260105-0042"
    state.customer_balances[customer_id] = 5000.0
    _add_scheduled_entry(state, customer_id, 5000.0, _BUSINESS_DAY, invoice_ref=ref)

    result = run(state, config, _BUSINESS_DAY)

    payment_events = [e for e in result if isinstance(e, PaymentEvent)]
    assert len(payment_events) == 1
    assert payment_events[0].invoice_ref == ref

    # Cleanup
    state.customer_balances[customer_id] = 0.0


# ---------------------------------------------------------------------------
# Test 12: scheduled_payments round-trips through SQLite
# ---------------------------------------------------------------------------


def test_scheduled_payments_round_trips_sqlite(config, tmp_path: Path):
    """state.scheduled_payments persists to SQLite and reloads correctly."""
    db = tmp_path / "sim_pay.db"
    s = SimulationState.from_new(config, db_path=db)

    entry = {
        "customer_id": "CUST-0001",
        "amount_pln": 9876.54,
        "due_date": "2026-03-01",
        "behaviour": "late",
        "invoice_ref": "SHIP-20260228-0003",
    }
    s.scheduled_payments.append(entry)
    s.save()

    s2 = SimulationState.from_db(config, db_path=db)
    assert len(s2.scheduled_payments) == 1
    loaded = s2.scheduled_payments[0]
    assert loaded["customer_id"] == entry["customer_id"]
    assert loaded["amount_pln"] == entry["amount_pln"]
    assert loaded["due_date"] == entry["due_date"]
    assert loaded["behaviour"] == entry["behaviour"]
    assert loaded["invoice_ref"] == entry["invoice_ref"]


# ---------------------------------------------------------------------------
# Test 13: load_lifecycle creates scheduled entry
# ---------------------------------------------------------------------------


def test_load_lifecycle_creates_scheduled_entry(config, tmp_path: Path):
    """load_lifecycle.run() on a delivered load creates a scheduled_payments entry."""
    from flowform.engines import load_lifecycle

    db = tmp_path / "sim_lc.db"
    s = SimulationState.from_new(config, db_path=db)

    customer = s.customers[0]
    customer_id = customer.customer_id

    # Plant a minimal order with an allocated line in open_orders
    order_id = "ORD-2026-TEST01"
    sku = s.catalog[0].sku
    s.open_orders[order_id] = {
        "event_type": "customer_order",
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "customer_id": customer_id,
        "simulation_date": "2026-01-05",
        "timestamp": "2026-01-05T10:00:00",
        "currency": "PLN",
        "priority": "standard",
        "status": "allocated",
        "requested_delivery_date": "2026-01-20",
        "lines": [
            {
                "line_id": f"{order_id}-L1",
                "sku": sku,
                "quantity_ordered": 10,
                "quantity_allocated": 10,
                "quantity_shipped": 0,
                "quantity_backordered": 0,
                "unit_price": 500.0,
                "line_status": "allocated",
            }
        ],
    }

    # Plant a delivered load that is NOT yet in pending_pod (out_for_delivery state)
    delivery_date = date(2026, 1, 5)
    s.active_loads["LOAD-0001"] = {
        "load_id": "LOAD-0001",
        "status": "out_for_delivery",
        "carrier_code": "DHL",
        "source_warehouse_id": "W01",
        "shipment_ids": ["SHIP-001"],
        "order_ids": [order_id],
        "total_weight_kg": 50.0,
        "weight_unit": "kg",
        "total_weight_reported": 50.0,
        "sync_window": "14",
        "priority": "standard",
        "customer_ids": [customer_id],
        "planned_date": "2026-01-04",
        "out_for_delivery_date": "2026-01-04",
        "delivery_window": 1,
    }

    # Force the reliability RNG to always succeed (return < reliability threshold)
    s.rng = random.Random(0)  # DHL reliability ~0.92; seed 0 gives small values

    load_lifecycle.run(s, config, delivery_date)

    # After delivery, a scheduled_payments entry should exist for this customer
    assert len(s.scheduled_payments) == 1
    entry = s.scheduled_payments[0]
    assert entry["customer_id"] == customer_id
    assert entry["behaviour"] in {"on_time", "late", "very_late"}
    assert entry["amount_pln"] > 0.0
    assert "due_date" in entry
    assert "invoice_ref" in entry


# ---------------------------------------------------------------------------
# Test 14: due_date jitter is seeded
# ---------------------------------------------------------------------------


def test_due_date_jitter_is_seeded(config, tmp_path: Path):
    """Same seed, same delivery → same due_date in scheduled_payments."""
    from flowform.engines import load_lifecycle

    def _run_delivery(seed: int) -> str:
        db = tmp_path / f"sim_jitter_{seed}.db"
        s = SimulationState.from_new(config, db_path=db)
        customer_id = s.customers[0].customer_id
        sku = s.catalog[0].sku
        order_id = f"ORD-2026-JITTER{seed}"
        s.open_orders[order_id] = {
            "event_type": "customer_order",
            "event_id": str(uuid.uuid4()),
            "order_id": order_id,
            "customer_id": customer_id,
            "simulation_date": "2026-01-05",
            "timestamp": "2026-01-05T10:00:00",
            "currency": "PLN",
            "priority": "standard",
            "status": "allocated",
            "requested_delivery_date": "2026-01-20",
            "lines": [
                {
                    "line_id": f"{order_id}-L1",
                    "sku": sku,
                    "quantity_ordered": 5,
                    "quantity_allocated": 5,
                    "quantity_shipped": 0,
                    "quantity_backordered": 0,
                    "unit_price": 300.0,
                    "line_status": "allocated",
                }
            ],
        }
        s.active_loads["LOAD-J001"] = {
            "load_id": "LOAD-J001",
            "status": "out_for_delivery",
            "carrier_code": "DHL",
            "source_warehouse_id": "W01",
            "shipment_ids": ["SHIP-J001"],
            "order_ids": [order_id],
            "total_weight_kg": 15.0,
            "weight_unit": "kg",
            "total_weight_reported": 15.0,
            "sync_window": "14",
            "priority": "standard",
            "customer_ids": [customer_id],
            "planned_date": "2026-01-04",
            "out_for_delivery_date": "2026-01-04",
            "delivery_window": 1,
        }
        s.rng = random.Random(42)
        load_lifecycle.run(s, config, date(2026, 1, 5))
        if s.scheduled_payments:
            return s.scheduled_payments[0]["due_date"]
        return ""

    due1 = _run_delivery(0)
    due2 = _run_delivery(0)
    assert due1 == due2, f"Due dates differ: {due1} vs {due2}"
    assert due1 != "", "Expected a due_date to be scheduled"


# ---------------------------------------------------------------------------
# Test 15: PaymentEvent schema includes invoice_ref
# ---------------------------------------------------------------------------


def test_payment_event_schema(state, config):
    """PaymentEvent model_dump must include invoice_ref plus all core fields."""
    customer = state.customers[0]
    customer_id = customer.customer_id
    ref = "SHIP-20260105-0099"
    state.customer_balances[customer_id] = 8000.0
    _add_scheduled_entry(state, customer_id, 8000.0, _BUSINESS_DAY, invoice_ref=ref)

    result = run(state, config, _BUSINESS_DAY)

    payment_events = [e for e in result if isinstance(e, PaymentEvent)]
    assert len(payment_events) >= 1
    d = payment_events[0].model_dump()

    expected_keys = {
        "event_type", "event_id", "simulation_date", "customer_id",
        "payment_amount_pln", "balance_before_pln", "balance_after_pln",
        "payment_behaviour", "payment_terms_days", "invoice_ref",
    }
    assert expected_keys.issubset(d.keys()), (
        f"Missing keys in model_dump: {expected_keys - d.keys()}"
    )
    assert d["invoice_ref"] == ref
    assert d["event_type"] == "payment"
    assert isinstance(d["payment_amount_pln"], float)
    assert isinstance(d["payment_terms_days"], int)

    # Cleanup
    state.customer_balances[customer_id] = 0.0
