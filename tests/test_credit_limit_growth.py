"""Tests for credit limit growth mechanism — Step 51."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from flowform.calendar import is_first_business_day_of_quarter
from flowform.config import load_config
from flowform.engines import master_data_changes
from flowform.state import SimulationState

_CONFIG_PATH = Path("/home/coder/sc-sim/config.yaml")

# A quarter-start business day: 2026-01-02 (Friday, first biz day of Q1;
# Jan 1 is a holiday)
_Q1_FIRST_BIZ = date(2026, 1, 2)
# Second business day of Q1 2026 (Jan 5 — Monday; Jan 6 is Epiphany holiday)
_Q1_SECOND_BIZ = date(2026, 1, 5)
# Saturday — not a business day
_SATURDAY = date(2026, 1, 3)
# Mid-quarter business day
_MID_QUARTER = date(2026, 2, 9)


@pytest.fixture
def config():
    return load_config(_CONFIG_PATH)


@pytest.fixture
def state(config, tmp_path: Path) -> SimulationState:
    return SimulationState.from_new(config, db_path=tmp_path / "sim.db")


# ---------------------------------------------------------------------------
# 1. is_first_business_day_of_quarter — Jan 2026
# ---------------------------------------------------------------------------

def test_is_first_business_day_of_quarter_jan():
    """2026-01-02 is the first business day of Q1 2026 (Jan 1 is a holiday)."""
    assert is_first_business_day_of_quarter(date(2026, 1, 2)) is True


# ---------------------------------------------------------------------------
# 2. is_first_business_day_of_quarter — Apr 2026
# ---------------------------------------------------------------------------

def test_is_first_business_day_of_quarter_apr():
    """First business day of April 2026 (Q2 start) should return True."""
    # Apr 1 2026 is Wednesday — a business day and no earlier biz day in Q2
    candidate = date(2026, 4, 1)
    assert is_first_business_day_of_quarter(candidate) is True


# ---------------------------------------------------------------------------
# 3. Non-first business day of quarter returns False
# ---------------------------------------------------------------------------

def test_is_first_business_day_of_quarter_non_first():
    """2026-01-05 is the second business day of Q1 (after Jan 2) — should return False."""
    assert is_first_business_day_of_quarter(_Q1_SECOND_BIZ) is False


# ---------------------------------------------------------------------------
# 4. Weekend returns False
# ---------------------------------------------------------------------------

def test_is_first_business_day_of_quarter_weekend():
    """Saturday is not a business day — should return False."""
    assert is_first_business_day_of_quarter(_SATURDAY) is False


# ---------------------------------------------------------------------------
# 5. No review on mid-quarter date
# ---------------------------------------------------------------------------

def test_no_review_on_non_quarter_start(state, config):
    """run() on a mid-quarter date produces no credit_limit change events."""
    state.sim_day = 90
    events = master_data_changes.run(state, config, _MID_QUARTER)
    credit_events = [e for e in events if e.field_changed == "credit_limit"]
    assert credit_events == []


# ---------------------------------------------------------------------------
# 6. Well-behaved customer (balance=0) gets an increase
# ---------------------------------------------------------------------------

def test_well_behaved_customer_gets_increase(state, config):
    """A customer with zero balance and no holds receives a credit limit increase."""
    state.sim_day = 90
    # Zero out all balances (already 0 at init, but be explicit)
    for cid in state.customer_balances:
        state.customer_balances[cid] = 0.0

    events = master_data_changes.run(state, config, _Q1_FIRST_BIZ)
    credit_events = [e for e in events if e.field_changed == "credit_limit"]

    # At least some customers should have received increases
    assert len(credit_events) > 0
    for ev in credit_events:
        assert ev.new_value > ev.old_value
        assert ev.reason == "quarterly_credit_review"
        assert ev.entity_type == "customer"


# ---------------------------------------------------------------------------
# 7. High-utilisation customer (≥50%) is skipped
# ---------------------------------------------------------------------------

def test_high_utilisation_customer_skipped(state, config):
    """A customer whose balance ≥ 50% of credit_limit is not reviewed."""
    state.sim_day = 90

    # Pick the first customer and set balance to exactly 50% of their limit
    target = state.customers[0]
    state.customer_balances[target.customer_id] = target.credit_limit * 0.50
    old_limit = target.credit_limit

    events = master_data_changes.run(state, config, _Q1_FIRST_BIZ)
    credit_events = [e for e in events if e.entity_id == target.customer_id
                     and e.field_changed == "credit_limit"]

    assert credit_events == []
    # Limit must be unchanged
    assert target.credit_limit == old_limit


# ---------------------------------------------------------------------------
# 8. max_multiplier cap is respected
# ---------------------------------------------------------------------------

def test_max_multiplier_cap(state, config):
    """A customer at 2.9× initial limit is capped at 3.0× initial limit."""
    state.sim_day = 90

    target = state.customers[0]
    # Zero balance so utilisation check passes
    state.customer_balances[target.customer_id] = 0.0

    initial = state.initial_credit_limits[target.customer_id]
    # Set current limit to 2.9× initial — very close to cap
    target.credit_limit = round(initial * 2.9, 2)
    cap = round(initial * config.credit_limit.max_multiplier, 2)

    events = master_data_changes.run(state, config, _Q1_FIRST_BIZ)
    credit_events = [e for e in events if e.entity_id == target.customer_id
                     and e.field_changed == "credit_limit"]

    # If an event was emitted, new_value must not exceed cap
    for ev in credit_events:
        assert ev.new_value <= cap


# ---------------------------------------------------------------------------
# 9. Credit limit mutated in state after review
# ---------------------------------------------------------------------------

def test_credit_limit_mutated_in_state(state, config):
    """After the review, customer.credit_limit equals the new_value in the event."""
    state.sim_day = 90
    for cid in state.customer_balances:
        state.customer_balances[cid] = 0.0

    events = master_data_changes.run(state, config, _Q1_FIRST_BIZ)
    credit_events = [e for e in events if e.field_changed == "credit_limit"]

    assert len(credit_events) > 0

    # Build lookup: customer_id → Customer object
    cust_map = {c.customer_id: c for c in state.customers}
    for ev in credit_events:
        assert cust_map[ev.entity_id].credit_limit == ev.new_value


# ---------------------------------------------------------------------------
# 10. initial_credit_limits persisted through save/load round-trip
# ---------------------------------------------------------------------------

def test_initial_credit_limits_persisted(config, tmp_path: Path):
    """from_new() populates initial_credit_limits; save()+from_db() round-trips it."""
    db_path = tmp_path / "sim.db"
    state1 = SimulationState.from_new(config, db_path=db_path)

    assert len(state1.initial_credit_limits) == len(state1.customers)
    for customer in state1.customers:
        assert customer.customer_id in state1.initial_credit_limits
        initial = state1.initial_credit_limits[customer.customer_id]
        assert initial == customer.credit_limit

    # Save and reload
    state1.save()
    state2 = SimulationState.from_db(config, db_path=db_path)

    assert state2.initial_credit_limits == state1.initial_credit_limits


# ---------------------------------------------------------------------------
# 11. Disabled config skips review
# ---------------------------------------------------------------------------

def test_disabled_config_skips_review(state, config):
    """config.credit_limit.enabled=False prevents any credit_limit events."""
    state.sim_day = 90

    # Disable credit limit growth
    config.credit_limit.enabled = False

    events = master_data_changes.run(state, config, _Q1_FIRST_BIZ)
    credit_events = [e for e in events if e.field_changed == "credit_limit"]
    assert credit_events == []


# ---------------------------------------------------------------------------
# 12. Customers with a credit_hold order are excluded
# ---------------------------------------------------------------------------

def test_held_customers_excluded(state, config):
    """A customer with a credit_hold order is not reviewed."""
    state.sim_day = 90

    target = state.customers[0]
    state.customer_balances[target.customer_id] = 0.0
    old_limit = target.credit_limit

    # Inject a synthetic credit_hold order for this customer
    state.open_orders["ORD-FAKE-0001"] = {
        "order_id": "ORD-FAKE-0001",
        "customer_id": target.customer_id,
        "status": "credit_hold",
        "currency": "PLN",
        "lines": [],
    }

    events = master_data_changes.run(state, config, _Q1_FIRST_BIZ)
    credit_events = [e for e in events if e.entity_id == target.customer_id
                     and e.field_changed == "credit_limit"]

    assert credit_events == []
    assert target.credit_limit == old_limit


# ---------------------------------------------------------------------------
# 13. No review before sim_day 90
# ---------------------------------------------------------------------------

def test_no_review_before_day_90(state, config):
    """_run_credit_limit_review returns [] when sim_day < 90."""
    from flowform.engines.master_data_changes import _run_credit_limit_review

    state.sim_day = 89
    events = _run_credit_limit_review(state, config, _Q1_FIRST_BIZ)
    assert events == [], (
        f"Expected no credit_limit events for sim_day=89, got {len(events)}"
    )


# ---------------------------------------------------------------------------
# 14. initial_credit_limits migration backfill on from_db()
# ---------------------------------------------------------------------------

def test_initial_credit_limits_migration(config, tmp_path: Path):
    """from_db() backfills initial_credit_limits from customers when absent in DB."""
    db_path = tmp_path / "sim.db"

    # Create state (initial_credit_limits populated by from_new)
    state1 = SimulationState.from_new(config, db_path=db_path)
    assert len(state1.initial_credit_limits) > 0

    # Manually wipe initial_credit_limits and save — simulating a pre-Step-51 DB
    state1.initial_credit_limits = {}
    state1.save()

    # Reload — from_db() should backfill from customers
    state2 = SimulationState.from_db(config, db_path=db_path)

    # Should have one entry per customer
    assert len(state2.initial_credit_limits) == len(state2.customers), (
        "Migration did not backfill initial_credit_limits from customers"
    )
    # Values should match current credit limits
    for customer in state2.customers:
        assert customer.customer_id in state2.initial_credit_limits
        assert state2.initial_credit_limits[customer.customer_id] == customer.credit_limit
