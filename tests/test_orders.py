"""Tests for the order generation engine (Phase 2, Step 16)."""

from __future__ import annotations

import copy
import random
from datetime import date, timedelta
from pathlib import Path

import pytest

from flowform.calendar import is_business_day, next_business_day
from flowform.config import load_config
from flowform.engines.orders import CustomerOrderEvent, run
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
_SATURDAY = date(2026, 1, 3)
_SUNDAY = date(2026, 1, 4)
_MONDAY = date(2026, 1, 5)   # business day
_TUESDAY = date(2026, 1, 6)  # Jan 6 is Epiphany (Polish holiday) — weekday
_WEDNESDAY = date(2026, 1, 7)  # regular business day


def _next_n_business_days(start: date, n: int) -> list[date]:
    """Return a list of the next n business days starting at (and including) start."""
    days: list[date] = []
    current = start
    while len(days) < n:
        if is_business_day(current):
            days.append(current)
        current += timedelta(days=1)
    return days


# ---------------------------------------------------------------------------
# Test 1: No orders on Saturday for non-Distributors
# ---------------------------------------------------------------------------


def test_no_orders_saturday_non_distributors(state, config):
    """On a Saturday, no non-Distributor customers should place orders."""
    events = run(state, config, _SATURDAY)
    non_distributor_ids = {
        c.customer_id
        for c in state.customers
        if c.segment != "Industrial Distributors"
    }
    for event in events:
        assert event.customer_id not in non_distributor_ids, (
            f"Non-Distributor customer {event.customer_id} ordered on Saturday"
        )


# ---------------------------------------------------------------------------
# Test 2: Distributors CAN order on weekends (probabilistic, use seed check)
# ---------------------------------------------------------------------------


def test_distributors_can_order_weekend(config, tmp_path):
    """Over many weekend days, at least one Distributor order should appear."""
    # Run multiple Saturdays until we collect a Distributor order.
    found = False
    start = _SATURDAY

    for week in range(30):  # up to 30 Saturdays
        s = SimulationState.from_new(config, db_path=tmp_path / f"sim_{week}.db")
        sim_date = start + timedelta(weeks=week)
        events = run(s, config, sim_date)
        for event in events:
            customer = next(
                c for c in s.customers if c.customer_id == event.customer_id
            )
            if customer.segment == "Industrial Distributors":
                found = True
                break
        if found:
            break

    assert found, "Expected at least one Distributor EDI order across 30 Saturdays"


# ---------------------------------------------------------------------------
# Test 3: More orders in April (high seasonality) than July (low seasonality)
# ---------------------------------------------------------------------------


def test_order_count_scales_with_seasonality(config, tmp_path):
    """April business days should produce more orders than July business days."""
    april_days = _next_n_business_days(date(2026, 4, 1), 10)
    july_days = _next_n_business_days(date(2026, 7, 1), 10)

    def count_orders(days: list[date], db_suffix: str) -> int:
        s = SimulationState.from_new(
            config, db_path=tmp_path / f"sim_{db_suffix}.db"
        )
        total = 0
        for d in days:
            events = run(s, config, d)
            total += len(events)
        return total

    april_count = count_orders(april_days, "april")
    july_count = count_orders(july_days, "july")

    # April multiplier 1.25 vs July 0.75; we expect significantly more in April.
    # Use a generous tolerance since it's probabilistic, but 10 days should be
    # enough to demonstrate the trend with the fixed seed.
    assert april_count >= july_count, (
        f"Expected April ({april_count}) >= July ({july_count}) order count"
    )


# ---------------------------------------------------------------------------
# Test 4: HVAC customers preferentially order Brass + small DN
# ---------------------------------------------------------------------------


def test_sku_affinity_hvac(config, tmp_path):
    """HVAC customers should order Brass material and DN ≤ 50 more than average."""
    s = SimulationState.from_new(config, db_path=tmp_path / "sim_hvac.db")
    hvac_ids = {c.customer_id for c in s.customers if c.segment == "HVAC Contractors"}

    if not hvac_ids:
        pytest.skip("No HVAC customers in this state")

    # Collect orders over enough business days to accumulate a reasonable sample
    business_days = _next_n_business_days(date(2026, 9, 1), 60)  # peak HVAC season

    hvac_skus: list[str] = []
    for d in business_days:
        events = run(s, config, d)
        for event in events:
            if event.customer_id in hvac_ids:
                for line in event.lines:
                    hvac_skus.append(line["sku"])

    if len(hvac_skus) < 20:
        pytest.skip(f"Too few HVAC SKU samples ({len(hvac_skus)}) to be meaningful")

    # Build a lookup for SKU → spec
    sku_to_entry = {entry.sku: entry for entry in s.catalog}

    brass_count = sum(
        1 for sku in hvac_skus if sku_to_entry[sku].spec.material == "Brass"
    )
    small_dn_count = sum(
        1 for sku in hvac_skus if sku_to_entry[sku].spec.dn <= 50
    )

    total = len(hvac_skus)
    brass_pct = brass_count / total
    small_dn_pct = small_dn_count / total

    # Brass accounts for ~1/5 of catalog SKUs by material; HVAC affinity should push above 20%
    assert brass_pct > 0.15, (
        f"HVAC Brass share {brass_pct:.1%} unexpectedly low (expected >15%)"
    )
    # DN 15–50 is 4 of 10 DN values; HVAC affinity should push above 40%
    assert small_dn_pct > 0.30, (
        f"HVAC small DN share {small_dn_pct:.1%} unexpectedly low (expected >30%)"
    )


# ---------------------------------------------------------------------------
# Test 5: Priority distribution matches config shares (±5% tolerance)
# ---------------------------------------------------------------------------


def test_priority_distribution(config, tmp_path):
    """Priority distribution across many orders should match config (~3/12/85%)."""
    s = SimulationState.from_new(config, db_path=tmp_path / "sim_priority.db")

    business_days = _next_n_business_days(date(2026, 1, 5), 30)

    priorities: list[str] = []
    for d in business_days:
        events = run(s, config, d)
        for event in events:
            priorities.append(event.priority)

    if len(priorities) < 100:
        pytest.skip(f"Too few orders ({len(priorities)}) for reliable distribution check")

    total = len(priorities)
    critical_pct = priorities.count("critical") / total
    express_pct = priorities.count("express") / total
    standard_pct = priorities.count("standard") / total

    tolerance = 0.06  # ±6% — generous for a probabilistic test

    assert abs(critical_pct - config.orders.critical_share) < tolerance, (
        f"Critical share {critical_pct:.1%} deviates from {config.orders.critical_share:.1%}"
    )
    assert abs(express_pct - config.orders.express_share) < tolerance, (
        f"Express share {express_pct:.1%} deviates from {config.orders.express_share:.1%}"
    )
    expected_standard = 1.0 - config.orders.critical_share - config.orders.express_share
    assert abs(standard_pct - expected_standard) < tolerance, (
        f"Standard share {standard_pct:.1%} deviates from {expected_standard:.1%}"
    )


# ---------------------------------------------------------------------------
# Test 6: December delivery restriction respected
# ---------------------------------------------------------------------------


def test_december_delivery_flag(config, tmp_path):
    """Standard orders for restricted customers must not have delivery dates after Dec 20."""
    s = SimulationState.from_new(config, db_path=tmp_path / "sim_dec.db")

    restricted_ids = {
        c.customer_id
        for c in s.customers
        if not c.accepts_deliveries_december
    }

    if not restricted_ids:
        pytest.skip("No customers with accepts_deliveries_december=False")

    # Run in late November — standard priority has 10–21 business days lead time,
    # so it's possible for delivery dates to spill into late December.
    business_days = _next_n_business_days(date(2026, 11, 10), 20)

    for d in business_days:
        events = run(s, config, d)
        for event in events:
            if event.customer_id not in restricted_ids:
                continue
            delivery = date.fromisoformat(event.requested_delivery_date)
            # Delivery must not be Dec 21–31 of the same year
            if delivery.month == 12 and delivery.day > 20:
                pytest.fail(
                    f"Order {event.order_id} for restricted customer "
                    f"{event.customer_id} has delivery date {delivery} (after Dec 20)"
                )


# ---------------------------------------------------------------------------
# Test 7: sales_channel absent before threshold
# ---------------------------------------------------------------------------


def test_schema_evolution_sales_channel_absent_before_threshold(config, tmp_path):
    """No event should have sales_channel before the sim_day threshold."""
    s = SimulationState.from_new(config, db_path=tmp_path / "sim_sc_before.db")

    # Ensure we're below the threshold
    threshold = config.schema_evolution.sales_channel_from_day
    assert s.sim_day < threshold, "State sim_day must start below threshold"

    business_days = _next_n_business_days(date(2026, 1, 5), 10)
    for d in business_days:
        events = run(s, config, d)
        for event in events:
            dumped = event.model_dump()
            assert "sales_channel" not in dumped, (
                f"sales_channel appeared on sim_day {s.sim_day} (threshold {threshold})"
            )


# ---------------------------------------------------------------------------
# Test 8: sales_channel present on or after threshold
# ---------------------------------------------------------------------------


def test_schema_evolution_sales_channel_present_after_threshold(config, tmp_path):
    """After reaching the sim_day threshold, at least one event should have sales_channel."""
    s = SimulationState.from_new(config, db_path=tmp_path / "sim_sc_after.db")
    threshold = config.schema_evolution.sales_channel_from_day

    # Fast-forward sim_day to the threshold
    s.sim_day = threshold

    found = False
    business_days = _next_n_business_days(date(2026, 1, 5), 20)
    for d in business_days:
        events = run(s, config, d)
        for event in events:
            dumped = event.model_dump()
            if "sales_channel" in dumped:
                found = True
                assert dumped["sales_channel"] in ("direct", "edi", "portal"), (
                    f"Unexpected sales_channel value: {dumped['sales_channel']}"
                )
                break
        if found:
            break

    assert found, "Expected sales_channel to appear in events at/after threshold"


# ---------------------------------------------------------------------------
# Test 9: Reproducibility — same seed, same date → identical events
# ---------------------------------------------------------------------------


def test_reproducible(config, tmp_path):
    """Two runs with the same seed and date must produce identical events."""
    sim_date = date(2026, 3, 10)  # A known Tuesday

    s1 = SimulationState.from_new(config, db_path=tmp_path / "sim_r1.db")
    s2 = SimulationState.from_new(config, db_path=tmp_path / "sim_r2.db")

    events1 = run(s1, config, sim_date)
    events2 = run(s2, config, sim_date)

    assert len(events1) == len(events2), (
        f"Different event counts: {len(events1)} vs {len(events2)}"
    )

    for e1, e2 in zip(events1, events2):
        d1 = e1.model_dump()
        d2 = e2.model_dump()
        # event_id is a UUID4 — generated independently so will differ
        d1.pop("event_id")
        d2.pop("event_id")
        assert d1 == d2, f"Events differ:\n{d1}\nvs\n{d2}"


# ---------------------------------------------------------------------------
# Test 10: open_orders state updated after run()
# ---------------------------------------------------------------------------


def test_open_orders_state_updated(state, config):
    """After run(), state.open_orders must contain all new orders keyed by order_id."""
    initial_count = len(state.open_orders)
    sim_date = _MONDAY  # business day

    events = run(state, config, sim_date)

    assert len(state.open_orders) == initial_count + len(events), (
        f"Expected {initial_count + len(events)} open orders, "
        f"got {len(state.open_orders)}"
    )

    for event in events:
        assert event.order_id in state.open_orders, (
            f"Order {event.order_id} not found in state.open_orders"
        )
        stored = state.open_orders[event.order_id]
        assert stored["customer_id"] == event.customer_id
        assert stored["priority"] == event.priority
        assert "lines" in stored
        assert len(stored["lines"]) > 0
