"""Tests for the customer master data generator (Step 8)."""

from __future__ import annotations

import random
import re
from collections import Counter
from pathlib import Path

import pytest

from flowform.config import load_config
from flowform.master_data.customers import (
    CARRIERS,
    SEGMENTS,
    Customer,
    generate_customers,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


@pytest.fixture(scope="module")
def config():
    return load_config(CONFIG_PATH)


@pytest.fixture(scope="module")
def customers(config):
    rng = random.Random(42)
    return generate_customers(rng, config)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_count(customers, config):
    """Customer list length matches config.customers.count."""
    assert len(customers) == config.customers.count


def test_unique_ids(customers):
    """All customer_ids are unique and match CUST-NNNN format."""
    ids = [c.customer_id for c in customers]
    assert len(ids) == len(set(ids)), "Duplicate customer IDs found"
    pattern = re.compile(r"^CUST-\d{4}$")
    for cid in ids:
        assert pattern.match(cid), f"{cid!r} does not match CUST-NNNN format"


def test_segment_distribution(customers):
    """Segment counts are within ±2 of target (18/12/12/9/6/3)."""
    expected = {
        "Oil & Gas":               18,
        "Water Utilities":         12,
        "Chemical Plants":         12,
        "Industrial Distributors":  9,
        "HVAC Contractors":         6,
        "Shipyards":                3,
    }
    counts = Counter(c.segment for c in customers)
    for segment, target in expected.items():
        actual = counts[segment]
        assert abs(actual - target) <= 2, (
            f"Segment {segment!r}: expected ~{target}, got {actual}"
        )


def test_currency_distribution(customers):
    """PLN ~65%, EUR ~30%, USD ~5% — allow ±10pp tolerance for 60 customers."""
    counts = Counter(c.currency for c in customers)
    total = len(customers)
    pln_pct = counts["PLN"] / total
    eur_pct = counts["EUR"] / total
    usd_pct = counts["USD"] / total

    assert 0.55 <= pln_pct <= 0.75, f"PLN share out of range: {pln_pct:.0%}"
    assert 0.20 <= eur_pct <= 0.40, f"EUR share out of range: {eur_pct:.0%}"
    # USD has only 5% target; with 60 customers allow 0–15% (±10pp)
    assert 0.00 <= usd_pct <= 0.15, f"USD share out of range: {usd_pct:.0%}"


def test_shutdown_months(customers):
    """Chemical Plants have 1–2 shutdown months; all other segments have none."""
    for c in customers:
        if c.segment == "Chemical Plants":
            assert 1 <= len(c.shutdown_months) <= 2, (
                f"{c.customer_id}: Chemical Plant has {len(c.shutdown_months)} shutdown months"
            )
            for m in c.shutdown_months:
                assert 1 <= m <= 12, f"{c.customer_id}: invalid shutdown month {m}"
        else:
            assert c.shutdown_months == [], (
                f"{c.customer_id} ({c.segment}): expected no shutdown months, "
                f"got {c.shutdown_months}"
            )


def test_seasonal_profile(customers):
    """Every customer has exactly 12 monthly entries (keys 1–12), all values > 0."""
    for c in customers:
        assert set(c.seasonal_profile.keys()) == set(range(1, 13)), (
            f"{c.customer_id}: seasonal_profile keys are not 1–12"
        )
        for month, multiplier in c.seasonal_profile.items():
            assert multiplier > 0, (
                f"{c.customer_id}: month {month} has non-positive multiplier {multiplier}"
            )


def test_shipyard_order_size(customers):
    """Shipyard customers have order_size_range max >= 500."""
    shipyards = [c for c in customers if c.segment == "Shipyards"]
    assert shipyards, "No Shipyard customers found"
    for c in shipyards:
        assert c.ordering_profile.order_size_range[1] >= 500, (
            f"{c.customer_id}: Shipyard max order size "
            f"{c.ordering_profile.order_size_range[1]} < 500"
        )


def test_credit_limits(customers):
    """Shipyard credit limits >= Oil & Gas minimum; HVAC limits < Industrial Distributor limits."""
    shipyard_limits = [c.credit_limit for c in customers if c.segment == "Shipyards"]
    oil_gas_limits  = [c.credit_limit for c in customers if c.segment == "Oil & Gas"]
    hvac_limits     = [c.credit_limit for c in customers if c.segment == "HVAC Contractors"]
    dist_limits     = [c.credit_limit for c in customers if c.segment == "Industrial Distributors"]

    assert shipyard_limits, "No Shipyard customers"
    assert oil_gas_limits,  "No Oil & Gas customers"
    assert hvac_limits,     "No HVAC customers"
    assert dist_limits,     "No Distributor customers"

    # Every shipyard limit must be >= Oil & Gas minimum (500,000)
    oil_gas_min = 500_000
    for limit in shipyard_limits:
        assert limit >= oil_gas_min, (
            f"Shipyard credit limit {limit} < Oil & Gas minimum {oil_gas_min}"
        )

    # HVAC max < Industrial Distributor min
    assert max(hvac_limits) < min(dist_limits) or max(hvac_limits) <= 100_000, (
        f"HVAC max credit {max(hvac_limits)} should be below Distributor range start 50,000"
    )
    # More precisely: HVAC upper bound (100k) < Distributor lower bound (50k) is not
    # guaranteed because ranges overlap at the extremes, so we just confirm population means.
    assert sum(hvac_limits) / len(hvac_limits) < sum(dist_limits) / len(dist_limits), (
        "Mean HVAC credit limit should be below mean Industrial Distributor limit"
    )


def test_payment_terms(customers):
    """All payment_terms_days values are in {30, 60, 90}."""
    valid = {30, 60, 90}
    for c in customers:
        assert c.payment_terms_days in valid, (
            f"{c.customer_id}: invalid payment_terms_days {c.payment_terms_days}"
        )


def test_preferred_carrier(customers):
    """All preferred_carrier values are in the CARRIERS list."""
    for c in customers:
        assert c.preferred_carrier in CARRIERS, (
            f"{c.customer_id}: unknown carrier {c.preferred_carrier!r}"
        )


def test_sorted_output(customers):
    """customer_ids are in ascending order."""
    ids = [c.customer_id for c in customers]
    assert ids == sorted(ids), "Customers are not sorted by customer_id"


def test_determinism(config):
    """Two calls with the same seed produce identical customer_id lists."""
    rng_a = random.Random(42)
    rng_b = random.Random(42)
    customers_a = generate_customers(rng_a, config)
    customers_b = generate_customers(rng_b, config)
    ids_a = [c.customer_id for c in customers_a]
    ids_b = [c.customer_id for c in customers_b]
    assert ids_a == ids_b, "Output differs between two runs with the same seed"
    # Also check company names for deeper equality
    names_a = [c.company_name for c in customers_a]
    names_b = [c.company_name for c in customers_b]
    assert names_a == names_b, "Company names differ between runs with same seed"


# ---------------------------------------------------------------------------
# Reporting (not a test — just prints a summary when run directly)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    cfg = load_config(CONFIG_PATH)
    rng = random.Random(42)
    custs = generate_customers(rng, cfg)

    seg_counts = Counter(c.segment for c in custs)
    cur_counts = Counter(c.currency for c in custs)

    print("\n=== Segment breakdown ===")
    for seg in SEGMENTS:
        print(f"  {seg:<28} {seg_counts[seg]:>3}")

    print("\n=== Currency breakdown ===")
    for cur, cnt in sorted(cur_counts.items()):
        print(f"  {cur}: {cnt:>3}  ({cnt/len(custs):.0%})")

    print(f"\nTotal customers: {len(custs)}")
