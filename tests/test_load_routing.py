"""Tests for TMS carrier route logic (Feature 2).

Covers BALTIC gate, country-based carrier selection, large-shipment DHL bump,
NUTS2 region mapping, and destination_region on emitted LoadEvents.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from flowform.engines.load_planning import (
    REGION_TO_NUTS2,
    _select_carrier,
    destination_region,
)


# ---------------------------------------------------------------------------
# Minimal mock fixtures
# ---------------------------------------------------------------------------


@dataclass
class _MockCustomer:
    customer_id: str
    segment: str
    country_code: str
    region: str
    preferred_carrier: str = ""


def _make_state(seed: int = 42) -> MagicMock:
    """Return a minimal mock state with a seeded rng."""
    state = MagicMock()
    state.rng = random.Random(seed)
    state.carriers = []  # not needed for _select_carrier
    return state


# ---------------------------------------------------------------------------
# Test 1: BALTIC only assigned to Shipyard or W02
# ---------------------------------------------------------------------------


def test_baltic_only_assigned_to_shipyard_or_w02():
    """BALTIC must not appear on non-Shipyard orders from W01."""
    segments = ["Oil & Gas", "Water Utilities", "Chemical Plants",
                "Industrial Distributors", "HVAC Contractors"]
    countries = ["PL", "DE", "NL", "NO", "SE", "FR", "CZ", "IT"]

    rng = random.Random(0)
    state = _make_state(0)

    for _ in range(100):
        segment = rng.choice(segments)
        country = rng.choice(countries)
        customer = _MockCustomer(
            customer_id="CUST-TEST",
            segment=segment,
            country_code=country,
            region="",
            preferred_carrier="",
        )
        carrier = _select_carrier(state, customer, "W01", 0.0)
        assert carrier != "BALTIC", (
            f"BALTIC selected for segment={segment}, country={country}, W01"
        )


# ---------------------------------------------------------------------------
# Test 2: non-Shipyard preferred_carrier=BALTIC is overridden to RABEN
# ---------------------------------------------------------------------------


def test_non_shipyard_preferred_baltic_overridden():
    state = _make_state()
    customer = _MockCustomer(
        customer_id="CUST-0001",
        segment="Oil & Gas",
        country_code="PL",
        region="Śląskie",
        preferred_carrier="BALTIC",
    )
    result = _select_carrier(state, customer, "W01", 0.0)
    assert result == "RABEN"


# ---------------------------------------------------------------------------
# Test 3: French customer should prefer GEODIS
# ---------------------------------------------------------------------------


def test_french_customer_prefers_geodis():
    state = _make_state(99)
    customer = _MockCustomer(
        customer_id="CUST-FR",
        segment="Industrial Distributors",
        country_code="FR",
        region="Île-de-France",
        preferred_carrier="",
    )
    counts: dict[str, int] = {}
    for _ in range(50):
        c = _select_carrier(state, customer, "W01", 0.0)
        counts[c] = counts.get(c, 0) + 1

    # GEODIS weight=50 vs DHL=30, DBSC=20 → GEODIS should be most frequent
    assert counts.get("GEODIS", 0) > counts.get("DHL", 0), (
        f"Expected GEODIS > DHL for FR customer, got {counts}"
    )
    assert counts.get("GEODIS", 0) > counts.get("DBSC", 0)


# ---------------------------------------------------------------------------
# Test 4: Nordic customer should prefer DHL
# ---------------------------------------------------------------------------


def test_nordic_customer_prefers_dhl():
    state = _make_state(7)
    customer = _MockCustomer(
        customer_id="CUST-NO",
        segment="Shipyards",
        country_code="NO",
        region="Vestland",
        preferred_carrier="",
    )
    counts: dict[str, int] = {}
    for _ in range(50):
        c = _select_carrier(state, customer, "W02", 0.0)
        counts[c] = counts.get(c, 0) + 1

    assert counts.get("DHL", 0) > counts.get("GEODIS", 0), (
        f"Expected DHL > GEODIS for NO customer, got {counts}"
    )
    assert counts.get("DHL", 0) > counts.get("DBSC", 0)


# ---------------------------------------------------------------------------
# Test 5: Polish customer should prefer RABEN
# ---------------------------------------------------------------------------


def test_pl_customer_prefers_raben():
    state = _make_state(13)
    customer = _MockCustomer(
        customer_id="CUST-PL",
        segment="Water Utilities",
        country_code="PL",
        region="Śląskie",
        preferred_carrier="",
    )
    counts: dict[str, int] = {}
    for _ in range(100):
        c = _select_carrier(state, customer, "W01", 0.0)
        counts[c] = counts.get(c, 0) + 1

    assert counts.get("RABEN", 0) > counts.get("DBSC", 0), (
        f"Expected RABEN > DBSC for PL customer, got {counts}"
    )
    assert counts.get("RABEN", 0) > counts.get("DHL", 0)


# ---------------------------------------------------------------------------
# Test 6: Large shipment makes DHL more likely even for PL customer
# ---------------------------------------------------------------------------


def test_large_shipment_prefers_dhl():
    """weight=600 kg should boost DHL above baseline for a PL customer."""
    # Baseline: PL without large-shipment bump → RABEN dominant
    state_base = _make_state(55)
    customer = _MockCustomer(
        customer_id="CUST-PL2",
        segment="Water Utilities",
        country_code="PL",
        region="Mazowieckie",
        preferred_carrier="",
    )

    counts_normal: dict[str, int] = {}
    for _ in range(50):
        c = _select_carrier(state_base, customer, "W01", 0.0)
        counts_normal[c] = counts_normal.get(c, 0) + 1

    # Large shipment: DHL gets +20 bump
    state_large = _make_state(55)
    counts_large: dict[str, int] = {}
    for _ in range(50):
        c = _select_carrier(state_large, customer, "W01", 600.0)
        counts_large[c] = counts_large.get(c, 0) + 1

    # DHL frequency must be strictly higher with 600 kg load
    # (PL baseline DHL=20, with bump DHL=40; total weights: 50+30+40=120 vs 50+30+20=100)
    dhl_normal = counts_normal.get("DHL", 0)
    dhl_large = counts_large.get("DHL", 0)
    assert dhl_large > dhl_normal, (
        f"Expected DHL more frequent with 600 kg, normal={dhl_normal}, large={dhl_large}"
    )


# ---------------------------------------------------------------------------
# Test 7: destination_region returns correct NUTS2 for known region
# ---------------------------------------------------------------------------


def test_destination_region_returns_nuts2_code():
    customer = _MockCustomer(
        customer_id="CUST-SL",
        segment="Oil & Gas",
        country_code="PL",
        region="Śląskie",
    )
    assert destination_region(customer) == "PL-SL"


# ---------------------------------------------------------------------------
# Test 8: destination_region covers all regions actually generated by customers
# ---------------------------------------------------------------------------


def test_destination_region_covers_all_segment_countries():
    """Every region string in customers._COUNTRY_REGIONS should map to non-XX."""
    from flowform.master_data.customers import _COUNTRY_REGIONS

    unmapped: list[tuple[str, str]] = []
    for country_code, regions in _COUNTRY_REGIONS.items():
        for region in regions:
            customer = _MockCustomer(
                customer_id="TEST",
                segment="Oil & Gas",
                country_code=country_code,
                region=region,
            )
            code = destination_region(customer)
            if code.endswith("-XX"):
                unmapped.append((country_code, region))

    assert not unmapped, (
        f"These regions are not mapped in REGION_TO_NUTS2: {unmapped}"
    )


# ---------------------------------------------------------------------------
# Test 9: destination_region is populated on emitted load_created events
# ---------------------------------------------------------------------------


def test_destination_region_on_load_event():
    """Run load_planning.run() for several business days and verify all
    load_created events carry a non-empty destination_region."""
    from flowform.config import load_config
    from flowform.engines import load_planning
    from flowform.state import SimulationState

    config_path = Path(__file__).parent.parent / "config.yaml"
    config = load_config(config_path)

    tmp_db = Path("/tmp/test_load_routing_state.db")
    if tmp_db.exists():
        tmp_db.unlink()
    tmp_db.parent.mkdir(parents=True, exist_ok=True)

    state = SimulationState.from_new(config, db_path=tmp_db)

    # Seed some open allocated orders so load_planning has something to plan
    from flowform.engines import allocation, orders
    from datetime import timedelta

    start = config.simulation.start_date
    all_events: list = []
    for offset in range(5):
        d = start + timedelta(days=offset)
        state.advance_day(d)
        all_events.extend(orders.run(state, config, d))
        all_events.extend(allocation.run(state, config, d))
        load_events = load_planning.run(state, config, d)
        all_events.extend(load_events)

    load_created = [
        e for e in all_events
        if hasattr(e, "event_subtype") and e.event_subtype == "load_created"
    ]

    if not load_created:
        pytest.skip("No load_created events emitted in 5 days — cannot assert")

    for evt in load_created:
        assert evt.destination_region, (
            f"destination_region is empty on {evt.load_id}"
        )

    # Clean up
    if tmp_db.exists():
        tmp_db.unlink()
