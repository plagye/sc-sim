"""Tests for the FlowForm pricing engine (src/flowform/catalog/pricing.py)."""

from __future__ import annotations

import pytest

from flowform.catalog.constraints import SKUSpec
from flowform.catalog.pricing import (
    ACTUATION_PRICE_MULT,
    DN_BASE_PRICE,
    MATERIAL_PRICE_MULT,
    PRESSURE_CLASS_PRICE_MULT,
    VALVE_TYPE_PRICE_MULT,
    apply_customer_discount,
    base_price_pln,
    convert_price,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _spec(**kwargs) -> SKUSpec:
    """Build a SKUSpec with sensible defaults, allowing partial override."""
    defaults = dict(
        valve_type="Ball",
        dn=50,
        material="CS",
        pressure_class="PN16",
        connection="Flanged",
        actuation="Manual",
    )
    defaults.update(kwargs)
    return SKUSpec(**defaults)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Test 1: Reference price anchor
# ---------------------------------------------------------------------------

def test_reference_price_anchor():
    """Ball/DN50/CS/PN16/Flanged/Manual must equal exactly PLN 850.00."""
    spec = _spec()
    assert base_price_pln(spec) == 850.0


# ---------------------------------------------------------------------------
# Test 2: Monotonic DN scaling
# ---------------------------------------------------------------------------

def test_dn_price_monotonically_increasing():
    """Price must strictly increase as DN grows, for the same spec shape."""
    dn_sizes = sorted(DN_BASE_PRICE.keys())
    prices = [
        base_price_pln(_spec(dn=dn))
        for dn in dn_sizes
    ]
    for i in range(len(prices) - 1):
        assert prices[i] < prices[i + 1], (
            f"Price did not increase from DN{dn_sizes[i]} to DN{dn_sizes[i+1]}: "
            f"{prices[i]} >= {prices[i+1]}"
        )


# ---------------------------------------------------------------------------
# Test 3: Material premium ordering
# ---------------------------------------------------------------------------

def test_material_premium_ordering():
    """CS < Brass < SS304 < SS316 < Duplex for the same base spec."""
    # Use a spec that is valid for all materials (Duplex forbids Manual → use Pneumatic;
    # Brass forbids PN25+ → use PN16; DN50 is valid for all).
    def price(mat: str) -> float:
        return base_price_pln(_spec(material=mat, actuation="Pneumatic", pressure_class="PN16"))

    assert price("CS") < price("Brass") < price("SS304") < price("SS316") < price("Duplex")


# ---------------------------------------------------------------------------
# Test 4: Actuation ordering
# ---------------------------------------------------------------------------

def test_actuation_price_ordering():
    """Manual < Pneumatic < Electric < Hydraulic for the same base spec."""
    # Hydraulic requires DN100+; use DN100.
    def price(act: str) -> float:
        return base_price_pln(_spec(dn=100, actuation=act))

    assert price("Manual") < price("Pneumatic") < price("Electric") < price("Hydraulic")


# ---------------------------------------------------------------------------
# Test 5: Pressure class ordering
# ---------------------------------------------------------------------------

def test_pressure_class_ordering():
    """PN10 < PN16 < PN25 < PN40 < PN63 for the same base spec."""
    def price(pn: str) -> float:
        return base_price_pln(_spec(pressure_class=pn))

    assert price("PN10") < price("PN16") < price("PN25") < price("PN40") < price("PN63")


# ---------------------------------------------------------------------------
# Test 6: Butterfly cheaper than Globe
# ---------------------------------------------------------------------------

def test_butterfly_cheaper_than_globe():
    """Butterfly valve must be priced below Globe for same DN/material/PN/actuation."""
    # Butterfly supports Wafer connection; Globe requires non-Wafer.
    # Use Flanged for a fair comparison (both support it).
    butterfly = base_price_pln(_spec(valve_type="Butterfly", connection="Flanged"))
    globe = base_price_pln(_spec(valve_type="Globe", connection="Flanged"))
    assert butterfly < globe, f"Butterfly ({butterfly}) not cheaper than Globe ({globe})"


# ---------------------------------------------------------------------------
# Test 7: Customer discount
# ---------------------------------------------------------------------------

def test_apply_customer_discount_ten_percent():
    assert apply_customer_discount(1000.0, 10) == pytest.approx(900.0, abs=0.005)


def test_apply_customer_discount_zero():
    assert apply_customer_discount(1234.56, 0) == pytest.approx(1234.56, abs=0.005)


def test_apply_customer_discount_full():
    assert apply_customer_discount(1000.0, 100) == pytest.approx(0.0, abs=0.005)


# ---------------------------------------------------------------------------
# Test 8: Currency conversion
# ---------------------------------------------------------------------------

_RATES = {"EUR_PLN": 4.30, "USD_PLN": 4.05}


def test_convert_to_eur():
    result = convert_price(4300.0, "EUR", _RATES)
    assert result == pytest.approx(1000.0, abs=0.01)


def test_convert_to_usd():
    result = convert_price(4050.0, "USD", _RATES)
    assert result == pytest.approx(1000.0, abs=0.01)


def test_convert_to_pln_unchanged():
    result = convert_price(1000.0, "PLN", _RATES)
    assert result == pytest.approx(1000.0, abs=0.01)


def test_convert_unknown_currency_raises():
    with pytest.raises(ValueError, match="Unsupported currency"):
        convert_price(1000.0, "GBP", _RATES)


# ---------------------------------------------------------------------------
# Test 9: Price range sanity
# ---------------------------------------------------------------------------

def test_smallest_valid_sku_price_positive():
    """Needle/DN15/Brass/PN10/Threaded/Manual — smallest possible price > 0."""
    spec = SKUSpec(
        valve_type="Needle",
        dn=15,
        material="Brass",
        pressure_class="PN10",
        connection="Threaded",
        actuation="Manual",
    )
    price = base_price_pln(spec)
    assert price > 0, f"Expected positive price, got {price}"


def test_largest_valid_sku_price_below_sanity_cap():
    """Globe/DN300/Duplex/PN63/Welded/Hydraulic — largest price must be < 500,000 PLN."""
    spec = SKUSpec(
        valve_type="Globe",
        dn=300,
        material="Duplex",
        pressure_class="PN63",
        connection="Welded",
        actuation="Hydraulic",
    )
    price = base_price_pln(spec)
    assert price < 500_000.0, f"Price {price} exceeds sanity upper bound of 500,000 PLN"


# ---------------------------------------------------------------------------
# Test 10: Rounding — all prices are exactly 2 decimal places
# ---------------------------------------------------------------------------

def test_base_price_rounded_to_two_decimal_places():
    """Spot-check a selection of specs: returned price must be 2dp."""
    specs = [
        _spec(),  # reference
        _spec(dn=100, material="SS316", pressure_class="PN40", actuation="Electric"),
        _spec(dn=150, material="Duplex", pressure_class="PN63", actuation="Pneumatic"),
        SKUSpec("Butterfly", 200, "SS304", "PN25", "Wafer", "Electric"),
        SKUSpec("Needle", 25, "Brass", "PN10", "Threaded", "Manual"),
    ]
    for spec in specs:
        price = base_price_pln(spec)
        # Check that the float, when formatted to 2dp, equals its str representation.
        assert price == round(price, 2), (
            f"Price {price!r} for {spec} is not rounded to 2dp"
        )


def test_discount_rounded_to_two_decimal_places():
    """Discounted price must always be 2dp."""
    price = apply_customer_discount(999.999, 7.5)
    assert price == round(price, 2)


def test_convert_price_rounded_to_two_decimal_places():
    """Converted price must always be 2dp."""
    result = convert_price(1357.13, "EUR", _RATES)
    assert result == round(result, 2)
