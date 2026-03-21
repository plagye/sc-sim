"""Pricing engine: base price by material, size, valve type, and actuation type.

All prices are in PLN (Polish Zloty).

Reference anchor: Ball valve, DN50, Carbon Steel, PN16, Flanged, Manual = PLN 850.00
"""

from __future__ import annotations

from typing import Final

from flowform.catalog.constraints import SKUSpec

# ---------------------------------------------------------------------------
# Price lookup tables
# ---------------------------------------------------------------------------

DN_BASE_PRICE: Final[dict[int, float]] = {
    15:  320.0,
    25:  480.0,
    40:  650.0,
    50:  850.0,
    80:  1_400.0,
    100: 2_200.0,
    150: 4_500.0,
    200: 8_000.0,
    250: 14_000.0,
    300: 22_000.0,
}

VALVE_TYPE_PRICE_MULT: Final[dict[str, float]] = {
    "Gate":      1.00,
    "Ball":      1.00,
    "Butterfly": 0.65,
    "Globe":     1.20,
    "Check":     0.85,
    "Needle":    1.10,
}

MATERIAL_PRICE_MULT: Final[dict[str, float]] = {
    "CS":     1.00,
    "SS304":  1.45,
    "SS316":  1.70,
    "Duplex": 2.20,
    "Brass":  1.15,
}

PRESSURE_CLASS_PRICE_MULT: Final[dict[str, float]] = {
    "PN10": 0.90,
    "PN16": 1.00,
    "PN25": 1.15,
    "PN40": 1.35,
    "PN63": 1.65,
}

CONNECTION_PRICE_MULT: Final[dict[str, float]] = {
    "Flanged":  1.00,
    "Threaded": 0.90,
    "Welded":   1.10,
    "Wafer":    0.80,
}

ACTUATION_PRICE_MULT: Final[dict[str, float]] = {
    "Manual":    1.00,
    "Pneumatic": 1.55,
    "Electric":  1.80,
    "Hydraulic": 2.10,
}

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def base_price_pln(spec: SKUSpec, max_price: float | None = None) -> float:
    """Return the base unit price in PLN for a given SKU spec.

    Price is computed multiplicatively from DN base price, valve type,
    material, pressure class, connection type, and actuation multipliers.
    Result is rounded to 2 decimal places.

    Args:
        spec: A valid SKUSpec instance from the product catalog.
        max_price: Optional soft cap in PLN. When provided, the computed price
            is clamped to ``min(price, max_price)`` before rounding. Pass
            ``config.catalog.max_unit_price_pln`` at call sites to apply the
            configured cap.

    Returns:
        Unit price in PLN rounded to 2 decimal places.

    Raises:
        KeyError: If any dimension value is not found in a lookup table.
    """
    price = (
        DN_BASE_PRICE[spec.dn]
        * VALVE_TYPE_PRICE_MULT[spec.valve_type]
        * MATERIAL_PRICE_MULT[spec.material]
        * PRESSURE_CLASS_PRICE_MULT[spec.pressure_class]
        * CONNECTION_PRICE_MULT[spec.connection]
        * ACTUATION_PRICE_MULT[spec.actuation]
    )
    if max_price is not None:
        price = min(price, max_price)
    return round(price, 2)


def apply_customer_discount(price: float, discount_pct: float) -> float:
    """Apply a percentage discount and return the discounted price.

    Args:
        price: Original price in any currency (non-negative).
        discount_pct: Discount percentage in the range 0–100.

    Returns:
        Discounted price rounded to 2 decimal places.
    """
    return round(price * (1.0 - discount_pct / 100.0), 2)


def convert_price(price_pln: float, currency: str, rates: dict[str, float]) -> float:
    """Convert a PLN price to another currency.

    Args:
        price_pln: Price in PLN.
        currency: Target currency — "PLN", "EUR", or "USD".
        rates: Exchange rate dict with keys "EUR_PLN" and "USD_PLN",
               each representing the amount of PLN per 1 foreign unit
               (e.g. EUR_PLN=4.30 means 1 EUR = 4.30 PLN).

    Returns:
        Price in the target currency, rounded to 2 decimal places.

    Raises:
        ValueError: If currency is not one of the supported values.
    """
    if currency == "PLN":
        return round(price_pln, 2)
    if currency == "EUR":
        return round(price_pln / rates["EUR_PLN"], 2)
    if currency == "USD":
        return round(price_pln / rates["USD_PLN"], 2)
    raise ValueError(
        f"Unsupported currency '{currency}'. Must be one of: PLN, EUR, USD."
    )
