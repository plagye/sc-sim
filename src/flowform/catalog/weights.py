"""Weight calculator: per-SKU weights from size × material × valve type × actuation.

Weight formula:
    weight_kg = DN_BASE_WEIGHT[dn]
                × MATERIAL_WEIGHT_MULT[material]
                × VALVE_TYPE_WEIGHT_MULT[valve_type]
                × ACTUATION_WEIGHT_MULT[actuation]

Connection type and pressure class do NOT affect weight.

BalticHaul (carrier code "BALTIC") is a legacy system that reports all weights
in lbs. All other carriers use kg. The conversion constant and helper functions
are provided here so the TMS layer always uses the correct unit.
"""

from __future__ import annotations

from typing import Final

from flowform.catalog.constraints import SKUSpec

# ---------------------------------------------------------------------------
# Lookup tables
# ---------------------------------------------------------------------------

DN_BASE_WEIGHT: Final[dict[int, float]] = {
    15:  1.2,
    25:  2.1,
    40:  3.8,
    50:  5.5,
    80:  12.0,
    100: 18.5,
    150: 35.0,
    200: 58.0,
    250: 85.0,
    300: 120.0,
}

MATERIAL_WEIGHT_MULT: Final[dict[str, float]] = {
    "CS":     1.00,
    "SS304":  1.00,
    "SS316":  1.00,
    "Duplex": 1.05,
    "Brass":  0.95,
}

VALVE_TYPE_WEIGHT_MULT: Final[dict[str, float]] = {
    "Gate":      1.00,
    "Ball":      0.90,
    "Butterfly": 0.60,
    "Globe":     1.10,
    "Check":     0.80,
    "Needle":    0.50,
}

ACTUATION_WEIGHT_MULT: Final[dict[str, float]] = {
    "Manual":    1.00,
    "Pneumatic": 1.15,
    "Electric":  1.25,
    "Hydraulic": 1.40,
}

# ---------------------------------------------------------------------------
# Unit conversion
# ---------------------------------------------------------------------------

KG_TO_LBS: Final[float] = 2.20462

# Carrier code that uses lbs instead of kg.
_BALTIC_CARRIER_CODE: Final[str] = "BALTIC"


def kg_to_lbs(weight_kg: float) -> float:
    """Convert kg to lbs, rounded to 3 decimal places."""
    return round(weight_kg * KG_TO_LBS, 3)


def lbs_to_kg(weight_lbs: float) -> float:
    """Convert lbs to kg, rounded to 3 decimal places."""
    return round(weight_lbs / KG_TO_LBS, 3)


# ---------------------------------------------------------------------------
# Core weight functions
# ---------------------------------------------------------------------------

def unit_weight_kg(spec: SKUSpec) -> float:
    """Return the unit weight in kg for a given SKU spec, rounded to 3dp.

    Only DN, material, valve type, and actuation affect weight.
    Connection type and pressure class are ignored.
    """
    base = DN_BASE_WEIGHT[spec.dn]
    material_mult = MATERIAL_WEIGHT_MULT[spec.material]
    type_mult = VALVE_TYPE_WEIGHT_MULT[spec.valve_type]
    actuation_mult = ACTUATION_WEIGHT_MULT[spec.actuation]
    return round(base * material_mult * type_mult * actuation_mult, 3)


def shipment_weight_kg(spec: SKUSpec, quantity: int) -> float:
    """Return total shipment weight in kg for quantity units, rounded to 3dp."""
    return round(unit_weight_kg(spec) * quantity, 3)


def shipment_weight_for_carrier(
    spec: SKUSpec,
    quantity: int,
    carrier_code: str,
) -> tuple[float, str]:
    """Return (weight_value, weight_unit) appropriate for the given carrier.

    BalticHaul (carrier_code == "BALTIC") reports weight in lbs — a legacy
    system constraint that every TMS consumer must respect. All other carriers
    use kg.

    Returns:
        A 2-tuple of (numeric weight rounded to 3dp, unit string "kg" or "lbs").
    """
    weight_kg = shipment_weight_kg(spec, quantity)
    if carrier_code.upper() == _BALTIC_CARRIER_CODE:
        return kg_to_lbs(weight_kg), "lbs"
    return weight_kg, "kg"
