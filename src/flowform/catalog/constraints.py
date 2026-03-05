"""SKU constraint engine: valid product combination rules.

FlowForm Industries manufactures industrial control valves. The theoretical
combination space is 6 × 10 × 5 × 5 × 4 × 4 = 24,000 SKUs. After applying
eight business/engineering constraints, the valid catalog lands at ~2,000–3,000
SKUs.

SKU encoding: FF-{TYPE_CODE}{DN}-{MAT_CODE}-{PN}-{CONN_CODE}{ACT_CODE}
Example: FF-BL100-S316-PN25-FP  (Ball, DN100, SS316, PN25, Flanged, Pneumatic)
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from typing import Final

# ---------------------------------------------------------------------------
# Dimension value lists
# ---------------------------------------------------------------------------

VALVE_TYPES: Final[list[str]] = ["Gate", "Ball", "Butterfly", "Globe", "Check", "Needle"]
DN_SIZES: Final[list[int]] = [15, 25, 40, 50, 80, 100, 150, 200, 250, 300]
MATERIALS: Final[list[str]] = ["CS", "SS304", "SS316", "Duplex", "Brass"]
PRESSURE_CLASSES: Final[list[str]] = ["PN10", "PN16", "PN25", "PN40", "PN63"]
CONNECTIONS: Final[list[str]] = ["Flanged", "Threaded", "Welded", "Wafer"]
ACTUATIONS: Final[list[str]] = ["Manual", "Pneumatic", "Electric", "Hydraulic"]

# ---------------------------------------------------------------------------
# Code mappings
# ---------------------------------------------------------------------------

_TYPE_CODE: Final[dict[str, str]] = {
    "Gate": "GT",
    "Ball": "BL",
    "Butterfly": "BF",
    "Globe": "GL",
    "Check": "CK",
    "Needle": "ND",
}

_MAT_CODE: Final[dict[str, str]] = {
    "CS": "CS",
    "SS304": "S304",
    "SS316": "S316",
    "Duplex": "DPX",
    "Brass": "BRS",
}

_CONN_CODE: Final[dict[str, str]] = {
    "Flanged": "F",
    "Threaded": "T",
    "Welded": "W",
    "Wafer": "X",
}

_ACT_CODE: Final[dict[str, str]] = {
    "Manual": "M",
    "Pneumatic": "P",
    "Electric": "E",
    "Hydraulic": "H",
}

# ---------------------------------------------------------------------------
# SKUSpec dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SKUSpec:
    """Immutable specification for a single FlowForm SKU."""

    valve_type: str
    dn: int
    material: str
    pressure_class: str
    connection: str
    actuation: str


# ---------------------------------------------------------------------------
# Constraint logic
# ---------------------------------------------------------------------------

def is_valid_combination(
    valve_type: str,
    dn: int,
    material: str,
    pressure_class: str,
    connection: str,
    actuation: str,
) -> bool:
    """Return True if this combination satisfies all eight product constraints.

    Constraints:
    1. Needle valves: DN15–DN50 only.
    2. Hydraulic actuation: DN100+ only.
    3. Brass material: PN10 and PN16 only.
    4. Wafer connection: Butterfly valves only.
    5. Duplex material: no Manual actuation.
    6. Globe valves: no Wafer connection (explicit; also covered by rule 4).
    7. Check valves: Manual or Pneumatic actuation only.
    8. DN250+: not available in Brass.
    """
    # Rule 1: Needle valves limited to small DN sizes.
    if valve_type == "Needle" and dn > 50:
        return False

    # Rule 2: Hydraulic actuation only makes engineering sense on large valves.
    if actuation == "Hydraulic" and dn < 100:
        return False

    # Rule 3: Brass has limited pressure ratings.
    if material == "Brass" and pressure_class not in ("PN10", "PN16"):
        return False

    # Rule 4: Wafer-style connection only suits Butterfly valve body geometry.
    if connection == "Wafer" and valve_type != "Butterfly":
        return False

    # Rule 5: Duplex steel is too hard to machine for manual stem packing.
    if material == "Duplex" and actuation == "Manual":
        return False

    # Rule 6: Globe valve internals are incompatible with Wafer face-to-face dims.
    # (Logically implied by rule 4, but stated explicitly per spec.)
    if valve_type == "Globe" and connection == "Wafer":
        return False

    # Rule 7: Check valves are self-actuating; only Manual or Pneumatic override.
    if valve_type == "Check" and actuation not in ("Manual", "Pneumatic"):
        return False

    # Rule 8: Large DN valves exceed Brass's structural strength envelope.
    if material == "Brass" and dn >= 250:
        return False

    return True


# ---------------------------------------------------------------------------
# SKU code generation
# ---------------------------------------------------------------------------

def get_sku_code(spec: SKUSpec) -> str:
    """Return the canonical FF-... SKU string for a given spec.

    Format: FF-{TYPE_CODE}{DN}-{MAT_CODE}-{PN}-{CONN_CODE}{ACT_CODE}
    Example: FF-BL100-S316-PN25-FP
    """
    type_code = _TYPE_CODE[spec.valve_type]
    mat_code = _MAT_CODE[spec.material]
    conn_code = _CONN_CODE[spec.connection]
    act_code = _ACT_CODE[spec.actuation]
    return f"FF-{type_code}{spec.dn}-{mat_code}-{spec.pressure_class}-{conn_code}{act_code}"


# ---------------------------------------------------------------------------
# Catalog builders (module-level cache — list is not hashable, so no lru_cache)
# ---------------------------------------------------------------------------

_valid_specs_cache: list[SKUSpec] | None = None
_valid_skus_cache: list[str] | None = None


def get_all_valid_specs() -> list[SKUSpec]:
    """Return all valid SKUSpec objects representing the live product catalog.

    Results are computed once and cached for the lifetime of the process.
    """
    global _valid_specs_cache
    if _valid_specs_cache is not None:
        return _valid_specs_cache

    specs: list[SKUSpec] = []
    for valve_type, dn, material, pressure_class, connection, actuation in product(
        VALVE_TYPES, DN_SIZES, MATERIALS, PRESSURE_CLASSES, CONNECTIONS, ACTUATIONS
    ):
        if is_valid_combination(valve_type, dn, material, pressure_class, connection, actuation):
            specs.append(
                SKUSpec(
                    valve_type=valve_type,
                    dn=dn,
                    material=material,
                    pressure_class=pressure_class,
                    connection=connection,
                    actuation=actuation,
                )
            )

    _valid_specs_cache = specs
    return _valid_specs_cache


def get_all_valid_skus() -> list[str]:
    """Return all valid SKU strings representing the live product catalog.

    Results are computed once and cached for the lifetime of the process.
    """
    global _valid_skus_cache
    if _valid_skus_cache is not None:
        return _valid_skus_cache

    _valid_skus_cache = [get_sku_code(spec) for spec in get_all_valid_specs()]
    return _valid_skus_cache
