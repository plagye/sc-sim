"""Catalog generator: samples a commercially realistic active SKU catalog.

The full valid combination space contains ~11,000 SKUs (after constraint
filtering). This module selects ~2,500 of them weighted toward the products
that actually dominate industrial valve markets: Ball and Butterfly types,
mid-range DN sizes, standard materials.

Weighting uses the Efraimidis-Spirakis weighted reservoir sampling algorithm:
    score = rng.random() ** (1.0 / weight)
then select the top target_size by score. This is O(n log n) and reproduces
deterministically from a given seed.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any

from flowform.catalog.constraints import (
    SKUSpec,
    get_all_valid_specs,
    get_sku_code,
    is_valid_combination,
)
from flowform.catalog.weights import unit_weight_kg

# ---------------------------------------------------------------------------
# Dimension weights — tuned for commercial realism
# ---------------------------------------------------------------------------

VALVE_TYPE_WEIGHTS: dict[str, float] = {
    "Ball":      3.0,
    "Butterfly": 2.5,
    "Gate":      2.0,
    "Globe":     1.5,
    "Check":     1.5,
    "Needle":    0.8,
}

DN_WEIGHTS: dict[int, float] = {
    15:  0.5,
    25:  0.8,
    40:  1.2,
    50:  2.0,
    80:  2.5,
    100: 3.0,
    150: 2.5,
    200: 1.5,
    250: 0.8,
    300: 0.5,
}

MATERIAL_WEIGHTS: dict[str, float] = {
    "CS":     3.0,
    "SS304":  2.5,
    "SS316":  2.5,
    "Duplex": 1.0,
    "Brass":  1.5,
}

PRESSURE_CLASS_WEIGHTS: dict[str, float] = {
    "PN10": 2.0,
    "PN16": 3.0,
    "PN25": 2.5,
    "PN40": 1.5,
    "PN63": 0.8,
}

CONNECTION_WEIGHTS: dict[str, float] = {
    "Flanged":  3.0,
    "Threaded": 2.0,
    "Welded":   1.5,
    "Wafer":    2.0,
}

ACTUATION_WEIGHTS: dict[str, float] = {
    "Manual":    3.0,
    "Pneumatic": 2.5,
    "Electric":  2.0,
    "Hydraulic": 1.0,
}


# ---------------------------------------------------------------------------
# CatalogEntry dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CatalogEntry:
    """A single active SKU in the FlowForm catalog."""

    spec: SKUSpec
    sku: str          # canonical FF-... string
    weight_kg: float  # unit weight in kg, from flowform.catalog.weights.unit_weight_kg
    is_active: bool = True


# ---------------------------------------------------------------------------
# Weight calculation helper
# ---------------------------------------------------------------------------

def _spec_weight(spec: SKUSpec) -> float:
    """Compute the inclusion weight for a single spec as the product of all dimension weights."""
    return (
        VALVE_TYPE_WEIGHTS[spec.valve_type]
        * DN_WEIGHTS[spec.dn]
        * MATERIAL_WEIGHTS[spec.material]
        * PRESSURE_CLASS_WEIGHTS[spec.pressure_class]
        * CONNECTION_WEIGHTS[spec.connection]
        * ACTUATION_WEIGHTS[spec.actuation]
    )


# ---------------------------------------------------------------------------
# Catalog generator
# ---------------------------------------------------------------------------

def generate_catalog(
    rng: random.Random,
    target_size: int = 2500,
) -> list[CatalogEntry]:
    """Sample target_size active SKUs from the valid combination space.

    Uses weighted reservoir sampling (Efraimidis-Spirakis algorithm):
    each spec is assigned a score = rng.random() ** (1.0 / weight), then
    the top target_size by score are selected. This is O(n log n), correct
    for weighted sampling without replacement, and fully deterministic for
    a given rng seed.

    Returns a list sorted by SKU string for determinism.
    """
    all_specs = get_all_valid_specs()

    if target_size > len(all_specs):
        raise ValueError(
            f"target_size={target_size} exceeds total valid specs ({len(all_specs)})"
        )

    # Efraimidis-Spirakis: score each spec, keep top target_size.
    scored: list[tuple[float, SKUSpec]] = []
    for spec in all_specs:
        w = _spec_weight(spec)
        # rng.random() is in (0, 1); exponentiation by reciprocal of weight
        # gives higher-weight items a systematically higher expected score.
        score = rng.random() ** (1.0 / w)
        scored.append((score, spec))

    # Partial sort: only materialise the top target_size entries.
    scored.sort(key=lambda t: t[0], reverse=True)
    selected_specs = [spec for _, spec in scored[:target_size]]

    # Build CatalogEntry objects, sort by SKU string.
    entries = [
        CatalogEntry(
            spec=spec,
            sku=get_sku_code(spec),
            weight_kg=unit_weight_kg(spec),
        )
        for spec in selected_specs
    ]
    entries.sort(key=lambda e: e.sku)
    return entries


# ---------------------------------------------------------------------------
# Stats helper
# ---------------------------------------------------------------------------

def get_catalog_stats(catalog: list[CatalogEntry]) -> dict[str, Any]:
    """Return a summary dict with counts broken down by each dimension.

    Keys: valve_type, dn, material, pressure_class, connection, actuation.
    Each value is a dict[str, int] mapping dimension value → count.
    """
    stats: dict[str, dict[str, int]] = {
        "valve_type":     {},
        "dn":             {},
        "material":       {},
        "pressure_class": {},
        "connection":     {},
        "actuation":      {},
    }

    for entry in catalog:
        s = entry.spec

        stats["valve_type"][s.valve_type] = stats["valve_type"].get(s.valve_type, 0) + 1
        dn_key = str(s.dn)
        stats["dn"][dn_key] = stats["dn"].get(dn_key, 0) + 1
        stats["material"][s.material] = stats["material"].get(s.material, 0) + 1
        stats["pressure_class"][s.pressure_class] = stats["pressure_class"].get(s.pressure_class, 0) + 1
        stats["connection"][s.connection] = stats["connection"].get(s.connection, 0) + 1
        stats["actuation"][s.actuation] = stats["actuation"].get(s.actuation, 0) + 1

    return stats
