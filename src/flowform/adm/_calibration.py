"""ADM quantity calibration helper — Step 57.

Computes expected monthly order volume per product_group by summing over all
customers their effective demand contribution weighted by SKU affinity.

Product group format: ``{TYPE_CODE}-DN{dn}-{MAT}``  e.g. ``BL-DN100-S316``.
"""

from __future__ import annotations

from flowform.config import Config
from flowform.state import SimulationState


def _parse_product_group(sku: str) -> str:
    """Derive product-group key from a catalog SKU string.

    SKU format: ``FF-{TYPE}{DN}-{MAT}-{PN}-{CONN}{ACT}``
    Returns ``{type_code}-DN{dn}-{mat}`` e.g. ``BL-DN100-S316``.
    """
    parts = sku.split("-")
    type_dn = parts[1]
    type_code = type_dn[:2]
    dn = type_dn[2:]
    mat = parts[2]
    return f"{type_code}-DN{dn}-{mat}"


def _expected_monthly_units_per_group(
    state: SimulationState,
    config: Config,  # noqa: ARG001 — reserved for future use
    _calibration_override: dict[str, float] | None = None,
) -> dict[str, float]:
    """Compute expected monthly order volume per product_group.

    The estimate is derived by summing over all 60 customers:
    ``base_order_frequency_per_month × order_size_midpoint × group_affinity_weight``

    where ``group_affinity_weight`` is the fraction of a customer's total SKU
    affinity mass (material × dn component only) that falls within the given
    product group.

    Args:
        state:                 Current simulation state (provides catalog + customers).
        config:                Simulation configuration (reserved, not currently used).
        _calibration_override: If not None, returned directly without computation.
                               Useful for testing.

    Returns:
        A dict mapping product_group string → expected monthly units (float ≥ 0).
        Returns an empty dict when the catalog contains no entries.
    """
    if _calibration_override is not None:
        return _calibration_override

    if not state.catalog:
        return {}

    # --- Build per-group catalog index ---
    # group → list of (material, dn) tuples for all catalog SKUs in that group
    group_mat_dn: dict[str, list[tuple[str, int]]] = {}
    for entry in state.catalog:
        group = _parse_product_group(entry.sku)
        if group not in group_mat_dn:
            group_mat_dn[group] = []
        group_mat_dn[group].append((entry.spec.material, entry.spec.dn))

    all_groups = list(group_mat_dn.keys())
    result: dict[str, float] = {g: 0.0 for g in all_groups}

    for customer in state.customers:
        profile = customer.ordering_profile
        freq = profile.base_order_frequency_per_month
        lo, hi = profile.order_size_range
        size_midpoint = (lo + hi) / 2.0
        monthly_base = freq * size_midpoint

        if monthly_base <= 0:
            continue

        affinity = profile.sku_affinity  # dict[str, dict[str|int, float]]

        # Normalise material sub-dict
        mat_weights: dict[str, float] = affinity.get("material", {})
        mat_total = sum(mat_weights.values()) or 1.0
        mat_norm: dict[str, float] = {k: v / mat_total for k, v in mat_weights.items()}

        # Normalise dn sub-dict
        dn_weights: dict[str | int, float] = affinity.get("dn", {})
        dn_total = sum(dn_weights.values()) or 1.0
        dn_norm: dict[str | int, float] = {k: v / dn_total for k, v in dn_weights.items()}

        # Compute raw weight per group = sum of mat_share × dn_share for all
        # catalog entries belonging to that group.
        raw_group_weight: dict[str, float] = {}
        for group, mat_dn_list in group_mat_dn.items():
            w = 0.0
            for mat, dn in mat_dn_list:
                w += mat_norm.get(mat, 0.0) * dn_norm.get(dn, 0.0)
            raw_group_weight[group] = w

        total_raw = sum(raw_group_weight.values()) or 1.0

        for group in all_groups:
            group_affinity_weight = raw_group_weight[group] / total_raw
            result[group] += monthly_base * group_affinity_weight

    return result
