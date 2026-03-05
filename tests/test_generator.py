"""Tests for flowform.catalog.generator — active catalog sampler."""

from __future__ import annotations

import random
from typing import Any

import pytest

from flowform.catalog.constraints import is_valid_combination
from flowform.catalog.generator import (
    CatalogEntry,
    generate_catalog,
    get_catalog_stats,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def catalog_42() -> list[CatalogEntry]:
    """Canonical catalog generated with seed 42, target 2500."""
    return generate_catalog(random.Random(42), target_size=2500)


@pytest.fixture(scope="module")
def stats_42(catalog_42: list[CatalogEntry]) -> dict[str, Any]:
    return get_catalog_stats(catalog_42)


# ---------------------------------------------------------------------------
# 1. Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_two_calls_same_seed_identical_skus(self) -> None:
        """Two calls with the same seed must produce identical SKU lists."""
        c1 = generate_catalog(random.Random(42), target_size=2500)
        c2 = generate_catalog(random.Random(42), target_size=2500)
        assert [e.sku for e in c1] == [e.sku for e in c2]


# ---------------------------------------------------------------------------
# 2. Target size
# ---------------------------------------------------------------------------

class TestTargetSize:
    def test_exact_target_size(self, catalog_42: list[CatalogEntry]) -> None:
        assert len(catalog_42) == 2500


# ---------------------------------------------------------------------------
# 3. All entries are valid
# ---------------------------------------------------------------------------

class TestAllEntriesValid:
    def test_every_sku_passes_constraints(self, catalog_42: list[CatalogEntry]) -> None:
        invalid = [
            e.sku
            for e in catalog_42
            if not is_valid_combination(
                e.spec.valve_type,
                e.spec.dn,
                e.spec.material,
                e.spec.pressure_class,
                e.spec.connection,
                e.spec.actuation,
            )
        ]
        assert invalid == [], f"Invalid SKUs found: {invalid[:5]}"


# ---------------------------------------------------------------------------
# 4. No duplicates
# ---------------------------------------------------------------------------

class TestNoDuplicates:
    def test_no_duplicate_sku_strings(self, catalog_42: list[CatalogEntry]) -> None:
        skus = [e.sku for e in catalog_42]
        assert len(set(skus)) == len(skus), "Duplicate SKU strings found in catalog"

    def test_no_duplicate_specs(self, catalog_42: list[CatalogEntry]) -> None:
        specs = [e.spec for e in catalog_42]
        assert len(set(specs)) == len(specs), "Duplicate SKUSpec objects found in catalog"


# ---------------------------------------------------------------------------
# 5. Distribution checks
# ---------------------------------------------------------------------------

class TestDistribution:
    def test_ball_butterfly_dominate(self, stats_42: dict[str, Any]) -> None:
        """Ball + Butterfly together should exceed 40% of the catalog."""
        vt = stats_42["valve_type"]
        ball_butterfly = vt.get("Ball", 0) + vt.get("Butterfly", 0)
        assert ball_butterfly > 0.40 * 2500, (
            f"Ball+Butterfly count {ball_butterfly} is not > 40% of 2500 "
            f"(valve_type breakdown: {vt})"
        )

    def test_mid_range_dn_dominate(self, stats_42: dict[str, Any]) -> None:
        """DN 50, 80, 100, 150 together should exceed 50% of the catalog."""
        dn = stats_42["dn"]
        mid_range = sum(dn.get(str(size), 0) for size in (50, 80, 100, 150))
        assert mid_range > 0.50 * 2500, (
            f"Mid-range DN count {mid_range} is not > 50% of 2500 "
            f"(dn breakdown: {dn})"
        )

    def test_hydraulic_less_than_manual(self, stats_42: dict[str, Any]) -> None:
        """Hydraulic actuation must be less common than Manual."""
        act = stats_42["actuation"]
        count_hydraulic = act.get("Hydraulic", 0)
        count_manual = act.get("Manual", 0)
        assert count_hydraulic < count_manual, (
            f"Hydraulic ({count_hydraulic}) should be < Manual ({count_manual})"
        )

    def test_needle_less_than_ball(self, stats_42: dict[str, Any]) -> None:
        """Needle valves must be less common than Ball valves."""
        vt = stats_42["valve_type"]
        assert vt.get("Needle", 0) < vt.get("Ball", 0), (
            f"Needle ({vt.get('Needle', 0)}) should be < Ball ({vt.get('Ball', 0)})"
        )


# ---------------------------------------------------------------------------
# 6. Sorted output
# ---------------------------------------------------------------------------

class TestSortedOutput:
    def test_catalog_sorted_by_sku_ascending(self, catalog_42: list[CatalogEntry]) -> None:
        skus = [e.sku for e in catalog_42]
        assert skus == sorted(skus), "Catalog SKUs are not in ascending lexicographic order"


# ---------------------------------------------------------------------------
# 7. Different seed → different catalog
# ---------------------------------------------------------------------------

class TestDifferentSeed:
    def test_different_seeds_produce_different_catalogs(self) -> None:
        c42 = generate_catalog(random.Random(42), target_size=2500)
        c99 = generate_catalog(random.Random(99), target_size=2500)
        assert [e.sku for e in c42] != [e.sku for e in c99], (
            "Seeds 42 and 99 produced identical catalogs — check RNG usage"
        )

    def test_different_seed_still_valid_size(self) -> None:
        c99 = generate_catalog(random.Random(99), target_size=2500)
        assert len(c99) == 2500

    def test_different_seed_still_valid_entries(self) -> None:
        c99 = generate_catalog(random.Random(99), target_size=2500)
        invalid = [
            e.sku
            for e in c99
            if not is_valid_combination(
                e.spec.valve_type,
                e.spec.dn,
                e.spec.material,
                e.spec.pressure_class,
                e.spec.connection,
                e.spec.actuation,
            )
        ]
        assert invalid == []


# ---------------------------------------------------------------------------
# 8. get_catalog_stats structure
# ---------------------------------------------------------------------------

class TestCatalogStatsStructure:
    EXPECTED_KEYS = {"valve_type", "dn", "material", "pressure_class", "connection", "actuation"}

    def test_stats_has_all_top_level_keys(self, stats_42: dict[str, Any]) -> None:
        assert set(stats_42.keys()) == self.EXPECTED_KEYS

    def test_each_value_is_dict_of_str_int(self, stats_42: dict[str, Any]) -> None:
        for key, breakdown in stats_42.items():
            assert isinstance(breakdown, dict), f"stats[{key!r}] is not a dict"
            for k, v in breakdown.items():
                assert isinstance(k, str), f"stats[{key!r}] has non-str key {k!r}"
                assert isinstance(v, int), f"stats[{key!r}][{k!r}] is not int, got {type(v)}"

    def test_stats_counts_sum_to_catalog_size(self, stats_42: dict[str, Any]) -> None:
        """Each dimension's breakdown should sum to the full catalog size."""
        for key, breakdown in stats_42.items():
            total = sum(breakdown.values())
            assert total == 2500, (
                f"stats[{key!r}] sums to {total}, expected 2500"
            )

    def test_valve_type_keys_are_known_types(self, stats_42: dict[str, Any]) -> None:
        known = {"Gate", "Ball", "Butterfly", "Globe", "Check", "Needle"}
        assert set(stats_42["valve_type"].keys()).issubset(known)

    def test_material_keys_are_known_materials(self, stats_42: dict[str, Any]) -> None:
        known = {"CS", "SS304", "SS316", "Duplex", "Brass"}
        assert set(stats_42["material"].keys()).issubset(known)

    def test_actuation_keys_are_known_types(self, stats_42: dict[str, Any]) -> None:
        known = {"Manual", "Pneumatic", "Electric", "Hydraulic"}
        assert set(stats_42["actuation"].keys()).issubset(known)
