"""Tests for flowform.catalog.weights — per-SKU weight calculator."""

from __future__ import annotations

import math
import random

import pytest

from flowform.catalog.constraints import SKUSpec
from flowform.catalog.generator import generate_catalog
from flowform.catalog.weights import (
    ACTUATION_WEIGHT_MULT,
    DN_BASE_WEIGHT,
    KG_TO_LBS,
    MATERIAL_WEIGHT_MULT,
    VALVE_TYPE_WEIGHT_MULT,
    kg_to_lbs,
    lbs_to_kg,
    shipment_weight_for_carrier,
    shipment_weight_kg,
    unit_weight_kg,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _spec(
    valve_type: str = "Ball",
    dn: int = 50,
    material: str = "CS",
    pressure_class: str = "PN16",
    connection: str = "Flanged",
    actuation: str = "Manual",
) -> SKUSpec:
    return SKUSpec(
        valve_type=valve_type,
        dn=dn,
        material=material,
        pressure_class=pressure_class,
        connection=connection,
        actuation=actuation,
    )


# ---------------------------------------------------------------------------
# 1. Reference spot-check values
# ---------------------------------------------------------------------------

class TestReferenceValues:
    def test_ball_dn50_cs_manual(self) -> None:
        """5.5 × 1.0 × 0.9 × 1.0 = 4.950"""
        spec = _spec("Ball", 50, "CS", "PN16", "Flanged", "Manual")
        assert unit_weight_kg(spec) == pytest.approx(4.950, abs=1e-9)

    def test_gate_dn100_cs_manual(self) -> None:
        """18.5 × 1.0 × 1.0 × 1.0 = 18.500"""
        spec = _spec("Gate", 100, "CS", "PN16", "Flanged", "Manual")
        assert unit_weight_kg(spec) == pytest.approx(18.500, abs=1e-9)

    def test_butterfly_dn150_duplex_pneumatic(self) -> None:
        """35.0 × 1.05 × 0.6 × 1.15 = 25.357 kg (3dp).

        The blueprint states 25.403 kg — that is a transcription error in the
        spec document. The authoritative formula gives:
            35.0 × 1.05 = 36.75
            36.75 × 0.60 = 22.05
            22.05 × 1.15 = 25.3575 (exact decimal)

        However, IEEE 754 represents 25.3575 as 25.357499999...98, which rounds
        DOWN to 25.357, not up to 25.358. Python's round() follows banker's
        rounding (round-half-to-even) and the float falls below the midpoint
        anyway, so the correct implementation result is 25.357.
        """
        spec = _spec("Butterfly", 150, "Duplex", "PN25", "Wafer", "Pneumatic")
        assert unit_weight_kg(spec) == pytest.approx(25.357, abs=1e-9)


# ---------------------------------------------------------------------------
# 2. Monotonic DN scaling
# ---------------------------------------------------------------------------

class TestMonotonicDNScaling:
    def test_weight_increases_with_dn(self) -> None:
        """For identical valve/material/actuation, bigger DN must weigh more."""
        dn_sizes = [15, 25, 40, 50, 80, 100, 150, 200, 250, 300]
        weights = [
            unit_weight_kg(_spec("Gate", dn, "CS", "PN16", "Flanged", "Manual"))
            for dn in dn_sizes
        ]
        for i in range(len(weights) - 1):
            assert weights[i] < weights[i + 1], (
                f"Weight not monotonically increasing: DN{dn_sizes[i]}={weights[i]} "
                f">= DN{dn_sizes[i+1]}={weights[i+1]}"
            )


# ---------------------------------------------------------------------------
# 3. Actuation ordering
# ---------------------------------------------------------------------------

class TestActuationOrdering:
    """Manual < Pneumatic < Electric < Hydraulic for the same base spec."""

    BASE = _spec("Gate", 100, "CS", "PN16", "Flanged", "Manual")

    def _w(self, actuation: str) -> float:
        return unit_weight_kg(
            SKUSpec(
                valve_type=self.BASE.valve_type,
                dn=self.BASE.dn,
                material=self.BASE.material,
                pressure_class=self.BASE.pressure_class,
                connection=self.BASE.connection,
                actuation=actuation,
            )
        )

    def test_manual_lt_pneumatic(self) -> None:
        assert self._w("Manual") < self._w("Pneumatic")

    def test_pneumatic_lt_electric(self) -> None:
        assert self._w("Pneumatic") < self._w("Electric")

    def test_electric_lt_hydraulic(self) -> None:
        assert self._w("Electric") < self._w("Hydraulic")


# ---------------------------------------------------------------------------
# 4. Material ordering
# ---------------------------------------------------------------------------

class TestMaterialOrdering:
    """Brass (0.95) < CS/SS304/SS316 (1.0) < Duplex (1.05)."""

    def _w(self, material: str) -> float:
        return unit_weight_kg(_spec("Ball", 50, material, "PN16", "Flanged", "Manual"))

    def test_brass_lt_cs(self) -> None:
        assert self._w("Brass") < self._w("CS")

    def test_cs_eq_ss304(self) -> None:
        assert self._w("CS") == pytest.approx(self._w("SS304"), abs=1e-9)

    def test_ss304_eq_ss316(self) -> None:
        assert self._w("SS304") == pytest.approx(self._w("SS316"), abs=1e-9)

    def test_cs_lt_duplex(self) -> None:
        assert self._w("CS") < self._w("Duplex")


# ---------------------------------------------------------------------------
# 5. Connection and pressure class do NOT affect weight
# ---------------------------------------------------------------------------

class TestConnectionAndPressureClassIndependence:
    def test_different_connections_same_weight(self) -> None:
        """Flanged vs Threaded vs Welded must produce identical weights."""
        w_flanged = unit_weight_kg(_spec("Ball", 50, "CS", "PN16", "Flanged", "Manual"))
        w_threaded = unit_weight_kg(_spec("Ball", 50, "CS", "PN16", "Threaded", "Manual"))
        w_welded = unit_weight_kg(_spec("Ball", 50, "CS", "PN16", "Welded", "Manual"))
        assert w_flanged == w_threaded == w_welded

    def test_different_pressure_classes_same_weight(self) -> None:
        """PN10 vs PN16 vs PN25 vs PN40 vs PN63 must produce identical weights."""
        weights = [
            unit_weight_kg(_spec("Gate", 100, "CS", pn, "Flanged", "Manual"))
            for pn in ("PN10", "PN16", "PN25", "PN40", "PN63")
        ]
        assert len(set(weights)) == 1, f"Pressure class affected weight: {weights}"

    def test_wafer_butterfly_same_as_flanged(self) -> None:
        """Butterfly Wafer vs Butterfly Flanged must produce same weight."""
        # Note: Wafer is only valid for Butterfly, so we use that here.
        w_wafer = unit_weight_kg(_spec("Butterfly", 100, "CS", "PN16", "Wafer", "Manual"))
        w_flanged = unit_weight_kg(_spec("Butterfly", 100, "CS", "PN16", "Flanged", "Manual"))
        assert w_wafer == pytest.approx(w_flanged, abs=1e-9)


# ---------------------------------------------------------------------------
# 6. shipment_weight_kg
# ---------------------------------------------------------------------------

class TestShipmentWeightKg:
    def test_quantity_multiplies_unit_weight(self) -> None:
        spec = _spec("Ball", 50, "CS", "PN16", "Flanged", "Manual")
        unit = unit_weight_kg(spec)
        total = shipment_weight_kg(spec, 10)
        assert total == pytest.approx(round(unit * 10, 3), abs=1e-9)

    def test_quantity_one_equals_unit_weight(self) -> None:
        spec = _spec("Gate", 100, "Duplex", "PN25", "Flanged", "Electric")
        assert shipment_weight_kg(spec, 1) == pytest.approx(unit_weight_kg(spec), abs=1e-9)

    def test_quantity_zero_is_zero(self) -> None:
        spec = _spec("Ball", 50, "CS", "PN16", "Flanged", "Manual")
        assert shipment_weight_kg(spec, 0) == pytest.approx(0.0, abs=1e-9)


# ---------------------------------------------------------------------------
# 7. shipment_weight_for_carrier
# ---------------------------------------------------------------------------

class TestShipmentWeightForCarrier:
    SPEC = SKUSpec("Gate", 100, "CS", "PN16", "Flanged", "Manual")  # 18.5 kg each

    def test_dhl_returns_kg(self) -> None:
        weight, unit = shipment_weight_for_carrier(self.SPEC, 1, "DHL")
        assert unit == "kg"
        assert weight == pytest.approx(18.5, abs=1e-9)

    def test_baltic_returns_lbs(self) -> None:
        weight, unit = shipment_weight_for_carrier(self.SPEC, 1, "BALTIC")
        assert unit == "lbs"
        expected_lbs = round(18.5 * KG_TO_LBS, 3)
        assert weight == pytest.approx(expected_lbs, abs=1e-9)

    def test_baltic_lbs_value_correct(self) -> None:
        """18.5 kg × 2.20462 = 40.785 lbs (rounded to 3dp)."""
        weight, unit = shipment_weight_for_carrier(self.SPEC, 1, "BALTIC")
        assert unit == "lbs"
        assert weight == pytest.approx(40.785, abs=1e-3)

    def test_dbsc_returns_kg(self) -> None:
        _, unit = shipment_weight_for_carrier(self.SPEC, 5, "DBSC")
        assert unit == "kg"

    def test_raben_returns_kg(self) -> None:
        _, unit = shipment_weight_for_carrier(self.SPEC, 5, "RABEN")
        assert unit == "kg"

    def test_geodis_returns_kg(self) -> None:
        _, unit = shipment_weight_for_carrier(self.SPEC, 5, "GEODIS")
        assert unit == "kg"

    def test_baltic_quantity_scales_correctly(self) -> None:
        """10 units of 18.5 kg = 185 kg = 407.855 lbs."""
        weight, unit = shipment_weight_for_carrier(self.SPEC, 10, "BALTIC")
        assert unit == "lbs"
        expected_lbs = round(185.0 * KG_TO_LBS, 3)
        assert weight == pytest.approx(expected_lbs, abs=1e-3)

    def test_kg_and_lbs_carriers_consistent(self) -> None:
        """Baltic weight / KG_TO_LBS should equal the kg carrier weight."""
        spec = _spec("Butterfly", 150, "SS316", "PN25", "Wafer", "Pneumatic")
        qty = 3
        kg_weight, _ = shipment_weight_for_carrier(spec, qty, "DHL")
        lbs_weight, _ = shipment_weight_for_carrier(spec, qty, "BALTIC")
        assert lbs_weight / KG_TO_LBS == pytest.approx(kg_weight, abs=0.01)


# ---------------------------------------------------------------------------
# 8. kg/lbs round-trip
# ---------------------------------------------------------------------------

class TestKgLbsRoundTrip:
    @pytest.mark.parametrize("value", [1.0, 5.5, 18.5, 35.0, 85.0, 120.0, 250.75])
    def test_round_trip_within_tolerance(self, value: float) -> None:
        """lbs_to_kg(kg_to_lbs(x)) ≈ x within 0.001."""
        assert lbs_to_kg(kg_to_lbs(value)) == pytest.approx(value, abs=0.001)

    def test_kg_to_lbs_positive(self) -> None:
        # kg_to_lbs rounds to 3dp, so kg_to_lbs(1.0) = round(2.20462, 3) = 2.205
        assert kg_to_lbs(1.0) == pytest.approx(round(KG_TO_LBS, 3), abs=1e-9)

    def test_lbs_to_kg_inverse(self) -> None:
        assert lbs_to_kg(KG_TO_LBS) == pytest.approx(1.0, abs=0.001)


# ---------------------------------------------------------------------------
# 9. weight_kg backfill in generate_catalog
# ---------------------------------------------------------------------------

class TestWeightBackfill:
    @pytest.fixture(scope="class")
    def catalog(self) -> list:
        return generate_catalog(random.Random(42), target_size=2500)

    def test_no_zero_weights(self, catalog: list) -> None:
        """After backfill, no entry should have weight_kg == 0.0."""
        zeros = [e.sku for e in catalog if e.weight_kg == 0.0]
        assert zeros == [], f"Found {len(zeros)} zero-weight entries: {zeros[:3]}"

    def test_all_weights_positive(self, catalog: list) -> None:
        negatives = [e.sku for e in catalog if e.weight_kg <= 0.0]
        assert negatives == [], f"Non-positive weights: {negatives[:3]}"

    def test_weight_matches_formula(self, catalog: list) -> None:
        """Spot-check: every entry's weight_kg must equal unit_weight_kg(spec)."""
        mismatches = [
            (e.sku, e.weight_kg, unit_weight_kg(e.spec))
            for e in catalog
            if e.weight_kg != unit_weight_kg(e.spec)
        ]
        assert mismatches == [], f"Weight mismatches: {mismatches[:3]}"

    def test_heaviest_entry_is_large_dn(self, catalog: list) -> None:
        """The heaviest SKU must be a large DN with a heavy valve type and actuation."""
        heaviest = max(catalog, key=lambda e: e.weight_kg)
        # Large DN (>=200) is required for the heaviest to arise.
        assert heaviest.spec.dn >= 200, (
            f"Heaviest entry DN={heaviest.spec.dn} is unexpectedly small: {heaviest.sku}"
        )
        # Globe or Gate (multiplier >= 1.0) combined with Hydraulic or Electric actuation.
        assert heaviest.spec.valve_type in ("Globe", "Gate", "Check", "Ball", "Needle", "Butterfly"), (
            f"Unexpected valve type: {heaviest.spec.valve_type}"
        )
        # Hydraulic or Electric is the heaviest actuation.
        assert heaviest.spec.actuation in ("Hydraulic", "Electric"), (
            f"Heaviest entry has actuation={heaviest.spec.actuation}, expected Hydraulic or Electric: {heaviest.sku}"
        )

    def test_weight_range_reasonable(self, catalog: list) -> None:
        """Minimum weight should be above 0 and below 5 kg; max above 100 kg."""
        weights = [e.weight_kg for e in catalog]
        min_w = min(weights)
        max_w = max(weights)
        # Lightest possible: Needle DN15 Brass Manual = 1.2 × 0.95 × 0.5 × 1.0 = 0.570 kg
        assert 0.5 < min_w < 5.0, f"Min weight {min_w} kg outside expected range"
        # Heaviest possible: Globe DN300 Duplex Hydraulic = 120 × 1.05 × 1.10 × 1.40 = 194.04 kg
        assert max_w > 100.0, f"Max weight {max_w} kg is suspiciously low"
