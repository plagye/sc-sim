"""Tests for carrier and warehouse master data (Step 9)."""

from __future__ import annotations

from datetime import date

import pytest

from flowform.master_data.carriers import (
    ALL_CARRIERS,
    BALTIC,
    CARRIER_BY_CODE,
    DHL,
    DBSC,
    RABEN,
    GEODIS,
    Carrier,
    get_carrier,
    reliability_on_date,
)
from flowform.master_data.warehouses import (
    ALL_WAREHOUSES,
    W01,
    W02,
    WAREHOUSE_BY_CODE,
    get_warehouse,
)


# ---------------------------------------------------------------------------
# Warehouse tests
# ---------------------------------------------------------------------------


def test_warehouse_count():
    assert len(ALL_WAREHOUSES) == 2


def test_warehouse_codes():
    codes = {w.code for w in ALL_WAREHOUSES}
    assert codes == {"W01", "W02"}


def test_inventory_shares_sum_to_one():
    total = sum(w.inventory_share for w in ALL_WAREHOUSES)
    assert abs(total - 1.0) < 1e-9, f"Inventory shares sum to {total}, expected 1.0"


def test_w01_is_main():
    assert W01.role == "main"
    assert W01.city == "Katowice"
    assert W01.inventory_share == 0.80


def test_w02_is_port():
    assert W02.role == "port"
    assert W02.city == "Gdańsk"
    assert W02.inventory_share == 0.20


def test_all_warehouses_poland():
    for w in ALL_WAREHOUSES:
        assert w.country_code == "PL", f"{w.code} country_code is not PL"


def test_warehouse_lookup_by_code():
    assert get_warehouse("W01") is W01
    assert get_warehouse("W02") is W02


def test_warehouse_lookup_missing_raises():
    with pytest.raises(KeyError):
        get_warehouse("W99")


def test_warehouse_by_code_dict():
    assert WAREHOUSE_BY_CODE["W01"] is W01
    assert WAREHOUSE_BY_CODE["W02"] is W02


# ---------------------------------------------------------------------------
# Carrier tests
# ---------------------------------------------------------------------------


def test_carrier_count():
    assert len(ALL_CARRIERS) == 5


def test_carrier_codes():
    codes = {c.code for c in ALL_CARRIERS}
    assert codes == {"DHL", "DBSC", "RABEN", "GEODIS", "BALTIC"}


def test_all_carriers_in_lookup():
    for carrier in ALL_CARRIERS:
        assert carrier.code in CARRIER_BY_CODE
        assert CARRIER_BY_CODE[carrier.code] is carrier


def test_carrier_lookup_by_code():
    assert get_carrier("DHL") is DHL
    assert get_carrier("DBSC") is DBSC
    assert get_carrier("RABEN") is RABEN
    assert get_carrier("GEODIS") is GEODIS
    assert get_carrier("BALTIC") is BALTIC


def test_carrier_lookup_missing_raises():
    with pytest.raises(KeyError):
        get_carrier("UNKNOWN")


def test_baltic_weight_unit_is_lbs():
    """BalticHaul uses pounds — this is the key quirk."""
    assert BALTIC.weight_unit == "lbs"


def test_all_other_carriers_weight_unit_is_kg():
    """Every carrier except BALTIC reports in kilograms."""
    for carrier in ALL_CARRIERS:
        if carrier.code != "BALTIC":
            assert carrier.weight_unit == "kg", (
                f"{carrier.code} weight_unit is {carrier.weight_unit!r}, expected 'kg'"
            )


def test_transit_days_valid():
    for carrier in ALL_CARRIERS:
        assert carrier.transit_days_min >= 1, f"{carrier.code} min transit < 1"
        assert carrier.transit_days_max >= carrier.transit_days_min, (
            f"{carrier.code} max transit < min transit"
        )


def test_reliability_bounds():
    for carrier in ALL_CARRIERS:
        assert 0.0 <= carrier.base_reliability <= 1.0, (
            f"{carrier.code} base_reliability out of [0,1]"
        )


def test_baltic_is_lowest_reliability():
    """BalticHaul has the lowest base reliability among all carriers."""
    reliabilities = [c.base_reliability for c in ALL_CARRIERS]
    assert BALTIC.base_reliability == min(reliabilities)


def test_dhl_highest_reliability():
    reliabilities = [c.base_reliability for c in ALL_CARRIERS]
    assert DHL.base_reliability == max(reliabilities)


def test_baltic_same_day_transit():
    assert BALTIC.transit_days_min == 1
    assert BALTIC.transit_days_max == 1


# ---------------------------------------------------------------------------
# Reliability seasonality tests
# ---------------------------------------------------------------------------


def test_reliability_baseline_june():
    """June is off-season for all penalties — should equal base reliability."""
    d = date(2025, 6, 15)
    for carrier in ALL_CARRIERS:
        r = reliability_on_date(carrier, d)
        assert r == carrier.base_reliability, (
            f"{carrier.code}: expected {carrier.base_reliability} in June, got {r}"
        )


def test_reliability_winter_penalty():
    """February applies −0.05 winter penalty."""
    d = date(2025, 2, 10)
    for carrier in ALL_CARRIERS:
        expected = max(0.0, carrier.base_reliability - 0.05)
        assert reliability_on_date(carrier, d) == pytest.approx(expected), (
            f"{carrier.code} winter reliability mismatch"
        )


def test_reliability_summer_penalty():
    """July applies −0.03 summer penalty."""
    d = date(2025, 7, 15)
    for carrier in ALL_CARRIERS:
        expected = max(0.0, carrier.base_reliability - 0.03)
        assert reliability_on_date(carrier, d) == pytest.approx(expected), (
            f"{carrier.code} summer reliability mismatch"
        )


def test_reliability_christmas_penalty():
    """December 25 applies −0.15 Christmas penalty (not the −0.05 winter penalty)."""
    d = date(2025, 12, 25)
    for carrier in ALL_CARRIERS:
        expected = max(0.0, carrier.base_reliability - 0.15)
        assert reliability_on_date(carrier, d) == pytest.approx(expected), (
            f"{carrier.code} Christmas reliability mismatch"
        )


def test_reliability_dec20_applies_christmas_not_winter():
    """Dec 20 gets the larger −0.15 holiday penalty, not the −0.05 winter penalty."""
    d = date(2025, 12, 20)
    r = reliability_on_date(DHL, d)
    # −0.15, not −0.05
    assert r == pytest.approx(DHL.base_reliability - 0.15)


def test_reliability_easter_week_2025():
    """Easter 2025 is April 20; April 21 should carry −0.08 penalty."""
    # Easter Sunday 2025 = April 20
    d = date(2025, 4, 21)  # Easter Monday — within Easter week
    for carrier in ALL_CARRIERS:
        expected = max(0.0, carrier.base_reliability - 0.08)
        assert reliability_on_date(carrier, d) == pytest.approx(expected), (
            f"{carrier.code} Easter-week reliability mismatch"
        )


def test_reliability_day_after_easter_week():
    """April 27, 2025 is just outside Easter week — no Easter penalty."""
    d = date(2025, 4, 27)  # 7 days after Easter Sunday April 20 → outside window
    r = reliability_on_date(DHL, d)
    assert r == pytest.approx(DHL.base_reliability)


def test_reliability_clamped_to_zero():
    """A hypothetically unreliable carrier never goes below 0."""

    class _LowCarrier:
        base_reliability = 0.05
        code = "TEST"

    # Patch a low-reliability carrier and hit the worst-case (Dec 25 during summer = impossible,
    # but Dec 25 alone is −0.15 → 0.05 − 0.15 = −0.10 → clamped to 0)
    low = Carrier(
        code="LOW",
        name="Low Reliability",
        base_reliability=0.05,
        transit_days_min=1,
        transit_days_max=1,
        cost_tier="low",
        primary_use="test",
        weight_unit="kg",
    )
    r = reliability_on_date(low, date(2025, 12, 25))
    assert r == 0.0


def test_reliability_january_winter():
    """January gets −0.05 winter penalty."""
    d = date(2025, 1, 15)
    r = reliability_on_date(DBSC, d)
    assert r == pytest.approx(DBSC.base_reliability - 0.05)


def test_reliability_november_winter():
    """November gets −0.05 winter penalty."""
    d = date(2025, 11, 1)
    r = reliability_on_date(RABEN, d)
    assert r == pytest.approx(RABEN.base_reliability - 0.05)


def test_reliability_august_summer():
    """August gets −0.03 summer penalty."""
    d = date(2025, 8, 20)
    r = reliability_on_date(GEODIS, d)
    assert r == pytest.approx(GEODIS.base_reliability - 0.03)
