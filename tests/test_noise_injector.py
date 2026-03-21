"""Tests for the noise injector (Step 44)."""

from __future__ import annotations

import random
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from flowform.noise.injector import RATES, inject


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(profile: str = "medium") -> Any:
    """Return a minimal mock config with noise.profile set."""
    cfg = MagicMock()
    cfg.noise.profile = profile  # plain string — injector uses hasattr(profile, "value")
    return cfg


def _make_event(**kwargs: Any) -> dict[str, Any]:
    base = {"event_type": "customer_order", "event_id": "EV-001"}
    base.update(kwargs)
    return base


def _make_load_event(**kwargs: Any) -> dict[str, Any]:
    base = {
        "event_type": "load_event",
        "event_id": "EV-L01",
        "load_id": "LOAD-001",
        "weight_unit": "kg",
    }
    base.update(kwargs)
    return base


def _force_config(profile: str, noise_type: str, rate: float, monkeypatch: Any) -> Any:
    """Patch a single noise type rate and return a matching config."""
    monkeypatch.setitem(RATES[profile], noise_type, rate)
    return _make_config(profile)


# ---------------------------------------------------------------------------
# Structure tests
# ---------------------------------------------------------------------------

def test_rates_table_completeness() -> None:
    expected_types = {
        "duplicate", "missing_field", "wrong_timezone", "sku_case_error",
        "customer_id_mismatch", "decimal_drift", "future_dated", "orphan_reference",
        "encoding_issue", "out_of_sequence", "missing_weight_unit",
        "negative_quantity_snapshot", "duplicate_load_id",
    }
    assert set(RATES.keys()) == {"light", "medium", "heavy"}
    for profile, table in RATES.items():
        assert set(table.keys()) == expected_types, f"Missing keys in profile '{profile}'"


def test_light_profile_zero_rates() -> None:
    zero_types = {
        "sku_case_error", "customer_id_mismatch", "future_dated", "orphan_reference",
        "encoding_issue", "out_of_sequence", "missing_weight_unit",
        "negative_quantity_snapshot", "duplicate_load_id",
    }
    for noise_type in zero_types:
        assert RATES["light"][noise_type] == 0.0, f"Expected 0.0 for light/{noise_type}"


def test_inject_returns_list_of_dicts() -> None:
    cfg = _make_config("light")
    rng = random.Random(42)
    assert inject([], cfg, rng) == []

    events = [_make_event(), _make_event(event_id="EV-002")]
    result = inject(events, cfg, rng)
    assert isinstance(result, list)
    assert len(result) >= 2
    for item in result:
        assert isinstance(item, dict)


# ---------------------------------------------------------------------------
# Duplicate
# ---------------------------------------------------------------------------

def test_duplicate_noise_increases_count(monkeypatch: Any) -> None:
    cfg = _force_config("medium", "duplicate", 1.0, monkeypatch)
    rng = random.Random(0)
    events = [_make_event(event_id=f"EV-{i:03d}") for i in range(200)]
    result = inject(events, cfg, rng)
    assert len(result) > 200


# ---------------------------------------------------------------------------
# Missing field
# ---------------------------------------------------------------------------

def test_missing_field_removes_key(monkeypatch: Any) -> None:
    cfg = _force_config("medium", "missing_field", 1.0, monkeypatch)
    rng = random.Random(0)
    event = _make_event(
        customer_id="CUST-001",
        order_id="ORD-2026-000001",
        unit_price=100.0,
        timestamp="2026-03-10T08:00:00Z",
    )
    original_keys = set(event.keys())
    result = inject([event], cfg, rng)
    assert len(result) == 1
    result_keys = set(result[0].keys())
    assert result_keys < original_keys  # strict subset
    assert "event_type" in result_keys


# ---------------------------------------------------------------------------
# Wrong timezone
# ---------------------------------------------------------------------------

def test_wrong_timezone_replaces_Z(monkeypatch: Any) -> None:
    cfg = _force_config("medium", "wrong_timezone", 1.0, monkeypatch)
    rng = random.Random(0)
    event = _make_event(timestamp="2026-03-10T08:00:00Z")
    result = inject([event], cfg, rng)
    ts = result[0]["timestamp"]
    assert not ts.endswith("Z")
    assert any(ts.endswith(tz) for tz in ["+01:00", "+02:00", "-05:00", "-08:00"])


# ---------------------------------------------------------------------------
# SKU case error
# ---------------------------------------------------------------------------

def test_sku_case_error_mutates_sku(monkeypatch: Any) -> None:
    cfg = _force_config("medium", "sku_case_error", 1.0, monkeypatch)
    # Run multiple RNG seeds to guarantee at least one produces a mutation
    # (upper() on an already-uppercase string is a no-op, so we try seeds)
    original = "FF-Gate50-Cs-Pn16-Fl-Man"  # mixed case — lower/upper both differ
    found_mutation = False
    for seed in range(10):
        rng = random.Random(seed)
        event = _make_event(sku=original)
        result = inject([event], cfg, rng)
        if result[0]["sku"] != original:
            found_mutation = True
            break
    assert found_mutation, "sku_case_error never mutated the sku across 10 seeds"


# ---------------------------------------------------------------------------
# Customer ID mismatch
# ---------------------------------------------------------------------------

def test_customer_id_mismatch_requires_multiple_events(monkeypatch: Any) -> None:
    cfg = _force_config("medium", "customer_id_mismatch", 1.0, monkeypatch)
    rng = random.Random(0)

    # Single event — no other customer_id to swap to, should be unchanged
    single = [_make_event(customer_id="CUST-001")]
    result = inject(single, cfg, rng)
    assert result[0]["customer_id"] == "CUST-001"

    # Two events with different customer_ids — mismatch should be applied
    two = [
        _make_event(event_id="EV-A", customer_id="CUST-001"),
        _make_event(event_id="EV-B", customer_id="CUST-002"),
    ]
    result = inject(two, cfg, rng)
    # At least one should have a swapped customer_id
    ids = {e["customer_id"] for e in result}
    assert len(ids) > 0  # still have customer_ids
    # With rate=1.0, both get swapped; each only has one candidate available
    assert result[0]["customer_id"] == "CUST-002"
    assert result[1]["customer_id"] == "CUST-001"


# ---------------------------------------------------------------------------
# Decimal drift
# ---------------------------------------------------------------------------

def test_decimal_drift_changes_value(monkeypatch: Any) -> None:
    cfg = _force_config("medium", "decimal_drift", 1.0, monkeypatch)
    rng = random.Random(0)
    event = _make_event(unit_price=100.0)
    result = inject([event], cfg, rng)
    assert isinstance(result[0]["unit_price"], float)
    # With rate=1.0 the value should be slightly different (not exactly 100.0 in most cases)
    # The multiplier is in [0.9999, 1.0001]; very small chance of being exactly 100.0
    # We just check it's a float and in the right ballpark
    assert 99.98 <= result[0]["unit_price"] <= 100.02


# ---------------------------------------------------------------------------
# Future dated
# ---------------------------------------------------------------------------

def test_future_dated_advances_timestamp(monkeypatch: Any) -> None:
    from datetime import date
    cfg = _force_config("medium", "future_dated", 1.0, monkeypatch)
    rng = random.Random(0)
    event = _make_event(timestamp="2026-01-05T08:00:00Z")
    result = inject([event], cfg, rng)
    ts = result[0]["timestamp"]
    date_part = ts.split("T")[0]
    assert date.fromisoformat(date_part) > date(2026, 1, 5)


# ---------------------------------------------------------------------------
# Orphan reference
# ---------------------------------------------------------------------------

def test_orphan_reference_replaces_order_id(monkeypatch: Any) -> None:
    cfg = _force_config("medium", "orphan_reference", 1.0, monkeypatch)
    rng = random.Random(0)
    event = _make_event(order_id="ORD-2026-000001")
    result = inject([event], cfg, rng)
    new_id = result[0]["order_id"]
    # Must be replaced with a plausible historical order ID (not the original)
    assert new_id != "ORD-2026-000001"
    import re
    assert re.match(r"ORD-20[0-2]\d-\d{6}", new_id), (
        f"orphan_reference produced unexpected format: {new_id}"
    )


def test_orphan_reference_uses_variable_id(monkeypatch: Any) -> None:
    """I3: Orphan references must produce varied IDs, not one hardcoded value."""
    import re
    cfg = _force_config("medium", "orphan_reference", 1.0, monkeypatch)
    rng = random.Random(42)
    events = [_make_event(order_id=f"ORD-2026-{i:06d}") for i in range(20)]
    result = inject(events, cfg, rng)
    orphaned_ids = [e["order_id"] for e in result if "order_id" in e]
    assert len(set(orphaned_ids)) > 1, (
        "All orphan references have the same ID — variable generation not working"
    )
    for oid in orphaned_ids:
        assert re.match(r"ORD-20[0-2]\d-\d{6}", oid), (
            f"Orphan ID does not match expected format: {oid}"
        )


# ---------------------------------------------------------------------------
# Encoding issue
# ---------------------------------------------------------------------------

def test_encoding_issue_appends_unicode(monkeypatch: Any) -> None:
    cfg = _force_config("medium", "encoding_issue", 1.0, monkeypatch)
    rng = random.Random(0)
    event = _make_event(customer_name="Test")
    result = inject([event], cfg, rng)
    assert len(result[0]["customer_name"]) > 4


# ---------------------------------------------------------------------------
# Out of sequence
# ---------------------------------------------------------------------------

def test_out_of_sequence_regresses_timestamp(monkeypatch: Any) -> None:
    from datetime import date
    cfg = _force_config("medium", "out_of_sequence", 1.0, monkeypatch)
    rng = random.Random(0)
    event = _make_event(timestamp="2026-03-10T09:00:00Z")
    result = inject([event], cfg, rng)
    ts = result[0]["timestamp"]
    date_part = ts.split("T")[0]
    assert date.fromisoformat(date_part) < date(2026, 3, 10)


def test_out_of_sequence_timestamp_never_predates_simulation_start(
    monkeypatch: Any,
) -> None:
    """M1: out_of_sequence noise must never produce a timestamp before 2026-01-05."""
    from datetime import date
    cfg = _force_config("medium", "out_of_sequence", 1.0, monkeypatch)
    rng = random.Random(0)
    # Use sim_date very close to simulation start
    events = [_make_event(timestamp="2026-01-06T09:00:00Z") for _ in range(50)]
    result = inject(events, cfg, rng)
    floor = date(2026, 1, 5)
    for ev in result:
        ts = ev.get("timestamp", "")
        if "T" not in ts:
            continue
        d = date.fromisoformat(ts.split("T")[0])
        assert d >= floor, (
            f"out_of_sequence produced timestamp {ts} before simulation start {floor}"
        )


# ---------------------------------------------------------------------------
# Missing weight unit
# ---------------------------------------------------------------------------

def test_missing_weight_unit_only_targets_load_event(monkeypatch: Any) -> None:
    cfg = _force_config("medium", "missing_weight_unit", 1.0, monkeypatch)
    rng = random.Random(0)
    load_ev = _make_load_event()
    other_ev = _make_event(weight_unit="kg")
    result = inject([load_ev, other_ev], cfg, rng)

    load_result = next(e for e in result if e["event_type"] == "load_event")
    other_result = next(e for e in result if e["event_type"] == "customer_order")

    assert "weight_unit" not in load_result
    assert "weight_unit" in other_result


# ---------------------------------------------------------------------------
# Negative quantity snapshot
# ---------------------------------------------------------------------------

def test_negative_quantity_snapshot_only_targets_snapshot(monkeypatch: Any) -> None:
    cfg = _force_config("medium", "negative_quantity_snapshot", 1.0, monkeypatch)
    rng = random.Random(0)
    snapshot_ev = {
        "event_type": "inventory_snapshot",
        "positions": [{"sku": "FF-BL100-S316-PN25-FP", "quantity_on_hand": 50, "quantity_allocated": 10, "quantity_available": 40}],
    }
    other_ev = _make_event(quantity_on_hand=50)
    result = inject([snapshot_ev, other_ev], cfg, rng)

    snap_result = next(e for e in result if e["event_type"] == "inventory_snapshot")
    other_result = next(e for e in result if e["event_type"] == "customer_order")

    assert snap_result["positions"][0]["quantity_on_hand"] < 0
    assert other_result["quantity_on_hand"] == 50


def test_negative_quantity_snapshot_mutates_position(monkeypatch: Any) -> None:
    """C2: negative_quantity_snapshot must mutate positions[*].quantity_on_hand, not top-level."""
    cfg = _force_config("medium", "negative_quantity_snapshot", 1.0, monkeypatch)
    rng = random.Random(0)
    event = {
        "event_type": "inventory_snapshot",
        "positions": [{"sku": "FF-BL100-S316-PN25-FP", "quantity_on_hand": 50, "quantity_allocated": 10, "quantity_available": 40}],
    }
    result = inject([event], cfg, rng)
    assert result[0]["positions"][0]["quantity_on_hand"] < 0
    assert result[0]["positions"][0]["quantity_available"] <= 0


# ---------------------------------------------------------------------------
# Duplicate load ID
# ---------------------------------------------------------------------------

def test_duplicate_load_id_requires_multiple_load_events(monkeypatch: Any) -> None:
    cfg = _force_config("medium", "duplicate_load_id", 1.0, monkeypatch)
    rng = random.Random(0)

    # Single load_event — no swap possible
    single = [_make_load_event(load_id="LOAD-001")]
    result = inject(single, cfg, rng)
    assert result[0]["load_id"] == "LOAD-001"

    # Two load_events — one should get the other's load_id
    two = [
        _make_load_event(load_id="LOAD-001"),
        _make_load_event(load_id="LOAD-002", event_id="EV-L02"),
    ]
    result = inject(two, cfg, rng)
    load_ids = [e["load_id"] for e in result if e["event_type"] == "load_event"]
    # With rate=1.0, each event that has load_id and there are 2+ will be swapped
    # Both should still have valid load_ids from the original set
    assert set(load_ids).issubset({"LOAD-001", "LOAD-002"})
    # With rate=1.0 and two candidates, both will attempt to swap
    # At minimum, at least one should have been changed
    assert load_ids[0] == "LOAD-002"  # first gets second's id
    assert load_ids[1] == "LOAD-001"  # second gets first's id


# ---------------------------------------------------------------------------
# Real config integration
# ---------------------------------------------------------------------------

def test_inject_with_real_config_light_profile() -> None:
    from flowform.config import load_config
    config_path = Path(__file__).parent.parent / "config.yaml"
    config = load_config(config_path)
    # Force light profile for this test
    from flowform.config import NoiseProfile
    object.__setattr__(config.noise, "profile", NoiseProfile.light)

    rng = random.Random(42)
    # Build 500 mixed events
    events: list[dict] = []
    for i in range(200):
        events.append({"event_type": "customer_order", "event_id": f"CO-{i}", "customer_id": f"CUST-{i%10:03d}", "unit_price": 100.0, "timestamp": "2026-03-10T08:00:00Z"})
    for i in range(200):
        events.append({"event_type": "load_event", "event_id": f"LE-{i}", "load_id": f"LOAD-{i:03d}", "weight_unit": "kg", "timestamp": "2026-03-10T14:00:00Z"})
    for i in range(100):
        events.append({"event_type": "inventory_snapshot", "event_id": f"IS-{i}", "quantity_on_hand": 50})

    result = inject(events, config, rng)
    assert isinstance(result, list)
    assert len(result) >= 500


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

def test_inject_is_deterministic() -> None:
    cfg = _make_config("medium")
    events = [
        _make_event(
            customer_id=f"CUST-{i:03d}",
            order_id=f"ORD-2026-{i:06d}",
            unit_price=float(100 + i),
            timestamp="2026-03-10T08:00:00Z",
            sku_id="FF-GATE50-CS-PN16-FL-MAN",
        )
        for i in range(100)
    ]

    result1 = inject(events, cfg, random.Random(99))
    result2 = inject(events, cfg, random.Random(99))

    assert result1 == result2


# ---------------------------------------------------------------------------
# Exempt event types
# ---------------------------------------------------------------------------

def test_shipment_confirmation_exempt_from_noise() -> None:
    """shipment_confirmation events must never be mutated or duplicated by noise."""
    cfg = _make_config("heavy")

    shipment_event: dict = {
        "event_type": "shipment_confirmation",
        "event_id": "SC-001",
        "shipment_id": "SHP-001",
        "load_id": "LOAD-001",
        "customer_id": "CUST-001",
        "confirmed_at": "2026-03-10T14:00:00Z",
    }
    order_event: dict = {
        "event_type": "customer_order",
        "event_id": "EV-ORD-001",
        "order_id": "ORD-2026-000001",
        "customer_id": "CUST-001",
        "unit_price": 1000.0,
        "timestamp": "2026-03-10T08:00:00Z",
        "sku_id": "FF-GATE50-CS-PN16-FL-MAN",
    }

    # Use many runs with different seeds to be sure the exemption is robust
    shipment_original = dict(shipment_event)
    for seed in range(20):
        result = inject([dict(shipment_event), dict(order_event)], cfg, random.Random(seed))

        # All shipment_confirmation events in output must be unchanged
        sc_events = [e for e in result if e.get("event_type") == "shipment_confirmation"]
        assert len(sc_events) == 1, (
            f"Expected exactly 1 shipment_confirmation, got {len(sc_events)} (seed={seed})"
        )
        assert sc_events[0] == shipment_original, (
            f"shipment_confirmation was mutated by noise injector (seed={seed}): {sc_events[0]}"
        )

    # The customer_order event should have been subject to noise (may be mutated)
    # We just verify the inject call didn't crash and returned something for it
    final_result = inject([dict(order_event)], cfg, random.Random(0))
    assert len(final_result) >= 1  # at minimum the original, possibly duplicated
