"""Noise injector: applies configurable data quality noise to serialised event dicts.

All 13 noise types are applied in order (duplicate insertion is last).
Each type uses an independent per-event Bernoulli trial at the profile rate.
All randomness is routed through the provided rng for deterministic output.
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from typing import Any

from flowform.config import Config

# ---------------------------------------------------------------------------
# Rate tables (per noise type, per profile)
# ---------------------------------------------------------------------------

RATES: dict[str, dict[str, float]] = {
    "light": {
        "duplicate": 0.002,
        "missing_field": 0.003,
        "wrong_timezone": 0.001,
        "sku_case_error": 0.000,
        "customer_id_mismatch": 0.000,
        "decimal_drift": 0.001,
        "future_dated": 0.000,
        "orphan_reference": 0.000,
        "encoding_issue": 0.000,
        "out_of_sequence": 0.000,
        "missing_weight_unit": 0.000,
        "negative_quantity_snapshot": 0.000,
        "duplicate_load_id": 0.000,
    },
    "medium": {
        "duplicate": 0.005,
        "missing_field": 0.010,
        "wrong_timezone": 0.005,
        "sku_case_error": 0.003,
        "customer_id_mismatch": 0.002,
        "decimal_drift": 0.005,
        "future_dated": 0.001,
        "orphan_reference": 0.001,
        "encoding_issue": 0.002,
        "out_of_sequence": 0.003,
        "missing_weight_unit": 0.005,
        "negative_quantity_snapshot": 0.001,
        "duplicate_load_id": 0.002,
    },
    "heavy": {
        "duplicate": 0.015,
        "missing_field": 0.030,
        "wrong_timezone": 0.020,
        "sku_case_error": 0.010,
        "customer_id_mismatch": 0.010,
        "decimal_drift": 0.015,
        "future_dated": 0.005,
        "orphan_reference": 0.005,
        "encoding_issue": 0.008,
        "out_of_sequence": 0.010,
        "missing_weight_unit": 0.020,
        "negative_quantity_snapshot": 0.003,
        "duplicate_load_id": 0.005,
    },
}

_NOISE_TYPES = [
    "missing_field",
    "wrong_timezone",
    "sku_case_error",
    "customer_id_mismatch",
    "decimal_drift",
    "future_dated",
    "orphan_reference",
    "encoding_issue",
    "out_of_sequence",
    "missing_weight_unit",
    "negative_quantity_snapshot",
    "duplicate_load_id",
    # duplicate is always last
    "duplicate",
]

_TIMEZONE_OFFSETS = ["+01:00", "+02:00", "-05:00", "-08:00"]
_ENCODING_CHARS = ["\u00e9", "\u00fc", "\u0142", "\ufffd"]
_DECIMAL_SUFFIXES = ("_pln", "_eur", "_usd")
_DECIMAL_FIELDS = {"unit_price", "payment_amount_pln"}
_CUSTOMER_ID_EVENT_TYPES = {"customer_order", "load_event", "payment"}


def _get_profile(config: Config) -> str:
    """Return profile string, unwrapping enum value if necessary."""
    profile = config.noise.profile
    return profile.value if hasattr(profile, "value") else str(profile)


# ---------------------------------------------------------------------------
# Individual noise mutators (operate in-place on the dict, return bool if triggered)
# ---------------------------------------------------------------------------


def _apply_missing_field(event: dict[str, Any], rng: random.Random) -> None:
    keys = [k for k in event if k != "event_type"]
    if len(keys) < 1:
        return
    key_to_remove = rng.choice(keys)
    del event[key_to_remove]


def _apply_wrong_timezone(event: dict[str, Any], rng: random.Random) -> None:
    ts = event.get("timestamp")
    if not isinstance(ts, str) or not ts.endswith("Z"):
        return
    replacement = rng.choice(_TIMEZONE_OFFSETS)
    event["timestamp"] = ts[:-1] + replacement


def _apply_sku_case_error(event: dict[str, Any], rng: random.Random) -> None:
    sku = event.get("sku_id")
    if not isinstance(sku, str):
        return
    choice = rng.randint(0, 2)
    if choice == 0:
        event["sku_id"] = sku.lower()
    elif choice == 1:
        event["sku_id"] = sku.upper()
    else:
        # Insert space before first '-'
        idx = sku.find("-")
        if idx >= 0:
            event["sku_id"] = sku[:idx] + " " + sku[idx:]
        else:
            event["sku_id"] = sku.lower()


def _apply_customer_id_mismatch(
    event: dict[str, Any],
    rng: random.Random,
    other_customer_ids: list[str],
) -> None:
    if event.get("event_type") not in _CUSTOMER_ID_EVENT_TYPES:
        return
    cid = event.get("customer_id")
    if not isinstance(cid, str):
        return
    # Find a different customer_id
    candidates = [c for c in other_customer_ids if c != cid]
    if not candidates:
        return
    event["customer_id"] = rng.choice(candidates)


def _apply_decimal_drift(event: dict[str, Any], rng: random.Random) -> None:
    for key, value in list(event.items()):
        if not isinstance(value, (int, float)):
            continue
        is_target = (
            any(key.endswith(s) for s in _DECIMAL_SUFFIXES) or key in _DECIMAL_FIELDS
        )
        if is_target:
            multiplier = rng.uniform(0.9999, 1.0001)
            places = rng.randint(0, 6)
            event[key] = round(float(value) * multiplier, places)


def _apply_future_dated(event: dict[str, Any], rng: random.Random) -> None:
    ts = event.get("timestamp")
    if not isinstance(ts, str) or "T" not in ts:
        return
    date_part, time_part = ts.split("T", 1)
    try:
        d = date.fromisoformat(date_part)
    except ValueError:
        return
    delta = rng.randint(1, 3)
    new_date = d + timedelta(days=delta)
    event["timestamp"] = f"{new_date.isoformat()}T{time_part}"


def _apply_orphan_reference(event: dict[str, Any], _rng: random.Random) -> None:
    if "order_id" in event:
        event["order_id"] = "ORD-9999-999999"


def _apply_encoding_issue(event: dict[str, Any], rng: random.Random) -> None:
    for field in ("customer_name", "message"):
        if field in event and isinstance(event[field], str):
            event[field] = event[field] + rng.choice(_ENCODING_CHARS)
            return  # Only one field per event


def _apply_out_of_sequence(event: dict[str, Any], rng: random.Random) -> None:
    ts = event.get("timestamp")
    if not isinstance(ts, str) or "T" not in ts:
        return
    date_part, time_part = ts.split("T", 1)
    try:
        d = date.fromisoformat(date_part)
    except ValueError:
        return
    delta = rng.randint(1, 2)
    new_date = d - timedelta(days=delta)
    event["timestamp"] = f"{new_date.isoformat()}T{time_part}"


def _apply_missing_weight_unit(event: dict[str, Any], _rng: random.Random) -> None:
    if event.get("event_type") != "load_event":
        return
    if "weight_unit" in event:
        del event["weight_unit"]


def _apply_negative_quantity_snapshot(
    event: dict[str, Any], _rng: random.Random
) -> None:
    if event.get("event_type") != "inventory_snapshot":
        return
    if "quantity_on_hand" in event:
        event["quantity_on_hand"] = -abs(event["quantity_on_hand"])


def _apply_duplicate_load_id(
    event: dict[str, Any],
    rng: random.Random,
    all_load_ids: list[str],
    current_load_id: str | None,
) -> None:
    if event.get("event_type") != "load_event":
        return
    if "load_id" not in event:
        return
    if len(all_load_ids) < 2:
        return
    candidates = [lid for lid in all_load_ids if lid != current_load_id]
    if not candidates:
        return
    event["load_id"] = rng.choice(candidates)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def inject(
    events: list[dict[str, Any]], config: Config, rng: random.Random
) -> list[dict[str, Any]]:
    """Apply all noise types in order to a list of serialised event dicts.

    Mutations are applied in-place on copies of each event dict. Duplicate
    insertion happens last so duplicates carry the already-mutated version.

    Args:
        events: Fully-serialised event dicts.
        config: Simulation config (provides noise.profile).
        rng: Seeded random instance for determinism.

    Returns:
        Mutated list of event dicts (may be longer due to duplicates).
    """
    if not events:
        return events

    profile = _get_profile(config)
    rates = RATES[profile]

    # Pre-collect lookup sets used by some noise types
    all_customer_ids: list[str] = [
        str(e["customer_id"])
        for e in events
        if "customer_id" in e and e.get("customer_id") is not None
    ]
    all_load_ids: list[str] = [
        str(e["load_id"])
        for e in events
        if e.get("event_type") == "load_event" and "load_id" in e
    ]

    # Work on shallow copies so originals aren't mutated
    result: list[dict[str, Any]] = [dict(e) for e in events]

    # Apply mutation passes (all except duplicate)
    mutation_passes = [n for n in _NOISE_TYPES if n != "duplicate"]

    for noise_type in mutation_passes:
        rate = rates[noise_type]
        if rate == 0.0:
            continue

        for event in result:
            if rng.random() >= rate:
                continue

            if noise_type == "missing_field":
                _apply_missing_field(event, rng)
            elif noise_type == "wrong_timezone":
                _apply_wrong_timezone(event, rng)
            elif noise_type == "sku_case_error":
                _apply_sku_case_error(event, rng)
            elif noise_type == "customer_id_mismatch":
                _apply_customer_id_mismatch(event, rng, all_customer_ids)
            elif noise_type == "decimal_drift":
                _apply_decimal_drift(event, rng)
            elif noise_type == "future_dated":
                _apply_future_dated(event, rng)
            elif noise_type == "orphan_reference":
                _apply_orphan_reference(event, rng)
            elif noise_type == "encoding_issue":
                _apply_encoding_issue(event, rng)
            elif noise_type == "out_of_sequence":
                _apply_out_of_sequence(event, rng)
            elif noise_type == "missing_weight_unit":
                _apply_missing_weight_unit(event, rng)
            elif noise_type == "negative_quantity_snapshot":
                _apply_negative_quantity_snapshot(event, rng)
            elif noise_type == "duplicate_load_id":
                current_lid = event.get("load_id")
                _apply_duplicate_load_id(event, rng, all_load_ids, str(current_lid) if current_lid is not None else None)

    # Duplicate pass — runs LAST so duplicates carry mutated versions
    dup_rate = rates["duplicate"]
    if dup_rate > 0.0:
        final: list[dict[str, Any]] = []
        for event in result:
            final.append(event)
            if rng.random() < dup_rate:
                final.append(dict(event))
        return final

    return result
