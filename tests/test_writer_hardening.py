"""Tests for Step 43: Multi-cadence file writer hardening.

Covers:
1.  demand_signal routes to per-event file adm/demand_signals/<signal_id>.json
2.  Per-event file is a single-item JSON array
3.  Multiple demand_signals produce multiple separate files
4.  demand_signal does NOT appear in adm/YYYY-MM-DD/ directory
5.  adm/demand_signals/ subfolder is created automatically
6.  Per-event file paths included in write_events return value
7.  Mixed day: demand_signals AND exchange_rates write to correct locations
8.  Existing daily batch file is appended to, not overwritten
9.  Existing TMS sync file is appended to, not overwritten
10. Missing signal_id uses fallback filename (no crash)
11. Per-event file content preserves all signal fields
12. is_monthly_event_type("demand_signal") still returns False
13. ADM_DAILY_EVENT_TYPES no longer contains "demand_signal"
14. ADM_PER_EVENT_TYPES contains "demand_signal"
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from flowform.output.writer import (
    ADM_DAILY_EVENT_TYPES,
    ADM_PER_EVENT_TYPES,
    is_monthly_event_type,
    write_events,
)

_SIM_DATE = date(2026, 3, 1)
_DATE_STR = "2026-03-01"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _signal(signal_id: str, **kwargs) -> dict:
    return {
        "event_type": "demand_signal",
        "signal_id": signal_id,
        "sim_date": _DATE_STR,
        **kwargs,
    }


def _exchange_rate(**kwargs) -> dict:
    return {"event_type": "exchange_rate", "sim_date": _DATE_STR, **kwargs}


def _load_event(sync_window: str, load_id: str) -> dict:
    return {
        "event_type": "load_event",
        "sync_window": sync_window,
        "load_id": load_id,
        "sim_date": _DATE_STR,
    }


# ---------------------------------------------------------------------------
# Test 1: demand_signal routes to per-event file
# ---------------------------------------------------------------------------


def test_demand_signal_routes_to_per_event_file(tmp_path: Path) -> None:
    event = _signal("SIG-20260301-0012")
    written = write_events([event], _SIM_DATE, output_dir=tmp_path)

    expected = tmp_path / "adm" / "demand_signals" / "SIG-20260301-0012.json"
    assert len(written) == 1
    assert written[0] == expected
    assert expected.exists()


# ---------------------------------------------------------------------------
# Test 2: Per-event file is a single-item JSON array
# ---------------------------------------------------------------------------


def test_demand_signal_per_event_file_is_single_item_array(tmp_path: Path) -> None:
    event = _signal("SIG-20260301-0012")
    write_events([event], _SIM_DATE, output_dir=tmp_path)

    path = tmp_path / "adm" / "demand_signals" / "SIG-20260301-0012.json"
    data = json.loads(path.read_text())
    assert isinstance(data, list)
    assert len(data) == 1


# ---------------------------------------------------------------------------
# Test 3: Multiple demand_signals produce multiple separate files
# ---------------------------------------------------------------------------


def test_multiple_demand_signals_produce_multiple_files(tmp_path: Path) -> None:
    events = [
        _signal("SIG-20260301-0001"),
        _signal("SIG-20260301-0002"),
        _signal("SIG-20260301-0003"),
    ]
    written = write_events(events, _SIM_DATE, output_dir=tmp_path)

    adm_dir = tmp_path / "adm" / "demand_signals"
    assert len(written) == 3
    for sig_id in ("SIG-20260301-0001", "SIG-20260301-0002", "SIG-20260301-0003"):
        assert (adm_dir / f"{sig_id}.json").exists()


# ---------------------------------------------------------------------------
# Test 4: demand_signal does NOT appear in adm/YYYY-MM-DD/ directory
# ---------------------------------------------------------------------------


def test_demand_signal_not_in_adm_daily_directory(tmp_path: Path) -> None:
    event = _signal("SIG-20260301-0001")
    write_events([event], _SIM_DATE, output_dir=tmp_path)

    adm_daily_dir = tmp_path / "adm" / _DATE_STR
    if adm_daily_dir.exists():
        file_names = {f.name for f in adm_daily_dir.iterdir()}
        assert "demand_signals.json" not in file_names


# ---------------------------------------------------------------------------
# Test 5: adm/demand_signals/ subfolder created automatically
# ---------------------------------------------------------------------------


def test_demand_signals_subfolder_created_automatically(tmp_path: Path) -> None:
    subfolder = tmp_path / "adm" / "demand_signals"
    assert not subfolder.exists()

    write_events([_signal("SIG-20260301-0001")], _SIM_DATE, output_dir=tmp_path)

    assert subfolder.exists()
    assert subfolder.is_dir()


# ---------------------------------------------------------------------------
# Test 6: Returned paths include per-event file paths
# ---------------------------------------------------------------------------


def test_write_events_returns_paths(tmp_path: Path) -> None:
    events = [
        _signal("SIG-20260301-0001"),
        _signal("SIG-20260301-0002"),
    ]
    written = write_events(events, _SIM_DATE, output_dir=tmp_path)

    for p in written:
        assert p.exists(), f"Returned path does not exist: {p}"
    assert len(written) == 2


# ---------------------------------------------------------------------------
# Test 7: Mixed day — demand_signals and exchange_rates in same call
# ---------------------------------------------------------------------------


def test_per_event_and_daily_mixed(tmp_path: Path) -> None:
    events = [
        _signal("SIG-20260301-0001"),
        _exchange_rate(eur_pln=4.30, usd_pln=4.05),
    ]
    written = write_events(events, _SIM_DATE, output_dir=tmp_path)

    # Per-event ADM file
    assert (tmp_path / "adm" / "demand_signals" / "SIG-20260301-0001.json").exists()
    # ERP daily batch file
    assert (tmp_path / "erp" / _DATE_STR / "exchange_rates.json").exists()
    assert len(written) == 2


# ---------------------------------------------------------------------------
# Test 8: Existing daily batch file is appended to, not overwritten
# ---------------------------------------------------------------------------


def test_existing_daily_batch_file_appended_not_overwritten(tmp_path: Path) -> None:
    event1 = _exchange_rate(eur_pln=4.30, usd_pln=4.05, seq=1)
    event2 = _exchange_rate(eur_pln=4.31, usd_pln=4.06, seq=2)

    write_events([event1], _SIM_DATE, output_dir=tmp_path)
    write_events([event2], _SIM_DATE, output_dir=tmp_path)

    path = tmp_path / "erp" / _DATE_STR / "exchange_rates.json"
    data = json.loads(path.read_text())
    assert isinstance(data, list)
    assert len(data) == 2
    seqs = {e["seq"] for e in data}
    assert seqs == {1, 2}


# ---------------------------------------------------------------------------
# Test 9: Existing TMS sync file is appended to, not overwritten
# ---------------------------------------------------------------------------


def test_existing_tms_sync_file_appended(tmp_path: Path) -> None:
    event1 = _load_event("08", "L001")
    event2 = _load_event("08", "L002")

    write_events([event1], _SIM_DATE, output_dir=tmp_path)
    write_events([event2], _SIM_DATE, output_dir=tmp_path)

    path = tmp_path / "tms" / _DATE_STR / "loads_08.json"
    data = json.loads(path.read_text())
    assert isinstance(data, list)
    assert len(data) == 2
    load_ids = {e["load_id"] for e in data}
    assert load_ids == {"L001", "L002"}


# ---------------------------------------------------------------------------
# Test 10: Missing signal_id uses fallback filename and does not crash
# ---------------------------------------------------------------------------


def test_missing_signal_id_uses_fallback_filename(tmp_path: Path) -> None:
    event = {"event_type": "demand_signal", "sim_date": _DATE_STR}
    # Should not raise
    written = write_events([event], _SIM_DATE, output_dir=tmp_path)

    assert len(written) == 1
    filename = written[0].name
    # Fallback name must be non-empty and end in .json
    assert filename.endswith(".json")
    assert len(filename) > len(".json")
    assert written[0].exists()


# ---------------------------------------------------------------------------
# Test 11: Per-event file content preserves all signal fields
# ---------------------------------------------------------------------------


def test_demand_signal_file_content_preserves_all_fields(tmp_path: Path) -> None:
    event = _signal(
        "SIG-20260301-0042",
        source="market_intelligence",
        signal_type="upside_risk",
        magnitude="high",
        confidence=0.75,
        horizon_weeks=8,
        product_group="FF-GA50",
    )
    write_events([event], _SIM_DATE, output_dir=tmp_path)

    path = tmp_path / "adm" / "demand_signals" / "SIG-20260301-0042.json"
    data = json.loads(path.read_text())
    record = data[0]

    assert record["signal_id"] == "SIG-20260301-0042"
    assert record["source"] == "market_intelligence"
    assert record["signal_type"] == "upside_risk"
    assert record["magnitude"] == "high"
    assert record["confidence"] == 0.75
    assert record["horizon_weeks"] == 8
    assert record["product_group"] == "FF-GA50"


# ---------------------------------------------------------------------------
# Test 12: is_monthly_event_type("demand_signal") still returns False
# ---------------------------------------------------------------------------


def test_is_monthly_event_type_unchanged_for_demand_signal() -> None:
    assert is_monthly_event_type("demand_signal") is False


# ---------------------------------------------------------------------------
# Test 13: ADM_DAILY_EVENT_TYPES no longer contains "demand_signal"
# ---------------------------------------------------------------------------


def test_adm_daily_event_types_no_longer_contains_demand_signal() -> None:
    assert "demand_signal" not in ADM_DAILY_EVENT_TYPES


# ---------------------------------------------------------------------------
# Test 14: ADM_PER_EVENT_TYPES contains "demand_signal"
# ---------------------------------------------------------------------------


def test_adm_per_event_types_contains_demand_signal() -> None:
    assert "demand_signal" in ADM_PER_EVENT_TYPES
