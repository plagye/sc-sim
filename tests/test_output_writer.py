"""Tests for the JSON output writer (Step 12).

Coverage:
1.  ERP event routes to correct path
2.  All ERP event types route to the correct files
3.  TMS load event with sync_window="08" routes to loads_08.json
4.  TMS load events are split across three sync files correctly
5.  TMS batch events (carrier_event, return_event) route correctly
6.  ADM daily events route correctly
7.  Monthly event types route correctly (demand_plan, inventory_target, demand_forecast)
8.  Empty event list → no files written, returns empty list
9.  Events of unknown type → raise ValueError with the unknown type name
10. Multiple events of same type → all end up in the same file as a JSON array
11. Written files are valid JSON and parse back to lists
12. write_events returns the list of paths actually written
13. Directories are created automatically (no pre-existing output dir)
14. is_monthly_event_type returns True for monthly types, False for daily
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from flowform.output.writer import (
    ERP_EVENT_TYPES,
    ADM_MONTHLY_EVENT_TYPES,
    FORECAST_MONTHLY_EVENT_TYPES,
    is_monthly_event_type,
    write_events,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SIM_DATE = date(2026, 3, 5)
_DATE_STR = "2026-03-05"


def _erp(event_type: str, **kwargs) -> dict:
    return {"event_type": event_type, "sim_date": _DATE_STR, **kwargs}


def _load(sync_window: str, **kwargs) -> dict:
    return {"event_type": "load_event", "sync_window": sync_window, "sim_date": _DATE_STR, **kwargs}


def _tms_batch(event_type: str, **kwargs) -> dict:
    return {"event_type": event_type, "sim_date": _DATE_STR, **kwargs}


def _adm(event_type: str, **kwargs) -> dict:
    return {"event_type": event_type, "sim_date": _DATE_STR, **kwargs}


# ---------------------------------------------------------------------------
# Test 1: ERP event routes to correct path
# ---------------------------------------------------------------------------


def test_erp_event_routes_to_correct_path(tmp_path: Path) -> None:
    event = _erp("production_completion", event_id="e001")
    written = write_events([event], _SIM_DATE, output_dir=tmp_path)

    assert len(written) == 1
    expected = tmp_path / "erp" / _DATE_STR / "production_completions.json"
    assert written[0] == expected
    assert expected.exists()


# ---------------------------------------------------------------------------
# Test 2: All ERP event types route to correct files
# ---------------------------------------------------------------------------


def test_all_erp_event_types_route_correctly(tmp_path: Path) -> None:
    events = [_erp(et, event_id=f"e{i:03d}") for i, et in enumerate(ERP_EVENT_TYPES)]
    written = write_events(events, _SIM_DATE, output_dir=tmp_path)

    # Multiple event_type keys may share the same file stem (e.g. singular and
    # plural variants, credit_hold/credit_release → credit_events.json).
    # Assert that every unique stem gets its own file, and that every event_type
    # is served by at least one written file.
    unique_stems = set(ERP_EVENT_TYPES.values())
    assert len(written) == len(unique_stems), (
        f"Expected {len(unique_stems)} files (one per unique stem), got {len(written)}: "
        f"{[p.name for p in written]}"
    )
    written_names = {p.name for p in written}
    for et, stem in ERP_EVENT_TYPES.items():
        assert f"{stem}.json" in written_names, f"Missing file for event_type={et!r}"


# ---------------------------------------------------------------------------
# Test 3: TMS load event with sync_window="08" routes to loads_08.json
# ---------------------------------------------------------------------------


def test_tms_load_08_routes_correctly(tmp_path: Path) -> None:
    event = _load("08", load_id="L001")
    written = write_events([event], _SIM_DATE, output_dir=tmp_path)

    assert len(written) == 1
    expected = tmp_path / "tms" / _DATE_STR / "loads_08.json"
    assert written[0] == expected
    assert expected.exists()


# ---------------------------------------------------------------------------
# Test 4: TMS load events split across three sync files
# ---------------------------------------------------------------------------


def test_tms_load_events_split_by_sync_window(tmp_path: Path) -> None:
    events = [
        _load("08", load_id="L001"),
        _load("08", load_id="L002"),
        _load("14", load_id="L003"),
        _load("20", load_id="L004"),
        _load("20", load_id="L005"),
    ]
    written = write_events(events, _SIM_DATE, output_dir=tmp_path)

    tms_dir = tmp_path / "tms" / _DATE_STR
    loads_08 = json.loads((tms_dir / "loads_08.json").read_text())
    loads_14 = json.loads((tms_dir / "loads_14.json").read_text())
    loads_20 = json.loads((tms_dir / "loads_20.json").read_text())

    assert len(loads_08) == 2
    assert len(loads_14) == 1
    assert len(loads_20) == 2
    assert len(written) == 3  # three distinct sync files


# ---------------------------------------------------------------------------
# Test 5: TMS batch events route correctly
# ---------------------------------------------------------------------------


def test_tms_batch_events_route_correctly(tmp_path: Path) -> None:
    events = [
        _tms_batch("carrier_event", event_id="C001"),
        _tms_batch("return_event", event_id="R001"),
    ]
    written = write_events(events, _SIM_DATE, output_dir=tmp_path)

    tms_dir = tmp_path / "tms" / _DATE_STR
    assert (tms_dir / "carrier_events.json").exists()
    assert (tms_dir / "returns.json").exists()
    assert len(written) == 2


# ---------------------------------------------------------------------------
# Test 6: ADM daily events route correctly
# ---------------------------------------------------------------------------


def test_adm_daily_events_route_correctly(tmp_path: Path) -> None:
    event = _adm("demand_signal", signal_id="S001")
    written = write_events([event], _SIM_DATE, output_dir=tmp_path)

    expected = tmp_path / "adm" / _DATE_STR / "demand_signals.json"
    assert len(written) == 1
    assert written[0] == expected
    assert expected.exists()


# ---------------------------------------------------------------------------
# Test 7: Monthly event types route correctly
# ---------------------------------------------------------------------------


def test_adm_monthly_event_types_route_correctly(tmp_path: Path) -> None:
    events = [
        _adm("demand_plan", plan_id="DP001"),
        _adm("inventory_target", target_id="IT001"),
    ]
    written = write_events(events, _SIM_DATE, output_dir=tmp_path)

    adm_dir = tmp_path / "adm" / _DATE_STR
    assert (adm_dir / "demand_plans.json").exists()
    assert (adm_dir / "inventory_targets.json").exists()
    assert len(written) == 2


def test_forecast_monthly_event_type_routes_correctly(tmp_path: Path) -> None:
    event = {"event_type": "demand_forecast", "sim_date": _DATE_STR, "forecast_id": "FC001"}
    written = write_events([event], _SIM_DATE, output_dir=tmp_path)

    expected = tmp_path / "forecast" / _DATE_STR / "demand_forecasts.json"
    assert len(written) == 1
    assert written[0] == expected
    assert expected.exists()


# ---------------------------------------------------------------------------
# Test 8: Empty event list → no files written, returns empty list
# ---------------------------------------------------------------------------


def test_empty_event_list_writes_nothing(tmp_path: Path) -> None:
    written = write_events([], _SIM_DATE, output_dir=tmp_path)
    assert written == []
    # The output directory itself should not be created (nothing to write).
    assert not (tmp_path / "erp").exists()


# ---------------------------------------------------------------------------
# Test 9: Unknown event_type raises ValueError with the type name
# ---------------------------------------------------------------------------


def test_unknown_event_type_raises_value_error(tmp_path: Path) -> None:
    event = {"event_type": "totally_unknown_type", "sim_date": _DATE_STR}
    with pytest.raises(ValueError, match="totally_unknown_type"):
        write_events([event], _SIM_DATE, output_dir=tmp_path)


def test_load_event_with_invalid_sync_window_raises_value_error(tmp_path: Path) -> None:
    event = {"event_type": "load_event", "sync_window": "99", "sim_date": _DATE_STR}
    with pytest.raises(ValueError, match="99"):
        write_events([event], _SIM_DATE, output_dir=tmp_path)


def test_load_event_with_missing_sync_window_raises_value_error(tmp_path: Path) -> None:
    event = {"event_type": "load_event", "sim_date": _DATE_STR}
    with pytest.raises(ValueError):
        write_events([event], _SIM_DATE, output_dir=tmp_path)


# ---------------------------------------------------------------------------
# Test 10: Multiple events of same type end up in the same file
# ---------------------------------------------------------------------------


def test_multiple_events_same_type_in_one_file(tmp_path: Path) -> None:
    events = [
        _erp("customer_order", order_id="O001"),
        _erp("customer_order", order_id="O002"),
        _erp("customer_order", order_id="O003"),
    ]
    written = write_events(events, _SIM_DATE, output_dir=tmp_path)

    assert len(written) == 1
    data = json.loads((tmp_path / "erp" / _DATE_STR / "customer_orders.json").read_text())
    assert isinstance(data, list)
    assert len(data) == 3
    order_ids = {e["order_id"] for e in data}
    assert order_ids == {"O001", "O002", "O003"}


# ---------------------------------------------------------------------------
# Test 11: Written files are valid JSON and parse back to lists
# ---------------------------------------------------------------------------


def test_written_files_are_valid_json_lists(tmp_path: Path) -> None:
    events = [
        _erp("exchange_rate", EUR=4.30, USD=4.05),
        _tms_batch("carrier_event", alert="capacity_warning"),
        _load("14", load_id="L999"),
    ]
    write_events(events, _SIM_DATE, output_dir=tmp_path)

    for path in tmp_path.rglob("*.json"):
        content = path.read_text(encoding="utf-8")
        parsed = json.loads(content)
        assert isinstance(parsed, list), f"{path} did not parse to a list"
        assert len(parsed) >= 1


# ---------------------------------------------------------------------------
# Test 12: write_events returns list of paths actually written
# ---------------------------------------------------------------------------


def test_write_events_returns_written_paths(tmp_path: Path) -> None:
    events = [
        _erp("production_completion", batch_id="B001"),
        _erp("inventory_movement", movement_id="M001"),
        _load("08", load_id="L001"),
        _load("20", load_id="L002"),
    ]
    written = write_events(events, _SIM_DATE, output_dir=tmp_path)

    # All returned paths must exist on disk.
    for p in written:
        assert p.exists(), f"Returned path does not exist: {p}"

    # Correct count: 2 ERP files + 2 TMS load files (08 and 20).
    assert len(written) == 4


# ---------------------------------------------------------------------------
# Test 13: Directories created automatically
# ---------------------------------------------------------------------------


def test_directories_created_automatically(tmp_path: Path) -> None:
    nested_output = tmp_path / "deep" / "nested" / "output"
    # The directory does NOT exist yet.
    assert not nested_output.exists()

    event = _erp("production_completion", batch_id="B001")
    written = write_events([event], _SIM_DATE, output_dir=nested_output)

    assert len(written) == 1
    assert written[0].exists()


# ---------------------------------------------------------------------------
# Test 14: is_monthly_event_type
# ---------------------------------------------------------------------------


def test_is_monthly_event_type_true_for_monthly_types() -> None:
    monthly_types = list(ADM_MONTHLY_EVENT_TYPES) + list(FORECAST_MONTHLY_EVENT_TYPES)
    for et in monthly_types:
        assert is_monthly_event_type(et) is True, f"Expected True for {et!r}"


def test_is_monthly_event_type_false_for_daily_types() -> None:
    daily_types = (
        list(ERP_EVENT_TYPES)
        + ["load_event", "carrier_event", "return_event", "demand_signal"]
    )
    for et in daily_types:
        assert is_monthly_event_type(et) is False, f"Expected False for {et!r}"


def test_is_monthly_event_type_false_for_unknown() -> None:
    assert is_monthly_event_type("completely_unknown") is False


# ---------------------------------------------------------------------------
# Additional edge-case tests
# ---------------------------------------------------------------------------


def test_mixed_event_types_write_to_correct_systems(tmp_path: Path) -> None:
    """One event of each system type — verify all four system dirs are created."""
    events = [
        _erp("exchange_rate", EUR=4.30),
        _load("08", load_id="L1"),
        _adm("demand_signal", id="S1"),
        {"event_type": "demand_forecast", "sim_date": _DATE_STR},
    ]
    written = write_events(events, _SIM_DATE, output_dir=tmp_path)

    systems = {p.parent.parent.name for p in written}
    assert systems == {"erp", "tms", "adm", "forecast"}


def test_json_indent_and_encoding(tmp_path: Path) -> None:
    """Files should be human-readable (indented) and UTF-8 encoded."""
    event = _erp("customer_order", company="\u00dcber GmbH")
    write_events([event], _SIM_DATE, output_dir=tmp_path)

    raw = (tmp_path / "erp" / _DATE_STR / "customer_orders.json").read_text(encoding="utf-8")
    # Indented: first character of the array body should be whitespace.
    assert "  " in raw  # 2-space indent
    # Non-ASCII character preserved (ensure_ascii=False).
    assert "\u00dc" in raw


def test_date_objects_serialised_as_strings(tmp_path: Path) -> None:
    """date/datetime objects in event dicts should serialise without error."""
    from datetime import datetime

    event = {
        "event_type": "exchange_rate",
        "sim_date": _DATE_STR,
        "as_of_date": date(2026, 3, 5),
        "recorded_at": datetime(2026, 3, 5, 17, 0, 0),
    }
    write_events([event], _SIM_DATE, output_dir=tmp_path)

    data = json.loads(
        (tmp_path / "erp" / _DATE_STR / "exchange_rates.json").read_text()
    )
    assert data[0]["as_of_date"] == "2026-03-05"
    assert "2026-03-05" in data[0]["recorded_at"]


# ---------------------------------------------------------------------------
# Pre-Fix 8: backorder and backorder_cancellation event routing
# ---------------------------------------------------------------------------


def test_backorder_event_routes_to_erp_file(tmp_path: Path) -> None:
    """A 'backorder' event_type must route to ERP/backorder_events_{date}.json."""
    event = _erp("backorder", order_id="O001", order_line_id="L001", sku="FF-GA15-CS-PN16-FL-MN")
    written = write_events([event], _SIM_DATE, output_dir=tmp_path)

    expected = tmp_path / "erp" / _DATE_STR / "backorder_events.json"
    assert len(written) == 1
    assert written[0] == expected
    assert expected.exists()

    data = json.loads(expected.read_text())
    assert isinstance(data, list)
    assert len(data) == 1


def test_backorder_cancellation_event_routes_to_erp_file(tmp_path: Path) -> None:
    """A 'backorder_cancellation' event_type must route to ERP/backorder_cancellation_events_{date}.json."""
    event = _erp("backorder_cancellation", order_id="O002", order_line_id="L002")
    written = write_events([event], _SIM_DATE, output_dir=tmp_path)

    expected = tmp_path / "erp" / _DATE_STR / "backorder_cancellation_events.json"
    assert len(written) == 1
    assert written[0] == expected
    assert expected.exists()


def test_zero_backorder_events_writes_no_file(tmp_path: Path) -> None:
    """When no backorder events are present, no backorder file should be written."""
    events = [_erp("exchange_rate", EUR=4.30)]
    write_events(events, _SIM_DATE, output_dir=tmp_path)

    erp_dir = tmp_path / "erp" / _DATE_STR
    if erp_dir.exists():
        files = list(erp_dir.iterdir())
        file_names = {f.name for f in files}
        assert "backorder_events.json" not in file_names, (
            "backorder_events.json should not exist when no backorder events were passed"
        )
