"""Tests for the POD (Proof of Delivery) engine (Step 26).

Covers:
T1:  No events on weekend
T2:  No events when pending_pod is empty
T3:  No event when pod_due_date is in the future
T4:  POD fires when pod_due_date == sim_date
T5:  POD fires when pod_due_date is in the past (overdue)
T6:  Load removed from pending_pod after POD fires
T7:  Load removed from active_loads after POD fires
T8:  Multiple loads — only due loads fire, future loads remain
T9:  POD event has correct load_id, carrier_code, and shipment_ids
T10: pending_pod survives save/load round-trip
T11: Integration — load_lifecycle delivery populates pending_pod
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from flowform.config import load_config
from flowform.engines import load_lifecycle, pod
from flowform.state import SimulationState

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"

_BIZ_DAY = date(2026, 3, 9)      # Monday — business day
_SATURDAY = date(2026, 3, 7)     # Saturday — weekend


@pytest.fixture()
def config():
    return load_config(_CONFIG_PATH)


@pytest.fixture()
def state(config, tmp_path):
    """Fresh simulation state isolated in a temp directory."""
    db = tmp_path / "sim.db"
    return SimulationState.from_new(config, db_path=db)


# ---------------------------------------------------------------------------
# Minimal pending_pod record factory
# ---------------------------------------------------------------------------

def _make_pod_record(
    pod_due_date: str, load_id: str = "LOAD-TEST-001"
) -> dict[str, Any]:
    """Return a minimal pending_pod record for use in tests."""
    return {
        "load_id": load_id,
        "carrier_code": "DHL",
        "source_warehouse_id": "W01",
        "shipment_ids": ["SHP-TEST-001"],
        "order_ids": ["ORD-TEST-001"],
        "total_weight_kg": 50.0,
        "weight_unit": "kg",
        "total_weight_reported": 50.0,
        "sync_window": "14",
        "priority": "standard",
        "customer_ids": ["CUST-0001"],
        "planned_date": "2026-03-05",
        "delivery_date": "2026-03-06",
        "pod_due_date": pod_due_date,
    }


# ---------------------------------------------------------------------------
# T1: No events on weekend
# ---------------------------------------------------------------------------


def test_no_events_on_weekend(state, config):
    """run() on a Saturday must return an empty list (business-day guard)."""
    state.pending_pod["LOAD-TEST-001"] = _make_pod_record(
        pod_due_date=_SATURDAY.isoformat()
    )
    events = pod.run(state, config, _SATURDAY)
    assert events == []


# ---------------------------------------------------------------------------
# T2: No events when pending_pod is empty
# ---------------------------------------------------------------------------


def test_no_events_when_pending_pod_empty(state, config):
    """run() with an empty pending_pod must return an empty list."""
    assert state.pending_pod == {}
    events = pod.run(state, config, _BIZ_DAY)
    assert events == []


# ---------------------------------------------------------------------------
# T3: No event when pod_due_date is in the future
# ---------------------------------------------------------------------------


def test_no_event_when_pod_due_in_future(state, config):
    """When pod_due_date is tomorrow, no POD event should fire today."""
    future_due = (_BIZ_DAY + timedelta(days=1)).isoformat()
    state.pending_pod["LOAD-TEST-001"] = _make_pod_record(pod_due_date=future_due)

    events = pod.run(state, config, _BIZ_DAY)

    assert events == []
    # Load must still be in pending_pod (not popped)
    assert "LOAD-TEST-001" in state.pending_pod


# ---------------------------------------------------------------------------
# T4: POD fires when pod_due_date == sim_date
# ---------------------------------------------------------------------------


def test_pod_fires_on_due_date(state, config):
    """When pod_due_date == sim_date, a pod_received event must be emitted."""
    state.pending_pod["LOAD-TEST-001"] = _make_pod_record(
        pod_due_date=_BIZ_DAY.isoformat()
    )

    events = pod.run(state, config, _BIZ_DAY)

    assert len(events) == 1
    ev = events[0]
    assert ev.event_subtype == "pod_received"
    assert ev.status == "pod_received"
    assert ev.event_type == "load_event"


# ---------------------------------------------------------------------------
# T5: POD fires when pod_due_date is in the past (overdue)
# ---------------------------------------------------------------------------


def test_pod_fires_when_overdue(state, config):
    """When pod_due_date is two days before sim_date, POD must still fire."""
    overdue = (_BIZ_DAY - timedelta(days=2)).isoformat()
    state.pending_pod["LOAD-TEST-001"] = _make_pod_record(pod_due_date=overdue)

    events = pod.run(state, config, _BIZ_DAY)

    assert len(events) == 1


# ---------------------------------------------------------------------------
# T6: Load removed from pending_pod after POD fires
# ---------------------------------------------------------------------------


def test_load_removed_from_pending_pod_after_pod(state, config):
    """After a POD fires, the load_id must not remain in state.pending_pod."""
    state.pending_pod["LOAD-TEST-001"] = _make_pod_record(
        pod_due_date=_BIZ_DAY.isoformat()
    )

    pod.run(state, config, _BIZ_DAY)

    assert "LOAD-TEST-001" not in state.pending_pod


# ---------------------------------------------------------------------------
# T7: Load removed from active_loads after POD fires
# ---------------------------------------------------------------------------


def test_load_removed_from_active_loads_after_pod(state, config):
    """After a POD fires, the load_id must not remain in state.active_loads."""
    state.pending_pod["LOAD-TEST-001"] = _make_pod_record(
        pod_due_date=_BIZ_DAY.isoformat()
    )
    # Insert a matching minimal active_loads record
    state.active_loads["LOAD-TEST-001"] = {
        "load_id": "LOAD-TEST-001",
        "status": "delivered",
        "carrier_code": "DHL",
    }

    pod.run(state, config, _BIZ_DAY)

    assert "LOAD-TEST-001" not in state.active_loads


# ---------------------------------------------------------------------------
# T8: Multiple loads — only due loads fire, future loads remain
# ---------------------------------------------------------------------------


def test_only_due_loads_fire(state, config):
    """Only loads whose pod_due_date <= sim_date must fire; future ones stay."""
    state.pending_pod["LOAD-DUE"] = _make_pod_record(
        load_id="LOAD-DUE",
        pod_due_date=_BIZ_DAY.isoformat(),
    )
    future_due = (_BIZ_DAY + timedelta(days=1)).isoformat()
    state.pending_pod["LOAD-FUTURE"] = _make_pod_record(
        load_id="LOAD-FUTURE",
        pod_due_date=future_due,
    )

    events = pod.run(state, config, _BIZ_DAY)

    assert len(events) == 1
    assert events[0].load_id == "LOAD-DUE"
    assert "LOAD-FUTURE" in state.pending_pod
    assert "LOAD-DUE" not in state.pending_pod


# ---------------------------------------------------------------------------
# T9: POD event has correct load_id, carrier_code, and shipment_ids
# ---------------------------------------------------------------------------


def test_pod_event_carries_correct_identifiers(state, config):
    """The emitted pod_received event must carry the correct load_id,
    carrier_code, and shipment_ids from the pending_pod record."""
    record = _make_pod_record(
        pod_due_date=_BIZ_DAY.isoformat(), load_id="LOAD-TEST-001"
    )
    record["carrier_code"] = "DHL"
    record["shipment_ids"] = ["SHP-TEST-001"]
    state.pending_pod["LOAD-TEST-001"] = record

    events = pod.run(state, config, _BIZ_DAY)

    assert len(events) == 1
    ev = events[0]
    assert ev.load_id == "LOAD-TEST-001"
    assert ev.carrier_code == "DHL"
    assert ev.shipment_ids == ["SHP-TEST-001"]


# ---------------------------------------------------------------------------
# T10: pending_pod survives save/load round-trip
# ---------------------------------------------------------------------------


def test_pending_pod_round_trips_through_sqlite(state, config):
    """pending_pod entries must survive a save() + from_db() cycle."""
    record = _make_pod_record(pod_due_date=_BIZ_DAY.isoformat())
    state.pending_pod["LOAD-TEST-001"] = record

    state.save()

    loaded = SimulationState.from_db(config, db_path=state._db_path)

    assert "LOAD-TEST-001" in loaded.pending_pod
    assert loaded.pending_pod["LOAD-TEST-001"]["pod_due_date"] == _BIZ_DAY.isoformat()


# ---------------------------------------------------------------------------
# T11 (bonus): Integration — load_lifecycle delivery populates pending_pod
# ---------------------------------------------------------------------------


def test_load_lifecycle_delivery_populates_pending_pod(state, config):
    """When load_lifecycle delivers a load, it must appear in state.pending_pod
    with a valid pod_due_date >= sim_date."""
    # Build a load that is in out_for_delivery and due for delivery today
    ofd_date = _BIZ_DAY - timedelta(days=2)
    load: dict[str, Any] = {
        "load_id": "LOAD-INTEG-001",
        "status": "out_for_delivery",
        "carrier_code": "RABEN",
        "source_warehouse_id": "W01",
        "planned_date": ofd_date.isoformat(),
        "estimated_departure": ofd_date.isoformat(),
        "estimated_arrival": ofd_date.isoformat(),
        "actual_departure": ofd_date.isoformat(),
        "actual_arrival": None,
        "delay_days": 0,
        "shipment_ids": ["SHP-INTEG-001"],
        "order_ids": [],
        "total_weight_kg": 75.0,
        "weight_unit": "kg",
        "total_weight_reported": 75.0,
        "sync_window": "08",
        "priority": "standard",
        "customer_ids": ["CUST-0001"],
        "events": [],
        "out_for_delivery_date": ofd_date.isoformat(),
        "delivery_window": 1,
    }
    state.active_loads["LOAD-INTEG-001"] = load

    _RELIABILITY_PATCH = "flowform.engines.load_lifecycle._carrier_reliability"

    with patch(_RELIABILITY_PATCH, return_value=1.0):
        load_lifecycle.run(state, config, _BIZ_DAY)

    # After delivery the load must be in pending_pod
    assert "LOAD-INTEG-001" in state.pending_pod

    pod_rec = state.pending_pod["LOAD-INTEG-001"]
    pod_due = date.fromisoformat(pod_rec["pod_due_date"])
    # pod_due_date must be strictly after sim_date (at least 1 biz day later)
    assert pod_due > _BIZ_DAY
