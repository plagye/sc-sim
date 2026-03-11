"""Integration tests for the FlowForm CLI and writer pipeline (Step 13).

All tests run ``python -m flowform.cli`` as a subprocess with cwd=tmp_path so
that ``state/`` and ``output/`` are fully isolated from the real project and
from each other.  A module-scoped fixture performs a single ``--reset`` that
many read-only tests share; tests that mutate simulation state (``--days``,
``--until``, second ``--reset``) each get their own independent tmp directory.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).parent.parent
_CONFIG_SRC = _PROJECT_ROOT / "config.yaml"

# ---------------------------------------------------------------------------
# Low-level subprocess helper
# ---------------------------------------------------------------------------


def _run(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    """Invoke ``python -m flowform.cli`` with the given args and working dir."""
    return subprocess.run(
        [sys.executable, "-m", "flowform.cli"] + args,
        capture_output=True,
        text=True,
        cwd=cwd,
    )


def _setup_tmp(tmp: Path) -> Path:
    """Copy config.yaml into *tmp* so the CWD fallback in cli._find_config() works."""
    shutil.copy(_CONFIG_SRC, tmp / "config.yaml")
    return tmp


# ---------------------------------------------------------------------------
# Output-parsing helpers
# ---------------------------------------------------------------------------


def _extract_sim_day(status_output: str) -> int:
    """Parse the ``Sim day:`` line from ``--status`` output.

    Example line::

        Sim day:       0

    Returns the integer value.
    """
    m = re.search(r"Sim day:\s+(\d+)", status_output)
    if m is None:
        raise ValueError(f"Could not find 'Sim day:' in output:\n{status_output}")
    return int(m.group(1))


def _extract_current_date(status_output: str) -> date:
    """Parse the ``Current date:`` line from ``--status`` output.

    Returns a :class:`datetime.date` object.
    """
    m = re.search(r"Current date:\s+(\d{4}-\d{2}-\d{2})", status_output)
    if m is None:
        raise ValueError(f"Could not find 'Current date:' in output:\n{status_output}")
    return date.fromisoformat(m.group(1))


def _extract_sku_count(output: str) -> int:
    """Extract the SKU count from ``--reset`` or ``--status`` output.

    Handles two formats:
    - ``--reset``: "Catalog: 2500 SKUs"
    - ``--status``: "Catalog SKUs:  2500"
    """
    # --reset format: "Catalog: 2500 SKUs"
    m = re.search(r"Catalog:\s+(\d+)\s+SKUs", output)
    if m:
        return int(m.group(1))
    # --status format: "Catalog SKUs:  2500"
    m = re.search(r"Catalog SKUs:\s+(\d+)", output)
    if m:
        return int(m.group(1))
    raise ValueError(f"Could not find SKU count in output:\n{output}")


def _extract_customer_count(output: str) -> int:
    """Extract the customer count from ``--reset`` or ``--status`` output.

    Handles two formats:
    - ``--reset``: "60 customers"
    - ``--status``: "Customers:     60"
    """
    # --reset format: "60 customers"
    m = re.search(r"(\d+)\s+customers", output)
    if m:
        return int(m.group(1))
    # --status format: "Customers:     60"
    m = re.search(r"Customers:\s+(\d+)", output)
    if m:
        return int(m.group(1))
    raise ValueError(f"Could not find customer count in output:\n{output}")


# ---------------------------------------------------------------------------
# Module-scoped fixture — single --reset shared by read-only tests
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def reset_env(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, str]:
    """Create an isolated tmp dir, copy config.yaml, run --reset, return (tmp, stdout)."""
    tmp = tmp_path_factory.mktemp("integration_shared")
    _setup_tmp(tmp)
    result = _run(["--reset"], tmp)
    assert result.returncode == 0, (
        f"--reset failed in shared fixture.\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    return tmp, result.stdout


# ---------------------------------------------------------------------------
# Group 1: --reset produces correct master data
# ---------------------------------------------------------------------------


class TestResetMasterData:
    def test_reset_creates_db(self, reset_env: tuple[Path, str]) -> None:
        """After ``--reset``, ``state/simulation.db`` exists."""
        tmp, _ = reset_env
        assert (tmp / "state" / "simulation.db").exists()

    def test_reset_output_is_clean(self, tmp_path: Path) -> None:
        """Second ``--reset`` wipes and recreates state; output/ absent or empty."""
        _setup_tmp(tmp_path)
        # First reset + simulate a day so output/ has something
        _run(["--reset"], tmp_path)
        _run(["--days", "1"], tmp_path)

        # Second reset
        result = _run(["--reset"], tmp_path)
        assert result.returncode == 0, result.stderr

        # DB should still exist (freshly written)
        assert (tmp_path / "state" / "simulation.db").exists()

        # output/ should not exist (wiped) or contain only master data files.
        # --reset writes static reference files to output/master/, so that
        # directory is expected to exist with exactly 6 JSON files.
        output_dir = tmp_path / "output"
        if output_dir.exists():
            all_files = list(output_dir.rglob("*"))
            json_files = [f for f in all_files if f.is_file()]
            # Only master/*.json are permitted; no simulation-day files
            non_master = [f for f in json_files if "master" not in f.parts]
            assert non_master == [], (
                f"output/ has unexpected non-master files after second --reset: {non_master}"
            )
            master_files = [f for f in json_files if "master" in f.parts]
            assert len(master_files) == 6, (
                f"Expected 6 master files after --reset, got {len(master_files)}: {master_files}"
            )

    def test_reset_prints_catalog_count(self, reset_env: tuple[Path, str]) -> None:
        """``--reset`` stdout contains 'SKUs' and a number >= 2000."""
        _, stdout = reset_env
        assert "SKUs" in stdout
        sku_count = _extract_sku_count(stdout)
        assert sku_count >= 2000, f"Expected >= 2000 SKUs, got {sku_count}"

    def test_reset_prints_customer_count(self, reset_env: tuple[Path, str]) -> None:
        """``--reset`` stdout contains 'customers' and the number 60."""
        _, stdout = reset_env
        assert "customers" in stdout
        customer_count = _extract_customer_count(stdout)
        assert customer_count == 60, f"Expected 60 customers, got {customer_count}"

    def test_reset_sim_day_zero(self, reset_env: tuple[Path, str]) -> None:
        """After ``--reset``, ``--status`` shows ``Sim day: 0``."""
        tmp, _ = reset_env
        status = _run(["--status"], tmp)
        assert status.returncode == 0, status.stderr
        assert _extract_sim_day(status.stdout) == 0

    def test_reset_start_date_matches_config(self, reset_env: tuple[Path, str]) -> None:
        """After ``--reset``, ``--status`` shows the start_date from config.yaml."""
        tmp, _ = reset_env
        # Read expected start_date from config.yaml
        config_path = tmp / "config.yaml"
        config_text = config_path.read_text()
        m = re.search(r'start_date:\s+"?(\d{4}-\d{2}-\d{2})"?', config_text)
        assert m is not None, "Could not find start_date in config.yaml"
        expected_start = m.group(1)

        status = _run(["--status"], tmp)
        assert expected_start in status.stdout, (
            f"Expected start date {expected_start!r} not found in status output:\n{status.stdout}"
        )


# ---------------------------------------------------------------------------
# Group 2: --status shows correct initial state
# ---------------------------------------------------------------------------


class TestStatusInitialState:
    def test_status_after_reset_exit_zero(self, reset_env: tuple[Path, str]) -> None:
        """``--status`` exits 0 after a ``--reset``."""
        tmp, _ = reset_env
        result = _run(["--status"], tmp)
        assert result.returncode == 0, result.stderr

    def test_status_shows_zero_open_orders(self, reset_env: tuple[Path, str]) -> None:
        """Initial ``open_orders`` count is 0."""
        tmp, _ = reset_env
        status = _run(["--status"], tmp)
        assert "Open orders:   0" in status.stdout, (
            f"Expected 'Open orders:   0' in:\n{status.stdout}"
        )

    def test_status_shows_zero_active_loads(self, reset_env: tuple[Path, str]) -> None:
        """Initial ``active_loads`` count is 0."""
        tmp, _ = reset_env
        status = _run(["--status"], tmp)
        assert "Active loads:  0" in status.stdout, (
            f"Expected 'Active loads:  0' in:\n{status.stdout}"
        )

    def test_status_shows_zero_backorders(self, reset_env: tuple[Path, str]) -> None:
        """Initial backorder queue length is 0."""
        tmp, _ = reset_env
        status = _run(["--status"], tmp)
        assert "Backorders:    0" in status.stdout, (
            f"Expected 'Backorders:    0' in:\n{status.stdout}"
        )

    def test_status_catalog_count_matches_reset_output(
        self, reset_env: tuple[Path, str]
    ) -> None:
        """SKU count in ``--status`` matches the count printed by ``--reset``."""
        tmp, reset_stdout = reset_env
        reset_sku_count = _extract_sku_count(reset_stdout)

        status = _run(["--status"], tmp)
        status_sku_count = _extract_sku_count(status.stdout)

        assert reset_sku_count == status_sku_count, (
            f"Reset printed {reset_sku_count} SKUs but status shows {status_sku_count}"
        )

    def test_status_customer_count(self, reset_env: tuple[Path, str]) -> None:
        """``--status`` shows 60 customers."""
        tmp, _ = reset_env
        status = _run(["--status"], tmp)
        count = _extract_customer_count(status.stdout)
        assert count == 60, f"Expected 60 customers, got {count}"


# ---------------------------------------------------------------------------
# Group 3: --days advances state correctly
# ---------------------------------------------------------------------------


class TestDaysAdvancement:
    def test_days_1_advances_sim_day(self, tmp_path: Path) -> None:
        """After ``--reset`` + ``--days 1``, ``--status`` shows ``Sim day: 1``."""
        _setup_tmp(tmp_path)
        _run(["--reset"], tmp_path)
        result = _run(["--days", "1"], tmp_path)
        assert result.returncode == 0, result.stderr

        status = _run(["--status"], tmp_path)
        assert _extract_sim_day(status.stdout) == 1

    def test_days_5_advances_sim_day(self, tmp_path: Path) -> None:
        """After ``--reset`` + ``--days 5``, ``--status`` shows ``Sim day: 5``."""
        _setup_tmp(tmp_path)
        _run(["--reset"], tmp_path)
        result = _run(["--days", "5"], tmp_path)
        assert result.returncode == 0, result.stderr

        status = _run(["--status"], tmp_path)
        assert _extract_sim_day(status.stdout) == 5

    def test_days_advances_calendar_day(self, tmp_path: Path) -> None:
        """After ``--reset`` + ``--days 1``, the current_date is exactly 1 day later.

        The simulation now advances every calendar day (including weekends and
        holidays).  Exchange rates and weekend EDI orders still run on non-business
        days, so the loop must advance 1 calendar day per ``--days 1`` call.
        """
        _setup_tmp(tmp_path)
        _run(["--reset"], tmp_path)

        status_before = _run(["--status"], tmp_path)
        start = _extract_current_date(status_before.stdout)

        _run(["--days", "1"], tmp_path)

        status_after = _run(["--status"], tmp_path)
        current = _extract_current_date(status_after.stdout)

        from datetime import timedelta
        expected = start + timedelta(days=1)
        assert current == expected, (
            f"Expected current_date to advance by exactly 1 calendar day: "
            f"{start} → {expected}, got {current}"
        )

    def test_days_is_cumulative(self, tmp_path: Path) -> None:
        """``--days 3`` then ``--days 3`` = sim_day 6 (state persists across calls)."""
        _setup_tmp(tmp_path)
        _run(["--reset"], tmp_path)
        r1 = _run(["--days", "3"], tmp_path)
        assert r1.returncode == 0, r1.stderr
        r2 = _run(["--days", "3"], tmp_path)
        assert r2.returncode == 0, r2.stderr

        status = _run(["--status"], tmp_path)
        assert _extract_sim_day(status.stdout) == 6

    def test_reset_resets_accumulated_days(self, tmp_path: Path) -> None:
        """``--days 5`` then ``--reset`` then ``--status`` shows ``Sim day: 0``."""
        _setup_tmp(tmp_path)
        _run(["--reset"], tmp_path)
        _run(["--days", "5"], tmp_path)

        # Second reset should zero everything
        r = _run(["--reset"], tmp_path)
        assert r.returncode == 0, r.stderr

        status = _run(["--status"], tmp_path)
        assert _extract_sim_day(status.stdout) == 0


# ---------------------------------------------------------------------------
# Group 4: --until
# ---------------------------------------------------------------------------


class TestUntil:
    def test_until_advances_to_date(self, tmp_path: Path) -> None:
        """``--until`` with a date ~10 business days ahead advances current_date to >= that date."""
        _setup_tmp(tmp_path)
        _run(["--reset"], tmp_path)

        # Read the start date
        status_before = _run(["--status"], tmp_path)
        current_before = _extract_current_date(status_before.stdout)

        # Pick a target ~14 calendar days ahead (guarantees >= 8 business days)
        target = current_before + timedelta(days=14)

        result = _run(["--until", target.isoformat()], tmp_path)
        assert result.returncode == 0, (
            f"--until failed.\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

        # current_date should be at or past start but <= target
        status_after = _run(["--status"], tmp_path)
        current_after = _extract_current_date(status_after.stdout)

        assert current_after > current_before, (
            f"Date did not advance: {current_before} → {current_after}"
        )
        assert current_after <= target, (
            f"Overshot the target: current={current_after}, target={target}"
        )

    def test_until_past_date_fails(self, tmp_path: Path) -> None:
        """``--until`` with a date before current_date → exit code 1."""
        _setup_tmp(tmp_path)
        _run(["--reset"], tmp_path)
        _run(["--days", "5"], tmp_path)

        # Get the current sim date
        status = _run(["--status"], tmp_path)
        current = _extract_current_date(status.stdout)

        # Pick a date strictly before the current sim date
        past = current - timedelta(days=1)
        result = _run(["--until", past.isoformat()], tmp_path)
        assert result.returncode == 1, (
            f"Expected exit code 1 for past date, got {result.returncode}.\n"
            f"stdout:\n{result.stdout}"
        )
        combined = result.stdout + result.stderr
        assert "error" in combined.lower(), (
            f"Expected an error message for past date:\n{combined}"
        )


# ---------------------------------------------------------------------------
# Group 5: write_events integration
# ---------------------------------------------------------------------------


class TestWriteEventsIntegration:
    """Direct-call tests for ``write_events`` — no subprocess, but using tmp_path
    for full isolation so no output files leak into the real project directory."""

    def test_write_events_erp_roundtrip(self, tmp_path: Path) -> None:
        """``write_events`` with an ERP event creates the expected file; JSON parses back."""
        from flowform.output.writer import write_events

        sim_date = date(2026, 1, 5)
        event = {
            "event_type": "customer_order",
            "order_id": "ORD-001",
            "customer_id": "CUST-001",
            "amount": 9999.99,
        }
        output_dir = tmp_path / "output"

        paths = write_events([event], sim_date, output_dir=output_dir)

        assert len(paths) == 1
        written_path = paths[0]
        assert written_path.exists()
        # Expected location: output/erp/2026-01-05/customer_orders.json
        assert written_path == output_dir / "erp" / "2026-01-05" / "customer_orders.json"

        loaded = json.loads(written_path.read_text(encoding="utf-8"))
        assert isinstance(loaded, list)
        assert len(loaded) == 1
        assert loaded[0]["order_id"] == "ORD-001"
        assert loaded[0]["event_type"] == "customer_order"

    def test_write_events_tms_loads_split(self, tmp_path: Path) -> None:
        """Three load events with different sync_windows write to three separate files."""
        from flowform.output.writer import write_events

        sim_date = date(2026, 1, 5)
        events = [
            {"event_type": "load_event", "sync_window": "08", "load_id": "LOAD-A"},
            {"event_type": "load_event", "sync_window": "14", "load_id": "LOAD-B"},
            {"event_type": "load_event", "sync_window": "20", "load_id": "LOAD-C"},
        ]
        output_dir = tmp_path / "output"

        paths = write_events(events, sim_date, output_dir=output_dir)

        assert len(paths) == 3, f"Expected 3 files, got {len(paths)}: {paths}"

        tms_dir = output_dir / "tms" / "2026-01-05"
        assert (tms_dir / "loads_08.json").exists()
        assert (tms_dir / "loads_14.json").exists()
        assert (tms_dir / "loads_20.json").exists()

        # Each file should contain exactly one event
        for stem, load_id in [("loads_08", "LOAD-A"), ("loads_14", "LOAD-B"), ("loads_20", "LOAD-C")]:
            loaded = json.loads((tms_dir / f"{stem}.json").read_text(encoding="utf-8"))
            assert len(loaded) == 1
            assert loaded[0]["load_id"] == load_id

    def test_write_events_returns_written_paths(self, tmp_path: Path) -> None:
        """``write_events`` return value is a list of Path objects that all exist."""
        from flowform.output.writer import write_events

        sim_date = date(2026, 1, 5)
        events = [
            {"event_type": "exchange_rate", "currency": "EUR", "rate": 4.30},
            {"event_type": "demand_signal", "signal_type": "external", "value": 1.05},
            {"event_type": "load_event", "sync_window": "14", "load_id": "LOAD-X"},
        ]
        output_dir = tmp_path / "output"

        paths = write_events(events, sim_date, output_dir=output_dir)

        # Must return a list (not None, not a generator)
        assert isinstance(paths, list)
        # All paths must be Path objects
        assert all(isinstance(p, Path) for p in paths), (
            f"Non-Path items in return value: {paths}"
        )
        # All paths must actually exist on disk
        for p in paths:
            assert p.exists(), f"Returned path does not exist: {p}"
        # Three distinct event types → three files
        assert len(paths) == 3
        # No duplicates
        assert len(set(paths)) == 3
