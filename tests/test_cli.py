"""Tests for the CLI entry point (Step 11).

Each test runs `python -m flowform.cli` as a subprocess with cwd=tmp_path so
that state/ and output/ are isolated, and a copy of config.yaml in the tmp
directory is found via the CWD fallback in _find_config().
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).parent.parent
_CONFIG_SRC = _PROJECT_ROOT / "config.yaml"


def _run(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    """Invoke the CLI as a subprocess with the given working directory."""
    return subprocess.run(
        [sys.executable, "-m", "flowform.cli"] + args,
        capture_output=True,
        text=True,
        cwd=cwd,
    )


def _setup_tmp(tmp_path: Path) -> Path:
    """Copy config.yaml into tmp_path so the CWD fallback finds it."""
    shutil.copy(_CONFIG_SRC, tmp_path / "config.yaml")
    return tmp_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_no_args_exits_nonzero(tmp_path: Path) -> None:
    """Invoking with no arguments should exit non-zero and print help."""
    _setup_tmp(tmp_path)
    result = _run([], tmp_path)
    assert result.returncode != 0
    # Should print something resembling usage
    output = result.stdout + result.stderr
    assert "usage" in output.lower() or "flowform" in output.lower()


def test_reset_exits_zero_and_prints_confirmation(tmp_path: Path) -> None:
    """--reset should exit 0 and print 'Reset complete'."""
    _setup_tmp(tmp_path)
    result = _run(["--reset"], tmp_path)
    assert result.returncode == 0, result.stderr
    assert "Reset complete" in result.stdout


def test_reset_creates_db(tmp_path: Path) -> None:
    """--reset should create state/simulation.db."""
    _setup_tmp(tmp_path)
    _run(["--reset"], tmp_path)
    assert (tmp_path / "state" / "simulation.db").exists()


def test_status_after_reset_shows_sim_day_zero(tmp_path: Path) -> None:
    """--status after --reset should show sim_day=0 and the start_date."""
    _setup_tmp(tmp_path)
    _run(["--reset"], tmp_path)
    result = _run(["--status"], tmp_path)
    assert result.returncode == 0, result.stderr
    assert "Sim day:       0" in result.stdout
    # start_date from config.yaml
    assert "Start date:" in result.stdout
    assert "Catalog SKUs:" in result.stdout
    assert "Customers:" in result.stdout


def test_days_after_reset_advances_sim_day(tmp_path: Path) -> None:
    """--days 3 after --reset should advance sim_day to 3."""
    _setup_tmp(tmp_path)
    _run(["--reset"], tmp_path)
    result = _run(["--days", "3"], tmp_path)
    assert result.returncode == 0, result.stderr
    assert "sim day 3" in result.stdout

    # Confirm --status also shows sim_day 3
    status = _run(["--status"], tmp_path)
    assert "Sim day:       3" in status.stdout


def test_days_without_reset_exits_one(tmp_path: Path) -> None:
    """--days without a prior --reset should exit 1 with a friendly message."""
    _setup_tmp(tmp_path)
    result = _run(["--days", "5"], tmp_path)
    assert result.returncode == 1
    assert "reset" in result.stdout.lower() or "reset" in result.stderr.lower()


def test_status_without_reset_exits_one(tmp_path: Path) -> None:
    """--status without a prior --reset should exit 1 with a friendly message."""
    _setup_tmp(tmp_path)
    result = _run(["--status"], tmp_path)
    assert result.returncode == 1
    assert "reset" in result.stdout.lower() or "reset" in result.stderr.lower()


def test_reset_and_days_mutually_exclusive(tmp_path: Path) -> None:
    """--reset --days 3 together should be rejected by argparse."""
    _setup_tmp(tmp_path)
    result = _run(["--reset", "--days", "3"], tmp_path)
    assert result.returncode != 0
    output = result.stdout + result.stderr
    assert "error" in output.lower() or "mutually exclusive" in output.lower()


def test_until_past_date_exits_one(tmp_path: Path) -> None:
    """--until with a date <= current sim date should exit 1."""
    _setup_tmp(tmp_path)
    _run(["--reset"], tmp_path)

    # Read start_date from status to pick a date in the past
    status = _run(["--status"], tmp_path)
    # Extract start_date line, e.g. "  Start date:    2026-01-02"
    start_date_str = None
    for line in status.stdout.splitlines():
        if "Start date:" in line:
            start_date_str = line.split(":", 1)[1].strip()
            break

    assert start_date_str is not None, f"Could not find Start date in: {status.stdout}"

    # Use the start_date itself (equal, not strictly after) — should fail
    result = _run(["--until", start_date_str], tmp_path)
    assert result.returncode == 1
    assert "error" in result.stdout.lower() or "error" in result.stderr.lower()


def test_until_invalid_date_format_exits_one(tmp_path: Path) -> None:
    """--until with a bad date format should exit 1 with an error message."""
    _setup_tmp(tmp_path)
    _run(["--reset"], tmp_path)
    result = _run(["--until", "not-a-date"], tmp_path)
    assert result.returncode == 1
    assert "invalid" in result.stdout.lower() or "invalid" in result.stderr.lower()


def test_until_advances_sim_correctly(tmp_path: Path) -> None:
    """--until should advance sim_day and report days simulated."""
    _setup_tmp(tmp_path)
    _run(["--reset"], tmp_path)

    # Simulate 5 days first
    _run(["--days", "5"], tmp_path)

    status_before = _run(["--status"], tmp_path)
    # Extract current date
    current_date_str = None
    for line in status_before.stdout.splitlines():
        if "Current date:" in line:
            current_date_str = line.split(":", 1)[1].strip()
            break
    assert current_date_str is not None

    from datetime import date, timedelta

    current = date.fromisoformat(current_date_str)
    # Pick a date 14 calendar days in the future — guarantees a few business days
    target = current + timedelta(days=14)
    result = _run(["--until", target.isoformat()], tmp_path)
    assert result.returncode == 0, result.stderr
    assert "Simulated" in result.stdout
    assert "sim day" in result.stdout


def test_reset_wipes_previous_state(tmp_path: Path) -> None:
    """A second --reset should restart sim_day from 0."""
    _setup_tmp(tmp_path)
    _run(["--reset"], tmp_path)
    _run(["--days", "5"], tmp_path)

    # Second reset
    _run(["--reset"], tmp_path)
    status = _run(["--status"], tmp_path)
    assert "Sim day:       0" in status.stdout


def test_reset_output_is_printed(tmp_path: Path) -> None:
    """--reset should print catalog size and customer count."""
    _setup_tmp(tmp_path)
    result = _run(["--reset"], tmp_path)
    assert "SKUs" in result.stdout
    assert "customers" in result.stdout


def test_main_module_alias(tmp_path: Path) -> None:
    """python -m flowform should work as an alias for python -m flowform.cli."""
    _setup_tmp(tmp_path)
    result = subprocess.run(
        [sys.executable, "-m", "flowform"],
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )
    # No args → non-zero + help output
    assert result.returncode != 0
    output = result.stdout + result.stderr
    assert "usage" in output.lower() or "flowform" in output.lower()
