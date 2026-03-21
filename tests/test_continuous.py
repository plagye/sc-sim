"""Tests for --continuous CLI mode (Step 52)."""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers (mirror pattern from test_cli.py)
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


def test_continuous_requires_reset_first(tmp_path: Path) -> None:
    """--continuous without prior --reset should exit non-zero and explain."""
    _setup_tmp(tmp_path)
    result = _run(["--continuous", "--days", "1", "--sleep", "0"], tmp_path)
    assert result.returncode != 0
    output = result.stdout + result.stderr
    assert "No simulation state" in output


def test_continuous_runs_n_days_then_exits(tmp_path: Path) -> None:
    """--continuous --days 3 should run 3 days and exit 0."""
    _setup_tmp(tmp_path)
    reset = _run(["--reset"], tmp_path)
    assert reset.returncode == 0, reset.stderr

    result = _run(["--continuous", "--days", "3", "--sleep", "0"], tmp_path)
    assert result.returncode == 0, result.stderr

    # Output directory should have at least 1 ERP date sub-dir
    erp_dir = tmp_path / "output" / "erp"
    assert erp_dir.exists()
    date_dirs = list(erp_dir.iterdir())
    assert len(date_dirs) >= 1


def test_continuous_status_line_printed_each_day(tmp_path: Path) -> None:
    """--continuous --days 2 should print 2 status lines and a shutdown message."""
    _setup_tmp(tmp_path)
    _run(["--reset"], tmp_path)

    result = _run(["--continuous", "--days", "2", "--sleep", "0"], tmp_path)
    assert result.returncode == 0, result.stderr

    # Count bracket status lines
    status_lines = re.findall(r"\[sim_day=", result.stdout)
    assert len(status_lines) == 2

    # Final line about shutdown
    assert "Shutting down cleanly after 2 days." in result.stdout


def test_sleep_default_is_one() -> None:
    """--continuous default sleep should be 1.0 second."""
    from flowform.cli import _build_parser

    parser = _build_parser()
    args = parser.parse_args(["--continuous"])
    assert args.sleep == 1.0


def test_sleep_zero_accepted() -> None:
    """--continuous --sleep 0 should parse sleep as 0.0."""
    from flowform.cli import _build_parser

    parser = _build_parser()
    args = parser.parse_args(["--continuous", "--sleep", "0"])
    assert args.sleep == 0.0


def test_continuous_and_days_not_mutex() -> None:
    """--continuous --days N should parse without error (not in same exclusive group)."""
    from flowform.cli import _build_parser

    parser = _build_parser()
    args = parser.parse_args(["--continuous", "--days", "5", "--sleep", "0"])
    assert args.continuous is True
    assert args.days == 5


def test_continuous_resumes_from_correct_sim_day(tmp_path: Path) -> None:
    """--continuous resumes from persisted state; a second run continues from where first stopped.

    Sequence:
      1. --reset          → sim_day=0
      2. --continuous --days 2 --sleep 0  → sim_day=2
      3. --continuous --days 1 --sleep 0  → sim_day=3  (NOT sim_day=1)
      4. --status         → assert "Sim day:       3"
    """
    _setup_tmp(tmp_path)

    # Step 1: reset
    result = _run(["--reset"], tmp_path)
    assert result.returncode == 0, result.stderr

    # Step 2: run 2 days
    result = _run(["--continuous", "--days", "2", "--sleep", "0"], tmp_path)
    assert result.returncode == 0, result.stderr

    # Step 3: run 1 more day
    result = _run(["--continuous", "--days", "1", "--sleep", "0"], tmp_path)
    assert result.returncode == 0, result.stderr

    # Step 4: check status — sim_day should be 3
    result = _run(["--status"], tmp_path)
    assert result.returncode == 0, result.stderr
    assert "Sim day:       3" in result.stdout, (
        f"Expected 'Sim day:       3' in status output, got:\n{result.stdout}"
    )
