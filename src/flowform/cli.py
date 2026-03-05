"""CLI entry point for the FlowForm Industries supply chain simulation engine.

Usage:
    python -m flowform.cli --reset
    python -m flowform.cli --days N
    python -m flowform.cli --until YYYY-MM-DD
    python -m flowform.cli --status
"""

from __future__ import annotations

import argparse
import shutil
import sys
from datetime import date
from pathlib import Path

# ---------------------------------------------------------------------------
# Config path resolution
# ---------------------------------------------------------------------------

def _find_config() -> Path:
    """Return the config.yaml path.

    Tries the project root (two levels up from src/flowform/) first, then
    falls back to the CWD.  Tests run with cwd=tmp_path and copy config.yaml
    there, so the CWD fallback makes isolation work naturally.
    """
    # src/flowform/cli.py → src/flowform → src → project_root
    project_root = Path(__file__).parent.parent.parent / "config.yaml"
    if project_root.exists():
        return project_root
    cwd_config = Path("config.yaml")
    return cwd_config  # may or may not exist; load_config will raise if not


# ---------------------------------------------------------------------------
# Day loop stub (Phase 2 will wire real engines here)
# ---------------------------------------------------------------------------

def _run_day_loop(state: object, config: object, sim_date: date) -> None:  # type: ignore[type-arg]
    """Stub day loop.  Replaced in Phase 2 with real engine calls."""
    # Access sim_day from state dynamically to keep import clean
    print(f"  Day {state.sim_day}: {sim_date}")  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Command implementations
# ---------------------------------------------------------------------------

def _cmd_reset(config: object) -> None:  # type: ignore[type-arg]
    """--reset: wipe state + output, generate fresh master data."""
    from flowform.state import SimulationState

    db_path = Path("state/simulation.db")
    output_dir = Path("output")

    # Wipe existing state
    if db_path.exists():
        db_path.unlink()
    if output_dir.exists():
        shutil.rmtree(output_dir)

    state = SimulationState.from_new(config)  # type: ignore[arg-type]
    print(
        f"Reset complete. Simulation starts at {state.start_date}. "
        f"Catalog: {len(state.catalog)} SKUs, {len(state.customers)} customers."
    )


def _cmd_status() -> None:
    """--status: print current simulation summary."""
    from flowform.config import load_config
    from flowform.state import SimulationState

    config = load_config(_find_config())
    try:
        state = SimulationState.from_db(config)
    except FileNotFoundError:
        print("No simulation state found. Run with --reset to initialise.")
        sys.exit(1)

    print("FlowForm Industries Simulation Status")
    print(f"  Sim day:       {state.sim_day}")
    print(f"  Current date:  {state.current_date}")
    print(f"  Start date:    {state.start_date}")
    print(f"  Catalog SKUs:  {len(state.catalog)}")
    print(f"  Customers:     {len(state.customers)}")
    print(f"  Open orders:   {len(state.open_orders)}")
    print(f"  Active loads:  {len(state.active_loads)}")
    print(f"  Backorders:    {len(state.backorder_queue)}")


def _simulate_days(config: object, n_days: int) -> None:  # type: ignore[type-arg]
    """Shared helper: simulate n_days business days and persist state."""
    from flowform.calendar import next_business_day
    from flowform.state import SimulationState

    try:
        state = SimulationState.from_db(config)  # type: ignore[arg-type]
    except FileNotFoundError:
        print("No simulation state found. Run with --reset to initialise.")
        sys.exit(1)

    for _ in range(n_days):
        next_date = next_business_day(state.current_date)
        state.advance_day(next_date)
        _run_day_loop(state, config, next_date)
        state.save()

    print(
        f"Simulated {n_days} business days. "
        f"Current date: {state.current_date} (sim day {state.sim_day})."
    )


def _cmd_days(config: object, n: int) -> None:  # type: ignore[type-arg]
    """--days N: simulate N business days."""
    _simulate_days(config, n)


def _cmd_until(config: object, target: date) -> None:  # type: ignore[type-arg]
    """--until DATE: simulate up to and including target date."""
    from flowform.calendar import next_business_day
    from flowform.state import SimulationState

    try:
        state = SimulationState.from_db(config)  # type: ignore[arg-type]
    except FileNotFoundError:
        print("No simulation state found. Run with --reset to initialise.")
        sys.exit(1)

    if target <= state.current_date:
        print(
            f"Error: --until date {target} is not after the current sim date "
            f"{state.current_date}. Choose a future date."
        )
        sys.exit(1)

    days_simulated = 0
    while state.current_date < target:
        next_date = next_business_day(state.current_date)
        if next_date > target:
            break
        state.advance_day(next_date)
        _run_day_loop(state, config, next_date)
        state.save()
        days_simulated += 1

    print(
        f"Simulated {days_simulated} business days. "
        f"Current date: {state.current_date} (sim day {state.sim_day})."
    )


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m flowform.cli",
        description="FlowForm Industries supply chain simulation engine.",
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--reset",
        action="store_true",
        help="Wipe state and output, generate fresh master data and initial state.",
    )
    group.add_argument(
        "--days",
        type=int,
        metavar="N",
        help="Simulate N business days from the current simulation date.",
    )
    group.add_argument(
        "--until",
        type=str,
        metavar="YYYY-MM-DD",
        help="Simulate up to and including this date.",
    )
    group.add_argument(
        "--status",
        action="store_true",
        help="Print current simulation status without simulating.",
    )

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    """Main CLI entry point."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    # No flags → print help and exit non-zero
    if not (args.reset or args.days is not None or args.until or args.status):
        parser.print_help()
        sys.exit(1)

    if args.status:
        _cmd_status()
        return

    # Load config for commands that need to run/init the simulation
    from flowform.config import load_config

    config_path = _find_config()
    try:
        config = load_config(config_path)
    except FileNotFoundError:
        print(f"Error: config file not found at {config_path.resolve()}.")
        sys.exit(1)

    if args.reset:
        _cmd_reset(config)
        return

    if args.days is not None:
        if args.days < 1:
            print("Error: --days must be a positive integer.")
            sys.exit(1)
        _cmd_days(config, args.days)
        return

    if args.until:
        try:
            target = date.fromisoformat(args.until)
        except ValueError:
            print(f"Error: invalid date format '{args.until}'. Use YYYY-MM-DD.")
            sys.exit(1)
        _cmd_until(config, target)
        return


if __name__ == "__main__":
    main()
