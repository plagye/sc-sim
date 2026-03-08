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
from datetime import date, timedelta
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
# Day loop — Steps 1–8 wired (Phase 2)
# ---------------------------------------------------------------------------

def _run_day_loop(
    state: object,
    config: object,
    sim_date: date,
    output_dir: Path | None = None,
) -> list:  # type: ignore[type-arg]
    """Run all wired simulation steps for *sim_date* and write output files.

    Steps implemented:
      2. Exchange rates  — runs every calendar day including weekends
      3. Production      — engine self-guards (business days only)
      5. Orders          — engine self-guards; weekend EDI handled internally
      6. Credit check    — engine self-guards (business days only)
      7. Modifications & cancellations — engine self-guards (business days only)
      8. Allocation with backorders    — engine self-guards (business days only)
      9. Load planning   — engine self-guards (business days, not holidays)
      + Inventory movements — after production (receipts) and after allocation (picks)

    Steps 1, 4, 12, 14–20 are not yet wired (see TODO comments).

    Args:
        state:      Mutable simulation state.
        config:     Validated simulation config.
        sim_date:   The simulated calendar date to process.
        output_dir: Override output root directory.  Defaults to ``Path("output")``
                    (relative to CWD).  Pass an absolute path in tests to avoid
                    writing to the real project directory.

    Returns:
        List of all raw event objects generated for this day (before serialisation).
    """
    from flowform.calendar import is_business_day
    from flowform.engines import (
        allocation,
        carrier_events,
        credit,
        exchange_rates,
        inventory_movements,
        load_lifecycle,
        load_planning,
        modifications,
        orders,
        pod,
        production,
        returns,
        transfers,
    )
    from flowform.output.writer import write_events

    all_events: list = []

    # Step 2: Exchange rates — every day including weekends
    all_events.extend(exchange_rates.run(state, config, sim_date))  # type: ignore[arg-type]

    # Step 3: Production (self-guards to business days)
    all_events.extend(production.run(state, config, sim_date))  # type: ignore[arg-type]

    # Step 4: Demand signals — TODO (Step 33)

    # Step 5: Orders (self-guards; weekend EDI for Distributors handled internally)
    all_events.extend(orders.run(state, config, sim_date))  # type: ignore[arg-type]

    # Step 6: Credit check (self-guards to business days)
    all_events.extend(credit.run(state, config, sim_date))  # type: ignore[arg-type]

    # Step 7: Modifications & cancellations (self-guards to business days)
    all_events.extend(modifications.run(state, config, sim_date))  # type: ignore[arg-type]

    # Step 8: Allocation with backorders (self-guards to business days)
    all_events.extend(allocation.run(state, config, sim_date))  # type: ignore[arg-type]

    # Step 9: Load planning (self-guards to business days, not holidays)
    all_events.extend(load_planning.run(state, config, sim_date))  # type: ignore[arg-type]

    # Step 9.5: Carrier events — business days only (self-guards internally)
    all_events.extend(carrier_events.run(state, config, sim_date))  # type: ignore[arg-type]

    # Step 10: Load lifecycle — every day, carriers run weekends
    all_events.extend(load_lifecycle.run(state, config, sim_date))  # type: ignore[arg-type]

    # Step 10.5: POD — business days only (self-guards internally)
    # Must run AFTER load_lifecycle so that loads delivered today are registered
    # in pending_pod before the POD engine checks them. (Loads delivered today
    # will not fire POD today because pod_due_date is at least 1 business day
    # in the future.)
    all_events.extend(pod.run(state, config, sim_date))  # type: ignore[arg-type]

    # Inventory movements: receipts (mirrors production) + picks (mirrors allocation)
    # + transfers, adjustments, scrap.  Must run after both production and allocation.
    all_events.extend(inventory_movements.run(state, config, sim_date))  # type: ignore[arg-type]

    # Step 11: Returns / RMA flow (self-guards to business days)
    all_events.extend(returns.run(state, config, sim_date))  # type: ignore[arg-type]

    # Step 13: Inter-warehouse transfers — demand-pull replenishment W01→W02
    all_events.extend(transfers.run(state, config, sim_date))  # type: ignore[arg-type]

    # Steps 12, 14–16: TODO (payments, demand planning, master data, snapshots)

    # Step 17: Schema evolution — TODO
    # Step 18: Noise injection — TODO

    # Serialise all events to output files
    # write_events takes (events_as_dicts, sim_date, output_dir)
    serialised = [
        e.model_dump() if hasattr(e, "model_dump") else dict(e)
        for e in all_events
    ]
    if output_dir is not None:
        write_events(serialised, sim_date, output_dir=output_dir)
    else:
        write_events(serialised, sim_date)

    day_type = "biz" if is_business_day(sim_date) else "weekend/holiday"
    print(f"  Day {state.sim_day}: {sim_date} ({day_type}) — {len(all_events)} events")  # type: ignore[attr-defined]

    return all_events


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
    """Shared helper: simulate *n_days* **calendar** days and persist state.

    Advances one calendar day at a time.  Weekend and holiday days are still
    processed (exchange rates run 7 days a week; weekend EDI orders arrive from
    Distributors).  Engines that are business-day-only self-guard internally.

    Args:
        config: Validated simulation config.
        n_days: Number of calendar days to advance.
    """
    from flowform.state import SimulationState

    try:
        state = SimulationState.from_db(config)  # type: ignore[arg-type]
    except FileNotFoundError:
        print("No simulation state found. Run with --reset to initialise.")
        sys.exit(1)

    for _ in range(n_days):
        next_date = state.current_date + timedelta(days=1)
        state.advance_day(next_date)
        _run_day_loop(state, config, next_date)
        state.save()

    print(
        f"Simulated {n_days} calendar days. "
        f"Current date: {state.current_date} (sim day {state.sim_day})."
    )


def _cmd_days(config: object, n: int) -> None:  # type: ignore[type-arg]
    """--days N: simulate N business days."""
    _simulate_days(config, n)


def _cmd_until(config: object, target: date) -> None:  # type: ignore[type-arg]
    """--until DATE: simulate every calendar day up to and including target date."""
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
        next_date = state.current_date + timedelta(days=1)
        state.advance_day(next_date)
        _run_day_loop(state, config, next_date)
        state.save()
        days_simulated += 1

    print(
        f"Simulated {days_simulated} calendar days. "
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
