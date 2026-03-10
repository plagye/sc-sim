"""Performance profiler for the FlowForm simulation engine.

Wraps a simulation run with cProfile + tracemalloc and returns structured
results. Used by scripts/profile_run.py for benchmarking.
"""

from __future__ import annotations

import cProfile
import io
import pstats
import shutil
import time
import tracemalloc
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path


@dataclass
class ProfileResult:
    """Structured output from a profiled simulation run."""

    total_seconds: float
    days_simulated: int
    seconds_per_day: float
    top_functions: list[tuple[str, float]]  # (func_name, cumtime_pct)
    peak_memory_mb: float


def profile_run(
    n_days: int,
    config_path: Path,
    output_dir: Path,
    *,
    reset: bool = True,
) -> ProfileResult:
    """Run *n_days* using cProfile + tracemalloc and return a :class:`ProfileResult`.

    Args:
        n_days:      Number of calendar days to simulate.
        config_path: Path to config.yaml.
        output_dir:  Directory for JSON output files.
        reset:       If True (default), wipe output_dir and create a fresh state
                     before running.  Set to False to profile a resume run.

    Returns:
        :class:`ProfileResult` with timing and memory data.
    """
    from flowform.cli import _run_day_loop
    from flowform.config import load_config
    from flowform.state import SimulationState

    config = load_config(config_path)

    db_path = output_dir.parent / "state" / "simulation.db"

    if reset:
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        state = SimulationState.from_new(config, db_path=db_path)
    else:
        state = SimulationState.from_db(config, db_path=db_path)

    tracemalloc.start()
    pr = cProfile.Profile()
    t0 = time.perf_counter()
    pr.enable()

    for _ in range(n_days):
        next_date = state.current_date + timedelta(days=1)
        state.advance_day(next_date)
        _run_day_loop(state, config, next_date, output_dir=output_dir)
        state.save()

    pr.disable()
    elapsed = time.perf_counter() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    stream = io.StringIO()
    ps = pstats.Stats(pr, stream=stream).sort_stats("cumulative")
    ps.print_stats(10)
    top = _parse_top_functions(stream.getvalue(), elapsed)

    return ProfileResult(
        total_seconds=round(elapsed, 3),
        days_simulated=n_days,
        seconds_per_day=round(elapsed / max(n_days, 1), 4),
        top_functions=top,
        peak_memory_mb=round(peak / 1024 / 1024, 1),
    )


def _parse_top_functions(
    pstats_output: str,
    total_sec: float,
) -> list[tuple[str, float]]:
    """Extract top-10 (func_name, cumtime_pct) pairs from pstats text output.

    pstats text format (after header rows):
        ncalls  tottime  percall  cumtime  percall  filename:lineno(function)

    Args:
        pstats_output: Raw text from pstats.Stats.print_stats().
        total_sec:     Total elapsed wall-clock time for percentage calculation.

    Returns:
        List of (name, pct) tuples sorted by cumulative time descending.
    """
    results: list[tuple[str, float]] = []
    in_data = False

    for line in pstats_output.splitlines():
        line = line.strip()

        # Header separator line signals start of data rows
        if line.startswith("ncalls"):
            in_data = True
            continue

        if not in_data or not line:
            continue

        parts = line.split()
        if len(parts) < 6:
            continue

        try:
            cumtime = float(parts[3])
        except (ValueError, IndexError):
            continue

        # Function name is last token (filename:lineno(function))
        func_info = parts[-1]
        # Shorten path: strip everything before last package directory
        if "/" in func_info:
            # Keep only the last two path components + function name
            path_part, func_part = func_info.rsplit(":", 1) if ":" in func_info else (func_info, "")
            path_components = path_part.split("/")
            short_path = "/".join(path_components[-2:]) if len(path_components) >= 2 else path_part
            func_name = f"{short_path}:{func_part}" if func_part else short_path
        else:
            func_name = func_info

        pct = round((cumtime / total_sec) * 100, 1) if total_sec > 0 else 0.0
        results.append((func_name, pct))

        if len(results) >= 10:
            break

    return results
