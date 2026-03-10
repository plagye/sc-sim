"""Tests for the performance profiler and optimisation infrastructure (Step 48).

Four tests:
1. profile_run returns a valid ProfileResult for a short run
2. credit._credit_dirty is cleared after credit.run()
3. state dirty serialisation preserves unchanged keys
4. injector early-exit when all rates are zero
"""

from __future__ import annotations

import random
from pathlib import Path
from unittest.mock import MagicMock

_PROJECT_ROOT = Path(__file__).parent.parent
_CONFIG_SRC = _PROJECT_ROOT / "config.yaml"


# ---------------------------------------------------------------------------
# Test 1: profile_run returns a valid ProfileResult
# ---------------------------------------------------------------------------


def test_profile_run_returns_result(tmp_path: Path) -> None:
    """profile_run(n_days=3) must return a ProfileResult with sensible values."""
    from flowform.profiler import ProfileResult, profile_run

    output_dir = tmp_path / "output"
    result = profile_run(
        n_days=3,
        config_path=_CONFIG_SRC,
        output_dir=output_dir,
        reset=True,
    )

    assert isinstance(result, ProfileResult)
    assert result.days_simulated == 3
    assert result.total_seconds > 0, "Wall time must be positive"
    assert result.seconds_per_day > 0, "Per-day time must be positive"
    assert result.peak_memory_mb > 0, "Peak memory must be positive"
    assert len(result.top_functions) > 0, "Must capture at least one profiled function"
    for name, pct in result.top_functions:
        assert isinstance(name, str) and name
        assert isinstance(pct, float)


# ---------------------------------------------------------------------------
# Test 2: credit._credit_dirty is cleared after credit.run()
# ---------------------------------------------------------------------------


def test_credit_dirty_set_cleared_after_run(tmp_path: Path) -> None:
    """After credit.run(), state._credit_dirty must be empty."""
    from datetime import date

    from flowform.config import load_config
    from flowform.engines import credit
    from flowform.state import SimulationState

    config = load_config(_CONFIG_SRC)
    db_path = tmp_path / "sim.db"
    state = SimulationState.from_new(config, db_path=db_path)

    # Simulate that allocation/modifications marked a customer dirty
    state._credit_dirty.add("CUST-001")

    # Run on a business day (Jan 7 is Wednesday; Jan 6 is Epiphany in Poland)
    credit.run(state, config, date(2026, 1, 7))  # Wednesday

    assert len(state._credit_dirty) == 0, (
        f"_credit_dirty should be empty after credit.run(), "
        f"but contains: {state._credit_dirty}"
    )


# ---------------------------------------------------------------------------
# Test 3: dirty serialisation preserves unchanged keys
# ---------------------------------------------------------------------------


def test_state_dirty_only_saves_changed_keys(tmp_path: Path) -> None:
    """After marking only 'inventory' dirty, a save/reload must preserve open_orders."""
    from flowform.config import load_config
    from flowform.state import SimulationState

    config = load_config(_CONFIG_SRC)
    db_path = tmp_path / "sim.db"
    state = SimulationState.from_new(config, db_path=db_path)

    # Add a synthetic order to open_orders
    state.open_orders["ORD-TEST-001"] = {
        "order_id": "ORD-TEST-001",
        "customer_id": "CUST-001",
        "status": "confirmed",
        "lines": [],
    }

    # Do a full save to persist the order
    state.save()

    # Now only mark inventory dirty (not open_orders)
    state.mark_dirty("inventory")
    state.save()  # Partial save — should not corrupt open_orders

    # Reload and verify open_orders was preserved from the first full save
    state2 = SimulationState.from_db(config, db_path=db_path)
    assert "ORD-TEST-001" in state2.open_orders, (
        "open_orders not preserved after partial (dirty-only) save"
    )


# ---------------------------------------------------------------------------
# Test 4: injector early-exit when all rates are zero
# ---------------------------------------------------------------------------


def test_injector_early_exit_all_zero_rates(monkeypatch: object) -> None:
    """inject() with all rates == 0.0 must return events unchanged (no mutations)."""
    from flowform.noise.injector import RATES, inject

    # Patch all 'medium' rates to 0.0
    monkeypatch.setattr(
        "flowform.noise.injector.RATES",
        {
            "light": RATES["light"],
            "medium": {k: 0.0 for k in RATES["medium"]},
            "heavy": RATES["heavy"],
        },
    )

    cfg = MagicMock()
    cfg.noise.profile = "medium"

    original_events = [
        {
            "event_type": "customer_order",
            "event_id": f"EV-{i:03d}",
            "customer_id": "CUST-001",
            "order_id": f"ORD-2026-{i:06d}",
            "unit_price": 100.0,
            "timestamp": "2026-03-10T08:00:00Z",
            "sku_id": "FF-GA15-CS-PN16-FL-MA",
        }
        for i in range(100)
    ]
    rng = random.Random(42)

    result = inject(list(original_events), cfg, rng)

    assert len(result) == 100, "No events should be added (duplicate rate=0.0)"
    for orig, got in zip(original_events, result):
        # All fields must be identical since no noise was applied
        for key in orig:
            assert orig[key] == got[key], (
                f"Field '{key}' was mutated despite all rates being 0.0"
            )
