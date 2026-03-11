"""Master data export: writes static reference tables to output/master/.

Called once after ``--reset`` to snapshot all generated master data to JSON
files that downstream systems can load as dimension tables.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

from flowform.catalog.pricing import base_price_pln
from flowform.state import SimulationState

# EUR/PLN anchor rate (used to derive base_price_eur; not a live rate)
_EUR_PLN_ANCHOR: float = 4.30


def write_master_data(state: SimulationState, output_dir: Path) -> None:
    """Write all master-data reference files to ``output_dir/master/``.

    Creates the directory if it does not exist.  Six JSON files are written:

    - ``catalog.json``                 — all active SKUs with pricing + weight
    - ``customers.json``               — all 60 customers with profiles
    - ``carriers.json``                — 5 carrier definitions including weight_unit
    - ``warehouses.json``              — 2 warehouse definitions
    - ``initial_inventory.json``       — per-warehouse SKU quantities at reset
    - ``initial_customer_balances.json`` — per-customer balances at reset (all 0.0)

    Args:
        state:      Fully initialised simulation state (post-``from_new``).
        output_dir: Root output directory (e.g. ``Path("output")``).
    """
    master_dir = output_dir / "master"
    master_dir.mkdir(parents=True, exist_ok=True)

    _write_catalog(state, master_dir)
    _write_customers(state, master_dir)
    _write_carriers(state, master_dir)
    _write_warehouses(state, master_dir)
    _write_initial_inventory(state, master_dir)
    _write_initial_customer_balances(state, master_dir)


# ---------------------------------------------------------------------------
# Per-file writers
# ---------------------------------------------------------------------------


def _write_catalog(state: SimulationState, master_dir: Path) -> None:
    """Write catalog.json — one entry per active SKU."""
    rows: list[dict] = []
    for entry in state.catalog:
        spec = entry.spec
        price_pln = base_price_pln(spec)
        rows.append({
            "sku": entry.sku,
            "valve_type": spec.valve_type,
            "dn": spec.dn,
            "material": spec.material,
            "pressure_class": spec.pressure_class,
            "connection": spec.connection,
            "actuation": spec.actuation,
            "weight_kg": entry.weight_kg,
            "base_price_pln": price_pln,
            "base_price_eur": round(price_pln / _EUR_PLN_ANCHOR, 2),
            "is_active": entry.is_active,
        })
    _dump_json(master_dir / "catalog.json", rows)


def _write_customers(state: SimulationState, master_dir: Path) -> None:
    """Write customers.json — one entry per customer."""
    rows: list[dict] = []
    for c in state.customers:
        d = dataclasses.asdict(c)
        # Convert date → ISO string
        d["onboarding_date"] = c.onboarding_date.isoformat()
        # Remove private/internal fields
        d = {k: v for k, v in d.items() if not k.startswith("_")}
        # Normalise seasonal_profile int keys to strings for JSON compatibility
        d["seasonal_profile"] = {str(k): v for k, v in c.seasonal_profile.items()}
        # Normalise dn keys in sku_affinity to strings
        raw_affinity = d["ordering_profile"]["sku_affinity"]
        d["ordering_profile"]["sku_affinity"] = {
            dim: {str(k): v for k, v in weights.items()}
            for dim, weights in raw_affinity.items()
        }
        rows.append(d)
    _dump_json(master_dir / "customers.json", rows)


def _write_carriers(state: SimulationState, master_dir: Path) -> None:
    """Write carriers.json — one entry per carrier, including weight_unit."""
    rows = [dataclasses.asdict(c) for c in state.carriers]
    _dump_json(master_dir / "carriers.json", rows)


def _write_warehouses(state: SimulationState, master_dir: Path) -> None:
    """Write warehouses.json — one entry per warehouse."""
    rows = [dataclasses.asdict(w) for w in state.warehouses]
    _dump_json(master_dir / "warehouses.json", rows)


def _write_initial_inventory(state: SimulationState, master_dir: Path) -> None:
    """Write initial_inventory.json — {warehouse_code: {sku: qty}}."""
    _dump_json(master_dir / "initial_inventory.json", state.inventory)


def _write_initial_customer_balances(state: SimulationState, master_dir: Path) -> None:
    """Write initial_customer_balances.json — {customer_id: float}."""
    _dump_json(master_dir / "initial_customer_balances.json", state.customer_balances)


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def _dump_json(path: Path, data: object) -> None:
    """Write *data* to *path* as indented JSON (UTF-8)."""
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
