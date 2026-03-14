# FlowForm Industries — Supply Chain Simulation Engine

A synthetic B2B supply chain data generator that produces realistic, messy, multi-system events (ERP, TMS, ADM, Forecast). Built around a fictional industrial valve manufacturer (FlowForm Industries, Katowice, Poland), the engine models customer orders, inventory movements, carrier loads, payments, demand planning, and schema evolution. It is designed for data engineering practice — pipeline building, data quality handling, schema evolution, SCD dimensions, late-arriving data, and incremental load patterns.

## Quick Start

```bash
git clone <repo-url>
cd flowform-sim
python -m venv venv && source venv/bin/activate
pip install -e ".[dev]"
python -m flowform.cli --reset
python -m flowform.cli --days 30
python -m flowform.cli --status
```

## CLI Reference

| Command | Description |
|---------|-------------|
| `--reset` | Wipe state and output; generate fresh master data (required before first run) |
| `--days N` | Advance N calendar days from the current simulation position |
| `--until DATE` | Advance until `DATE` (YYYY-MM-DD) |
| `--status` | Print simulation state summary without simulating |

> Run `--reset` once before the very first `--days` to initialise state and master data. After that, run `--days 1` (or more) repeatedly to advance the simulation day by day. Each day writes to a new date-stamped subdirectory (`erp/YYYY-MM-DD/`, `tms/YYYY-MM-DD/`, etc.), so resuming is always safe — previously written files are never touched.

## Output Structure

```
output/
  erp/YYYY-MM-DD/
    customer_orders.json          daily order batch (all segments)
    inventory_movements.json      picks, receipts, adjustments, returns
    credit_events.json            holds and releases
    payments.json                 customer payment records
    backorder_events.json         backorder creation and release
    exchange_rates.json           daily EUR/PLN, USD/PLN
    schema_evolution_events.json  field activation audit trail
    inventory_snapshots.json      end-of-day warehouse positions
    master_data_changes.json      SCD events (address, segment, terms)
    order_modifications.json      quantity/date/priority/line changes
    order_cancellations.json      full and partial cancellations
    production_completions.json   finished goods entering inventory
  tms/YYYY-MM-DD/
    loads_08.json                 TMS sync window 08:00
    loads_14.json                 TMS sync window 14:00
    loads_20.json                 TMS sync window 20:00 (maintenance burst target)
    carrier_events.json           capacity alerts, disruptions, rate changes
    return_events.json            RMA flow events
  adm/
    demand_signals/SIG-YYYYMMDD-NNNN.json   per-event demand signals
    demand_plans/DP-YYYY-MM.json            monthly consensus plan
    inventory_targets/TGT-*.json            monthly safety stock targets
  forecast/
    FCST-YYYY-MM.json             statistical forecast (model v2.3→v3.0 at day 90)
  supply_disruptions/
    DSRP-*.json                   disruption events (when enabled in config)
```

## Configuration

Key knobs in `config.yaml`:

| Key | Default | Effect |
|-----|---------|--------|
| `simulation.seed` | 42 | RNG seed — same seed = same output |
| `noise.profile` | `medium` | `light` / `medium` / `heavy` noise mix |
| `noise.erp_defer_probability` | 0.02 | Fraction of inventory movements deferred to next day |
| `noise.tms_late_probability` | 0.05 | Fraction of TMS events back-dated to previous biz day |
| `noise.maintenance_probability` | 0.045 | Probability of TMS maintenance burst per business day |
| `disruptions.enabled` | `false` | Enable supply disruption events |
| `payments.on_time_probability` | 0.82 | Fraction of on-time customer payments |
| `demand.signal_probability_daily` | 0.05 | Bernoulli rate per trial per day |
| `schema_evolution.sales_channel_from_day` | 60 | sim_day when `sales_channel` appears on orders |
| `schema_evolution.forecast_model_upgrade_day` | 90 | sim_day when forecast model upgrades v2.3→v3.0 |
| `schema_evolution.incoterms_from_day` | 120 | sim_day when `incoterms` appears on orders |
| `company.warehouse_transfer_threshold` | 0.3 | W02 stock fraction below which replenishment triggers |

## Data Engineering Challenges

| Challenge | Description |
|-----------|-------------|
| Multi-source joins | `order_id` ties ERP orders to TMS loads; `customer_id` links ERP, ADM, and TMS |
| Late-arriving data | ~5% of TMS events land in the wrong sync file; ~2% of ERP movements land in the next day's file |
| Schema evolution | Three fields appear mid-simulation; downstream pipelines must handle both old and new schemas |
| SCD Type 2 dimensions | Customer addresses, segments, and payment terms change; `master_data_changes.json` is the source |
| Partial fulfilment | Order lines can be split across multiple shipments and loads |
| Data quality noise | Duplicates, missing fields, timezone errors, orphan references, encoding issues (configurable) |
| Backorders and escalation | Lines escalate from standard→express after 14 days; auto-cancel after 30 days |
| Credit hold cycles | Customer balance > limit → orders held; payment → release |
| Monthly cadence differences | ERP daily; TMS 3x/day; ADM signals per-event; Forecast monthly |
| Maintenance bursts | Monthly TMS maintenance flushes all load events to sync window 20 |

## System Architecture

Day loop execution order (20 steps):

```
 1  Calendar check              11  Returns processing (RMA)
 2  Exchange rates (daily)      12  Payment processing
 3  Production                  13  Inter-warehouse transfers
 3.5 Supply disruptions         14  Demand planning (1st biz day/month)
 4  Demand signals              14b Demand forecasts (1st biz day/month)
 5  Order generation            14c Inventory targets (1st biz day/month)
 6  Credit check                15  Master data changes
 7  Modifications/cancels       16  Inventory snapshots
 8  Allocation + backorders     17  Schema evolution check
 9  Load planning               18  Noise injection
 9.5 Carrier events             18a Late-arriving data
10  Load lifecycle (every day)  19  Write events (per-system cadence)
10.5 POD handling               20  Save state
```

Entity hierarchy:
```
Order (ERP)
  └── Order Lines (1-N per order)
        └── Shipment (logical grouping)
              └── Load (physical transport, TMS)
```

Cross-system links: `order_id` ties ERP↔TMS; `customer_id` ties ERP↔ADM↔TMS. Noise layer deliberately corrupts ~0.5–3% of events per type (medium profile) — all injected via post-processing, core logic stays clean.

## Schema Evolution

Three notifications appear in `erp/*/schema_evolution_events.json`:

| sim_day | field_name | system | change |
|---------|------------|--------|--------|
| 60 | `sales_channel` | `customer_order` | absent → string |
| 90 | `forecast_model_version` | `demand_forecast` | v2.3 → v3.0 |
| 120 | `incoterms` | `customer_order` | absent → string |

Each fires exactly once, on the first business day at or after the configured threshold. The corresponding fields appear on the actual event records from that point forward.

## Reproducibility

```bash
python -m flowform.cli --reset    # uses seed=42 from config.yaml
python -m flowform.cli --days 180
# Identical output on every machine given the same seed and Python version.
```

## Running Tests

```bash
python -m pytest tests/ -q        # 794 tests, ~100s
ruff check src/ tests/            # lint
```
