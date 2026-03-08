# FlowForm Industries — Supply Chain Simulation Engine

## What This Is

A Python simulation engine that generates realistic supply chain event data for a fictional B2B industrial valve manufacturer (FlowForm Industries, Katowice, Poland). Simulates one business day at a time, maintains persistent state, outputs raw JSON files organized by source system (ERP, TMS, ADM, Forecast). Downstream pipeline/warehouse/dashboard is NOT part of this project.

## Environment

- Virtual environment at `venv/`. Always activate (`source venv/bin/activate`) before running anything.
- Dependencies tracked in `requirements.txt`. After installing: `pip freeze > requirements.txt`.
- `.gitignore` covers: `output/`, `state/`, `venv/`, `__pycache__/`, `*.pyc`.

## Project Structure

```
flowform-sim/
├── .claude/
│   ├── CLAUDE.md
│   └── agents/
│       └── sim-engine.md
├── .gitignore
├── src/flowform/
│   ├── cli.py                 # CLI entry point
│   ├── config.py              # Config loader + validation
│   ├── state.py               # SimulationState + SQLite persistence
│   ├── calendar.py            # Polish holidays, business day logic, Easter calc
│   ├── schema_evolution.py    # Manages field appearance by sim day number
│   ├── catalog/               # SKU constraints, catalog generation, pricing, weights
│   ├── master_data/           # Customer, carrier, warehouse generators
│   ├── engines/               # One module per simulation step
│   ├── noise/                 # Data quality noise injection
│   └── output/                # JSON file writer (multi-cadence)
├── config.yaml
├── requirements.txt
├── output/                    # Generated JSON (gitignored)
├── state/                     # SQLite state DB (gitignored)
├── venv/                      # Virtual environment (gitignored)
└── tests/
```

## How to Run

```bash
python -m flowform.cli --reset              # Init fresh: master data + initial state
python -m flowform.cli --days 30            # Simulate 30 business days
python -m flowform.cli --days 1             # Single day (debug)
python -m flowform.cli --until 2026-06-30   # Simulate until date
python -m flowform.cli --status             # Current date + summary stats
```

## The Company: FlowForm Industries

- Industrial control valves, ~2500 active SKUs, SKU encoding: `FF-{TYPE}{DN}-{MAT}-{PN}-{CONN}{ACT}`
- B2B only, 50–80 customers across 6 segments: Oil & Gas, Water Utilities, Chemical Plants, Industrial Distributors, HVAC Contractors, Shipyards
- 2 warehouses: W01 Katowice (main, 80%), W02 Gdańsk (port, 20%)
- 5 carriers: DHL, DB Schenker, Raben, Geodis, BalticHaul (BalticHaul reports weight in lbs, all others in kg)
- Multi-currency: PLN (65%), EUR (30%), USD (5%)

## Source Systems & Event Types

### ERP (`output/erp/`) — daily batch files
- `production_completions` — finished goods entering inventory
- `production_reclassifications` — retroactive grade changes (A→B, 2–5 days later)
- `customer_orders` — new orders with line-level tracking (ordered/allocated/shipped/backordered)
- `inventory_movements` — picks, receipts, unpicks, transfers, adjustments, scrap, return restocks
- `inventory_snapshots` — daily full dump per warehouse (end of day)
- `order_modifications` — changes to open orders
- `order_cancellations` — full or partial
- `exchange_rates` — daily EUR/PLN, USD/PLN rates
- `master_data_changes` — customer/carrier/product changes (SCD source)

### TMS (`output/tms/`) — 3 sync files per day (08:00, 14:00, 20:00)
- `loads` — physical transport lifecycle (load_created → pod_received, 5–10 events per load)
- `carrier_events` — capacity alerts, disruptions, rate changes
- `returns` — reverse logistics (RMA flow)

### ADM (`output/adm/`) — per-event files for signals, monthly for plans
- `demand_signals` — external/internal demand indicators
- `demand_plans` — monthly consensus plans
- `inventory_targets` — safety stock and reorder levels

### Forecast (`output/forecast/`) — monthly files
- `demand_forecasts` — statistical forecasts (separate from demand plans, intentionally divergent)

## Entity Hierarchy (Critical for Data Modeling)

```
Order (ERP) → has many Order Lines
  Order Line → can be partially_allocated, backordered, split across shipments
    Shipment → logical grouping of lines for same customer
      Load (TMS) → physical truck, carries 1+ shipments
```

One load can carry multiple shipments. One order can split across multiple shipments/loads. Many-to-many.

## Key Behaviors

### Backorders & Partial Fulfillment
- Allocation is priority-based (critical > express > standard, then by date)
- Partial fills: allocate available quantity, backorder remainder
- Backordered lines re-evaluated daily, escalate after 14 days, customers cancel after 30 days
- Partial shipments: allocated portion ships immediately, backorder ships later

### Credit Holds
- Customer balance tracked (invoiced − paid)
- Orders go to `credit_hold` when balance exceeds limit
- Released when payment brings balance below limit

### Seasonality
- Polish public holidays: no production, no outbound shipping
- Day-of-week: production/orders have different weekday multipliers, weekend EDI from Distributors
- Monthly curve: January slow (0.70) → April peak (1.25) → July summer dip (0.75) → December dead (0.55)
- Segment overrides: Water Utilities spring peak, Chemical Plant shutdown clusters, HVAC heating season, Distributor Q4 budget flush
- Carrier reliability degrades in winter and holidays

### Schema Evolution
- Sim day 60: `sales_channel` field appears on orders
- Sim day 90: forecast model upgrades v2.3 → v3.0
- Sim day 120: `incoterms` field appears on orders

### Late-Arriving Data
- ~5% of TMS events land in wrong sync file (previous day's timestamp)
- ~2% of ERP movements land in next day's file
- Monthly TMS maintenance causes event burst in next sync

### Data Quality Noise
- Configurable: light/medium/heavy
- Duplicates, missing fields, timezone errors, SKU case issues, orphan references, missing weight_unit, encoding issues

## Day Loop (Order of Operations)

1. Calendar check (business day / weekend / holiday)
2. Exchange rates (daily, even weekends)
3. Production (business days)
4. Demand signals (business days)
5. Order generation (business days + weekend EDI)
6. Credit check
7. Modifications & cancellations (business days)
8. Allocation with backorders (business days)
9. Load planning (business days, not holidays)
10. Load lifecycle (every day — carriers run weekends)
11. Returns processing (business days)
12. Payment processing (business days)
13. Inter-warehouse transfers (business days)
14. Demand planning (1st business day of month)
15. Master data changes (random)
16. Inventory snapshots (business days)
17. Schema evolution check
18. Noise injection
19. Write events (per-system cadence)
20. Save state

## Coding Conventions

- Python 3.12+, type hints on all functions
- Pydantic v2 for all event schemas and config
- SQLite for state (stdlib sqlite3, no ORM)
- All randomness through `state.rng` (seeded, reproducible)
- Each engine module: `run(state, config, date) -> list[Event]`
- Events serialized via `.model_dump()`
- pytest, minimum one test per engine module
- ruff for formatting/linting

## Key Design Decisions

1. **State in SQLite** — must be resumable. Never hold state only in memory.
2. **Events are immutable** — state changes happen through new events, not editing originals.
3. **Noise is post-processing** — core logic stays clean.
4. **Systems are independent** — cross-references (order_id in ERP + TMS) are the link. Noise layer creates deliberate mismatches.
5. **Forecasts ≠ Demand Plans** — separate, intentionally divergent.
6. **Loads ≠ Shipments** — load is physical transport, shipment is logical grouping. Many-to-many with orders.
7. **File cadence varies by system** — ERP daily, TMS 3x/day, ADM per-event, Forecast monthly.

## Current Status

- [x] Phase 1: Skeleton & Master Data (steps 1–13) — **complete**
  - [x] Step 1: pyproject.toml, requirements.txt, .gitignore, full directory skeleton with `__init__.py` stubs, `config.yaml`
  - [x] Step 2: Config loader — full Pydantic v2 model hierarchy (`Config`, 11 nested models), `load_config()`, 13 tests
  - [x] Step 3: Polish holiday calendar — Easter (Meeus/Jones/Butcher), `polish_holidays()`, business day predicates, DOW multiplier tables, 57 tests
  - [x] Step 4: SKU constraint engine — `SKUSpec`, `is_valid_combination()` (8 rules), `get_all_valid_specs/skus()`, 62 tests; valid space is ~11k combinations
  - [x] Step 5: Catalog generator — Efraimidis-Spirakis weighted reservoir sampling → 2,500 active SKUs; Ball+Butterfly 54%, DN50-150 62%, 19 tests
  - [x] Step 6: Pricing engine — multiplicative formula anchored at PLN 850 (Ball/DN50/CS/PN16/Flanged/Manual); range PLN 328–221k; `apply_customer_discount()`, `convert_price()`; 18 tests
  - [x] Step 7: Weight calculator — DN×material×valve type×actuation formula (3dp); `shipment_weight_for_carrier()` handles BALTIC lbs conversion; backfills `CatalogEntry.weight_kg`; range 0.570–184.800 kg; 39 tests
  - [x] Step 8: Customer master data generator — 60 customers, 6 segments (18/12/12/9/6/3), Faker addresses by locale, segment-specific ordering profiles + SKU affinity weights + seasonal profiles, Chemical Plant shutdown months; 12 tests
  - [x] Step 9: Carrier + warehouse definitions — 5 carriers (DHL/DBSC/RABEN/GEODIS/BALTIC), 2 warehouses (W01/W02), `reliability_on_date()` with 4 seasonal offsets, BALTIC weight_unit=lbs quirk; 32 tests
  - [x] Step 10: SimulationState + SQLite persistence — `from_new()`, `from_db()`, `save()`, `advance_day()`; `sim_meta` + `kv_state` schema; RNG state via pickle BLOB; all operational tables round-trip; 11 tests
  - [x] Step 11: CLI skeleton — `--reset`, `--days N`, `--until DATE`, `--status`; argparse mutually exclusive group; day loop stub; `__main__.py`; 14 tests
  - [x] Step 12: JSON output writer — `write_events()` routes by event_type to ERP/TMS/ADM/Forecast paths; TMS loads split by sync_window (08/14/20); no empty files written; `is_monthly_event_type()`; 22 tests
  - [x] Step 13: Integration test — `--reset` produces ≥2000 SKUs + 60 customers; `--status` shows correct initial state; `--days` advances sim_day and skips weekends; `--until` past-date guard; `write_events` roundtrip; 22 tests
- [x] Pre-Phase 2 fixes: `counters` + `production_pipeline` added to SimulationState; initial inventory seeded (~65% of SKUs, W01 20–300 units, W02 5–25% of W01); `next_id()` helper; 4 new state tests
- [x] Phase 2: Core Day Loop (steps 14–22) — **complete**
  - [x] Step 14: Exchange rate generator — mean-reverting random walk ±0.3%/day, 10% reversion pull toward anchors (EUR/PLN 4.30, USD/PLN 4.05); 2 events/day every day incl. weekends; `ExchangeRateEvent` schema; 6 tests
  - [x] Step 15: Production engine — business days only; 3–8 batches/day; 200–800 units × DOW multiplier × monthly seasonality × year-end (30% Dec 25+); 2.5% stoppage; 90/8/2% A/B/REJECT grading; Grade A+B added to W01 inventory; 1% reclassification after 2–5 days via `production_pipeline`; 10 tests
  - [x] Step 16: Order generation — business days all segments + weekend EDI for Distributors only; effective probability = (freq/22) × global_monthly × customer_seasonal × dow_multiplier; SKU affinity weights cached per customer; 1–5 lines, quantity distributed across lines; pricing in customer currency; December delivery restriction; schema evolution fields (sales_channel day 60, incoterms day 120) absent before threshold; orders stored in `state.open_orders`; 10 tests
  - [x] Step 17: Credit check engine — business days only; pre-computes per-customer total exposure (balance + all confirmed/held open orders converted to PLN); hold → `"credit_hold"` + CreditHoldEvent; release → `"confirmed"` + CreditReleaseEvent; held orders still count toward exposure; 8 tests
  - [x] Step 18: Inventory movement tracker — business days only; `InventoryMovementEvent` schema; receipt movements from `state.daily_production_receipts` (populated by production engine); probabilistic transfers (40%/day, 0–2, W01↔W02, conserves total stock); cycle-count adjustments (5%/day, 1–5 SKUs, ±5%); scrap (1%/day, 1–3 SKUs, W01 only); `movement_id` counter via `state.counters["movement"]`; `daily_production_receipts` cleared in `advance_day()`; 10 tests
  - [x] Step 19: Allocation with backorder logic — business days only; priority sort (critical > express > standard, then requested_delivery_date, then FIFO); full/partial/zero-stock allocation; pick movements via `InventoryMovementEvent`; `BackorderEvent` for no_stock and insufficient_stock; `backorder_days` counter increments each day; >14 days standard → express escalation; >30 days 15% daily cancel probability → `BackorderCancellationEvent`; credit_hold orders skipped; order-level status recalculated from lines; 12 tests
  - [x] Step 20: Modification + cancellation engine — business days only; customer-initiated modifications (10%: quantity_change 35%, date_change 30%, priority_change 20%, line_removal 10%, line_addition 5%); quantity_change floored at allocated+shipped; date_change clamped at sim_date; priority_change escalates standard→express/critical or express→critical; line_removal only when >1 active line, returns allocated qty to inventory; line_addition appends new line with affinity-weighted SKU; cancellations (2.5%, full 60%/partial 40%); modified orders excluded from cancellation pass; credit_hold and cancelled orders skipped; 14 tests
  - [x] Step 21: Wire steps 1–8 of day loop — calendar-day iteration (every day incl. weekends/holidays); `_run_day_loop` calls exchange_rates, production, orders, credit, modifications, allocation, inventory_movements in order; `write_events` called with serialised events; `--days N` = N calendar days; `--until DATE` advances 1 calendar day at a time; extracted `_order_utils.update_order_status` shared by allocation+modifications; `_return_allocated_to_inventory` now emits unpick `InventoryMovementEvent`; `_apply_quantity_change` emits unpick for alloc reductions; `_apply_line_addition` uses discount+currency conversion; writer routes credit_hold/credit_release → `credit_events.json`, plural event_type aliases for customer_orders/inventory_movements/order_modifications/order_cancellations; smoke test: 7 days (holiday+weekends+biz) produces correct files per day type
  - [x] Step 22: Test: 14 days spanning weekend + holiday — 2025-04-14→2025-04-27; 9 biz days, 2 weekends, Easter Monday; 43 assertions across all wired engines; `_run_day_loop` now returns events list
- [x] Phase 3: Logistics (steps 23–31) — **in progress**
  - [x] Step 23: Load planning — allocated orders → shipments → loads; carrier assignment (preferred_carrier or warehouse-weighted random); `LoadEvent` schema; sync window distribution across 08/14/20; BALTIC lbs conversion; `state.active_loads` + `load_id`/`shipment_id` stamped on orders; 12 tests
  - [x] Step 24: Load lifecycle — `load_created → in_transit → out_for_delivery → delivered`; at most one step/day; carrier reliability check (seasonal) for transitions 2+3; delay_days on failure; BALTIC 4–7 day transit; order lines set to shipped on delivery; wired at Step 10 (runs every day, carriers work weekends); `LoadEvent.event_subtype`/`status` loosened from `Literal` to `str`; BALTIC `transit_days_min/max` corrected to 4/7 in carriers.py; 14 tests
  - [x] Step 25: Carrier events — fleet-level carrier signals (not per-load); 3 subtypes: `capacity_alert` (5%/carrier/day), `service_disruption` (2%/carrier/day, one at a time, duration 1–5 days), `rate_change` (1/22 prob, always sync_window="14"); `CarrierEvent` Pydantic schema with `carrier_code`, `impact_severity`, `message`, `duration_days` fields; active disruptions stored in `state.carrier_disruptions` (persisted via SQLite kv_state); disruption severity penalty applied to load_lifecycle transitions 2+3 (low=0.10, medium=0.20, high=0.35); wired as Step 9.5 in day loop (business days only); output writer + output package recreated (was missing from disk); 11 tests
  - [x] Step 26: POD handling — `state.pending_pod` dict added (persisted via SQLite kv_state); load_lifecycle populates `pending_pod` on delivery with 1–3 biz-day POD window via `_advance_business_days()`; `pod.py` engine checks `pending_pod` each business day, fires `pod_received` LoadEvent for due/overdue loads, removes from `pending_pod` and `active_loads`; wired as Step 10.5 in day loop after load_lifecycle; output routes through existing TMS load_event writer; 11 tests
- [ ] Phase 4: Demand Management & Financials (steps 32–40)
- [ ] Phase 5: Realism & Polish (steps 41–49)

**Test count: 486 passing** (13 config + 57 calendar + 62 constraints + 19 generator + 18 pricing + 39 weights + 12 customers + 32 carriers/warehouses + 15 state + 14 CLI + 22 output writer + 22 integration + 6 exchange rates + 10 production + 10 orders + 8 credit check + 10 inventory movements + 12 allocation + 14 modifications + 43 phase2 integration + 12 load planning + 14 load lifecycle + 11 carrier events + 11 pod)
