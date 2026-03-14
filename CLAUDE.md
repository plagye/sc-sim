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
- [x] Phase 3: Logistics (steps 23–31) — **complete**
  - [x] Step 23: Load planning — allocated orders → shipments → loads; carrier assignment (preferred_carrier or warehouse-weighted random); `LoadEvent` schema; sync window distribution across 08/14/20; BALTIC lbs conversion; `state.active_loads` + `load_id`/`shipment_id` stamped on orders; 12 tests
  - [x] Step 24: Load lifecycle — `load_created → in_transit → out_for_delivery → delivered`; at most one step/day; carrier reliability check (seasonal) for transitions 2+3; delay_days on failure; BALTIC 4–7 day transit; order lines set to shipped on delivery; wired at Step 10 (runs every day, carriers work weekends); `LoadEvent.event_subtype`/`status` loosened from `Literal` to `str`; BALTIC `transit_days_min/max` corrected to 4/7 in carriers.py; 14 tests
  - [x] Step 25: Carrier events — fleet-level carrier signals (not per-load); 3 subtypes: `capacity_alert` (5%/carrier/day), `service_disruption` (2%/carrier/day, one at a time, duration 1–5 days), `rate_change` (1/22 prob, always sync_window="14"); `CarrierEvent` Pydantic schema with `carrier_code`, `impact_severity`, `message`, `duration_days` fields; active disruptions stored in `state.carrier_disruptions` (persisted via SQLite kv_state); disruption severity penalty applied to load_lifecycle transitions 2+3 (low=0.10, medium=0.20, high=0.35); wired as Step 9.5 in day loop (business days only); output writer + output package recreated (was missing from disk); 11 tests
  - [x] Step 26: POD handling — `state.pending_pod` dict added (persisted via SQLite kv_state); load_lifecycle populates `pending_pod` on delivery with 1–3 biz-day POD window via `_advance_business_days()`; `pod.py` engine checks `pending_pod` each business day, fires `pod_received` LoadEvent for due/overdue loads, removes from `pending_pod` and `active_loads`; wired as Step 10.5 in day loop after load_lifecycle; output routes through existing TMS load_event writer; 11 tests
  - [x] Step 27: Carrier events extended — `maintenance_window` (1/30 prob/carrier/day, sub-day, sync_window="08", duration_days=0, no state change) and `holiday_schedule` (deterministic, fires for ALL 5 carriers on business day before any Polish public holiday, sync_window="14", duration_days=1, end_date=holiday date); `is_holiday` added to calendar import; both added outside/after the per-carrier disruption loop to preserve RNG sequence; bug fix applied: `_find_next_holiday()` scans up to 3 calendar days ahead to correctly handle Monday holidays (e.g. Easter Monday) where Friday+1=Saturday is not a holiday; 10 tests
  - [x] Step 28: Returns / RMA Flow — business days only; 3% return rate per shipped order; full RMA lifecycle: return_requested → return_authorized → pickup_scheduled → in_transit_return → received_at_warehouse → inspected → restocked (60% Grade-A→W01, 25% Grade-B→W02) | scrapped (15%); restock emits InventoryMovementEvent(return_restock) with reference_id='GRADE-A'/'GRADE-B'; RMA state in state.in_transit_returns; wired as Step 11 in day loop; 12 tests
  - [x] Step 29: Inter-warehouse transfers (replenishment) — demand-pull replenishment W01→W02; threshold read from config.company.warehouse_transfer_threshold; target 25%; 70% daily probability gate; max 5 SKUs/day; two InventoryMovementEvent per transfer (out+in, reason_code=replenishment); ad-hoc transfers in inventory_movements.py get reason_code='adhoc_transfer'; only SKUs W02 already carries are eligible; wired as Step 13 in day loop; 12 tests
  - [x] Step 30: Wire steps 9–13 of day loop — load_planning (step 9), carrier_events (step 9.5), load_lifecycle (step 10), pod (step 10.5), returns (step 11), payments stub (step 12), transfers (step 13); all engines imported and called in correct order; stub engines (payments, planning, master_data_changes, snapshots, demand_signals, schema_evolution) all return []; `--days` docstring updated to say "calendar days"
  - [x] Step 31: tests/test_phase3_integration.py — 30-calendar-day integration test; 8 test classes / 19 tests covering active_loads (asserts on load_created event stream, not active_loads size), POD, returns, replenishment, inventory invariants, reproducibility, weekend lifecycle, no-orphan loads; all 19 tests passing
  - [x] Pre-Step 30 fixes applied: `OPEN_STATUSES` in credit.py expanded to include allocated/partiallyallocated/backordered; `state.allocated` dict now populated by allocation.py; allocation eligibility includes backordered/partiallyallocated orders; stale onhand snapshot bug fixed in inventory_movements.py; backorder/backorder_cancellation events routed by writer; order ID format changed to `ORD-YYYY-NNNNNN`; `next_movement_id()` helper added to SimulationState; load_lifecycle guards delivery_completed loads; pod_due_date stored as ISO string
  - [x] Post-Step 31 test fixes applied: `test_active_loads_populated` now asserts on load_created event stream; `test_no_pending_pod_overdue` fixed dict access (`pod_rec["pod_due_date"]`); `test_active_load_statuses_are_valid` valid_statuses corrected (`"delivered"` replaces `"delivery_completed"`/`"pod_received"`); test_allocation.py fixture order IDs updated to `ORD-2026-9000NN` format; `_order_utils.py` comment corrected to use `"partially_allocated"` (with underscore)
- [x] Phase 4: Demand Management & Financials (steps 32–40) — **complete**
  - [x] Step 32: Payment processing — business days only; per-customer daily payment probability = 1/payment_terms_days; on_time (82%) pays 100%, late (15%) pays 50–80%, very_late (3%) pays 10–30% of outstanding balance; balance clamped to 0.0; `PaymentEvent` schema with balance_before/after/payment_amount/behaviour/payment_terms_days fields; after each payment, re-computes customer exposure and releases any `credit_hold` orders via `CreditReleaseEvent` if exposure <= credit_limit; imports `CreditReleaseEvent` and `_compute_open_orders_value_pln` from credit.py; `"payment"` routed to `erp/YYYY-MM-DD/payments.json` via writer; load_lifecycle now invoices shipped order value into `state.customer_balances` so payments engine has positive balances to process; 13 tests
  - [x] Step 33: Demand signals — business days only; 10 Bernoulli trials/day at `config.demand.signal_probability_daily`; 6 weighted signal sources (`customer_communication`, `market_intelligence`, `seasonal_model`, `consumption_trend`, `contract_renewal`, `competitor_event`); `market_intelligence` and `competitor_event` have `customer_id=None`; 5 signal types (`upside_risk`, `downside_risk`, `timing_shift`, `mix_change`, `new_demand`); magnitude `low`/`medium`/`high`; confidence uniform [0.1, 0.9]; horizon_weeks [2, 26]; `signal_id` = `SIG-YYYYMMDD-NNNN` via `state.counters["signal"]`; product_group derived from random catalog SKU; routed to `adm/YYYY-MM-DD/demand_signals.json`; wired as Step 4 in day loop; 14 tests
  - [x] Step 34: Demand plans — monthly consensus on 1st biz day of month; DemandPlanEvent with 6-month horizon, 8–15 product groups × 6 segments; confidence bounds ±25%/30%; wired as Step 14 in day loop; 14 tests
  - [x] Step 35: Demand forecasts — monthly FCST-YYYY-MM event, 20–40 product groups × 6 periods, model_version v2.3→v3.0 at sim day 90, wider CI in v3.0; wired as Step 14b in day loop; 14 tests
  - [x] Step 36: Inventory targets — monthly InventoryTargetEvent per SKU+warehouse on 1st biz day; safety_stock/reorder_point/target_stock derived from on-hand with random multipliers; wired as Step 14c in day loop; routed to adm/inventory_targets.json; 14 tests
  - [x] Step 37: Master data changes — MasterDataChangeEvent; 5 change types (address/segment/payment_terms/carrier_rate/sku_discontinuation); address+SKU mutations in state; carrier rate fires once at sim_day>=90 (uses >= with fired-flag to handle holiday collisions); CatalogEntry frozen so replaced via dataclasses.replace(); wired as Step 15 in day loop; phase2 integration tests relaxed to allow 1–2 production stoppage days; 13 tests
  - [x] Step 38: Inventory snapshots — end-of-day full dump per warehouse (W01/W02); InventorySnapshotEvent with positions (on_hand/allocated/available); business days only; SNAP-YYYYMMDD-W0N ID; 13 tests
  - [x] Step 39: Schema evolution engine — `SchemaEvolutionEvent` at first business day at or after sim_day 60/90/120; fired-flag in `state.schema_flags` prevents duplicates even when threshold day falls on holiday; `"schema_evolution_event"` routed to `erp/schema_evolution_events.json` via writer; wired as Step 17 in day loop; 6 tests
  - [x] Step 40: Phase 4 integration test — 130-calendar-day run crossing all three schema evolution thresholds; 8 test classes / 20 tests covering demand plans, forecasts, inventory targets, schema evolution (field activation + model upgrade), master data changes (including carrier rate), payments, inventory snapshots, reproducibility
  - [x] Pre-Phase 5 fixes: state.allocated cleanup on shipment (load_lifecycle removes line entry from state.allocated when line_status → shipped); partially_shipped confirmed NOT in credit SKIP_STATUSES (already in _OPEN_STATUSES — verified); snapshot available clamped to >=0 (already correct — verified); PLN invoicing confirmed in load_lifecycle with EUR/USD→PLN conversion; `is_first_business_day_of_month()` moved to calendar.py as public function, removed from planning/forecasts/inventory_targets, test imports updated; schema evolution confirmed via state.sim_day (not state.sim_day_number); signal counter added to initial state counters (`"signal": 0`), guard removed from demand_signals.py; reproducibility test strengthened (30-day run, asserts event count + active_loads keys + first payment amount)
- [x] Phase 5: Realism & Polish (steps 41–49) — **complete**
  - [x] Step 41: Schema evolution field tests — 15 tests covering sales_channel (day 60) and incoterms (day 120) boundary behaviour on customer_order events; thresholds config-driven; value sets validated
  - [x] Step 42: Late-arriving data — TMS 5% timestamp back-dating, ERP 2% movement carry-forward via state.deferred_erp_movements, TMS monthly maintenance burst all-to-sync-20; post-processing in late_arriving.apply() wired as Step 18a; 12 tests
  - [x] Step 43: Multi-cadence writer hardening — demand_signal routed to per-event files (adm/demand_signals/SIG-YYYYMMDD-NNNN.json); write_events appends to existing daily/TMS files instead of overwriting; ADM_PER_EVENT_TYPES routing table added and exported; 14 tests
  - [x] Step 44: Noise injector — 13 noise types (duplicate/missing_field/wrong_timezone/sku_case_error/customer_id_mismatch/decimal_drift/future_dated/orphan_reference/encoding_issue/out_of_sequence/missing_weight_unit/negative_quantity_snapshot/duplicate_load_id); light/medium/heavy rate table; inject() wired as Step 18 before late_arriving; 18 tests
  - [x] Step 45: Wire steps 17–18 of day loop — schema_evolution/noise/late_arriving confirmed wired in cli.py (correct order: schema_evolution → noise → late_arriving → marker strip → write); test_phase5_integration.py: 14-day smoke test covering file validity, marker stripping, deferred movement bounds, maintenance month, schema flag thresholds, sync_window values, reproducibility; 15 tests
  - [x] Step 46: Supply disruptions — SupplyDisruptionEvent; 3 categories × subtypes (production/warehouse/supply_chain); gated by config.disruptions.enabled; state.supply_disruptions persisted; expiry removes stale entries; get_active_production_severity() helper; wired as Step 3.5 in day loop; 15 tests
  - [x] Step 47: 180-day run + review fixes — credit exposure uses unshipped qty (qty_ordered − qty_shipped), exposure_cache invalidated on hold/release so subsequent orders recompute, partially_allocated orders with at least one "allocated" line now eligible for load planning, BackorderEvent routed to backorder_events.json (event_type="backorder"), payment fields protected from missing_field noise (_PROTECTED_FIELDS set in injector.py), confirmed inventory_movements plural alias is load-bearing (InventoryMovementEvent uses it) and retained with corrected comment; 3 new tests (test_exposure_excludes_shipped_quantity, test_backorder_event_type_is_backorder, test_partially_allocated_order_eligible_for_load)
  - [x] Step 47 post-review: credit catch-22 fix — `_compute_open_orders_value_pln` in credit.py now explicitly excludes `credit_hold` orders from exposure calculation; held orders were previously included, creating an unbreakable cycle where a customer's held orders prevented their own release even when the balance was zero; fix: added `if status == "credit_hold": continue` guard in the function loop; also updated Test 5 (cumulative exposure — first order that tips limit gets held, but subsequent orders evaluated with that order already excluded now do NOT get held), rewrote Test 6 (held orders excluded from new order evaluation — ratio reduced to 40%/30% so post-release total stays under limit), added Test 12 `test_held_orders_excluded_from_exposure` (5 held orders × 50K each + 1 confirmed 30K against a high-limit customer — confirmed order not held, all held orders released); fixed `test_phase2_integration::test_customer_orders_have_valid_priority` to skip noise-corrupted events (missing_field noise legitimately drops `priority`); 180-day verification: 473 credit_release, 592 load_events, 75 payments, 1113 customer_orders
  - [x] Pre-48/49 fixes: C1 — schema_evolution.py fully implemented (was returning [] stub from engines/schema_evolution.py; real engine is src/flowform/schema_evolution.py — confirmed wired in cli.py); test_schema_evolution.py replaced with 9 new tests (below-threshold, day-60/90/120 firing, no-double-fire, flag persistence across save/reload, weekend suppression, holiday-shifted threshold, 180-day end-to-end 3 records); C2 — negative_quantity_snapshot noise fixed to mutate positions[*].on_hand not top-level quantity_on_hand; existing snapshot test updated; 1 new test; I1 — maintenance burst in late_arriving.apply() now guarded by is_business_day() to prevent firing on weekends; 1 new test; I2 — (removed: false TMS duplicate warning from cli.py; each day writes to a new date-stamped directory so resuming is always safe);I3 — orphan_reference now generates plausible historical IDs (ORD-20XX-NNNNNN) instead of hardcoded ORD-9999-999999; existing test updated, 1 new test; M1 — out_of_sequence timestamp floored at simulation start date (2026-01-05) to prevent pre-simulation timestamps; 1 new test; M2 — ERP_DEFER_PROB/TMS_LATE_PROB/MAINTENANCE_PROB moved to config.yaml + NoiseConfig, read from config in late_arriving.apply(); config-value assertion added to existing test; M3 — TMS_LOAD_TYPE renamed to MAINTENANCE_BURST_TARGET_TYPE in late_arriving.py; M4 — test_phase5_integration.py reproducibility test strengthened to compare first payment amount + sorted load IDs in addition to total event count
  - [x] Step 48: Performance profiling + optimisation — src/flowform/profiler.py (cProfile + tracemalloc, ProfileResult dataclass, _parse_top_functions); scripts/profile_run.py (--days N, --no-reset); 4 optimisations: A (credit._credit_dirty set cleared at start of business-day run), B (state._dirty tracking + mark_dirty() + partial _write_operational()), C (injector early-exit when all rates 0.0), D (allocation sort hash cache state._orders_snapshot_hash/_sorted_order_ids); docs/performance.md with measured benchmarks (30d=21.5s, 90d=68.5s, 180d=145.4s, ~0.8s/day, 8.2 MB peak); 4 new tests in test_profiler.py
  - [x] Step 49: README.md — 8 sections: quick-start, CLI reference, output structure, configuration table, data engineering challenges, system architecture (20-step day loop + entity hierarchy), schema evolution table, reproducibility, test/performance references

**Test count: 794 passing** (13 config + 63 calendar + 62 constraints + 19 generator + 18 pricing + 39 weights + 12 customers + 32 carriers/warehouses + 16 state + 14 CLI + 24 output writer + 22 integration + 6 exchange rates + 10 production + 10 orders + 12 credit check + 11 inventory movements + 15 allocation + 14 modifications + 43 phase2 integration + 13 load planning + 16 load lifecycle + 11 carrier events + 10 carrier events step27 + 11 pod + 12 returns + 12 transfers + 19 phase3 integration + 13 payments + 14 demand signals + 14 demand plans + 14 demand forecasts + 14 inventory targets + 13 master data changes + 14 snapshots + 9 schema evolution + 20 phase4 integration + 15 schema evolution orders + 13 late arriving + 14 writer hardening + 21 noise injector + 15 phase5 integration + 15 supply disruptions + 4 profiler)
