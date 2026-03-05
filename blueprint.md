# FlowForm Industries — Simulation Engine Blueprint (v2)

## 1. What We're Building

A Python-based simulation engine that generates realistic supply chain event data for a B2B manufacturing company. The engine simulates one business day at a time, maintains persistent state between runs, and outputs raw JSON files organized by source system and date. Everything downstream (ETL, warehouse, dashboards, orchestration) is out of scope — this is purely the data generator.

---

## 2. The Company

**FlowForm Industries** — mid-size manufacturer of industrial control valves. Single product family, massive variation matrix, B2B only. Headquartered in Katowice, Poland. Sells domestically and across the EU.

### Product: Industrial Control Valves

| Dimension | Values | Count |
|---|---|---|
| Valve Type | Gate, Ball, Butterfly, Globe, Check, Needle | 6 |
| Nominal Size (DN) | 15, 25, 40, 50, 80, 100, 150, 200, 250, 300 | 10 |
| Body Material | Carbon Steel, SS304, SS316, Duplex, Brass | 5 |
| Pressure Class | PN10, PN16, PN25, PN40, PN63 | 5 |
| Connection | Flanged, Threaded, Welded, Wafer | 4 |
| Actuation | Manual, Pneumatic, Electric, Hydraulic | 4 |

Theoretical combinations: 24,000. After constraint filtering, the active catalog lands at **~2,000–3,000 SKUs**.

### Constraint Examples

- Needle valves only available in DN15–DN50
- Hydraulic actuation only for DN100+
- Brass only available in PN10, PN16
- Wafer connection only for Butterfly valves
- Duplex material not available with manual actuation
- Globe valves not available with Wafer connection
- Check valves only Manual or Pneumatic actuation
- DN250+ not available in Brass

The catalog is master data — stable across runs, but see Section 10 for lifecycle changes (SKU discontinuation).

### SKU Encoding

`FF-{TYPE}{DN}-{MAT}-{PN}-{CONN}{ACT}`

Example: `FF-BL100-S316-PN25-FP` = Ball valve, DN100, Stainless 316, PN25, Flanged, Pneumatic.

Type codes: GT=Gate, BL=Ball, BF=Butterfly, GL=Globe, CK=Check, ND=Needle
Material codes: CS=Carbon Steel, S304=SS304, S316=SS316, DPX=Duplex, BRS=Brass
Connection codes: F=Flanged, T=Threaded, W=Welded, X=Wafer
Actuation codes: M=Manual, P=Pneumatic, E=Electric, H=Hydraulic

### Product Weights

Each SKU has a unit weight in kg derived from size, material, valve type, and actuation:

| DN | Base Weight (CS) kg |
|---|---|
| 15 | 1.2 |
| 25 | 2.1 |
| 40 | 3.8 |
| 50 | 5.5 |
| 80 | 12.0 |
| 100 | 18.5 |
| 150 | 35.0 |
| 200 | 58.0 |
| 250 | 85.0 |
| 300 | 120.0 |

Material multipliers: CS 1.0, SS304 1.0, SS316 1.0, Duplex 1.05, Brass 0.95.
Valve type multipliers: Gate 1.0, Ball 0.9, Butterfly 0.6, Globe 1.1, Check 0.8, Needle 0.5.
Actuation adders: Manual +0%, Pneumatic +15%, Electric +25%, Hydraulic +40%.

Weight is critical because TMS uses it for load planning and freight cost.

---

## 3. Customers

50–80 active B2B customers across 6 segments:

| Segment | % Revenue | Ordering Pattern | Typical Order | SKU Preference |
|---|---|---|---|---|
| Oil & Gas Operators | 30% | Large quarterly blanket orders, weekly call-offs | 50–500 units | High-spec (SS316, Duplex, PN40+) |
| Water Utilities | 20% | Predictable monthly, seasonal spring peak | 20–200 units | Mid-spec (CS, SS304, PN16–25) |
| Chemical Plants | 20% | Irregular, clustered around planned shutdowns (1–2x/year) | 100–1000 during shutdown, near-zero otherwise | High-spec, corrosion resistant |
| Industrial Distributors | 15% | Frequent small replenishment orders | 5–50 units | Broad mix, mostly standard |
| HVAC Contractors | 10% | Project-based, lumpy | 10–100 units | Small DN, Brass, manual |
| Shipyards | 5% | Very large, very rare, tied to vessel build schedules | 200–2000 units | High-spec, large DN |

### Customer Master Data

Each customer record includes:
- `customer_id`, company name, segment
- Country code and region (PL, DE, CZ, NL, NO, SE, etc.)
- Primary and secondary delivery addresses
- Credit limit (in customer's currency), payment terms (Net 30/60/90)
- `currency`: PLN (Polish domestic ~65%), EUR (EU customers ~30%), USD (rare ~5%)
- Preferred carrier
- Ordering profile: base frequency, average order size, SKU affinity weights
- Contract discount percentage
- Active/inactive status, onboarding date
- `accepts_deliveries_december`: boolean — many customers close warehouses Dec 23–31
- `shutdown_months`: list — Chemical Plant customers have defined shutdown months

### Customer Credit System

Each customer has a credit limit. The simulation tracks outstanding balances (invoiced but unpaid):
- When outstanding balance exceeds credit limit, new orders go to `credit_hold` status
- Credit-held orders do NOT enter allocation or shipment planning
- When a payment arrives and balance drops below limit, held orders release automatically
- This creates a branching order lifecycle that pipelines must handle

---

## 4. Warehouses & Carriers

### Warehouses

| Code | Location | Role | Capacity Share |
|---|---|---|---|
| W01 | Katowice | Main production + distribution hub | 80% of inventory |
| W02 | Gdańsk | Port warehouse for shipyard/export orders | 20% of inventory |

Inter-warehouse transfers occur when Gdańsk stock drops below safety level or a large shipyard order can't be fulfilled locally.

### Carriers

| Carrier | Code | Reliability | Speed (days) | Cost Tier | Primary Use | Weight Unit |
|---|---|---|---|---|---|---|
| DHL Freight | DHL | 92% | 3–5 | High | International, large | kg |
| DB Schenker | DBSC | 89% | 3–5 | Medium | Domestic + EU | kg |
| Raben Group | RABEN | 85% | 1–3 | Medium | Poland + CEE | kg |
| Geodis | GEODIS | 90% | 3–5 | High | Western EU | kg |
| BalticHaul | BALTIC | 80% | 1 | Low | Gdańsk, shipyard | **lbs** |

**BalticHaul reports weights in pounds** (legacy US-based software). The `weight_unit` field on TMS events indicates the unit, but in the noise layer it's sometimes missing. Your ETL must handle unit conversion.

### Carrier Reliability Seasonality

On-time rates are not constant:

| Period | Reliability Modifier |
|---|---|
| November–February | −5% (winter roads) |
| Week of Easter | −8% (reduced staff) |
| July–August | −3% (vacation coverage) |
| December 20–31 | −15% (holiday skeleton crews) |
| Rest of year | baseline |

---

## 5. Seasonality & Calendar

### Polish Public Holidays (No Production, No Outbound Shipping)

| Date | Holiday |
|---|---|
| January 1 | New Year's Day |
| January 6 | Epiphany |
| Moveable | Easter Monday |
| May 1 | Labour Day |
| May 3 | Constitution Day |
| Moveable | Corpus Christi (60 days after Easter) |
| August 15 | Assumption of Mary |
| November 1 | All Saints' Day |
| November 11 | Independence Day |
| December 25 | Christmas Day |
| December 26 | Second Day of Christmas |

On public holidays: no production, no new outbound dispatches. But in-transit loads still advance (carriers operate), EDI orders can still arrive, no payments processed.

The simulation must calculate Easter dynamically for each simulated year.

### Day-of-Week Patterns

Multipliers on the base daily rate:

| Activity | Mon | Tue | Wed | Thu | Fri | Sat | Sun |
|---|---|---|---|---|---|---|---|
| Production | 0.80 | 1.00 | 1.10 | 1.10 | 0.85 | 0 | 0 |
| Orders arriving | 0.90 | 1.05 | 1.05 | 1.00 | 0.85 | 0.10 | 0.05 |
| Carrier pickups | Yes | Yes | Yes | Yes | Yes | Critical only | No |
| Payment processing | Yes | Yes | Yes | Yes | Yes | No | No |

Saturday/Sunday orders are EDI auto-replenishment from Industrial Distributors only.

### Monthly Demand Curve

| Month | Multiplier | Notes |
|---|---|---|
| January | 0.70 | Post-holiday hangover |
| February | 0.85 | Budget allocation, ramp-up |
| March | 1.15 | Spring infrastructure begins |
| April | 1.25 | Peak — Water Utilities spring push |
| May | 1.05 | Steady, holidays reduce working days |
| June | 1.00 | Baseline |
| July | 0.75 | Summer vacation (EU-wide) |
| August | 0.80 | Vacation continues, late-month ramp |
| September | 1.15 | Autumn push, Chemical Plant shutdowns |
| October | 1.10 | Pre-winter prep |
| November | 1.15 | Pre-winter rush + Q4 budget flush |
| December | 0.55 | Winds down hard after week 2 |

### Segment-Specific Seasonal Overrides

Applied ON TOP of the base monthly curve:

- **Water Utilities**: +30% in March–April, −20% in Dec–Jan
- **Chemical Plants**: +200% during their defined shutdown months, −60% otherwise
- **HVAC Contractors**: +40% Sep–Nov (heating season prep), −30% Jun–Aug
- **Industrial Distributors**: +25% November (Q4 budget flush), −15% Jul–Aug
- **Shipyards**: no seasonal pattern (project-driven)
- **Oil & Gas**: +10% in Q1 and Q3 (blanket order renewals)

### Year-End Behavior

- Customers with `accepts_deliveries_december = false` won't accept deliveries after Dec 20
- Orders placed in late December get `requested_delivery_date` in January
- Production drops to ~30% in the last week of December
- Carrier reliability drops sharply (see Section 4)

---

## 6. Source Systems & Event Schemas

Four independent systems, each with its own file cadence, schema quirks, and notion of time.

### Output Structure

```
output/
├── erp/
│   ├── production_completions/       # daily batch
│   │   └── 2026-03-01.json
│   ├── production_reclassifications/  # daily batch (often empty)
│   ├── customer_orders/              # daily batch
│   ├── inventory_movements/          # daily batch
│   ├── inventory_snapshots/          # daily batch (one per warehouse)
│   ├── order_modifications/          # daily batch
│   ├── order_cancellations/          # daily batch
│   ├── exchange_rates/               # daily
│   └── master_data_changes/          # daily batch (often empty)
├── tms/
│   ├── loads/                        # 3x daily syncs
│   │   ├── 2026-03-01_08.json
│   │   ├── 2026-03-01_14.json
│   │   └── 2026-03-01_20.json
│   ├── carrier_events/              # daily batch
│   └── returns/                     # daily batch
├── adm/
│   ├── demand_signals/              # per-event file
│   │   └── SIG-20260301-0012.json
│   ├── demand_plans/                # monthly
│   └── inventory_targets/           # monthly
└── forecast/
    └── demand_forecasts/            # monthly
```

### File Cadence

| System | Cadence | Notes |
|---|---|---|
| ERP | Daily batch | One file per event type per day, written at 17:00 |
| TMS | 3x daily | Files at 08:00, 14:00, 20:00. Events accumulate between syncs. |
| ADM signals | Per-event | Each signal is its own file |
| ADM plans/targets | Monthly | 1st business day |
| Forecast | Monthly | 1st business day |

Your pipeline must handle all these cadences with different ingestion logic.

### Late-Arriving Data

- ~5% of TMS events have `event_timestamp` from previous business day but land in current sync
- ~2% of ERP inventory movements land in next day's file
- Monthly TMS maintenance window (4 hours) causes event burst in next sync with correct but out-of-sequence timestamps

This forces a core ETL decision: partition by file date or event timestamp?

### 6.1 ERP Events

**Production Completions**

```json
{
  "event_type": "production_completion",
  "batch_id": "BATCH-20260301-0042",
  "timestamp": "2026-03-01T06:15:00Z",
  "sku": "FF-BL100-S316-PN25-FP",
  "quantity": 48,
  "warehouse": "W01",
  "quality_grade": "A",
  "production_line": "LINE-03"
}
```

Quality: A ~90%, B ~8%, REJECT ~2% (does not enter inventory).

**Retroactive Reclassification**: ~1% of Grade A batches get downgraded to B after QC review 2–5 days later:

```json
{
  "event_type": "production_reclassification",
  "reclassification_id": "RECL-20260305-0002",
  "timestamp": "2026-03-05T10:00:00Z",
  "original_batch_id": "BATCH-20260301-0042",
  "original_date": "2026-03-01",
  "sku": "FF-BL100-S316-PN25-FP",
  "quantity_affected": 12,
  "old_grade": "A",
  "new_grade": "B",
  "reason": "post_production_qc_review"
}
```

**Customer Orders**

```json
{
  "event_type": "customer_order",
  "order_id": "ORD-2026-018742",
  "timestamp": "2026-03-01T09:22:00Z",
  "customer_id": "CUST-0037",
  "requested_delivery_date": "2026-03-15",
  "priority": "standard",
  "status": "confirmed",
  "currency": "EUR",
  "lines": [
    {
      "line_number": 1,
      "sku": "FF-GT200-CS-PN40-FE",
      "quantity_ordered": 24,
      "quantity_allocated": 0,
      "quantity_shipped": 0,
      "quantity_backordered": 0,
      "unit_price": 385.00,
      "line_status": "open"
    }
  ]
}
```

Order statuses: `confirmed` → `credit_hold` (if credit exceeded) → `processing` → `partially_shipped` → `shipped` → `delivered` → `closed`. Also: `cancelled`.

Line statuses: `open` → `partially_allocated` → `allocated` → `picked` → `shipped` → `delivered` → `closed`. Also: `backordered`, `cancelled`.

**Schema Evolution**: After sim day 60, orders gain `sales_channel` ("direct", "edi", "portal"). After day 120, `incoterms` appears ("EXW", "FCA", "DAP", "DDP"). Before these days, the fields don't exist. Your ETL must handle evolving schemas.

**Inventory Movements**

```json
{
  "event_type": "inventory_movement",
  "movement_id": "MOV-20260301-0188",
  "timestamp": "2026-03-01T11:45:00Z",
  "movement_type": "pick",
  "sku": "FF-GT200-CS-PN40-FE",
  "warehouse": "W01",
  "quantity": -24,
  "reference_type": "order",
  "reference_id": "ORD-2026-018742",
  "line_number": 1,
  "balance_after": 312
}
```

Movement types: `receipt`, `pick`, `unpick`, `transfer_out`, `transfer_in`, `adjustment`, `scrap`, `return_restock`.

**Inventory Snapshots** — daily full dump per warehouse at end of day:

```json
{
  "event_type": "inventory_snapshot",
  "snapshot_id": "SNAP-20260301-W01",
  "timestamp": "2026-03-01T23:59:00Z",
  "warehouse": "W01",
  "positions": [
    {
      "sku": "FF-BL100-S316-PN25-FP",
      "quantity_on_hand": 340,
      "quantity_allocated": 85,
      "quantity_available": 255
    }
  ]
}
```

The snapshot and running balance from movements will NOT always agree — timing, late-arriving movements, rounding. Reconciling them is a core DE exercise.

**Order Modifications** (~10% of open orders)

```json
{
  "event_type": "order_modification",
  "modification_id": "MOD-20260302-0015",
  "timestamp": "2026-03-02T14:30:00Z",
  "order_id": "ORD-2026-018742",
  "modification_type": "quantity_change",
  "line_number": 1,
  "field_changed": "quantity",
  "old_value": 24,
  "new_value": 36,
  "reason": "customer_request"
}
```

Types: quantity_change, date_change, line_addition, line_removal, priority_change.

**Order Cancellations** (~2–3% of orders)

```json
{
  "event_type": "order_cancellation",
  "cancellation_id": "CAN-20260305-0003",
  "timestamp": "2026-03-05T10:00:00Z",
  "order_id": "ORD-2026-018600",
  "scope": "full",
  "reason": "customer_budget_freeze",
  "lines_cancelled": [1, 2, 3]
}
```

**Exchange Rates** — daily:

```json
{
  "event_type": "exchange_rate",
  "date": "2026-03-01",
  "rates": {
    "EUR_PLN": 4.32,
    "USD_PLN": 4.07,
    "EUR_USD": 1.06
  }
}
```

Rates fluctuate via small random walk (±0.3%/day, mean-reverting around EUR/PLN 4.30, USD/PLN 4.05).

**Master Data Changes** — SCD source events:

```json
{
  "event_type": "master_data_change",
  "change_id": "MDC-20260315-0001",
  "timestamp": "2026-03-15T09:00:00Z",
  "entity_type": "customer",
  "entity_id": "CUST-0037",
  "field_changed": "delivery_address_primary",
  "old_value": {"street": "ul. Kościuszki 12", "city": "Kraków", "postal": "30-001"},
  "new_value": {"street": "ul. Mickiewicza 45", "city": "Kraków", "postal": "30-059"},
  "effective_date": "2026-04-01",
  "reason": "office_relocation"
}
```

Frequencies: ~2 address changes/month, ~1 segment change/quarter, ~1 payment terms change/quarter, carrier rate change once (~day 90), ~5 SKU discontinuations/quarter.

### 6.2 TMS Events

**Load Lifecycle** — a load is a physical transport (truck/container), the core TMS entity.

Entity hierarchy:
- **Order line** (ERP) → gets allocated/picked
- **Shipment** → logical grouping of order lines for same customer
- **Load** → physical vehicle carrying one or more shipments

Relationships:
- One load can carry multiple shipments (LTL/groupage)
- One shipment belongs to exactly one load
- One shipment can reference multiple orders (consolidation)
- One order can split across multiple shipments (partial fulfillment)

```json
{
  "event_type": "load_event",
  "load_id": "LOAD-2026-004821",
  "event_id": "LE-20260301-0088",
  "timestamp": "2026-03-01T14:30:00Z",
  "status": "dispatched",
  "carrier_code": "DHL",
  "origin_warehouse": "W01",
  "destination_region": "PL-MA",
  "shipments": [
    {
      "shipment_id": "SHP-2026-008821",
      "customer_id": "CUST-0037",
      "address_id": "ADDR-0037-02",
      "order_refs": ["ORD-2026-018742"],
      "line_refs": [
        {"order_id": "ORD-2026-018742", "line_number": 1, "quantity_shipped": 24}
      ],
      "parcels": 4,
      "weight_value": 892.5,
      "weight_unit": "kg"
    }
  ],
  "total_weight_value": 892.5,
  "total_weight_unit": "kg",
  "estimated_pickup": "2026-03-01T13:00:00Z",
  "actual_pickup": "2026-03-01T13:45:00Z",
  "estimated_delivery": "2026-03-04",
  "actual_delivery": null,
  "exception": null,
  "freight_cost": 485.00,
  "freight_currency": "EUR"
}
```

Load statuses:
```
load_created → carrier_assigned → pickup_scheduled → pickup_completed →
in_transit → at_hub → out_for_delivery → delivery_completed → pod_received
```

Exception statuses: `delayed`, `failed_delivery`, `damaged_in_transit`, `customs_hold`, `returned_to_sender`.

Each status change = new event (same `load_id`, new `event_id`). A single load generates 5–10 events over its lifecycle.

**POD**: `pod_received` arrives 1–3 business days after `delivery_completed` (signed confirmation uploaded into TMS).

**Carrier Events**

```json
{
  "event_type": "carrier_event",
  "event_id": "CE-20260301-0044",
  "timestamp": "2026-03-01T16:00:00Z",
  "carrier_code": "RABEN",
  "event_subtype": "capacity_alert",
  "affected_region": "PL-SL",
  "message": "Reduced capacity Silesia region, 2-day delay expected",
  "duration_days": 2,
  "impact_severity": "medium"
}
```

Subtypes: `capacity_alert`, `rate_change`, `service_disruption`, `maintenance_window`, `holiday_schedule`.

**Returns / RMA** — ~1.5% of delivered shipments get a return within 7 days:

```json
{
  "event_type": "return_event",
  "return_id": "RMA-2026-000142",
  "event_id": "RE-20260310-0008",
  "timestamp": "2026-03-10T11:00:00Z",
  "status": "return_authorized",
  "original_load_id": "LOAD-2026-004821",
  "original_shipment_id": "SHP-2026-008821",
  "customer_id": "CUST-0037",
  "reason": "quality_complaint",
  "lines": [
    {"sku": "FF-GT200-CS-PN40-FE", "quantity": 4, "disposition": "pending_inspection"}
  ]
}
```

Return statuses: `return_requested` → `return_authorized` → `pickup_scheduled` → `in_transit_return` → `received_at_warehouse` → `inspected` → `restocked` or `scrapped`.

After inspection: ~60% restocked Grade A, ~25% restocked Grade B, ~15% scrapped. Restock triggers `inventory_movement` type `return_restock`.

### 6.3 ADM (Active Demand Management) Events

**Demand Signals**

```json
{
  "event_type": "demand_signal",
  "signal_id": "SIG-20260301-0012",
  "timestamp": "2026-03-01T08:00:00Z",
  "signal_source": "customer_communication",
  "customer_id": "CUST-0015",
  "product_group": "BL-DN100-S316",
  "signal_type": "upside_risk",
  "magnitude": "medium",
  "horizon_weeks": 8,
  "description": "Customer indicated potential expansion project requiring 2x normal quarterly volume",
  "confidence": 0.6
}
```

Sources: `customer_communication`, `market_intelligence`, `seasonal_model`, `consumption_trend`, `contract_renewal`, `competitor_event`.
Types: `upside_risk`, `downside_risk`, `timing_shift`, `mix_change`, `new_demand`.

**Demand Plans** — monthly consensus (1st business day):

```json
{
  "event_type": "demand_plan",
  "plan_id": "DP-2026-03",
  "timestamp": "2026-03-01T07:00:00Z",
  "planning_horizon_months": 6,
  "plan_lines": [
    {
      "product_group": "BL-DN100-S316",
      "customer_segment": "oil_gas",
      "periods": [
        {"month": "2026-03", "planned_quantity": 480, "confidence_low": 380, "confidence_high": 580},
        {"month": "2026-04", "planned_quantity": 520, "confidence_low": 390, "confidence_high": 650}
      ]
    }
  ]
}
```

**Inventory Targets**

```json
{
  "event_type": "inventory_target",
  "target_id": "TGT-20260301-BL100",
  "timestamp": "2026-03-01T07:30:00Z",
  "sku": "FF-BL100-S316-PN25-FP",
  "warehouse": "W01",
  "safety_stock": 120,
  "reorder_point": 200,
  "target_stock": 350,
  "review_trigger": "demand_plan_update"
}
```

### 6.4 Forecast System Events

**Statistical Demand Forecasts** — monthly, algorithm-driven, intentionally 10–25% off from actuals:

```json
{
  "event_type": "demand_forecast",
  "forecast_id": "FCST-2026-03",
  "timestamp": "2026-03-01T06:00:00Z",
  "model_version": "v2.3",
  "horizon_months": 6,
  "lines": [
    {
      "sku": "FF-BL100-S316-PN25-FP",
      "periods": [
        {"month": "2026-03", "forecast_quantity": 145, "lower_bound": 110, "upper_bound": 185}
      ]
    }
  ]
}
```

Forecast and demand plan are deliberately separate and don't always agree.

**Model version change**: At sim day 90, model upgrades from v2.3 to v3.0 (wider confidence intervals, slightly different estimates). Tests tracking across model versions.

---

## 7. Backorders & Partial Fulfillment

### Allocation Logic

Each day during allocation:
1. Sort open order lines by: priority (critical > express > standard), then requested_delivery_date (earliest first), then order_id (FIFO).
2. For each line, check available inventory (on_hand − allocated) at the assigned warehouse.
3. **Full fill**: available ≥ ordered → allocate full quantity, line_status → `allocated`.
4. **Partial fill**: 0 < available < ordered → allocate what's available, line_status → `partially_allocated`, remainder → backorder.
5. **No fill**: available = 0 → entire line → `backordered`.

### Backorder Behavior

- Re-evaluated every day during allocation (new production may fill them)
- >14 calendar days unresolved → auto-escalates to `express` priority
- >30 calendar days → 15% daily probability customer cancels it
- Tracked via `backorder_created` and `backorder_released` inventory movements

### Partial Shipments

- Allocated portion can ship immediately, creating a shipment for just that quantity
- Backordered remainder ships separately when eventually fulfilled
- One order line can result in multiple shipments over time
- Per-line tracking: `quantity_ordered`, `quantity_allocated`, `quantity_shipped`, `quantity_backordered`
- Invariant: `quantity_allocated + quantity_shipped + quantity_backordered ≤ quantity_ordered`

---

## 8. Currency & Exchange Rates

- Polish customers: PLN (~65%)
- EU customers: EUR (~30%)
- Rare: USD (~5%)
- Catalog base prices are in PLN, converted at contract rate at order time
- Each order includes `currency` with prices in that currency
- Daily exchange rate file in `output/erp/exchange_rates/`
- Rates fluctuate via random walk (±0.3%/day, mean-reverting)
- Analytics challenge: which rate to use (order date, delivery date, payment date) for different reports

---

## 9. Data Quality Noise

Configurable via `noise_profile`:

| Noise Type | Light | Medium | Heavy |
|---|---|---|---|
| Duplicate events | 0.2% | 0.5% | 1.5% |
| Missing required fields | 0.3% | 1.0% | 3.0% |
| Wrong timezone on timestamps | 0.1% | 0.5% | 2.0% |
| SKU whitespace/case errors | 0% | 0.3% | 1.0% |
| Customer ID mismatch (ERP vs TMS) | 0% | 0.2% | 1.0% |
| Decimal precision drift | 0.1% | 0.5% | 1.5% |
| Future-dated events (clock skew) | 0% | 0.1% | 0.5% |
| Orphan references | 0% | 0.1% | 0.5% |
| Encoding issues (unicode) | 0% | 0.2% | 0.8% |
| Out-of-sequence events | 0% | 0.3% | 1.0% |
| Missing weight_unit on TMS | 0% | 0.5% | 2.0% |
| Negative quantity in snapshot | 0% | 0.1% | 0.3% |
| Duplicate load_id across sync files | 0% | 0.2% | 0.5% |

Applied as post-processing. Core logic stays clean.

---

## 10. Simulation Engine Architecture

### State Object (SQLite)

- `current_date`, `sim_day_number`
- `inventory` — per-SKU, per-warehouse: on_hand, allocated, available
- `open_orders` — with line-level fulfillment tracking
- `backorders` — separate queue with aging
- `credit_holds` — orders blocked by credit limit
- `active_loads` — loads between load_created and pod_received
- `active_returns` — returns in progress
- `customer_balances` — outstanding amounts (for credit checks)
- `production_pipeline` — batches in production (1–5 day lead times)
- `demand_signals_active`, `demand_plan_current`
- `exchange_rates` — current rates
- `counters` — auto-incrementing IDs per entity
- `rng_state` — reproducibility

### Day Loop

1. **Calendar Check** — business day, weekend, or holiday? Set flags.
2. **Exchange Rates** — daily (including weekends).
3. **Production** — (business days only) complete batches, start new ones, reclassifications.
4. **Demand Signals** — (business days only).
5. **Order Generation** — (mostly business days; EDI on weekends from Distributors). Apply all seasonality layers.
6. **Credit Check** — evaluate new/existing orders vs credit limits.
7. **Order Modifications & Cancellations** — (business days only). Includes backorder aging cancellations.
8. **Order Allocation** — (business days only). Priority-based with partial fill + backorder.
9. **Load Planning** — (business days, not holidays). Group allocated lines → shipments → loads.
10. **Load Lifecycle** — (every day — carriers operate weekends). Advance active loads, apply seasonal reliability.
11. **Returns Processing** — (business days only).
12. **Payment Processing** — (business days only). Updates balances, triggers credit hold releases.
13. **Inter-Warehouse Transfers** — (business days only).
14. **Demand Planning** — (1st business day of month).
15. **Master Data Changes** — (random, ~2–3/month).
16. **Inventory Snapshots** — (business days only, end of day).
17. **Schema Evolution Check** — activate new fields at day thresholds.
18. **Data Quality Noise** — post-process all events.
19. **Write Events** — per-system cadence.
20. **Save State** — persist to SQLite.

### CLI

```
python simulate.py --days 30
python simulate.py --days 1
python simulate.py --until 2026-06-30
python simulate.py --reset
python simulate.py --status
```

---

## 11. Configuration

```yaml
simulation:
  start_date: "2026-01-05"
  seed: 42
  country: "PL"

company:
  daily_production_range: [200, 800]
  production_stoppage_probability: 0.025
  production_lines: 5
  warehouse_transfer_threshold: 0.3

customers:
  count: 60
  churn_rate_monthly: 0.01
  new_customer_rate_monthly: 0.02
  currency_distribution:
    PLN: 0.65
    EUR: 0.30
    USD: 0.05

demand:
  base_monthly_multipliers:
    1: 0.70
    2: 0.85
    3: 1.15
    4: 1.25
    5: 1.05
    6: 1.00
    7: 0.75
    8: 0.80
    9: 1.15
    10: 1.10
    11: 1.15
    12: 0.55
  signal_probability_daily: 0.05

orders:
  modification_probability: 0.10
  cancellation_probability: 0.025
  express_share: 0.12
  critical_share: 0.03
  backorder_escalation_days: 14
  backorder_cancel_probability_after_30d: 0.15

loads:
  exception_probability: 0.08
  consolidation_window_days: 3
  return_probability: 0.015
  pod_delay_days: [1, 3]

payments:
  on_time_probability: 0.82
  late_probability: 0.15
  very_late_probability: 0.03

exchange_rates:
  base:
    EUR_PLN: 4.30
    USD_PLN: 4.05
  daily_volatility: 0.003

schema_evolution:
  sales_channel_from_day: 60
  incoterms_from_day: 120
  forecast_model_upgrade_day: 90

noise:
  profile: "medium"

disruptions:
  enabled: false
```

---

## 12. Project Structure

```
flowform-sim/
├── .claude/
│   ├── CLAUDE.md
│   └── agents/
│       └── sim-engine.md
├── .gitignore
├── src/
│   └── flowform/
│       ├── __init__.py
│       ├── cli.py
│       ├── config.py
│       ├── state.py
│       ├── calendar.py                # holiday calendar, business day logic
│       ├── catalog/
│       │   ├── __init__.py
│       │   ├── constraints.py
│       │   ├── generator.py
│       │   ├── pricing.py
│       │   └── weights.py
│       ├── master_data/
│       │   ├── __init__.py
│       │   ├── customers.py
│       │   ├── carriers.py
│       │   └── warehouses.py
│       ├── engines/
│       │   ├── __init__.py
│       │   ├── exchange_rates.py
│       │   ├── production.py
│       │   ├── demand_signals.py
│       │   ├── orders.py
│       │   ├── credit.py
│       │   ├── modifications.py
│       │   ├── allocation.py
│       │   ├── load_planning.py
│       │   ├── load_lifecycle.py
│       │   ├── returns.py
│       │   ├── payments.py
│       │   ├── transfers.py
│       │   ├── planning.py
│       │   ├── master_data_changes.py
│       │   └── snapshots.py
│       ├── noise/
│       │   ├── __init__.py
│       │   └── injector.py
│       ├── schema_evolution.py
│       └── output/
│           ├── __init__.py
│           └── writer.py
├── config.yaml
├── requirements.txt
├── tests/
├── output/
├── state/
├── venv/
├── pyproject.toml
└── README.md
```

---

## 13. Tech Stack

| Component | Choice |
|---|---|
| Python | 3.12+ |
| Package manager | venv + pip |
| Data models | Pydantic v2 |
| State | SQLite (stdlib) |
| Fake data | Faker |
| Config | PyYAML or tomllib |
| CLI | click or typer |
| Testing | pytest |
| Linting | ruff |

---

## 14. Implementation Phases

### Phase 1: Skeleton & Master Data (Steps 1–13)

1. pyproject.toml, requirements.txt, .gitignore, directory structure
2. Config loader + Pydantic validation
3. Polish holiday calendar + business day logic (Easter calculation)
4. SKU constraint engine
5. Catalog generator
6. Pricing engine
7. Weight calculator
8. Customer master data generator (segments, currencies, credit limits, seasonal profiles)
9. Carrier + warehouse definitions (weight unit quirks)
10. SimulationState + SQLite persistence
11. CLI skeleton
12. JSON output writer with per-system cadence support
13. Test: --reset, --status

### Phase 2: Core Day Loop (Steps 14–22)

14. Exchange rate generator
15. Production engine (rhythm, holidays, reclassifications)
16. Order generation (all seasonality: monthly × segment × day-of-week × holiday, weekend EDI)
17. Credit check engine
18. Inventory movement tracker
19. Allocation with backorders + partial fulfillment
20. Modifications + cancellations (including backorder aging)
21. Wire steps 1–8 of day loop
22. Test: 14 days spanning a weekend + holiday

### Phase 3: Logistics (Steps 23–31)

23. Load planning (order lines → shipments → loads, consolidation)
24. Load lifecycle (status progression, transit, seasonal carrier reliability)
25. Load exceptions
26. POD handling
27. Carrier events
28. Returns / RMA flow
29. Inter-warehouse transfers
30. Wire steps 9–13 of day loop
31. Test: 30 days

### Phase 4: Demand Management & Financials (Steps 32–40)

32. Payment processing (on-time/late/very-late, credit limit interaction)
33. Demand signals
34. Demand plans
35. Forecasts (with model version change at day 90)
36. Inventory targets
37. Master data changes (SCD events)
38. Inventory snapshots
39. Wire steps 14–16 of day loop
40. Test: 90 days

### Phase 5: Realism & Polish (Steps 41–49)

41. Schema evolution (sales_channel day 60, incoterms day 120)
42. Late-arriving data logic
43. Multi-cadence file writer (daily, 3x daily, per-event, monthly)
44. Noise injector (all types)
45. Wire steps 17–18 of day loop
46. Disruptions (optional)
47. Full 180-day simulation run + review
48. Performance profiling
49. README

---

## 15. Data Engineering Practice Challenges

Why each feature exists:

| Feature | DE Challenge |
|---|---|
| Multiple file cadences | Different ingestion patterns per source |
| Late-arriving data | File date ≠ event date, incremental load logic |
| Schema evolution | ETL handles missing fields, schema registry |
| Snapshots vs movements | Reconciliation, discrepancy detection |
| Load → Shipment → Order hierarchy | Many-to-many joins, fan-out/fan-in |
| Backorders + partial fulfillment | Non-linear order line lifecycle, multiple shipments per line |
| Credit holds | Order status branching |
| Currency + exchange rates | Conversion logic, rate selection per report type |
| Weight unit mismatch (kg vs lbs) | Unit conversion, missing metadata |
| Master data changes | SCD Type 2 dimensions |
| Returns | Separate fact or direction flag? Design decision |
| Noise | Deduplication, validation, quality framework |
| Forecast vs demand plan | Accuracy metrics, plan vs actual |
| Retroactive corrections | Idempotent loads, late-arriving facts |
| Carrier reliability seasonality | Time-varying dimensions |
| Holiday calendar | Business day calculations, SLA metrics |
| Payment behavior | DSO, aging buckets, cash flow |
