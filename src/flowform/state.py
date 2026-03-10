"""SimulationState: central state object + SQLite persistence.

All mutable simulation state lives here and is persisted to SQLite after
every simulated day.  This makes the simulation fully resumable and, because
all randomness flows through ``state.rng``, fully reproducible from a given
seed.

DB location: ``state/simulation.db`` (relative to CWD, or configured via
``config``).  The ``state/`` directory is created automatically if absent.
"""

from __future__ import annotations

import dataclasses
import json
import pickle
import random
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

from flowform.catalog.generator import CatalogEntry, generate_catalog
from flowform.config import Config
from flowform.master_data.carriers import ALL_CARRIERS, Carrier
from flowform.master_data.customers import Customer, generate_customers
from flowform.master_data.warehouses import ALL_WAREHOUSES, Warehouse

# ---------------------------------------------------------------------------
# Default DB path
# ---------------------------------------------------------------------------

_DEFAULT_DB_PATH = Path("state/simulation.db")

# ---------------------------------------------------------------------------
# JSON serialisation helpers for dataclasses
# ---------------------------------------------------------------------------


def _customer_to_dict(c: Customer) -> dict[str, Any]:
    """Serialise a Customer to a JSON-safe dict."""
    d = dataclasses.asdict(c)
    # date → ISO string
    d["onboarding_date"] = c.onboarding_date.isoformat()
    # seasonal_profile keys are ints; JSON requires string keys
    d["seasonal_profile"] = {str(k): v for k, v in c.seasonal_profile.items()}
    # ordering_profile sku_affinity dn keys are ints too
    sku_affinity = {}
    for dim, weights in c.ordering_profile.sku_affinity.items():
        sku_affinity[dim] = {str(k): v for k, v in weights.items()}
    d["ordering_profile"]["sku_affinity"] = sku_affinity
    return d


def _customer_from_dict(d: dict[str, Any]) -> Customer:
    """Deserialise a Customer from the dict produced by ``_customer_to_dict``."""
    from flowform.master_data.customers import CustomerAddress, OrderingProfile

    primary = CustomerAddress(**d["primary_address"])
    secondary = CustomerAddress(**d["secondary_address"]) if d["secondary_address"] else None

    # Restore int keys in seasonal_profile and dn weights inside sku_affinity
    seasonal_profile = {int(k): v for k, v in d["seasonal_profile"].items()}

    raw_affinity = d["ordering_profile"]["sku_affinity"]
    sku_affinity: dict[str, dict[str | int, float]] = {}
    for dim, weights in raw_affinity.items():
        if dim == "dn":
            sku_affinity[dim] = {int(k): v for k, v in weights.items()}
        else:
            sku_affinity[dim] = {k: v for k, v in weights.items()}

    ordering_profile = OrderingProfile(
        base_order_frequency_per_month=d["ordering_profile"]["base_order_frequency_per_month"],
        order_size_range=tuple(d["ordering_profile"]["order_size_range"]),  # type: ignore[arg-type]
        sku_affinity=sku_affinity,
    )

    return Customer(
        customer_id=d["customer_id"],
        company_name=d["company_name"],
        segment=d["segment"],
        country_code=d["country_code"],
        region=d["region"],
        primary_address=primary,
        secondary_address=secondary,
        credit_limit=d["credit_limit"],
        payment_terms_days=d["payment_terms_days"],
        currency=d["currency"],
        preferred_carrier=d["preferred_carrier"],
        contract_discount_pct=d["contract_discount_pct"],
        ordering_profile=ordering_profile,
        seasonal_profile=seasonal_profile,
        shutdown_months=d["shutdown_months"],
        accepts_deliveries_december=d["accepts_deliveries_december"],
        active=d["active"],
        onboarding_date=date.fromisoformat(d["onboarding_date"]),
    )


def _carrier_to_dict(c: Carrier) -> dict[str, Any]:
    return dataclasses.asdict(c)


def _carrier_from_dict(d: dict[str, Any]) -> Carrier:
    return Carrier(**d)


def _warehouse_to_dict(w: Warehouse) -> dict[str, Any]:
    return dataclasses.asdict(w)


def _warehouse_from_dict(d: dict[str, Any]) -> Warehouse:
    from flowform.master_data.warehouses import Warehouse as W
    return W(**d)


# ---------------------------------------------------------------------------
# Inventory seeding helper
# ---------------------------------------------------------------------------


def _seed_initial_inventory(
    catalog: list[CatalogEntry],
    warehouses: list[Warehouse],
    rng: random.Random,
) -> dict[str, dict[str, int]]:
    """Generate realistic starting inventory across warehouses.

    ~65% of catalog SKUs get initial stock.  W01 is primary (80% split);
    W02 gets a fraction of W01 stock ~50% of the time.

    Args:
        catalog:    Full catalog of 2 500 active SKUs.
        warehouses: List of Warehouse objects (must include W01 and W02).
        rng:        Seeded RNG for reproducibility.

    Returns:
        Nested dict ``{warehouse_code: {sku: quantity}}``.
    """
    inventory: dict[str, dict[str, int]] = {w.code: {} for w in warehouses}

    for entry in catalog:
        if rng.random() > 0.65:
            continue  # ~35% of SKUs not stocked at start

        w01_qty = rng.randint(20, 300)
        inventory["W01"][entry.sku] = w01_qty

        if rng.random() < 0.50:
            inventory["W02"][entry.sku] = max(1, int(w01_qty * rng.uniform(0.05, 0.25)))

    return inventory


# ---------------------------------------------------------------------------
# SimulationState
# ---------------------------------------------------------------------------


class SimulationState:
    """Central state object for the FlowForm supply chain simulation.

    Do **not** instantiate directly.  Use :meth:`from_new` or :meth:`from_db`.
    """

    # ------------------------------------------------------------------
    # Internal constructor
    # ------------------------------------------------------------------

    def __init__(
        self,
        *,
        db_path: Path,
        sim_day: int,
        current_date: date,
        start_date: date,
        rng: random.Random,
        catalog: list[CatalogEntry],
        customers: list[Customer],
        carriers: list[Carrier],
        warehouses: list[Warehouse],
        inventory: dict[str, dict[str, int]],
        allocated: dict[str, dict[str, int]],
        open_orders: dict[str, dict[str, Any]],
        customer_balances: dict[str, float],
        backorder_queue: list[dict[str, Any]],
        active_loads: dict[str, dict[str, Any]],
        in_transit_returns: list[dict[str, Any]],
        exchange_rates: dict[str, float],
        schema_flags: dict[str, bool],
        counters: dict[str, int],
        production_pipeline: list[dict[str, Any]],
        daily_production_receipts: list[dict[str, Any]] | None = None,
        carrier_disruptions: dict[str, dict[str, Any]] | None = None,
        pending_pod: dict[str, dict[str, Any]] | None = None,
        deferred_erp_movements: list[dict[str, Any]] | None = None,
        tms_maintenance_fired_month: str = "",
        supply_disruptions: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self._db_path = db_path
        self.sim_day = sim_day
        self.current_date = current_date
        self.start_date = start_date
        self.rng = rng
        self.catalog = catalog
        self.customers = customers
        self.carriers = carriers
        self.warehouses = warehouses
        self.inventory = inventory
        self.allocated = allocated
        self.open_orders = open_orders
        self.customer_balances = customer_balances
        self.backorder_queue = backorder_queue
        self.active_loads = active_loads
        self.in_transit_returns = in_transit_returns
        self.exchange_rates = exchange_rates
        self.schema_flags = schema_flags
        self.counters = counters
        self.production_pipeline = production_pipeline
        # Ephemeral: cleared at start of each day; NOT persisted to SQLite.
        # Populated by the production engine; read by inventory_movements engine.
        self.daily_production_receipts: list[dict[str, Any]] = (
            daily_production_receipts if daily_production_receipts is not None else []
        )
        # Active carrier disruptions keyed by carrier_code.
        # Each value is a dict with at least "end_date" and "severity".
        self.carrier_disruptions: dict[str, dict[str, Any]] = (
            carrier_disruptions if carrier_disruptions is not None else {}
        )
        # POD pending loads: key=load_id, value={load snapshot dict + delivery metadata}
        self.pending_pod: dict[str, dict[str, Any]] = (
            pending_pod if pending_pod is not None else {}
        )
        # ERP inventory movements deferred to the next day's output (~2% carry-forward).
        self.deferred_erp_movements: list[dict[str, Any]] = (
            deferred_erp_movements if deferred_erp_movements is not None else []
        )
        # Calendar month (YYYY-MM) when TMS maintenance burst last fired; "" if never.
        self.tms_maintenance_fired_month: str = tms_maintenance_fired_month
        # Active supply disruptions keyed by disruption_id.
        # Each value is a dict with at least "end_date", "severity", "disruption_category".
        self.supply_disruptions: dict[str, dict[str, Any]] = (
            supply_disruptions if supply_disruptions is not None else {}
        )

        # ---------------------------------------------------------------------------
        # Performance optimisation caches (ephemeral — not persisted to SQLite)
        # ---------------------------------------------------------------------------

        # Opt-A: Set of customer_ids whose credit exposure needs recomputing.
        # Populated by allocation/modifications when order status changes.
        # Cleared by credit.run() after processing.
        self._credit_dirty: set[str] = set()

        # Opt-B: Set of kv_state keys modified since the last save().
        # _write_operational() only serialises keys in this set.
        # Always serialises sim_meta and counters regardless.
        self._dirty: set[str] = set()

        # Opt-D: Sorted order-ID list + hash for allocation sort cache.
        # When open_orders hasn't changed, reuse the previous sorted list.
        self._sorted_order_ids: list[str] = []
        self._orders_snapshot_hash: int = 0

    # ------------------------------------------------------------------
    # Public class methods
    # ------------------------------------------------------------------

    @classmethod
    def from_new(cls, config: Config, db_path: Path | None = None) -> "SimulationState":
        """Create a fresh simulation state.

        Generates all master data, initialises operational tables to zero /
        empty, and persists everything to SQLite.  Called on ``--reset``.

        Args:
            config:  Validated simulation config.
            db_path: Override the SQLite DB path (used in tests).  Defaults to
                     ``state/simulation.db``.
        """
        resolved_db = db_path or _DEFAULT_DB_PATH
        resolved_db.parent.mkdir(parents=True, exist_ok=True)

        rng = random.Random(config.simulation.seed)

        # Generate master data
        catalog = generate_catalog(rng)
        customers = generate_customers(rng, config)
        carriers = list(ALL_CARRIERS)
        warehouses = list(ALL_WAREHOUSES)

        # Initialise operational tables
        inventory = _seed_initial_inventory(catalog, warehouses, rng)
        allocated: dict[str, dict[str, int]] = {w.code: {} for w in warehouses}
        open_orders: dict[str, dict[str, Any]] = {}
        customer_balances: dict[str, float] = {c.customer_id: 0.0 for c in customers}
        backorder_queue: list[dict[str, Any]] = []
        active_loads: dict[str, dict[str, Any]] = {}
        in_transit_returns: list[dict[str, Any]] = []
        exchange_rates: dict[str, float] = {"EUR": 4.30, "USD": 4.05}
        schema_flags: dict[str, bool] = {"sales_channel": False, "incoterms": False}
        counters: dict[str, int] = {
            "order": 1000,
            "batch": 1000,
            "shipment": 1000,
            "load": 1000,
            "return": 1000,
            "signal": 0,
            "disruption": 0,
        }
        production_pipeline: list[dict[str, Any]] = []

        state = cls(
            db_path=resolved_db,
            sim_day=0,
            current_date=config.simulation.start_date,
            start_date=config.simulation.start_date,
            rng=rng,
            catalog=catalog,
            customers=customers,
            carriers=carriers,
            warehouses=warehouses,
            inventory=inventory,
            allocated=allocated,
            open_orders=open_orders,
            customer_balances=customer_balances,
            backorder_queue=backorder_queue,
            active_loads=active_loads,
            in_transit_returns=in_transit_returns,
            exchange_rates=exchange_rates,
            schema_flags=schema_flags,
            counters=counters,
            production_pipeline=production_pipeline,
            carrier_disruptions={},
            pending_pod={},
            deferred_erp_movements=[],
            tms_maintenance_fired_month="",
            supply_disruptions={},
        )
        state.save()
        return state

    @classmethod
    def from_db(cls, config: Config, db_path: Path | None = None) -> "SimulationState":
        """Resume an existing simulation from SQLite.

        Args:
            config:  Validated simulation config (used only for type info; the
                     actual state values are loaded from the DB).
            db_path: Override the SQLite DB path (used in tests).

        Raises:
            FileNotFoundError: If the DB file does not exist.
        """
        resolved_db = db_path or _DEFAULT_DB_PATH
        if not resolved_db.exists():
            raise FileNotFoundError(
                f"Simulation DB not found at {resolved_db}. Run --reset first."
            )

        conn = sqlite3.connect(resolved_db)
        try:
            return cls._load_from_conn(conn, resolved_db)
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self) -> None:
        """Persist all mutable state to SQLite.

        Called at the end of every simulated day.  The DB is created (or
        overwritten) at ``self._db_path``.
        """
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self._db_path)
        try:
            self._create_schema(conn)
            self._write_meta(conn)
            self._write_master_data(conn)
            self._write_operational(conn)
            conn.commit()
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # State mutation helpers
    # ------------------------------------------------------------------

    def advance_day(self, new_date: date) -> None:
        """Increment ``sim_day`` and update ``current_date``.

        Also clears ephemeral per-day buffers (``daily_production_receipts``).

        Args:
            new_date: The new simulated calendar date (must be strictly after
                      ``current_date``).
        """
        self.sim_day += 1
        self.current_date = new_date
        # Clear per-day ephemeral buffer so engines see only today's receipts.
        self.daily_production_receipts = []

    def next_id(self, counter_name: str) -> int:
        """Increment and return the next sequential ID for *counter_name*.

        Args:
            counter_name: One of ``"order"``, ``"batch"``, ``"shipment"``,
                          ``"load"``, or ``"return"``.

        Returns:
            The new counter value (post-increment).
        """
        self.counters[counter_name] += 1
        return self.counters[counter_name]

    def next_movement_id(self) -> str:
        """Allocate and return the next MOV-NNN movement ID.

        Initialises the ``"movement"`` counter to 1000 on first call (matching
        the behaviour previously duplicated in ``inventory_movements.py`` and
        ``transfers.py``).

        Returns:
            A string of the form ``"MOV-{N}"`` where N is the new counter value.
        """
        if "movement" not in self.counters:
            self.counters["movement"] = 1000
        self.counters["movement"] += 1
        return f"MOV-{self.counters['movement']}"

    def mark_dirty(self, key: str) -> None:
        """Mark a kv_state key as modified so _write_operational() serialises it.

        Engines that mutate a specific field (e.g. 'inventory', 'open_orders')
        call this to avoid a full re-serialisation of all 16 kv_state keys on
        every save.  Keys 'sim_meta' and 'counters' are always serialised
        regardless of dirty tracking.

        Args:
            key: A kv_state key name (e.g. 'inventory', 'open_orders').
        """
        self._dirty.add(key)

    # ------------------------------------------------------------------
    # Internal: schema creation
    # ------------------------------------------------------------------

    @staticmethod
    def _create_schema(conn: sqlite3.Connection) -> None:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS sim_meta (
                id              INTEGER PRIMARY KEY CHECK (id = 1),
                sim_day         INTEGER NOT NULL,
                sim_current_date TEXT NOT NULL,
                sim_start_date  TEXT NOT NULL,
                rng_state       BLOB NOT NULL
            );

            CREATE TABLE IF NOT EXISTS catalog_skus (
                sku         TEXT PRIMARY KEY,
                weight_kg   REAL NOT NULL,
                spec_json   TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS customers_json (
                customer_id TEXT PRIMARY KEY,
                data        TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS carriers_json (
                code TEXT PRIMARY KEY,
                data TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS warehouses_json (
                code TEXT PRIMARY KEY,
                data TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS kv_state (
                key  TEXT PRIMARY KEY,
                data TEXT NOT NULL
            );
        """)

    # ------------------------------------------------------------------
    # Internal: write helpers
    # ------------------------------------------------------------------

    def _write_meta(self, conn: sqlite3.Connection) -> None:
        rng_blob = pickle.dumps(self.rng.getstate())
        conn.execute(
            """
            INSERT INTO sim_meta (id, sim_day, sim_current_date, sim_start_date, rng_state)
            VALUES (1, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                sim_day          = excluded.sim_day,
                sim_current_date = excluded.sim_current_date,
                sim_start_date   = excluded.sim_start_date,
                rng_state        = excluded.rng_state
            """,
            (
                self.sim_day,
                self.current_date.isoformat(),
                self.start_date.isoformat(),
                rng_blob,
            ),
        )

    def _write_master_data(self, conn: sqlite3.Connection) -> None:
        # Catalog SKUs
        conn.execute("DELETE FROM catalog_skus")
        for entry in self.catalog:
            spec_dict = {
                "valve_type": entry.spec.valve_type,
                "dn": entry.spec.dn,
                "material": entry.spec.material,
                "pressure_class": entry.spec.pressure_class,
                "connection": entry.spec.connection,
                "actuation": entry.spec.actuation,
            }
            conn.execute(
                "INSERT OR REPLACE INTO catalog_skus (sku, weight_kg, spec_json) VALUES (?, ?, ?)",
                (entry.sku, entry.weight_kg, json.dumps(spec_dict)),
            )

        # Customers
        conn.execute("DELETE FROM customers_json")
        for customer in self.customers:
            conn.execute(
                "INSERT INTO customers_json (customer_id, data) VALUES (?, ?)",
                (customer.customer_id, json.dumps(_customer_to_dict(customer))),
            )

        # Carriers
        conn.execute("DELETE FROM carriers_json")
        for carrier in self.carriers:
            conn.execute(
                "INSERT INTO carriers_json (code, data) VALUES (?, ?)",
                (carrier.code, json.dumps(_carrier_to_dict(carrier))),
            )

        # Warehouses
        conn.execute("DELETE FROM warehouses_json")
        for warehouse in self.warehouses:
            conn.execute(
                "INSERT INTO warehouses_json (code, data) VALUES (?, ?)",
                (warehouse.code, json.dumps(_warehouse_to_dict(warehouse))),
            )

    def _write_operational(self, conn: sqlite3.Connection) -> None:
        all_ops: dict[str, Any] = {
            "inventory": self.inventory,
            "allocated": self.allocated,
            "open_orders": self.open_orders,
            "customer_balances": self.customer_balances,
            "backorder_queue": self.backorder_queue,
            "active_loads": self.active_loads,
            "in_transit_returns": self.in_transit_returns,
            "exchange_rates": self.exchange_rates,
            "schema_flags": self.schema_flags,
            "counters": self.counters,
            "production_pipeline": self.production_pipeline,
            "carrier_disruptions": self.carrier_disruptions,
            "pending_pod": self.pending_pod,
            "deferred_erp_movements": self.deferred_erp_movements,
            "tms_maintenance_fired_month": self.tms_maintenance_fired_month,
            "supply_disruptions": self.supply_disruptions,
        }

        # Opt-B: Only write keys that changed, plus always-required keys.
        # On first save (dirty set is empty), write everything.
        always_write = {"counters", "schema_flags"}
        if self._dirty:
            ops = {k: v for k, v in all_ops.items() if k in self._dirty or k in always_write}
        else:
            ops = all_ops

        for key, value in ops.items():
            conn.execute(
                "INSERT INTO kv_state (key, data) VALUES (?, ?)"
                " ON CONFLICT(key) DO UPDATE SET data = excluded.data",
                (key, json.dumps(value)),
            )

        # Clear dirty set after a successful write
        self._dirty.clear()

    # ------------------------------------------------------------------
    # Internal: load from connection
    # ------------------------------------------------------------------

    @classmethod
    def _load_from_conn(cls, conn: sqlite3.Connection, db_path: Path) -> "SimulationState":
        # Meta
        row = conn.execute(
            "SELECT sim_day, sim_current_date, sim_start_date, rng_state FROM sim_meta WHERE id = 1"
        ).fetchone()
        if row is None:
            raise RuntimeError("sim_meta table is empty — DB may be corrupt.")

        sim_day = row[0]
        current_date = date.fromisoformat(row[1])
        start_date = date.fromisoformat(row[2])
        rng = random.Random()
        rng.setstate(pickle.loads(row[3]))  # noqa: S301 — internal use only

        # Catalog
        catalog: list[CatalogEntry] = []
        from flowform.catalog.constraints import SKUSpec
        for sku_row in conn.execute("SELECT sku, weight_kg, spec_json FROM catalog_skus"):
            sku, weight_kg, spec_json = sku_row
            spec_dict = json.loads(spec_json)
            spec = SKUSpec(**spec_dict)
            catalog.append(CatalogEntry(spec=spec, sku=sku, weight_kg=weight_kg))

        # Customers
        customers: list[Customer] = []
        for cust_row in conn.execute("SELECT data FROM customers_json"):
            customers.append(_customer_from_dict(json.loads(cust_row[0])))

        # Carriers
        carriers: list[Carrier] = []
        for carrier_row in conn.execute("SELECT data FROM carriers_json"):
            carriers.append(_carrier_from_dict(json.loads(carrier_row[0])))

        # Warehouses
        warehouses: list[Warehouse] = []
        for wh_row in conn.execute("SELECT data FROM warehouses_json"):
            warehouses.append(_warehouse_from_dict(json.loads(wh_row[0])))

        # Operational tables
        def _kv(key: str) -> Any:
            row = conn.execute(
                "SELECT data FROM kv_state WHERE key = ?", (key,)
            ).fetchone()
            if row is None:
                return None
            return json.loads(row[0])

        inventory: dict[str, dict[str, int]] = _kv("inventory") or {}
        allocated: dict[str, dict[str, int]] = _kv("allocated") or {}
        open_orders: dict[str, dict[str, Any]] = _kv("open_orders") or {}
        customer_balances: dict[str, float] = _kv("customer_balances") or {}
        backorder_queue: list[dict[str, Any]] = _kv("backorder_queue") or []
        active_loads: dict[str, dict[str, Any]] = _kv("active_loads") or {}
        in_transit_returns: list[dict[str, Any]] = _kv("in_transit_returns") or []
        exchange_rates: dict[str, float] = _kv("exchange_rates") or {"EUR": 4.30, "USD": 4.05}
        schema_flags: dict[str, bool] = _kv("schema_flags") or {"sales_channel": False, "incoterms": False}
        counters: dict[str, int] = _kv("counters") or {
            "order": 1000, "batch": 1000, "shipment": 1000, "load": 1000, "return": 1000,
        }
        production_pipeline: list[dict[str, Any]] = _kv("production_pipeline") or []
        carrier_disruptions: dict[str, dict[str, Any]] = (
            _kv("carrier_disruptions") or {}
        )
        pending_pod: dict[str, dict[str, Any]] = _kv("pending_pod") or {}
        deferred_erp_movements: list[dict[str, Any]] = (
            _kv("deferred_erp_movements") or []
        )
        tms_maintenance_fired_month: str = (
            _kv("tms_maintenance_fired_month") or ""
        )
        supply_disruptions: dict[str, dict[str, Any]] = (
            _kv("supply_disruptions") or {}
        )

        return cls(
            db_path=db_path,
            sim_day=sim_day,
            current_date=current_date,
            start_date=start_date,
            rng=rng,
            catalog=catalog,
            customers=customers,
            carriers=carriers,
            warehouses=warehouses,
            inventory=inventory,
            allocated=allocated,
            open_orders=open_orders,
            customer_balances=customer_balances,
            backorder_queue=backorder_queue,
            active_loads=active_loads,
            in_transit_returns=in_transit_returns,
            exchange_rates=exchange_rates,
            schema_flags=schema_flags,
            counters=counters,
            production_pipeline=production_pipeline,
            carrier_disruptions=carrier_disruptions,
            pending_pod=pending_pod,
            deferred_erp_movements=deferred_erp_movements,
            tms_maintenance_fired_month=tms_maintenance_fired_month,
            supply_disruptions=supply_disruptions,
        )
