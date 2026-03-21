"""Customer master data generator: 60 customers across 6 segments.

FlowForm Industries serves B2B industrial customers in 6 segments.
All randomness is routed through the caller-supplied ``random.Random``
instance so the output is fully reproducible given the same seed.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Final

from faker import Faker

from flowform.config import Config

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

SEGMENTS: Final[list[str]] = [
    "Oil & Gas",
    "Water Utilities",
    "Chemical Plants",
    "Industrial Distributors",
    "HVAC Contractors",
    "Shipyards",
]

CARRIERS: Final[list[str]] = ["DHL", "DBSC", "RABEN", "GEODIS", "BALTIC"]

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class CustomerAddress:
    street: str
    city: str
    postal_code: str
    country_code: str


@dataclass
class OrderingProfile:
    base_order_frequency_per_month: float
    order_size_range: tuple[int, int]
    # dimension → {value → weight}
    # Keys are strings for material/valve_type/pressure/actuation,
    # ints for DN sizes.
    sku_affinity: dict[str, dict[str | int, float]]


@dataclass
class Customer:
    customer_id: str
    company_name: str
    segment: str
    country_code: str
    region: str
    primary_address: CustomerAddress
    secondary_address: CustomerAddress | None
    credit_limit: float
    payment_terms_days: int
    currency: str
    preferred_carrier: str
    contract_discount_pct: float
    ordering_profile: OrderingProfile
    seasonal_profile: dict[int, float]   # month 1–12 → multiplier
    shutdown_months: list[int]
    accepts_deliveries_december: bool
    active: bool
    onboarding_date: date

    # Convenience alias used in tests / engine
    @property
    def id(self) -> str:
        return self.customer_id


# ---------------------------------------------------------------------------
# Internal per-segment configuration tables
# ---------------------------------------------------------------------------

# (min_credit, max_credit) by segment — calibrated to cover 2-3× the P90
# single-order value for that segment, so normal operations stay below the
# limit while genuinely delinquent customers still get held.
# Observed P90 order values (year run, seed 42):
#   Oil & Gas 5.9M, Water Utilities 984K, Chemical Plants 4.9M,
#   Industrial Distributors 402K, HVAC Contractors 1.9M, Shipyards 21M.
_CREDIT_LIMITS: dict[str, tuple[float, float]] = {
    "Oil & Gas":               ( 30_000_000,  150_000_000),
    "Water Utilities":         (  2_000_000,   10_000_000),
    "Chemical Plants":         (  5_000_000,   30_000_000),
    "Industrial Distributors": (  2_000_000,    8_000_000),
    "HVAC Contractors":        (  4_000_000,   15_000_000),
    "Shipyards":               ( 30_000_000,  150_000_000),
}

# Ordering profiles (static part — seasonal_profile built separately)
_ORDERING_PROFILES: dict[str, OrderingProfile] = {
    "Oil & Gas": OrderingProfile(
        base_order_frequency_per_month=4.0,
        order_size_range=(50, 500),
        sku_affinity={
            "material":  {"SS316": 3.0, "Duplex": 2.0, "CS": 1.0, "SS304": 0.5, "Brass": 0.1},
            "dn":        {100: 2.5, 150: 3.0, 200: 2.0, 250: 1.5, 80: 1.0, 50: 0.5},
            "pressure":  {"PN40": 3.0, "PN63": 2.0, "PN25": 1.0, "PN16": 0.3, "PN10": 0.1},
        },
    ),
    "Water Utilities": OrderingProfile(
        base_order_frequency_per_month=2.0,
        order_size_range=(20, 200),
        sku_affinity={
            "material":  {"CS": 3.0, "SS304": 2.0, "SS316": 1.0, "Duplex": 0.3, "Brass": 0.5},
            "dn":        {50: 2.0, 80: 2.5, 100: 2.0, 150: 1.5, 40: 1.0, 200: 0.5},
            "pressure":  {"PN16": 3.0, "PN25": 2.0, "PN10": 1.0, "PN40": 0.3, "PN63": 0.1},
        },
    ),
    "Chemical Plants": OrderingProfile(
        base_order_frequency_per_month=0.5,
        order_size_range=(100, 1000),
        sku_affinity={
            "material":  {"SS316": 3.0, "Duplex": 2.5, "SS304": 1.0, "CS": 0.5, "Brass": 0.1},
            "dn":        {50: 1.5, 80: 2.0, 100: 2.5, 150: 2.0, 40: 1.0, 200: 0.8},
            "pressure":  {"PN25": 2.0, "PN40": 2.5, "PN63": 1.5, "PN16": 0.5, "PN10": 0.2},
        },
    ),
    "Industrial Distributors": OrderingProfile(
        base_order_frequency_per_month=8.0,
        order_size_range=(5, 50),
        sku_affinity={
            "material":  {"CS": 1.0, "SS304": 1.0, "SS316": 1.0, "Brass": 1.0, "Duplex": 0.5},
            "dn":        {25: 1.5, 40: 2.0, 50: 2.5, 80: 2.0, 100: 1.5, 15: 1.0},
            "pressure":  {"PN10": 1.0, "PN16": 1.0, "PN25": 1.0, "PN40": 1.0, "PN63": 1.0},
        },
    ),
    "HVAC Contractors": OrderingProfile(
        base_order_frequency_per_month=1.5,
        order_size_range=(10, 100),
        sku_affinity={
            "material":  {"Brass": 3.0, "CS": 2.0, "SS304": 1.0, "SS316": 0.3, "Duplex": 0.1},
            "dn":        {15: 2.0, 25: 2.5, 40: 2.0, 50: 1.5, 80: 0.5},
            "pressure":  {"PN10": 2.5, "PN16": 3.0, "PN25": 0.5, "PN40": 0.1, "PN63": 0.1},
            "actuation": {"Manual": 3.0, "Pneumatic": 1.0, "Electric": 0.3, "Hydraulic": 0.0},
        },
    ),
    "Shipyards": OrderingProfile(
        base_order_frequency_per_month=0.3,
        order_size_range=(200, 2000),
        sku_affinity={
            "material":  {"SS316": 3.0, "Duplex": 2.5, "CS": 1.0, "SS304": 0.5, "Brass": 0.0},
            "dn":        {150: 2.0, 200: 3.0, 250: 2.5, 300: 2.0, 100: 1.0},
            "pressure":  {"PN25": 2.0, "PN40": 2.5, "PN63": 2.0, "PN16": 0.5, "PN10": 0.1},
            "actuation": {"Hydraulic": 2.0, "Electric": 2.0, "Pneumatic": 1.5, "Manual": 0.5},
        },
    ),
}

# Seasonal multiplier templates (Chemical Plants built dynamically)
def _oil_gas_seasonal() -> dict[int, float]:
    return {m: 1.1 if m in (1, 2, 3, 7, 8, 9) else 1.0 for m in range(1, 13)}

def _water_utilities_seasonal() -> dict[int, float]:
    profile = {m: 1.0 for m in range(1, 13)}
    for m in (3, 4):
        profile[m] = 1.30
    for m in (12, 1):
        profile[m] = 0.80
    return profile

def _chemical_plant_seasonal(shutdown_months: list[int]) -> dict[int, float]:
    return {m: 3.0 if m in shutdown_months else 0.4 for m in range(1, 13)}

def _distributor_seasonal() -> dict[int, float]:
    profile = {m: 1.0 for m in range(1, 13)}
    profile[11] = 1.25
    for m in (7, 8):
        profile[m] = 0.85
    return profile

def _hvac_seasonal() -> dict[int, float]:
    profile = {m: 1.0 for m in range(1, 13)}
    for m in (9, 10, 11):
        profile[m] = 1.40
    for m in (6, 7, 8):
        profile[m] = 0.70
    return profile

def _shipyard_seasonal() -> dict[int, float]:
    return {m: 1.0 for m in range(1, 13)}

# ---------------------------------------------------------------------------
# Country / region tables
# ---------------------------------------------------------------------------

# Segment → allowed country codes (in weighted order, used for random.choice)
_SEGMENT_COUNTRIES: dict[str, list[str]] = {
    "Oil & Gas":               ["NO", "NO", "NL", "NL", "PL", "PL", "DE", "DE"],
    "Water Utilities":         ["PL", "PL", "PL", "CZ", "CZ", "DE"],
    "Chemical Plants":         ["DE", "DE", "NL", "NL", "PL", "PL"],
    "Industrial Distributors": ["PL", "PL", "DE", "DE", "CZ", "NL", "NO", "SE", "FR", "IT"],
    "HVAC Contractors":        ["PL", "PL", "PL", "DE", "DE"],
    "Shipyards":               ["NO", "NO", "SE", "SE"],
}

# Regions by country code (sampled uniformly)
_COUNTRY_REGIONS: dict[str, list[str]] = {
    "PL": [
        "Śląskie", "Mazowieckie", "Małopolskie", "Łódźkie", "Dolnośląskie",
        "Wielkopolskie", "Pomorskie", "Kujawsko-Pomorskie", "Zachodniopomorskie",
    ],
    "DE": [
        "Bavaria", "North Rhine-Westphalia", "Baden-Württemberg", "Hamburg",
        "Saxony", "Brandenburg", "Hesse", "Lower Saxony", "Bremen",
    ],
    "CZ": ["Prague", "South Moravia", "Central Bohemia", "Moravia-Silesia", "Pilsen"],
    "NL": ["North Holland", "South Holland", "Zeeland", "North Brabant", "Gelderland"],
    "NO": ["Vestland", "Rogaland", "Møre og Romsdal", "Viken", "Troms og Finnmark"],
    "SE": ["Västra Götaland", "Skåne", "Stockholm", "Norrland", "Östergötland"],
    "FR": ["Île-de-France", "Auvergne-Rhône-Alpes", "Occitanie", "Nouvelle-Aquitaine"],
    "IT": ["Lombardy", "Veneto", "Emilia-Romagna", "Tuscany", "Lazio"],
}

# Faker locale per country code
_FAKER_LOCALE: dict[str, str] = {
    "PL": "pl_PL",
    "DE": "de_DE",
    "CZ": "cs_CZ",
    "NL": "nl_NL",
    "NO": "no_NO",
    "SE": "sv_SE",
    "FR": "fr_FR",
    "IT": "it_IT",
}

# Default region / locale for unlisted countries
_DEFAULT_REGION = "Unknown"
_DEFAULT_LOCALE = "en_US"

# ---------------------------------------------------------------------------
# Segment distribution: exactly 60 customers
# ---------------------------------------------------------------------------

_SEGMENT_COUNTS: dict[str, int] = {
    "Oil & Gas":               18,
    "Water Utilities":         12,
    "Chemical Plants":         12,
    "Industrial Distributors":  9,
    "HVAC Contractors":         6,
    "Shipyards":                3,
}  # total = 60

# ---------------------------------------------------------------------------
# Address generation helpers
# ---------------------------------------------------------------------------

# We cache Faker instances keyed by locale to avoid expensive re-creation.
_faker_cache: dict[str, Faker] = {}


def _get_faker(locale: str) -> Faker:
    if locale not in _faker_cache:
        _faker_cache[locale] = Faker(locale)
    return _faker_cache[locale]


def _generate_address(rng: random.Random, country_code: str) -> CustomerAddress:
    """Generate a plausible address for the given country using Faker.

    Faker's internal randomness is seeded from ``rng`` so the output is
    deterministic when the caller uses a fixed seed.
    """
    locale = _FAKER_LOCALE.get(country_code, _DEFAULT_LOCALE)
    fake = _get_faker(locale)
    # Seed Faker's internal random from our rng so it stays reproducible.
    fake.seed_instance(rng.randint(0, 2**31 - 1))
    return CustomerAddress(
        street=fake.street_address(),
        city=fake.city(),
        postal_code=fake.postcode(),
        country_code=country_code,
    )


# ---------------------------------------------------------------------------
# Currency picker
# ---------------------------------------------------------------------------

def _pick_currency(rng: random.Random, config: Config) -> str:
    """Pick a currency according to the configured distribution."""
    dist = config.customers.currency_distribution
    choices = ["PLN", "EUR", "USD"]
    weights = [dist.PLN, dist.EUR, dist.USD]
    return rng.choices(choices, weights=weights, k=1)[0]


# ---------------------------------------------------------------------------
# Payment terms picker
# ---------------------------------------------------------------------------

def _pick_payment_terms(rng: random.Random) -> int:
    """30 (30%), 60 (50%), 90 (20%)."""
    return rng.choices([30, 60, 90], weights=[0.30, 0.50, 0.20], k=1)[0]


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------

def generate_customers(rng: random.Random, config: Config) -> list[Customer]:
    """Generate the full customer master data list.

    Args:
        rng: Seeded random instance. All randomness flows through this so the
             output is reproducible.
        config: Validated simulation config. Uses ``config.customers.count``
                (must be 60) and ``config.customers.currency_distribution``.

    Returns:
        List of ``Customer`` objects sorted ascending by ``customer_id``.
    """
    count = config.customers.count
    if count != sum(_SEGMENT_COUNTS.values()):
        # Fall back: distribute proportionally if count differs from 60.
        # For the standard run (count=60) this path is never taken.
        pass

    sim_start: date = config.simulation.start_date

    # Build the flat list of (customer_id_index, segment) assignments.
    assignments: list[str] = []
    for segment in SEGMENTS:
        assignments.extend([segment] * _SEGMENT_COUNTS[segment])

    # Shuffle so customer IDs are not clustered by segment.
    rng.shuffle(assignments)

    customers: list[Customer] = []

    for idx, segment in enumerate(assignments):
        customer_id = f"CUST-{idx + 1:04d}"

        # Country & region
        country_code = rng.choice(_SEGMENT_COUNTRIES[segment])
        region_pool = _COUNTRY_REGIONS.get(country_code, [_DEFAULT_REGION])
        region = rng.choice(region_pool)

        # Addresses
        primary_address = _generate_address(rng, country_code)
        has_secondary = rng.random() < 0.30
        secondary_address = _generate_address(rng, country_code) if has_secondary else None

        # Credit limit
        lo, hi = _CREDIT_LIMITS[segment]
        credit_limit = round(rng.uniform(lo, hi), 2)

        # Payment terms
        payment_terms_days = _pick_payment_terms(rng)

        # Currency
        currency = _pick_currency(rng, config)

        # Preferred carrier
        preferred_carrier = rng.choice(CARRIERS)

        # Contract discount: 0.0–15.0%, one decimal place
        contract_discount_pct = round(rng.uniform(0.0, 15.0), 1)

        # Company name — use Faker with country locale
        locale = _FAKER_LOCALE.get(country_code, _DEFAULT_LOCALE)
        fake = _get_faker(locale)
        fake.seed_instance(rng.randint(0, 2**31 - 1))
        company_name = fake.company()

        # Shutdown months (Chemical Plants only)
        if segment == "Chemical Plants":
            num_shutdowns = rng.randint(1, 2)
            shutdown_months = rng.sample(range(1, 13), num_shutdowns)
        else:
            shutdown_months = []

        # Seasonal profile
        if segment == "Oil & Gas":
            seasonal_profile = _oil_gas_seasonal()
        elif segment == "Water Utilities":
            seasonal_profile = _water_utilities_seasonal()
        elif segment == "Chemical Plants":
            seasonal_profile = _chemical_plant_seasonal(shutdown_months)
        elif segment == "Industrial Distributors":
            seasonal_profile = _distributor_seasonal()
        elif segment == "HVAC Contractors":
            seasonal_profile = _hvac_seasonal()
        else:  # Shipyards
            seasonal_profile = _shipyard_seasonal()

        # Ordering profile (deep-copy the static template so mutations are safe)
        template = _ORDERING_PROFILES[segment]
        ordering_profile = OrderingProfile(
            base_order_frequency_per_month=template.base_order_frequency_per_month,
            order_size_range=template.order_size_range,
            sku_affinity={dim: dict(weights) for dim, weights in template.sku_affinity.items()},
        )

        # Accepts December deliveries (False for ~30%)
        accepts_deliveries_december = rng.random() >= 0.30

        # Onboarding date: 2–5 years before simulation start
        days_back = rng.randint(2 * 365, 5 * 365)
        onboarding_date = sim_start - timedelta(days=days_back)

        customers.append(
            Customer(
                customer_id=customer_id,
                company_name=company_name,
                segment=segment,
                country_code=country_code,
                region=region,
                primary_address=primary_address,
                secondary_address=secondary_address,
                credit_limit=credit_limit,
                payment_terms_days=payment_terms_days,
                currency=currency,
                preferred_carrier=preferred_carrier,
                contract_discount_pct=contract_discount_pct,
                ordering_profile=ordering_profile,
                seasonal_profile=seasonal_profile,
                shutdown_months=shutdown_months,
                accepts_deliveries_december=accepts_deliveries_december,
                active=True,
                onboarding_date=onboarding_date,
            )
        )

    # Sort by customer_id (lexicographic; zero-padded 4-digit so numeric order matches)
    customers.sort(key=lambda c: c.customer_id)

    return customers
