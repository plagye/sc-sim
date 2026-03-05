"""Carrier master data: five carriers with reliability, transit times, and weight units.

BalticHaul (BALTIC) is the anomaly — it reports shipment weights in **pounds**
(legacy US-based TMS software).  All other carriers report in kilograms.  The
``weight_unit`` field on every ``Carrier`` records which unit is in use.

Reliability seasonality (applied as additive offsets to ``base_reliability``):
- November–February:  −0.05  (winter road conditions)
- Week of Easter:     −0.08  (reduced carrier staff)
- July–August:        −0.03  (vacation coverage gaps)
- December 20–31:     −0.15  (holiday skeleton crews)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Final

from flowform.calendar import easter_sunday


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Carrier:
    code: str                    # DHL, DBSC, RABEN, GEODIS, BALTIC
    name: str
    base_reliability: float      # on-time rate, 0–1
    transit_days_min: int
    transit_days_max: int
    cost_tier: str               # "high" | "medium" | "low"
    primary_use: str
    weight_unit: str             # "kg" or "lbs"


# ---------------------------------------------------------------------------
# Static carrier definitions
# ---------------------------------------------------------------------------

DHL = Carrier(
    code="DHL",
    name="DHL Freight",
    base_reliability=0.92,
    transit_days_min=3,
    transit_days_max=5,
    cost_tier="high",
    primary_use="International and large-volume shipments",
    weight_unit="kg",
)

DBSC = Carrier(
    code="DBSC",
    name="DB Schenker",
    base_reliability=0.89,
    transit_days_min=3,
    transit_days_max=5,
    cost_tier="medium",
    primary_use="Domestic Poland and EU",
    weight_unit="kg",
)

RABEN = Carrier(
    code="RABEN",
    name="Raben Group",
    base_reliability=0.85,
    transit_days_min=1,
    transit_days_max=3,
    cost_tier="medium",
    primary_use="Poland and Central & Eastern Europe",
    weight_unit="kg",
)

GEODIS = Carrier(
    code="GEODIS",
    name="Geodis",
    base_reliability=0.90,
    transit_days_min=3,
    transit_days_max=5,
    cost_tier="high",
    primary_use="Western EU",
    weight_unit="kg",
)

BALTIC = Carrier(
    code="BALTIC",
    name="BalticHaul",
    base_reliability=0.80,
    transit_days_min=1,
    transit_days_max=1,
    cost_tier="low",
    primary_use="Gdańsk port and shipyard deliveries",
    weight_unit="lbs",  # legacy US-based TMS — reports pounds, not kilograms
)

ALL_CARRIERS: Final[list[Carrier]] = [DHL, DBSC, RABEN, GEODIS, BALTIC]

# Lookup by code
CARRIER_BY_CODE: Final[dict[str, Carrier]] = {c.code: c for c in ALL_CARRIERS}


def get_carrier(code: str) -> Carrier:
    """Return a Carrier by its code, raising KeyError if not found."""
    return CARRIER_BY_CODE[code]


# ---------------------------------------------------------------------------
# Reliability with seasonal adjustment
# ---------------------------------------------------------------------------

# Easter-week is the 7 days starting from Easter Sunday (Sun–Sat inclusive).
_EASTER_WEEK_SPAN = 7


def _is_easter_week(d: date) -> bool:
    """Return True if *d* falls within Easter Sunday ± 0–6 days after."""
    sunday = easter_sunday(d.year)
    return sunday <= d < sunday + timedelta(days=_EASTER_WEEK_SPAN)


def reliability_on_date(carrier: Carrier, d: date) -> float:
    """Compute a carrier's on-time reliability for a specific date.

    Applies additive seasonal offsets to ``carrier.base_reliability`` and
    clamps the result to [0.0, 1.0].

    Args:
        carrier: The carrier whose reliability to compute.
        d:       The date of interest.

    Returns:
        Adjusted reliability in [0.0, 1.0].
    """
    offset = 0.0

    month = d.month
    day = d.day

    # December 20–31 (highest impact, check first)
    if month == 12 and day >= 20:
        offset -= 0.15
    # November–February winter penalty (skip if already Dec 20–31)
    elif month in (11, 12, 1, 2):
        offset -= 0.05

    # July–August summer coverage gap
    if month in (7, 8):
        offset -= 0.03

    # Easter week
    if _is_easter_week(d):
        offset -= 0.08

    raw = carrier.base_reliability + offset
    return max(0.0, min(1.0, raw))
