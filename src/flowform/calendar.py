"""Polish holiday calendar, business day logic, and dynamic Easter calculation.

All functions are pure (no state dependencies). Use Python's datetime.date
throughout — no datetime, no timezone handling.
"""

from __future__ import annotations

import functools
from datetime import date, timedelta

# ---------------------------------------------------------------------------
# Day-of-week multiplier tables
# weekday() returns: Monday=0, Tuesday=1, ..., Saturday=5, Sunday=6
# ---------------------------------------------------------------------------

PRODUCTION_DOW_MULTIPLIERS: dict[int, float] = {
    0: 0.80,  # Monday
    1: 1.00,  # Tuesday
    2: 1.10,  # Wednesday
    3: 1.10,  # Thursday
    4: 0.85,  # Friday
    5: 0.00,  # Saturday
    6: 0.00,  # Sunday
}

ORDER_DOW_MULTIPLIERS: dict[int, float] = {
    0: 0.90,  # Monday
    1: 1.05,  # Tuesday
    2: 1.05,  # Wednesday
    3: 1.00,  # Thursday
    4: 0.85,  # Friday
    5: 0.10,  # Saturday (EDI only)
    6: 0.05,  # Sunday (EDI only)
}


# ---------------------------------------------------------------------------
# Easter calculation — Anonymous Gregorian (Meeus/Jones/Butcher) algorithm
# ---------------------------------------------------------------------------

def easter_sunday(year: int) -> date:
    """Return Easter Sunday for the given year using the Meeus/Jones/Butcher algorithm."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


# ---------------------------------------------------------------------------
# Polish public holidays
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=32)
def polish_holidays(year: int) -> frozenset[date]:
    """Return all Polish public holidays for the given year.

    Fixed dates:
        Jan 1  — New Year's Day
        Jan 6  — Epiphany
        May 1  — Labour Day
        May 3  — Constitution Day
        Aug 15 — Assumption of Mary
        Nov 1  — All Saints' Day
        Nov 11 — Independence Day
        Dec 25 — Christmas Day
        Dec 26 — Second Day of Christmas

    Moveable dates (depend on Easter):
        Easter Monday  — day after Easter Sunday
        Corpus Christi — 60 days after Easter Sunday
    """
    easter = easter_sunday(year)
    easter_monday = easter + timedelta(days=1)
    corpus_christi = easter + timedelta(days=60)

    fixed = {
        date(year, 1, 1),   # New Year's Day
        date(year, 1, 6),   # Epiphany
        date(year, 5, 1),   # Labour Day
        date(year, 5, 3),   # Constitution Day
        date(year, 8, 15),  # Assumption of Mary
        date(year, 11, 1),  # All Saints' Day
        date(year, 11, 11), # Independence Day
        date(year, 12, 25), # Christmas Day
        date(year, 12, 26), # Second Day of Christmas
    }

    return frozenset(fixed | {easter_monday, corpus_christi})


# ---------------------------------------------------------------------------
# Predicate helpers
# ---------------------------------------------------------------------------

def is_holiday(d: date) -> bool:
    """Return True if d is a Polish public holiday."""
    return d in polish_holidays(d.year)


def is_weekend(d: date) -> bool:
    """Return True if d is Saturday or Sunday."""
    return d.weekday() >= 5


def is_business_day(d: date) -> bool:
    """Return True if d is a weekday and not a Polish public holiday."""
    return not is_weekend(d) and not is_holiday(d)


# ---------------------------------------------------------------------------
# Business day navigation
# ---------------------------------------------------------------------------

def next_business_day(d: date) -> date:
    """Return the next business day strictly after d."""
    candidate = d + timedelta(days=1)
    while not is_business_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def business_days_between(start: date, end: date) -> int:
    """Return count of business days in [start, end) — start inclusive, end exclusive."""
    if end <= start:
        return 0
    count = 0
    current = start
    while current < end:
        if is_business_day(current):
            count += 1
        current += timedelta(days=1)
    return count


# ---------------------------------------------------------------------------
# Multiplier accessors
# ---------------------------------------------------------------------------

def production_multiplier(d: date) -> float:
    """Return the day-of-week production multiplier.

    Returns 0.0 on weekends (already encoded in the table) and also on Polish
    public holidays, even when those fall on a weekday.
    """
    if is_holiday(d):
        return 0.0
    return PRODUCTION_DOW_MULTIPLIERS[d.weekday()]


def order_multiplier(d: date) -> float:
    """Return the day-of-week order arrival multiplier.

    Holidays do NOT suppress orders — EDI can still arrive and carriers still
    run.  Return the normal DOW multiplier regardless of holiday status.
    """
    return ORDER_DOW_MULTIPLIERS[d.weekday()]


def is_edi_only_day(d: date) -> bool:
    """Return True if d is Saturday or Sunday (EDI auto-orders only from Distributors)."""
    return is_weekend(d)
