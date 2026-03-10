"""Tests for flowform.calendar — Polish holidays, Easter, business day logic."""

from __future__ import annotations

from datetime import date

import pytest

from flowform.calendar import (
    ORDER_DOW_MULTIPLIERS,
    PRODUCTION_DOW_MULTIPLIERS,
    business_days_between,
    easter_sunday,
    is_business_day,
    is_edi_only_day,
    is_first_business_day_of_month,
    is_holiday,
    is_weekend,
    next_business_day,
    order_multiplier,
    polish_holidays,
    production_multiplier,
)


# ---------------------------------------------------------------------------
# Easter accuracy
# ---------------------------------------------------------------------------

class TestEasterSunday:
    """Verify the MJB algorithm against known Easter Sundays."""

    @pytest.mark.parametrize("year, expected", [
        (2024, date(2024, 3, 31)),
        (2025, date(2025, 4, 20)),
        (2026, date(2026, 4, 5)),
        (2027, date(2027, 3, 28)),
        (2028, date(2028, 4, 16)),
        (2030, date(2030, 4, 21)),
    ])
    def test_known_easter_sundays(self, year: int, expected: date) -> None:
        assert easter_sunday(year) == expected

    def test_easter_is_always_sunday(self) -> None:
        for year in range(2020, 2041):
            assert easter_sunday(year).weekday() == 6, (
                f"Easter {year} is not a Sunday: {easter_sunday(year)}"
            )


# ---------------------------------------------------------------------------
# Holiday set for 2026
# ---------------------------------------------------------------------------

class TestPolishHolidays2026:
    """Validate the complete holiday set for 2026."""

    def setup_method(self) -> None:
        self.holidays = polish_holidays(2026)
        # Easter Sunday 2026 is April 5
        self.easter = date(2026, 4, 5)

    def test_total_count(self) -> None:
        assert len(self.holidays) == 11

    def test_easter_monday(self) -> None:
        easter_monday = date(2026, 4, 6)
        assert easter_monday in self.holidays

    def test_corpus_christi(self) -> None:
        # 60 days after April 5 = June 4
        corpus_christi = date(2026, 6, 4)
        assert corpus_christi in self.holidays

    def test_all_fixed_holidays_present(self) -> None:
        fixed = [
            date(2026, 1, 1),   # New Year's Day
            date(2026, 1, 6),   # Epiphany
            date(2026, 5, 1),   # Labour Day
            date(2026, 5, 3),   # Constitution Day
            date(2026, 8, 15),  # Assumption of Mary
            date(2026, 11, 1),  # All Saints' Day
            date(2026, 11, 11), # Independence Day
            date(2026, 12, 25), # Christmas Day
            date(2026, 12, 26), # Second Day of Christmas
        ]
        for d in fixed:
            assert d in self.holidays, f"{d} should be a holiday"

    def test_easter_sunday_itself_not_in_set(self) -> None:
        # Easter Sunday is not a statutory public holiday in Poland
        assert self.easter not in self.holidays

    def test_returns_frozenset(self) -> None:
        assert isinstance(self.holidays, frozenset)


# ---------------------------------------------------------------------------
# is_holiday
# ---------------------------------------------------------------------------

class TestIsHoliday:
    def test_new_years_day(self) -> None:
        assert is_holiday(date(2026, 1, 1))

    def test_easter_monday_2026(self) -> None:
        assert is_holiday(date(2026, 4, 6))

    def test_corpus_christi_2026(self) -> None:
        assert is_holiday(date(2026, 6, 4))

    def test_regular_tuesday(self) -> None:
        assert not is_holiday(date(2026, 4, 7))

    def test_regular_saturday(self) -> None:
        assert not is_holiday(date(2026, 3, 14))


# ---------------------------------------------------------------------------
# is_weekend / is_business_day
# ---------------------------------------------------------------------------

class TestIsWeekend:
    def test_saturday(self) -> None:
        assert is_weekend(date(2026, 3, 14))  # Saturday

    def test_sunday(self) -> None:
        assert is_weekend(date(2026, 3, 15))  # Sunday

    def test_monday(self) -> None:
        assert not is_weekend(date(2026, 3, 16))  # Monday

    def test_friday(self) -> None:
        assert not is_weekend(date(2026, 3, 20))  # Friday


class TestIsBusinessDay:
    def test_new_years_day_not_business(self) -> None:
        assert not is_business_day(date(2026, 1, 1))

    def test_easter_monday_not_business(self) -> None:
        assert not is_business_day(date(2026, 4, 6))

    def test_tuesday_after_easter_is_business(self) -> None:
        assert is_business_day(date(2026, 4, 7))

    def test_corpus_christi_not_business(self) -> None:
        assert not is_business_day(date(2026, 6, 4))

    def test_regular_saturday_not_business(self) -> None:
        assert not is_business_day(date(2026, 3, 14))

    def test_regular_monday_is_business(self) -> None:
        assert is_business_day(date(2026, 3, 16))

    def test_christmas_not_business(self) -> None:
        assert not is_business_day(date(2026, 12, 25))


# ---------------------------------------------------------------------------
# next_business_day
# ---------------------------------------------------------------------------

class TestNextBusinessDay:
    def test_over_easter_weekend_and_holiday(self) -> None:
        # Good Friday (Apr 3) is not a Polish holiday, so Apr 3 is a business day.
        # Easter Sunday Apr 5 + Easter Monday Apr 6 (holiday) → next BD is Apr 7.
        # Starting from Friday Apr 3:
        # next_business_day(Apr 3) skips Apr 4 (Sat), Apr 5 (Sun), Apr 6 (Easter Mon) → Apr 7 (Tue)
        assert next_business_day(date(2026, 4, 3)) == date(2026, 4, 7)

    def test_friday_to_monday(self) -> None:
        # A plain Friday with no holiday on Monday
        assert next_business_day(date(2026, 3, 20)) == date(2026, 3, 23)

    def test_same_day_not_returned(self) -> None:
        # next_business_day is strictly AFTER d, even if d itself is a business day
        monday = date(2026, 3, 16)
        assert next_business_day(monday) == date(2026, 3, 17)

    def test_over_new_years(self) -> None:
        # Dec 31 2026 is a Thursday.
        # Jan 1 2027 = Friday (holiday), Jan 2 = Saturday, Jan 3 = Sunday.
        # → next business day is Monday Jan 4 2027.
        result = next_business_day(date(2026, 12, 31))
        assert result == date(2027, 1, 4)


# ---------------------------------------------------------------------------
# business_days_between
# ---------------------------------------------------------------------------

class TestBusinessDaysBetween:
    def test_week_with_holiday(self) -> None:
        # Week of Easter 2026: Mon Apr 6 (holiday) through Fri Apr 10
        # [Apr 6, Apr 10) = Apr 6, 7, 8, 9 — but Apr 6 is holiday → 3 business days
        count = business_days_between(date(2026, 4, 6), date(2026, 4, 10))
        assert count == 3  # Apr 7, 8, 9

    def test_normal_week(self) -> None:
        # Mon Mar 16 to Sat Mar 21 — 5 business days
        count = business_days_between(date(2026, 3, 16), date(2026, 3, 21))
        assert count == 5

    def test_end_before_start_returns_zero(self) -> None:
        assert business_days_between(date(2026, 3, 20), date(2026, 3, 16)) == 0

    def test_equal_dates_returns_zero(self) -> None:
        d = date(2026, 3, 16)
        assert business_days_between(d, d) == 0

    def test_weekend_only_range(self) -> None:
        # Sat–Sun span: [Mar 14, Mar 16) — 0 business days
        assert business_days_between(date(2026, 3, 14), date(2026, 3, 16)) == 0

    def test_single_business_day(self) -> None:
        # [Mon, Tue) = 1 business day
        assert business_days_between(date(2026, 3, 16), date(2026, 3, 17)) == 1

    def test_over_new_years(self) -> None:
        # Dec 31 2026 (Thu) inclusive to Jan 5 2027 (Tue) exclusive.
        # Dec 31 = BD, Jan 1 = holiday (Fri), Jan 2 = Sat, Jan 3 = Sun, Jan 4 = BD (Mon)
        # → 2 business days.
        count = business_days_between(date(2026, 12, 31), date(2027, 1, 5))
        assert count == 2


# ---------------------------------------------------------------------------
# production_multiplier
# ---------------------------------------------------------------------------

class TestProductionMultiplier:
    def test_wednesday_returns_1_10(self) -> None:
        # Mar 18 2026 is a Wednesday
        assert production_multiplier(date(2026, 3, 18)) == pytest.approx(1.10)

    def test_saturday_returns_0(self) -> None:
        assert production_multiplier(date(2026, 3, 14)) == pytest.approx(0.0)

    def test_sunday_returns_0(self) -> None:
        assert production_multiplier(date(2026, 3, 15)) == pytest.approx(0.0)

    def test_holiday_on_weekday_returns_0(self) -> None:
        # Easter Monday 2026 is a Tuesday in the weekday sense — it's Monday April 6
        # Actually Easter Monday is always Monday (weekday 0) by definition.
        # May 1 2026 is a Friday — still a holiday
        assert production_multiplier(date(2026, 5, 1)) == pytest.approx(0.0)

    def test_monday_returns_0_80(self) -> None:
        assert production_multiplier(date(2026, 3, 16)) == pytest.approx(0.80)

    def test_friday_returns_0_85(self) -> None:
        assert production_multiplier(date(2026, 3, 20)) == pytest.approx(0.85)

    def test_full_dow_table(self) -> None:
        # Use a clean week with no holidays: Mar 16–22 2026 (Mon–Sun)
        week_start = date(2026, 3, 16)  # Monday
        expected = [0.80, 1.00, 1.10, 1.10, 0.85, 0.00, 0.00]
        for offset, exp in enumerate(expected):
            d = date(2026, 3, 16 + offset)
            assert production_multiplier(d) == pytest.approx(exp), (
                f"Wrong multiplier for {d}: expected {exp}"
            )


# ---------------------------------------------------------------------------
# order_multiplier
# ---------------------------------------------------------------------------

class TestOrderMultiplier:
    def test_saturday_returns_0_10(self) -> None:
        assert order_multiplier(date(2026, 3, 14)) == pytest.approx(0.10)

    def test_sunday_returns_0_05(self) -> None:
        assert order_multiplier(date(2026, 3, 15)) == pytest.approx(0.05)

    def test_monday_returns_0_90(self) -> None:
        assert order_multiplier(date(2026, 3, 16)) == pytest.approx(0.90)

    def test_holiday_still_returns_dow_multiplier(self) -> None:
        # Easter Monday 2026 = Apr 6 = Monday → should return 0.90, not 0
        assert order_multiplier(date(2026, 4, 6)) == pytest.approx(0.90)

    def test_corpus_christi_is_thursday_2026(self) -> None:
        # Jun 4 2026 is a Thursday → order multiplier = 1.00
        assert order_multiplier(date(2026, 6, 4)) == pytest.approx(1.00)

    def test_full_dow_table(self) -> None:
        week_start = date(2026, 3, 16)  # Monday
        expected = [0.90, 1.05, 1.05, 1.00, 0.85, 0.10, 0.05]
        for offset, exp in enumerate(expected):
            d = date(2026, 3, 16 + offset)
            assert order_multiplier(d) == pytest.approx(exp), (
                f"Wrong order multiplier for {d}: expected {exp}"
            )


# ---------------------------------------------------------------------------
# is_edi_only_day
# ---------------------------------------------------------------------------

class TestIsEdiOnlyDay:
    def test_saturday_is_edi_only(self) -> None:
        assert is_edi_only_day(date(2026, 3, 14))

    def test_sunday_is_edi_only(self) -> None:
        assert is_edi_only_day(date(2026, 3, 15))

    def test_monday_is_not_edi_only(self) -> None:
        assert not is_edi_only_day(date(2026, 3, 16))

    def test_holiday_monday_is_not_edi_only(self) -> None:
        # Easter Monday is still a weekday structurally, not an EDI-only day
        assert not is_edi_only_day(date(2026, 4, 6))


# ---------------------------------------------------------------------------
# is_first_business_day_of_month
# ---------------------------------------------------------------------------

class TestIsFirstBusinessDayOfMonth:
    def test_jan_holiday_shifted(self) -> None:
        """Jan 1 is a holiday, so Jan 2 (Friday) is the first business day."""
        assert is_first_business_day_of_month(date(2026, 1, 2)) is True

    def test_jan_1_not_first_biz_day(self) -> None:
        """Jan 1 is a holiday — not a business day at all."""
        assert is_first_business_day_of_month(date(2026, 1, 1)) is False

    def test_march_sunday_shifted(self) -> None:
        """March 1 2026 is a Sunday, so March 2 (Monday) is the first business day."""
        assert is_first_business_day_of_month(date(2026, 3, 2)) is True

    def test_march_1_not_first_biz_day(self) -> None:
        """March 1 2026 is a Sunday — not a business day."""
        assert is_first_business_day_of_month(date(2026, 3, 1)) is False

    def test_second_business_day_is_not_first(self) -> None:
        """Jan 5 (Monday) is NOT the first business day of January 2026 (Jan 2 is)."""
        assert is_first_business_day_of_month(date(2026, 1, 5)) is False

    def test_weekend_is_not_first_biz_day(self) -> None:
        """A Saturday can never be the first business day of the month."""
        assert is_first_business_day_of_month(date(2026, 2, 7)) is False
