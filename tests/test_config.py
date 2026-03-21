"""Tests for the config loader and Pydantic validation (Step 2)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError

from flowform.config import Config, NoiseProfile, load_config

# Absolute path to the project root so tests work from any cwd.
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_PATH = PROJECT_ROOT / "config.yaml"


# ---------------------------------------------------------------------------
# Helper: build a minimal valid raw config dict so individual section tests
# can swap out just the part they care about.
# ---------------------------------------------------------------------------

def _base_raw() -> dict:
    return {
        "simulation": {
            "start_date": "2026-01-05",
            "seed": 42,
            "country": "PL",
        },
        "company": {
            "daily_production_range": [200, 800],
            "production_stoppage_probability": 0.025,
            "production_lines": 5,
            "warehouse_transfer_threshold": 0.3,
        },
        "customers": {
            "count": 60,
            "churn_rate_monthly": 0.01,
            "new_customer_rate_monthly": 0.02,
            "currency_distribution": {"PLN": 0.65, "EUR": 0.30, "USD": 0.05},
        },
        "demand": {
            "base_monthly_multipliers": {
                1: 0.70, 2: 0.85, 3: 1.15, 4: 1.25, 5: 1.05, 6: 1.00,
                7: 0.75, 8: 0.80, 9: 1.15, 10: 1.10, 11: 1.15, 12: 0.55,
            },
            "signal_probability_daily": 0.05,
        },
        "orders": {
            "modification_probability": 0.10,
            "cancellation_probability": 0.025,
            "express_share": 0.12,
            "critical_share": 0.03,
            "backorder_escalation_days": 14,
            "backorder_cancel_probability_after_30d": 0.15,
        },
        "loads": {
            "exception_probability": 0.08,
            "consolidation_window_days": 3,
            "return_probability": 0.015,
            "pod_delay_days": [1, 3],
        },
        "payments": {
            "on_time_probability": 0.82,
            "late_probability": 0.15,
            "very_late_probability": 0.03,
        },
        "exchange_rates": {
            "base": {"EUR_PLN": 4.30, "USD_PLN": 4.05},
            "daily_volatility": 0.003,
        },
        "schema_evolution": {
            "sales_channel_from_day": 60,
            "incoterms_from_day": 120,
            "forecast_model_upgrade_day": 90,
        },
        "noise": {"profile": "medium"},
        "disruptions": {"enabled": False},
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestLoadConfigValid:
    """Load the real config.yaml and assert key field values."""

    def test_load_config_valid(self) -> None:
        config = load_config(CONFIG_PATH)

        # simulation section
        assert config.simulation.seed == 42
        assert config.simulation.start_date == date(2026, 1, 5)
        assert config.simulation.country == "PL"

        # company section
        assert config.company.daily_production_range == (200, 800)
        assert config.company.production_lines == 5

        # customers section
        assert config.customers.count == 60
        assert config.customers.currency_distribution.PLN == pytest.approx(0.65)
        assert config.customers.currency_distribution.EUR == pytest.approx(0.30)
        assert config.customers.currency_distribution.USD == pytest.approx(0.05)

        # demand section
        assert len(config.demand.base_monthly_multipliers) == 12
        assert config.demand.base_monthly_multipliers[1] == pytest.approx(0.70)
        assert config.demand.base_monthly_multipliers[12] == pytest.approx(0.55)

        # orders section
        assert config.orders.express_share == pytest.approx(0.12)
        assert config.orders.critical_share == pytest.approx(0.03)
        assert config.orders.backorder_escalation_days == 14

        # loads section
        assert config.loads.pod_delay_days == (1, 3)
        assert config.loads.exception_probability == pytest.approx(0.08)

        # payments section
        assert config.payments.on_time_probability == pytest.approx(0.82)
        assert config.payments.late_probability == pytest.approx(0.15)
        assert config.payments.very_late_probability == pytest.approx(0.03)

        # exchange rates section
        assert config.exchange_rates.base.EUR_PLN == pytest.approx(4.30)
        assert config.exchange_rates.base.USD_PLN == pytest.approx(4.05)
        assert config.exchange_rates.daily_volatility == pytest.approx(0.003)

        # schema evolution section
        assert config.schema_evolution.sales_channel_from_day == 60
        assert config.schema_evolution.incoterms_from_day == 120
        assert config.schema_evolution.forecast_model_upgrade_day == 90

        # noise section
        assert config.noise.profile == NoiseProfile.medium

        # disruptions section
        assert config.disruptions.enabled is True

    def test_returns_config_instance(self) -> None:
        config = load_config(CONFIG_PATH)
        assert isinstance(config, Config)


class TestLoadConfigInvalidProbabilities:
    """Validation errors are raised when invariants are violated."""

    def test_payments_sum_not_one(self) -> None:
        raw = _base_raw()
        raw["payments"]["on_time_probability"] = 0.50  # sum = 0.68, not 1.0
        with pytest.raises(ValidationError) as exc_info:
            Config.model_validate(raw)
        assert "payments" in str(exc_info.value).lower() or "sum" in str(exc_info.value).lower()

    def test_currency_distribution_sum_not_one(self) -> None:
        raw = _base_raw()
        raw["customers"]["currency_distribution"]["PLN"] = 0.90  # sum = 1.25
        with pytest.raises(ValidationError):
            Config.model_validate(raw)

    def test_production_range_min_ge_max(self) -> None:
        raw = _base_raw()
        raw["company"]["daily_production_range"] = [800, 200]
        with pytest.raises(ValidationError):
            Config.model_validate(raw)

    def test_express_plus_critical_exceeds_one(self) -> None:
        raw = _base_raw()
        raw["orders"]["express_share"] = 0.70
        raw["orders"]["critical_share"] = 0.40  # sum = 1.10
        with pytest.raises(ValidationError):
            Config.model_validate(raw)

    def test_demand_monthly_multipliers_missing_key(self) -> None:
        raw = _base_raw()
        del raw["demand"]["base_monthly_multipliers"][6]  # remove June
        with pytest.raises(ValidationError):
            Config.model_validate(raw)

    def test_demand_monthly_multipliers_extra_key(self) -> None:
        raw = _base_raw()
        raw["demand"]["base_monthly_multipliers"][13] = 1.0  # month 13 doesn't exist
        with pytest.raises(ValidationError):
            Config.model_validate(raw)

    def test_noise_profile_invalid(self) -> None:
        raw = _base_raw()
        raw["noise"]["profile"] = "extreme"
        with pytest.raises(ValidationError):
            Config.model_validate(raw)

    def test_production_lines_below_one(self) -> None:
        raw = _base_raw()
        raw["company"]["production_lines"] = 0
        with pytest.raises(ValidationError):
            Config.model_validate(raw)

    def test_exchange_rate_zero(self) -> None:
        raw = _base_raw()
        raw["exchange_rates"]["base"]["EUR_PLN"] = 0.0
        with pytest.raises(ValidationError):
            Config.model_validate(raw)


class TestLoadConfigMissingFile:
    """FileNotFoundError is raised for non-existent paths."""

    def test_missing_file(self, tmp_path: Path) -> None:
        nonexistent = tmp_path / "does_not_exist.yaml"
        with pytest.raises(FileNotFoundError) as exc_info:
            load_config(nonexistent)
        assert "does_not_exist.yaml" in str(exc_info.value)

    def test_missing_file_string_path(self) -> None:
        with pytest.raises(FileNotFoundError):
            load_config("/tmp/definitely_not_here_flowform_xyz.yaml")
