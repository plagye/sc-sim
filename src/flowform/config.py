"""Config loader and Pydantic v2 validation for FlowForm simulation engine."""

from __future__ import annotations

from datetime import date
from enum import Enum
from pathlib import Path
from typing import Annotated

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class SimulationConfig(BaseModel):
    start_date: date
    seed: int
    country: str = "PL"


class CompanyConfig(BaseModel):
    daily_production_range: tuple[int, int]
    production_stoppage_probability: Annotated[float, Field(ge=0.0, le=1.0)]
    production_lines: Annotated[int, Field(ge=1)]
    warehouse_transfer_threshold: Annotated[float, Field(ge=0.0, le=1.0)]

    @field_validator("daily_production_range")
    @classmethod
    def validate_production_range(cls, v: tuple[int, int]) -> tuple[int, int]:
        if v[0] >= v[1]:
            raise ValueError(
                f"daily_production_range min ({v[0]}) must be less than max ({v[1]})"
            )
        return v


class CurrencyDistribution(BaseModel):
    PLN: Annotated[float, Field(ge=0.0, le=1.0)]
    EUR: Annotated[float, Field(ge=0.0, le=1.0)]
    USD: Annotated[float, Field(ge=0.0, le=1.0)]

    @model_validator(mode="after")
    def validate_sum(self) -> "CurrencyDistribution":
        total = self.PLN + self.EUR + self.USD
        if abs(total - 1.0) > 0.001:
            raise ValueError(
                f"currency_distribution values must sum to 1.0, got {total:.4f}"
            )
        return self


class CustomersConfig(BaseModel):
    count: Annotated[int, Field(ge=1)]
    churn_rate_monthly: Annotated[float, Field(ge=0.0, le=1.0)]
    new_customer_rate_monthly: Annotated[float, Field(ge=0.0, le=1.0)]
    currency_distribution: CurrencyDistribution


class DemandConfig(BaseModel):
    base_monthly_multipliers: dict[int, float]
    signal_probability_daily: Annotated[float, Field(ge=0.0, le=1.0)]

    @field_validator("base_monthly_multipliers")
    @classmethod
    def validate_monthly_keys(cls, v: dict[int, float]) -> dict[int, float]:
        expected = set(range(1, 13))
        actual = set(v.keys())
        if actual != expected:
            missing = expected - actual
            extra = actual - expected
            parts: list[str] = []
            if missing:
                parts.append(f"missing keys: {sorted(missing)}")
            if extra:
                parts.append(f"unexpected keys: {sorted(extra)}")
            raise ValueError(
                f"base_monthly_multipliers must have exactly keys 1–12; {', '.join(parts)}"
            )
        return v


class OrdersConfig(BaseModel):
    modification_probability: Annotated[float, Field(ge=0.0, le=1.0)]
    cancellation_probability: Annotated[float, Field(ge=0.0, le=1.0)]
    express_share: Annotated[float, Field(ge=0.0, le=1.0)]
    critical_share: Annotated[float, Field(ge=0.0, le=1.0)]
    backorder_escalation_days: Annotated[int, Field(ge=1)]
    backorder_cancel_probability_after_30d: Annotated[float, Field(ge=0.0, le=1.0)]

    @model_validator(mode="after")
    def validate_order_shares(self) -> "OrdersConfig":
        total = self.express_share + self.critical_share
        if total > 1.0:
            raise ValueError(
                f"express_share + critical_share must be <= 1.0, got {total:.4f}"
            )
        return self


class LoadsConfig(BaseModel):
    exception_probability: Annotated[float, Field(ge=0.0, le=1.0)]
    consolidation_window_days: Annotated[int, Field(ge=0)]
    return_probability: Annotated[float, Field(ge=0.0, le=1.0)]
    pod_delay_days: tuple[int, int]

    @field_validator("pod_delay_days")
    @classmethod
    def validate_pod_delay(cls, v: tuple[int, int]) -> tuple[int, int]:
        if v[0] > v[1]:
            raise ValueError(
                f"pod_delay_days min ({v[0]}) must be <= max ({v[1]})"
            )
        return v


class PaymentsConfig(BaseModel):
    on_time_probability: Annotated[float, Field(ge=0.0, le=1.0)]
    late_probability: Annotated[float, Field(ge=0.0, le=1.0)]
    very_late_probability: Annotated[float, Field(ge=0.0, le=1.0)]

    @model_validator(mode="after")
    def validate_sum(self) -> "PaymentsConfig":
        total = self.on_time_probability + self.late_probability + self.very_late_probability
        if abs(total - 1.0) > 0.001:
            raise ValueError(
                f"payments probabilities must sum to 1.0, got {total:.4f}"
            )
        return self


class ExchangeRateBase(BaseModel):
    EUR_PLN: Annotated[float, Field(gt=0.0)]
    USD_PLN: Annotated[float, Field(gt=0.0)]


class ExchangeRatesConfig(BaseModel):
    base: ExchangeRateBase
    daily_volatility: Annotated[float, Field(gt=0.0)]


class SchemaEvolutionConfig(BaseModel):
    sales_channel_from_day: Annotated[int, Field(ge=0)]
    incoterms_from_day: Annotated[int, Field(ge=0)]
    forecast_model_upgrade_day: Annotated[int, Field(ge=0)]


class NoiseProfile(str, Enum):
    light = "light"
    medium = "medium"
    heavy = "heavy"


class NoiseConfig(BaseModel):
    profile: NoiseProfile
    erp_defer_probability: float = 0.02
    tms_late_probability: float = 0.05
    maintenance_probability: float = 0.045


class DisruptionsConfig(BaseModel):
    enabled: bool = False


class Config(BaseModel):
    simulation: SimulationConfig
    company: CompanyConfig
    customers: CustomersConfig
    demand: DemandConfig
    orders: OrdersConfig
    loads: LoadsConfig
    payments: PaymentsConfig
    exchange_rates: ExchangeRatesConfig
    schema_evolution: SchemaEvolutionConfig
    noise: NoiseConfig
    disruptions: DisruptionsConfig


def load_config(path: Path | str = "config.yaml") -> Config:
    """Load and validate the simulation configuration from a YAML file.

    Args:
        path: Path to the config.yaml file. Defaults to "config.yaml".

    Returns:
        A validated Config instance.

    Raises:
        FileNotFoundError: If the config file does not exist.
        pydantic.ValidationError: If any field fails validation.
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path.resolve()}"
        )

    with config_path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    return Config.model_validate(raw)
