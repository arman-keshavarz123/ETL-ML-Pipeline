"""Pydantic schema for raw OHLCV (Open-High-Low-Close-Volume) price data."""

from __future__ import annotations

from pydantic import BaseModel, field_validator


class OHLCVRecord(BaseModel):
    """A single daily price record from a financial API."""

    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float

    @field_validator("open", "high", "low", "close")
    @classmethod
    def price_must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError(f"price must be positive, got {v}")
        return v

    @field_validator("volume")
    @classmethod
    def volume_must_be_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"volume must be non-negative, got {v}")
        return v
