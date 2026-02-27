"""Financial technical indicator transformer.

Computes common technical indicators from OHLCV price data:
  - 14-day RSI (Relative Strength Index)
  - 50-day SMA (Simple Moving Average of close)
  - 20-day Bollinger Bands (Upper and Lower)
  - MACD (12/26/9 EMA crossover)

Rolling-window calculations produce NaN values for the initial rows where
the window is not yet full.  These rows are dropped before output so
downstream loaders always receive clean data.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from data_extractor.registry import register_transformer
from data_extractor.transformers.base import BaseTransformer

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"date", "open", "high", "low", "close", "volume"}


@register_transformer("technical_indicators")
class TechnicalIndicatorTransformer(BaseTransformer):
    """Compute technical indicators from OHLCV data."""

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._rsi_period: int = config.get("rsi_period", 14)
        self._sma_period: int = config.get("sma_period", 50)
        self._bb_period: int = config.get("bb_period", 20)
        self._bb_std: float = config.get("bb_std", 2.0)
        self._macd_fast: int = config.get("macd_fast", 12)
        self._macd_slow: int = config.get("macd_slow", 26)
        self._macd_signal: int = config.get("macd_signal", 9)

    def validate(self, df: pd.DataFrame) -> None:
        missing = REQUIRED_COLUMNS - set(c.lower() for c in df.columns)
        if missing:
            raise ValueError(
                f"TechnicalIndicatorTransformer requires columns "
                f"{sorted(REQUIRED_COLUMNS)}. Missing: {sorted(missing)}"
            )

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        # Normalize column names to lowercase for consistent access
        result.columns = [c.lower() for c in result.columns]

        # Parse dates and convert to ISO-8601 strings for SQLite compat.
        # format="mixed" handles the variety of date formats financial APIs return
        # (e.g. "2024-01-15", "2024-01-15 16:00:00-04:00", "20240115").
        result["date"] = pd.to_datetime(
            result["date"], utc=True, format="mixed"
        ).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Ensure numeric types for price/volume columns
        for col in ("open", "high", "low", "close", "volume"):
            result[col] = pd.to_numeric(result[col], errors="coerce")

        # Sort by date ascending so rolling windows are chronological
        result = result.sort_values("date").reset_index(drop=True)

        rows_before = len(result)

        # ── Indicators ────────────────────────────────────────────
        close = result["close"]

        result["sma_50"] = self._compute_sma(close, self._sma_period)
        result["rsi_14"] = self._compute_rsi(close, self._rsi_period)
        result["bb_upper"], result["bb_lower"] = self._compute_bollinger(
            close, self._bb_period, self._bb_std
        )
        result["macd"], result["macd_signal"], result["macd_histogram"] = (
            self._compute_macd(
                close, self._macd_fast, self._macd_slow, self._macd_signal
            )
        )

        # ── Drop rows with NaN from rolling-window warmup ─────────
        result = result.dropna().reset_index(drop=True)
        rows_after = len(result)

        logger.info(
            "%s: %d→%d rows (%d warmup rows dropped)",
            self.name,
            rows_before,
            rows_after,
            rows_before - rows_after,
        )
        return result

    # ------------------------------------------------------------------
    # Indicator calculations (pure functions over Series)
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_sma(close: pd.Series, period: int) -> pd.Series:
        """Simple Moving Average."""
        return close.rolling(window=period).mean()

    @staticmethod
    def _compute_rsi(close: pd.Series, period: int) -> pd.Series:
        """Relative Strength Index (Wilder's smoothed)."""
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)

        avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    @staticmethod
    def _compute_bollinger(
        close: pd.Series, period: int, num_std: float
    ) -> tuple[pd.Series, pd.Series]:
        """Bollinger Bands (upper, lower)."""
        sma = close.rolling(window=period).mean()
        std = close.rolling(window=period).std()
        upper = sma + num_std * std
        lower = sma - num_std * std
        return upper, lower

    @staticmethod
    def _compute_macd(
        close: pd.Series, fast: int, slow: int, signal: int
    ) -> tuple[pd.Series, pd.Series, pd.Series]:
        """MACD line, signal line, and histogram."""
        ema_fast = close.ewm(span=fast, adjust=False).mean()
        ema_slow = close.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram
