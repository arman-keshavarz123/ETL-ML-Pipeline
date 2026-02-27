"""Tests for TechnicalIndicatorTransformer and OHLCVRecord schema."""

from __future__ import annotations

import json
import math
from pathlib import Path
from textwrap import dedent

import pandas as pd
import pytest
from pydantic import ValidationError

from data_extractor.schemas.ohlcv import OHLCVRecord
from data_extractor.transformers.finance_transformer import (
    TechnicalIndicatorTransformer,
)


# ── Helpers ─────────────────────────────────────────────────────────


def _make_ohlcv_df(n: int = 100, start_price: float = 100.0) -> pd.DataFrame:
    """Generate synthetic OHLCV data with a slight upward drift.

    Produces deterministic prices so indicator values are reproducible.
    """
    import numpy as np

    rng = np.random.default_rng(42)
    dates = pd.date_range("2023-01-01", periods=n, freq="B")
    close = start_price + np.cumsum(rng.normal(0.1, 1.5, n))
    high = close + rng.uniform(0.5, 2.0, n)
    low = close - rng.uniform(0.5, 2.0, n)
    open_ = close + rng.normal(0, 0.5, n)
    volume = rng.integers(1_000_000, 10_000_000, n).astype(float)

    return pd.DataFrame(
        {
            "date": dates.strftime("%Y-%m-%d"),
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
    )


# =====================================================================
# OHLCVRecord Pydantic schema
# =====================================================================


class TestOHLCVRecord:
    """Pydantic validation of raw financial records."""

    def test_valid_record(self):
        r = OHLCVRecord(
            date="2024-01-15", open=150.0, high=155.0,
            low=149.0, close=153.0, volume=5000000,
        )
        assert r.close == 153.0

    def test_negative_price_rejected(self):
        with pytest.raises(ValidationError, match="positive"):
            OHLCVRecord(
                date="2024-01-15", open=-1.0, high=155.0,
                low=149.0, close=153.0, volume=5000000,
            )

    def test_zero_price_rejected(self):
        with pytest.raises(ValidationError, match="positive"):
            OHLCVRecord(
                date="2024-01-15", open=0.0, high=155.0,
                low=149.0, close=153.0, volume=5000000,
            )

    def test_negative_volume_rejected(self):
        with pytest.raises(ValidationError, match="non-negative"):
            OHLCVRecord(
                date="2024-01-15", open=150.0, high=155.0,
                low=149.0, close=153.0, volume=-100,
            )

    def test_zero_volume_accepted(self):
        r = OHLCVRecord(
            date="2024-01-15", open=150.0, high=155.0,
            low=149.0, close=153.0, volume=0,
        )
        assert r.volume == 0

    def test_missing_field_rejected(self):
        with pytest.raises(ValidationError):
            OHLCVRecord(
                date="2024-01-15", open=150.0,
                low=149.0, close=153.0, volume=5000000,
            )


# =====================================================================
# TechnicalIndicatorTransformer — validation
# =====================================================================


class TestTransformerValidation:
    """Pre-condition checks on input DataFrame."""

    def test_missing_columns_raises(self):
        df = pd.DataFrame({"date": ["2024-01-01"], "close": [100]})
        t = TechnicalIndicatorTransformer({})
        with pytest.raises(ValueError, match="Missing"):
            t.validate(df)

    def test_valid_columns_pass(self):
        df = _make_ohlcv_df(5)
        t = TechnicalIndicatorTransformer({})
        t.validate(df)  # should not raise

    def test_case_insensitive_columns(self):
        """Columns like 'Close' or 'DATE' should still pass validation."""
        df = _make_ohlcv_df(5)
        df.columns = [c.upper() for c in df.columns]
        t = TechnicalIndicatorTransformer({})
        t.validate(df)  # should not raise


# =====================================================================
# TechnicalIndicatorTransformer — indicator calculations
# =====================================================================


class TestIndicatorCalculations:
    """Verify computed indicator columns and their values."""

    @pytest.fixture()
    def result_df(self) -> pd.DataFrame:
        """Run the transformer on 100 rows of synthetic data."""
        df = _make_ohlcv_df(100)
        t = TechnicalIndicatorTransformer({})
        return t.transform(df)

    def test_output_has_indicator_columns(self, result_df):
        expected = {
            "sma_50", "rsi_14", "bb_upper", "bb_lower",
            "macd", "macd_signal", "macd_histogram",
        }
        assert expected.issubset(set(result_df.columns))

    def test_no_nan_values_in_output(self, result_df):
        """Rolling-window warmup rows must be dropped."""
        assert result_df.isna().sum().sum() == 0

    def test_rows_dropped_from_warmup(self):
        """100 rows in → fewer rows out (SMA-50 needs 49 warmup rows)."""
        df = _make_ohlcv_df(100)
        t = TechnicalIndicatorTransformer({})
        result = t.transform(df)
        assert len(result) < 100
        # SMA-50 is the widest window → at least 49 rows dropped
        assert len(result) <= 51

    def test_rsi_bounded_0_to_100(self, result_df):
        assert (result_df["rsi_14"] >= 0).all()
        assert (result_df["rsi_14"] <= 100).all()

    def test_sma_50_is_mean_of_last_50(self, result_df):
        """Spot-check: SMA at a given row should equal the mean of the prior 50 closes."""
        # Re-run on raw data to get aligned indexes
        df = _make_ohlcv_df(100)
        df = df.sort_values("date").reset_index(drop=True)
        close = pd.to_numeric(df["close"])
        # Check the value at index 60 (after warmup)
        expected_sma = close.iloc[11:61].mean()
        t = TechnicalIndicatorTransformer({})
        full_result = t.transform(df)
        # The output is reset after dropna, so find the row by matching close price
        row = full_result[full_result["close"] == close.iloc[60]]
        if not row.empty:
            assert abs(row.iloc[0]["sma_50"] - expected_sma) < 0.01

    def test_bollinger_upper_above_lower(self, result_df):
        assert (result_df["bb_upper"] > result_df["bb_lower"]).all()

    def test_macd_histogram_equals_macd_minus_signal(self, result_df):
        diff = result_df["macd"] - result_df["macd_signal"]
        assert (abs(diff - result_df["macd_histogram"]) < 1e-10).all()

    def test_original_columns_preserved(self, result_df):
        for col in ("date", "open", "high", "low", "close", "volume"):
            assert col in result_df.columns


# =====================================================================
# Date handling
# =====================================================================


class TestDateHandling:
    """Financial APIs return dates in various formats — all must be handled."""

    def _run_with_dates(self, date_strings: list[str]) -> pd.DataFrame:
        n = len(date_strings)
        df = _make_ohlcv_df(n)
        df["date"] = date_strings
        t = TechnicalIndicatorTransformer({"sma_period": 2, "bb_period": 2, "rsi_period": 2})
        return t.transform(df)

    def test_iso_date_parsed(self):
        dates = [f"2024-01-{d:02d}" for d in range(1, 31)]
        result = self._run_with_dates(dates)
        assert all("T" in d for d in result["date"])

    def test_datetime_with_timezone_parsed(self):
        """Alpha Vantage style: '2024-02-26 16:00:00-04:00'."""
        dates = [f"2024-01-{d:02d} 16:00:00-04:00" for d in range(1, 31)]
        result = self._run_with_dates(dates)
        # Should be converted to UTC ISO strings
        assert all(d.endswith("Z") for d in result["date"])

    def test_datetime_naive_parsed(self):
        dates = [f"2024-01-{d:02d} 09:30:00" for d in range(1, 31)]
        result = self._run_with_dates(dates)
        assert all("T" in d for d in result["date"])

    def test_dates_are_iso_strings_for_sqlite(self):
        """Output dates should be plain strings (not Timestamp objects)."""
        df = _make_ohlcv_df(60)
        t = TechnicalIndicatorTransformer({"sma_period": 5, "bb_period": 5, "rsi_period": 5})
        result = t.transform(df)
        # Pandas 3.x uses StringDtype ("str"/"string"), older uses "object"
        assert pd.api.types.is_string_dtype(result["date"])
        # Each value should be a Python str
        assert isinstance(result["date"].iloc[0], str)


# =====================================================================
# Custom periods via config
# =====================================================================


class TestCustomPeriods:
    """Verify that config overrides for indicator periods work."""

    def test_shorter_periods_produce_more_rows(self):
        """With smaller windows, fewer warmup rows are dropped."""
        df = _make_ohlcv_df(60)
        default = TechnicalIndicatorTransformer({})
        custom = TechnicalIndicatorTransformer(
            {"sma_period": 5, "bb_period": 5, "rsi_period": 5}
        )
        result_default = default.transform(df)
        result_custom = custom.transform(df)
        assert len(result_custom) > len(result_default)

    def test_config_values_propagated(self):
        t = TechnicalIndicatorTransformer(
            {"rsi_period": 7, "sma_period": 20, "bb_period": 10, "bb_std": 1.5}
        )
        assert t._rsi_period == 7
        assert t._sma_period == 20
        assert t._bb_period == 10
        assert t._bb_std == 1.5


# =====================================================================
# Edge cases
# =====================================================================


class TestEdgeCases:
    """Boundary conditions and degenerate inputs."""

    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])
        t = TechnicalIndicatorTransformer({})
        result = t.transform(df)
        assert result.empty

    def test_too_few_rows_returns_empty(self):
        """If fewer rows than the largest window, all are dropped as NaN."""
        df = _make_ohlcv_df(10)
        t = TechnicalIndicatorTransformer({"sma_period": 50})
        result = t.transform(df)
        assert result.empty

    def test_unsorted_dates_get_sorted(self):
        """Dates not in order should be sorted before indicator computation."""
        df = _make_ohlcv_df(60)
        # Reverse the order
        df = df.iloc[::-1].reset_index(drop=True)
        t = TechnicalIndicatorTransformer({"sma_period": 5, "bb_period": 5, "rsi_period": 5})
        result = t.transform(df)
        dates = result["date"].tolist()
        assert dates == sorted(dates)

    def test_string_numeric_columns_coerced(self):
        """Price columns that arrive as strings should be coerced to float."""
        df = _make_ohlcv_df(60)
        df["close"] = df["close"].astype(str)
        df["volume"] = df["volume"].astype(str)
        t = TechnicalIndicatorTransformer({"sma_period": 5, "bb_period": 5, "rsi_period": 5})
        result = t.transform(df)
        assert result["close"].dtype in ("float64", "float32")

    def test_uppercase_columns_handled(self):
        """Columns like 'Close', 'HIGH' should be lowercased."""
        df = _make_ohlcv_df(60)
        df.columns = [c.upper() for c in df.columns]
        t = TechnicalIndicatorTransformer({"sma_period": 5, "bb_period": 5, "rsi_period": 5})
        result = t.transform(df)
        assert "close" in result.columns
        assert "sma_50" in result.columns


# =====================================================================
# Full pipeline E2E with finance transformer + SQL loader
# =====================================================================


class TestFinancePipelineE2E:
    """End-to-end: JSON file → technical_indicators → SQLite."""

    def test_ohlcv_to_sqlite(self, tmp_path: Path):
        """Synthetic OHLCV data through the full pipeline to SQLite."""
        # Write synthetic data
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        df = _make_ohlcv_df(100)
        records = df.to_dict(orient="records")
        (data_dir / "prices.json").write_text(json.dumps(records))

        cfg_dir = tmp_path / "configs"
        cfg_dir.mkdir()
        (cfg_dir / "source.yaml").write_text(
            f'file_path: "{data_dir / "prices.json"}"\norient: "records"\n'
        )
        (cfg_dir / "indicators.yaml").write_text(dedent("""\
            rsi_period: 14
            sma_period: 50
            bb_period: 20
        """))

        db_path = tmp_path / "finance.db"
        (cfg_dir / "loader.yaml").write_text(dedent(f"""\
            connection_string: "sqlite:///{db_path}"
            table_name: "daily_features"
            if_exists: "replace"
            index: false
        """))

        pipeline_yaml = (
            f'version: "1.0"\n'
            f"pipeline:\n"
            f'  name: "finance_e2e"\n'
            f"  extract:\n"
            f'    source: "json_file"\n'
            f'    config_file: "{cfg_dir / "source.yaml"}"\n'
            f"  transform:\n"
            f'    - name: "technical_indicators"\n'
            f'      config_file: "{cfg_dir / "indicators.yaml"}"\n'
            f"  load:\n"
            f'    destination: "sql_database"\n'
            f'    config_file: "{cfg_dir / "loader.yaml"}"\n'
            f"settings:\n"
            f'  log_level: "WARNING"\n'
            f"  retry:\n"
            f"    max_attempts: 1\n"
            f"    backoff_seconds: 0\n"
        )
        config_path = tmp_path / "pipeline.yaml"
        config_path.write_text(pipeline_yaml)

        from data_extractor.engine import PipelineEngine

        PipelineEngine(config_path).run()

        # Verify data in SQLite
        from sqlalchemy import create_engine, text

        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT * FROM daily_features")).fetchall()
            cols = conn.execute(text("PRAGMA table_info(daily_features)")).fetchall()

        col_names = [c[1] for c in cols]
        assert "sma_50" in col_names
        assert "rsi_14" in col_names
        assert "bb_upper" in col_names
        assert "macd" in col_names
        assert len(rows) > 0
        # No NaN values should have made it to the database
        for row in rows:
            for val in row:
                if isinstance(val, float):
                    assert not math.isnan(val), f"NaN found in database row: {row}"
