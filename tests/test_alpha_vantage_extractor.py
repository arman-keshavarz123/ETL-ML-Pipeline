"""Tests for AlphaVantageExtractor."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from data_extractor.extractors.alpha_vantage import AlphaVantageExtractor


# ── Fixtures ────────────────────────────────────────────────────────


def _make_av_response(n: int = 5) -> dict:
    """Build a fake Alpha Vantage TIME_SERIES_DAILY response."""
    import pandas as _pd

    dates = _pd.date_range("2024-01-01", periods=n, freq="B")
    series = {}
    for i, dt in enumerate(dates):
        date = dt.strftime("%Y-%m-%d")
        series[date] = {
            "1. open": f"{100 + i}.0000",
            "2. high": f"{105 + i}.0000",
            "3. low": f"{95 + i}.0000",
            "4. close": f"{102 + i}.0000",
            "5. volume": str(1_000_000 + i * 100_000),
        }
    return {
        "Meta Data": {
            "1. Information": "Daily Prices",
            "2. Symbol": "IBM",
            "3. Last Refreshed": dates[-1].strftime("%Y-%m-%d"),
        },
        "Time Series (Daily)": series,
    }


def _mock_extractor(response_data: dict, config: dict | None = None) -> AlphaVantageExtractor:
    """Create an extractor with a mocked HTTP client."""
    cfg = config or {
        "base_url": "https://www.alphavantage.co",
        "endpoint": "/query",
        "query_params": {"function": "TIME_SERIES_DAILY", "symbol": "IBM", "apikey": "demo"},
    }
    extractor = AlphaVantageExtractor(cfg)

    mock_response = MagicMock()
    mock_response.json.return_value = response_data
    mock_response.raise_for_status = MagicMock()

    mock_client = MagicMock()
    mock_client.get.return_value = mock_response
    extractor._client = mock_client

    return extractor


# =====================================================================
# Response parsing
# =====================================================================


class TestResponseParsing:
    """Verify correct flattening of the nested Alpha Vantage response."""

    def test_returns_dataframe_with_ohlcv_columns(self):
        ext = _mock_extractor(_make_av_response(3))
        df = ext.extract()
        assert set(df.columns) == {"date", "open", "high", "low", "close", "volume"}

    def test_correct_row_count(self):
        ext = _mock_extractor(_make_av_response(10))
        df = ext.extract()
        assert len(df) == 10

    def test_date_column_from_dict_keys(self):
        ext = _mock_extractor(_make_av_response(3))
        df = ext.extract()
        dates = sorted(df["date"].tolist())
        # Business days starting 2024-01-01 (Mon)
        assert dates == ["2024-01-01", "2024-01-02", "2024-01-03"]

    def test_numeric_columns_are_float(self):
        ext = _mock_extractor(_make_av_response(3))
        df = ext.extract()
        for col in ("open", "high", "low", "close", "volume"):
            assert pd.api.types.is_numeric_dtype(df[col]), f"{col} is not numeric"

    def test_values_parsed_correctly(self):
        ext = _mock_extractor(_make_av_response(1))
        df = ext.extract()
        row = df.iloc[0]
        assert row["open"] == 100.0
        assert row["high"] == 105.0
        assert row["low"] == 95.0
        assert row["close"] == 102.0
        assert row["volume"] == 1_000_000.0

    def test_column_prefix_stripped(self):
        """'1. open' should become 'open', not '1. open'."""
        ext = _mock_extractor(_make_av_response(1))
        df = ext.extract()
        assert "1. open" not in df.columns
        assert "open" in df.columns


# =====================================================================
# Series key detection
# =====================================================================


class TestSeriesKeyDetection:
    """The time-series key varies by function — auto-detect it."""

    def test_daily_key_detected(self):
        ext = _mock_extractor(_make_av_response(1))
        df = ext.extract()
        assert len(df) == 1

    def test_weekly_key_detected(self):
        response = {
            "Meta Data": {"1. Symbol": "IBM"},
            "Weekly Time Series": {
                "2024-01-05": {
                    "1. open": "100.0", "2. high": "105.0",
                    "3. low": "95.0", "4. close": "102.0", "5. volume": "5000000",
                },
            },
        }
        ext = _mock_extractor(response)
        df = ext.extract()
        assert len(df) == 1

    def test_custom_series_key_override(self):
        response = {
            "Meta Data": {},
            "My Custom Key": {
                "2024-01-01": {
                    "1. open": "100.0", "2. high": "105.0",
                    "3. low": "95.0", "4. close": "102.0", "5. volume": "5000000",
                },
            },
        }
        ext = _mock_extractor(
            response,
            config={
                "endpoint": "/query",
                "query_params": {},
                "series_key": "My Custom Key",
            },
        )
        df = ext.extract()
        assert len(df) == 1

    def test_no_series_key_raises(self):
        response = {"Meta Data": {"1. Symbol": "IBM"}}
        ext = _mock_extractor(response)
        with pytest.raises(KeyError, match="Could not find time-series data"):
            ext.extract()


# =====================================================================
# API error handling
# =====================================================================


class TestAPIErrorHandling:
    """Alpha Vantage returns errors as JSON, not HTTP status codes."""

    def test_error_message_raises(self):
        response = {"Error Message": "Invalid API call. Please check the parameters."}
        ext = _mock_extractor(response)
        with pytest.raises(ValueError, match="Alpha Vantage API error"):
            ext.extract()

    def test_rate_limit_note_raises(self):
        response = {
            "Note": "Thank you for using Alpha Vantage! "
            "Our standard API rate limit is 25 requests per day."
        }
        ext = _mock_extractor(response)
        with pytest.raises(ValueError, match="rate limit"):
            ext.extract()


# =====================================================================
# Config / auth
# =====================================================================


class TestConfig:
    """Config-related behavior."""

    def test_api_key_from_env(self, monkeypatch):
        monkeypatch.setenv("AV_API_KEY", "my_secret_key")
        ext = AlphaVantageExtractor({
            "base_url": "https://www.alphavantage.co",
            "endpoint": "/query",
            "query_params": {"function": "TIME_SERIES_DAILY", "symbol": "IBM"},
            "api_key_env": "AV_API_KEY",
        })
        ext.connect()
        assert ext._config["query_params"]["apikey"] == "my_secret_key"
        ext.disconnect()

    def test_empty_response_returns_empty_df(self):
        response = {
            "Meta Data": {"1. Symbol": "IBM"},
            "Time Series (Daily)": {},
        }
        ext = _mock_extractor(response)
        df = ext.extract()
        assert df.empty
        assert set(df.columns) == {"date", "open", "high", "low", "close", "volume"}


# =====================================================================
# Full pipeline E2E (mocked HTTP)
# =====================================================================


class TestAlphaVantagePipelineE2E:
    """End-to-end: Alpha Vantage → technical_indicators → SQLite."""

    def test_full_pipeline(self, tmp_path):
        from textwrap import dedent
        from data_extractor.engine import PipelineEngine

        # Write configs
        cfg_dir = tmp_path / "configs"
        cfg_dir.mkdir()

        (cfg_dir / "source.yaml").write_text(dedent("""\
            base_url: "https://www.alphavantage.co"
            endpoint: "/query"
            query_params:
              function: "TIME_SERIES_DAILY"
              symbol: "IBM"
              apikey: "demo"
        """))
        (cfg_dir / "indicators.yaml").write_text(dedent("""\
            rsi_period: 14
            sma_period: 20
            bb_period: 10
        """))

        db_path = tmp_path / "finance.db"
        (cfg_dir / "loader.yaml").write_text(dedent(f"""\
            connection_string: "sqlite:///{db_path}"
            table_name: "features"
            if_exists: "replace"
            index: false
        """))

        pipeline_yaml = (
            f'version: "1.0"\n'
            f"pipeline:\n"
            f'  name: "av_e2e"\n'
            f"  extract:\n"
            f'    source: "alpha_vantage"\n'
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
        (tmp_path / "pipeline.yaml").write_text(pipeline_yaml)

        # Mock the HTTP call
        av_response = _make_av_response(60)
        mock_resp = MagicMock()
        mock_resp.json.return_value = av_response
        mock_resp.raise_for_status = MagicMock()

        with patch("data_extractor.extractors.alpha_vantage.httpx.Client") as MockClient:
            MockClient.return_value.__enter__ = MagicMock(return_value=MockClient.return_value)
            MockClient.return_value.__exit__ = MagicMock(return_value=False)
            MockClient.return_value.get.return_value = mock_resp
            MockClient.return_value.close = MagicMock()

            PipelineEngine(tmp_path / "pipeline.yaml").run()

        # Verify data landed in SQLite
        from sqlalchemy import create_engine, text
        engine = create_engine(f"sqlite:///{db_path}")
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT * FROM features")).fetchall()
            cols = conn.execute(text("PRAGMA table_info(features)")).fetchall()

        col_names = [c[1] for c in cols]
        assert "sma_50" in col_names
        assert "rsi_14" in col_names
        assert "macd" in col_names
        assert len(rows) > 0
