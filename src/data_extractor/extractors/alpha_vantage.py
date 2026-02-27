"""Alpha Vantage extractor — fetches OHLCV data from the Alpha Vantage API.

The API returns a nested dict-of-dicts keyed by date with prefixed column
names (e.g. "1. open", "2. high").  This extractor flattens the response
into a standard DataFrame with columns: date, open, high, low, close, volume.

Config example (configs/sources/alpha_vantage.yaml)::

    base_url: "https://www.alphavantage.co"
    endpoint: "/query"
    query_params:
      function: "TIME_SERIES_DAILY"
      symbol: "IBM"
      outputsize: "full"
      apikey: "demo"
    timeout: 30
    # Optional — override if the API changes its response key names:
    # series_key: "Time Series (Daily)"
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx
import pandas as pd

from data_extractor.extractors.base import BaseExtractor
from data_extractor.registry import register_extractor

logger = logging.getLogger(__name__)

# Alpha Vantage response columns → clean names
_COLUMN_MAP = {
    "1. open": "open",
    "2. high": "high",
    "3. low": "low",
    "4. close": "close",
    "5. volume": "volume",
}


@register_extractor("alpha_vantage")
class AlphaVantageExtractor(BaseExtractor):
    """Fetch daily OHLCV data from Alpha Vantage and return a flat DataFrame."""

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._client: httpx.Client | None = None

    def connect(self) -> None:
        base_url = self._config.get("base_url", "https://www.alphavantage.co")

        # Allow API key to come from env var for security
        api_key_env = self._config.get("api_key_env")
        if api_key_env:
            api_key = os.environ.get(api_key_env, "")
            if api_key:
                self._config.setdefault("query_params", {})["apikey"] = api_key
            else:
                logger.warning(
                    "api_key_env=%r is set but the env var is empty/unset",
                    api_key_env,
                )

        self._client = httpx.Client(
            base_url=base_url,
            timeout=self._config.get("timeout", 30),
        )
        logger.info("Connected to %s", base_url)

    def extract(self) -> pd.DataFrame:
        if self._client is None:
            self.connect()

        endpoint: str = self._config.get("endpoint", "/query")
        query = dict(self._config.get("query_params", {}))

        logger.info("Requesting %s with params %s", endpoint, query)

        resp = self._client.get(endpoint, params=query)  # type: ignore[union-attr]
        resp.raise_for_status()
        data = resp.json()

        # Check for API error messages
        if "Error Message" in data:
            raise ValueError(f"Alpha Vantage API error: {data['Error Message']}")
        if "Note" in data:
            raise ValueError(
                f"Alpha Vantage rate limit: {data['Note']}"
            )
        if "Information" in data:
            raise ValueError(
                f"Alpha Vantage info: {data['Information']}"
            )

        # Extract the time series dict — key varies by function
        series_key = self._config.get("series_key")
        if series_key is None:
            series_key = self._detect_series_key(data)

        time_series: dict[str, dict[str, str]] = data[series_key]

        # Flatten: date keys become a "date" column, prefixed keys get renamed
        rows: list[dict[str, str]] = []
        for date_str, values in time_series.items():
            row = {"date": date_str}
            for raw_key, clean_key in _COLUMN_MAP.items():
                row[clean_key] = values[raw_key]
            rows.append(row)

        if not rows:
            logger.warning("Alpha Vantage returned empty time series")
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(rows)

        # Cast price/volume from strings to numeric
        for col in ("open", "high", "low", "close", "volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

        logger.info("Extracted %d rows from Alpha Vantage", len(df))
        return df

    def disconnect(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            logger.info("Disconnected HTTP client")

    @staticmethod
    def _detect_series_key(data: dict[str, Any]) -> str:
        """Find the time-series key in the response (skip 'Meta Data')."""
        for key in data:
            if key != "Meta Data":
                return key
        raise KeyError(
            "Could not find time-series data in Alpha Vantage response. "
            f"Keys found: {list(data.keys())}"
        )
