"""Data cleaning transformer — configurable Pandas cleaning rules.

Rules are applied in a fixed deterministic order regardless of config key
ordering.  Missing columns referenced in rules trigger a WARNING and are
skipped — they never crash the pipeline.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from data_extractor.registry import register_transformer
from data_extractor.transformers.base import BaseTransformer

logger = logging.getLogger(__name__)


@register_transformer("data_cleaning")
class DataCleaningTransformer(BaseTransformer):
    """Apply configurable cleaning rules in a fixed order."""

    # Ordered list of (config_key, handler_method_name) — defines execution order.
    _RULES: list[tuple[str, str]] = [
        ("drop_columns", "_drop_columns"),
        ("rename_columns", "_rename_columns"),
        ("lowercase_columns", "_lowercase_columns"),
        ("strip_whitespace", "_strip_whitespace"),
        ("fill_nulls", "_fill_nulls"),
        ("drop_nulls", "_drop_nulls"),
        ("drop_null_columns", "_drop_null_columns"),
        ("deduplicate", "_deduplicate"),
        ("deduplicate_columns", "_deduplicate_columns"),
        ("standardize_dates", "_standardize_dates"),
        ("cast_types", "_cast_types"),
    ]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        rows_before = len(result)

        for key, method_name in self._RULES:
            if key not in self._config:
                continue
            handler = getattr(self, method_name)
            result = handler(result, self._config[key])

        rows_after = len(result)
        removed = rows_before - rows_after
        logger.info(
            "%s: %d→%d rows (%d removed)", self.name, rows_before, rows_after, removed
        )
        return result

    # ------------------------------------------------------------------
    # Rule handlers (static methods, each returns a DataFrame)
    # ------------------------------------------------------------------

    @staticmethod
    def _drop_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        existing = [c for c in columns if c in df.columns]
        missing = set(columns) - set(existing)
        if missing:
            logger.warning("drop_columns: columns not found, skipping: %s", missing)
        return df.drop(columns=existing)

    @staticmethod
    def _rename_columns(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
        missing = set(mapping) - set(df.columns)
        if missing:
            logger.warning("rename_columns: columns not found, skipping: %s", missing)
        return df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})

    @staticmethod
    def _lowercase_columns(df: pd.DataFrame, enabled: bool) -> pd.DataFrame:
        if not enabled:
            return df
        return df.rename(columns={c: c.lower() for c in df.columns})

    @staticmethod
    def _strip_whitespace(df: pd.DataFrame, enabled: bool) -> pd.DataFrame:
        if not enabled:
            return df
        str_cols = df.select_dtypes(include=["object", "string"]).columns
        result = df.copy()
        for col in str_cols:
            result[col] = result[col].str.strip()
        return result

    @staticmethod
    def _fill_nulls(df: pd.DataFrame, mapping: dict[str, Any]) -> pd.DataFrame:
        missing = set(mapping) - set(df.columns)
        if missing:
            logger.warning("fill_nulls: columns not found, skipping: %s", missing)
        fill = {k: v for k, v in mapping.items() if k in df.columns}
        return df.fillna(fill)

    @staticmethod
    def _drop_nulls(df: pd.DataFrame, enabled: bool) -> pd.DataFrame:
        if not enabled:
            return df
        return df.dropna()

    @staticmethod
    def _drop_null_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        existing = [c for c in columns if c in df.columns]
        missing = set(columns) - set(existing)
        if missing:
            logger.warning("drop_null_columns: columns not found, skipping: %s", missing)
        if not existing:
            return df
        return df.dropna(subset=existing)

    @staticmethod
    def _deduplicate(df: pd.DataFrame, enabled: bool) -> pd.DataFrame:
        if not enabled:
            return df
        return df.drop_duplicates()

    @staticmethod
    def _deduplicate_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        existing = [c for c in columns if c in df.columns]
        missing = set(columns) - set(existing)
        if missing:
            logger.warning(
                "deduplicate_columns: columns not found, skipping: %s", missing
            )
        if not existing:
            return df
        return df.drop_duplicates(subset=existing)

    @staticmethod
    def _standardize_dates(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        result = df.copy()
        for col in columns:
            if col not in result.columns:
                logger.warning(
                    "standardize_dates: column %r not found, skipping", col
                )
                continue
            result[col] = pd.to_datetime(result[col], errors="coerce")
        return result

    @staticmethod
    def _cast_types(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
        result = df.copy()
        for col, dtype in mapping.items():
            if col not in result.columns:
                logger.warning("cast_types: column %r not found, skipping", col)
                continue
            try:
                result[col] = result[col].astype(dtype)
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "cast_types: failed to cast %r to %s — %s", col, dtype, exc
                )
        return result
