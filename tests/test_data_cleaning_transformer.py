"""Tests for the DataCleaningTransformer."""

from __future__ import annotations

import numpy as np
import pandas as pd

from data_extractor.transformers.data_cleaning import DataCleaningTransformer


class TestDataCleaningTransformer:
    """Exercise each cleaning rule independently and in combination."""

    @staticmethod
    def _make(config: dict) -> DataCleaningTransformer:
        return DataCleaningTransformer(config)

    # -- drop_columns -------------------------------------------------------

    def test_drop_columns(self):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        t = self._make({"drop_columns": ["b", "c"]})
        result = t.transform(df)
        assert list(result.columns) == ["a"]

    def test_drop_columns_missing_col_skipped(self):
        df = pd.DataFrame({"a": [1]})
        t = self._make({"drop_columns": ["nonexistent"]})
        result = t.transform(df)
        assert list(result.columns) == ["a"]

    # -- rename_columns -----------------------------------------------------

    def test_rename_columns(self):
        df = pd.DataFrame({"old_name": [1]})
        t = self._make({"rename_columns": {"old_name": "new_name"}})
        result = t.transform(df)
        assert "new_name" in result.columns
        assert "old_name" not in result.columns

    def test_rename_columns_missing_col_skipped(self):
        df = pd.DataFrame({"a": [1]})
        t = self._make({"rename_columns": {"nope": "also_nope"}})
        result = t.transform(df)
        assert list(result.columns) == ["a"]

    # -- lowercase_columns --------------------------------------------------

    def test_lowercase_columns(self):
        df = pd.DataFrame({"UserId": [1], "EMAIL": [2]})
        t = self._make({"lowercase_columns": True})
        result = t.transform(df)
        assert list(result.columns) == ["userid", "email"]

    def test_lowercase_columns_disabled(self):
        df = pd.DataFrame({"UserId": [1]})
        t = self._make({"lowercase_columns": False})
        result = t.transform(df)
        assert list(result.columns) == ["UserId"]

    # -- strip_whitespace ---------------------------------------------------

    def test_strip_whitespace(self):
        df = pd.DataFrame({"name": ["  Alice  ", "Bob  "], "id": [1, 2]})
        t = self._make({"strip_whitespace": True})
        result = t.transform(df)
        assert list(result["name"]) == ["Alice", "Bob"]
        assert list(result["id"]) == [1, 2]  # numeric untouched

    # -- fill_nulls ---------------------------------------------------------

    def test_fill_nulls(self):
        df = pd.DataFrame({"a": [1, None, 3], "b": [None, "x", None]})
        t = self._make({"fill_nulls": {"a": 0, "b": "missing"}})
        result = t.transform(df)
        assert result["a"].tolist() == [1.0, 0.0, 3.0]
        assert result["b"].tolist() == ["missing", "x", "missing"]

    # -- drop_nulls ---------------------------------------------------------

    def test_drop_nulls(self):
        df = pd.DataFrame({"a": [1, None, 3], "b": ["x", "y", None]})
        t = self._make({"drop_nulls": True})
        result = t.transform(df)
        assert len(result) == 1
        assert result.iloc[0]["a"] == 1

    # -- drop_null_columns --------------------------------------------------

    def test_drop_null_columns(self):
        df = pd.DataFrame({"a": [1, None, 3], "b": ["x", "y", "z"]})
        t = self._make({"drop_null_columns": ["a"]})
        result = t.transform(df)
        assert len(result) == 2  # row with None in 'a' dropped

    # -- deduplicate --------------------------------------------------------

    def test_deduplicate(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
        t = self._make({"deduplicate": True})
        result = t.transform(df)
        assert len(result) == 2

    # -- deduplicate_columns ------------------------------------------------

    def test_deduplicate_columns(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "y", "z"]})
        t = self._make({"deduplicate_columns": ["a"]})
        result = t.transform(df)
        assert len(result) == 2  # first duplicate of a=1 kept

    # -- standardize_dates --------------------------------------------------

    def test_standardize_dates(self):
        df = pd.DataFrame({"d": ["2024-01-15", "not-a-date", "2024-06-01"]})
        t = self._make({"standardize_dates": ["d"]})
        result = t.transform(df)
        assert pd.api.types.is_datetime64_any_dtype(result["d"])
        assert pd.isna(result.iloc[1]["d"])  # "not-a-date" → NaT

    def test_standardize_dates_missing_col(self):
        df = pd.DataFrame({"a": [1]})
        t = self._make({"standardize_dates": ["nonexistent"]})
        result = t.transform(df)  # should not raise
        assert len(result) == 1

    # -- cast_types ---------------------------------------------------------

    def test_cast_types(self):
        df = pd.DataFrame({"a": ["1", "2", "3"]})
        t = self._make({"cast_types": {"a": "int64"}})
        result = t.transform(df)
        assert result["a"].dtype == np.int64

    def test_cast_types_bad_cast_warns(self):
        df = pd.DataFrame({"a": ["hello", "world"]})
        t = self._make({"cast_types": {"a": "int64"}})
        result = t.transform(df)
        # cast fails, column keeps original dtype (object or StringDtype)
        assert result["a"].dtype != np.int64

    # -- combined rules -----------------------------------------------------

    def test_multiple_rules_applied_in_order(self):
        df = pd.DataFrame(
            {
                "Name": ["  Alice  ", "  Alice  ", "  Bob  "],
                "AGE": ["25", "25", "30"],
            }
        )
        t = self._make(
            {
                "lowercase_columns": True,
                "strip_whitespace": True,
                "deduplicate": True,
                "cast_types": {"age": "int64"},
            }
        )
        result = t.transform(df)
        assert list(result.columns) == ["name", "age"]
        assert len(result) == 2  # deduplicated
        assert list(result["name"]) == ["Alice", "Bob"]
        assert result["age"].dtype == np.int64

    def test_empty_config_is_noop(self):
        df = pd.DataFrame({"a": [1, 2]})
        t = self._make({})
        result = t.transform(df)
        pd.testing.assert_frame_equal(result, df)
