"""Tests for the PassThroughTransformer."""

from __future__ import annotations

import pandas as pd

from data_extractor.transformers.pass_through import PassThroughTransformer


class TestPassThroughTransformer:
    def test_returns_copy(self, todo_df: pd.DataFrame):
        t = PassThroughTransformer({})
        result = t.transform(todo_df)
        pd.testing.assert_frame_equal(result, todo_df)
        # must be a copy, not the same object
        assert result is not todo_df

    def test_empty_df(self):
        df = pd.DataFrame()
        t = PassThroughTransformer({})
        result = t.transform(df)
        assert result.empty
