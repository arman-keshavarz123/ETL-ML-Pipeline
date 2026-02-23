"""Tests for the JSONLocalLoader."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from data_extractor.loaders.json_local import JSONLocalLoader


class TestJSONLocalLoader:
    def test_writes_json(self, tmp_path: Path, todo_df: pd.DataFrame):
        out = tmp_path / "out.json"
        loader = JSONLocalLoader({"output_path": str(out), "orient": "records", "indent": 2})
        with loader:
            loader.load(todo_df)
        data = json.loads(out.read_text())
        assert len(data) == 3
        assert data[0]["title"] == "task one"

    def test_creates_parent_dirs(self, tmp_path: Path, todo_df: pd.DataFrame):
        out = tmp_path / "deep" / "nested" / "out.json"
        loader = JSONLocalLoader({"output_path": str(out)})
        with loader:
            loader.load(todo_df)
        assert out.exists()
