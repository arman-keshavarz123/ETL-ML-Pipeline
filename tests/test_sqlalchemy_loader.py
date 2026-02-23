"""Tests for the SQLAlchemyLoader using file-based SQLite."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from data_extractor.loaders.sqlalchemy_loader import SQLAlchemyLoader


def _sqlite_url(tmp_path: Path) -> str:
    return f"sqlite:///{tmp_path / 'test.db'}"


def _read_table(db_url: str, table: str) -> pd.DataFrame:
    engine = create_engine(db_url)
    with engine.connect() as conn:
        df = pd.read_sql(text(f"SELECT * FROM {table}"), conn)
    engine.dispose()
    return df


class TestSQLAlchemyLoader:
    def test_writes_dataframe(self, tmp_path: Path, todo_df: pd.DataFrame):
        db_url = _sqlite_url(tmp_path)
        loader = SQLAlchemyLoader(
            {"connection_string": db_url, "table_name": "todos"}
        )
        with loader:
            loader.load(todo_df)

        result = _read_table(db_url, "todos")
        assert len(result) == 3
        assert "title" in result.columns

    def test_append_mode(self, tmp_path: Path, todo_df: pd.DataFrame):
        db_url = _sqlite_url(tmp_path)
        config = {
            "connection_string": db_url,
            "table_name": "todos",
            "if_exists": "append",
        }
        loader = SQLAlchemyLoader(config)
        with loader:
            loader.load(todo_df)
            loader.load(todo_df)

        result = _read_table(db_url, "todos")
        assert len(result) == 6

    def test_fail_mode_raises(self, tmp_path: Path, todo_df: pd.DataFrame):
        db_url = _sqlite_url(tmp_path)
        config = {
            "connection_string": db_url,
            "table_name": "todos",
            "if_exists": "fail",
        }
        loader = SQLAlchemyLoader(config)
        with loader:
            loader.load(todo_df)
            with pytest.raises(ValueError):
                loader.load(todo_df)

    def test_disconnect_disposes_engine(self, tmp_path: Path):
        db_url = _sqlite_url(tmp_path)
        loader = SQLAlchemyLoader(
            {"connection_string": db_url, "table_name": "t"}
        )
        loader.connect()
        assert loader._engine is not None
        loader.disconnect()
        assert loader._engine is None

    def test_auto_connect(self, tmp_path: Path, todo_df: pd.DataFrame):
        db_url = _sqlite_url(tmp_path)
        loader = SQLAlchemyLoader(
            {"connection_string": db_url, "table_name": "todos"}
        )
        # load() without calling connect() first
        loader.load(todo_df)

        result = _read_table(db_url, "todos")
        assert len(result) == 3
        loader.disconnect()

    def test_empty_dataframe(self, tmp_path: Path):
        db_url = _sqlite_url(tmp_path)
        loader = SQLAlchemyLoader(
            {"connection_string": db_url, "table_name": "empty"}
        )
        with loader:
            loader.load(pd.DataFrame({"a": [], "b": []}))

        result = _read_table(db_url, "empty")
        assert len(result) == 0
        assert list(result.columns) == ["a", "b"]
