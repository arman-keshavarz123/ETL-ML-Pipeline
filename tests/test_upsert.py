"""Tests for SQLAlchemyLoader upsert support."""

from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from data_extractor.loaders.sqlalchemy_loader import SQLAlchemyLoader


def _make_loader(tmp_path, *, primary_keys=None, table_name="items", **overrides):
    """Create a loader pointing at a temporary SQLite DB."""
    db_path = tmp_path / "test.db"
    config = {
        "connection_string": f"sqlite:///{db_path}",
        "table_name": table_name,
        "if_exists": "upsert",
        "primary_keys": primary_keys or ["id"],
        **overrides,
    }
    return SQLAlchemyLoader(config)


class TestUpsertInsert:
    """Inserts new rows when table is empty."""

    def test_inserts_new_rows(self, tmp_path):
        loader = _make_loader(tmp_path)
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        loader.connect()
        loader.load(df)

        engine = create_engine(f"sqlite:///{tmp_path / 'test.db'}")
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT * FROM items ORDER BY id")).fetchall()
        assert len(rows) == 3
        assert rows[0] == (1, "a")
        assert rows[2] == (3, "c")


class TestUpsertUpdate:
    """Updates existing rows on conflict."""

    def test_updates_existing_rows(self, tmp_path):
        loader = _make_loader(tmp_path)
        df1 = pd.DataFrame({"id": [1, 2], "name": ["alice", "bob"]})
        df2 = pd.DataFrame({"id": [2, 3], "name": ["BOB_UPDATED", "charlie"]})

        loader.connect()
        loader.load(df1)
        loader.load(df2)

        engine = create_engine(f"sqlite:///{tmp_path / 'test.db'}")
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT * FROM items ORDER BY id")).fetchall()
        assert len(rows) == 3
        assert rows[0] == (1, "alice")
        assert rows[1] == (2, "BOB_UPDATED")
        assert rows[2] == (3, "charlie")


class TestUpsertCompositeKey:
    """Composite primary key support."""

    def test_composite_primary_key(self, tmp_path):
        loader = _make_loader(tmp_path, primary_keys=["org", "repo"])
        df1 = pd.DataFrame({
            "org": ["a", "a"],
            "repo": ["r1", "r2"],
            "stars": [10, 20],
        })
        df2 = pd.DataFrame({
            "org": ["a"],
            "repo": ["r1"],
            "stars": [999],
        })

        loader.connect()
        loader.load(df1)
        loader.load(df2)

        engine = create_engine(f"sqlite:///{tmp_path / 'test.db'}")
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT * FROM items ORDER BY org, repo")
            ).fetchall()
        assert len(rows) == 2
        assert rows[0] == ("a", "r1", 999)
        assert rows[1] == ("a", "r2", 20)


class TestUpsertEdgeCases:
    """Edge cases and validation."""

    def test_missing_primary_keys_raises(self, tmp_path):
        config = {
            "connection_string": f"sqlite:///{tmp_path / 'test.db'}",
            "table_name": "items",
            "if_exists": "upsert",
            # no primary_keys
        }
        loader = SQLAlchemyLoader(config)
        loader.connect()
        df = pd.DataFrame({"id": [1], "name": ["x"]})

        with pytest.raises(ValueError, match="primary_keys must be specified"):
            loader.load(df)

    def test_empty_dataframe_noop(self, tmp_path):
        loader = _make_loader(tmp_path)
        loader.connect()
        loader.load(pd.DataFrame())
        # No table created — just a no-op
        engine = create_engine(f"sqlite:///{tmp_path / 'test.db'}")
        from sqlalchemy import inspect as sa_inspect
        assert not sa_inspect(engine).has_table("items")

    def test_table_created_with_unique_index(self, tmp_path):
        loader = _make_loader(tmp_path)
        df = pd.DataFrame({"id": [1], "name": ["x"]})
        loader.connect()
        loader.load(df)

        engine = create_engine(f"sqlite:///{tmp_path / 'test.db'}")
        from sqlalchemy import inspect as sa_inspect
        indexes = sa_inspect(engine).get_indexes("items")
        idx_names = [idx["name"] for idx in indexes]
        assert "uq_items_id" in idx_names
