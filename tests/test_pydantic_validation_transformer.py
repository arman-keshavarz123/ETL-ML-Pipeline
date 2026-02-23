"""Tests for the PydanticValidationTransformer."""

from __future__ import annotations

import pandas as pd
import pytest

from data_extractor.transformers.pydantic_validation import PydanticValidationTransformer


class TestPydanticValidationTransformer:
    """Exercise the pydantic_validation transformer with various data shapes."""

    def _make_transformer(self, model_path: str, **overrides) -> PydanticValidationTransformer:
        config = {"model": model_path, "chunk_size": 2, "strict": False}
        config.update(overrides)
        return PydanticValidationTransformer(config)

    # -- TodoItem model -----------------------------------------------------

    def test_all_valid_todos(self, todo_df: pd.DataFrame):
        t = self._make_transformer("data_extractor.schemas.todo.TodoItem")
        result = t.transform(todo_df)
        assert len(result) == 3
        assert list(result.columns) == list(todo_df.columns)

    def test_drops_invalid_todo_rows(self):
        df = pd.DataFrame(
            [
                {"userId": 1, "id": 1, "title": "ok", "completed": True},   # valid
                {"userId": 0, "id": 2, "title": "bad", "completed": False},  # userId < 1
                {"userId": 1, "id": 3, "title": "",    "completed": True},   # empty title
            ]
        )
        t = self._make_transformer("data_extractor.schemas.todo.TodoItem")
        result = t.transform(df)
        assert len(result) == 1
        assert result.iloc[0]["id"] == 1

    # -- User model ---------------------------------------------------------

    def test_user_validation_drops_bad_emails(self, user_df_with_bad_rows: pd.DataFrame):
        t = self._make_transformer("data_extractor.schemas.user.User")
        result = t.transform(user_df_with_bad_rows)
        # rows 0 (Alice) and 4 (Eve) are the only valid ones
        assert len(result) == 2
        assert list(result["name"]) == ["Alice", "Eve"]

    # -- Edge cases ---------------------------------------------------------

    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=["userId", "id", "title", "completed"])
        t = self._make_transformer("data_extractor.schemas.todo.TodoItem")
        result = t.transform(df)
        assert result.empty
        assert list(result.columns) == ["userId", "id", "title", "completed"]

    def test_all_rows_fail(self):
        df = pd.DataFrame(
            [
                {"userId": 0, "id": 0, "title": "", "completed": True},
                {"userId": -1, "id": -1, "title": "", "completed": False},
            ]
        )
        t = self._make_transformer("data_extractor.schemas.todo.TodoItem")
        result = t.transform(df)
        assert result.empty
        assert list(result.columns) == ["userId", "id", "title", "completed"]

    def test_chunk_size_respected(self, todo_df: pd.DataFrame):
        """Transformer should work correctly regardless of chunk_size."""
        t = self._make_transformer(
            "data_extractor.schemas.todo.TodoItem", chunk_size=1
        )
        result = t.transform(todo_df)
        assert len(result) == 3

    def test_bad_model_path_raises(self):
        with pytest.raises(ImportError):
            self._make_transformer("nonexistent.module.Model")

    def test_non_basemodel_raises(self):
        with pytest.raises(TypeError, match="not a BaseModel subclass"):
            self._make_transformer("builtins.dict")
