"""Shared fixtures for the test suite."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture()
def todo_df() -> pd.DataFrame:
    """Clean DataFrame matching JSONPlaceholder /todos shape."""
    return pd.DataFrame(
        [
            {"userId": 1, "id": 1, "title": "task one", "completed": False},
            {"userId": 1, "id": 2, "title": "task two", "completed": True},
            {"userId": 2, "id": 3, "title": "task three", "completed": False},
        ]
    )


@pytest.fixture()
def user_df_with_bad_rows() -> pd.DataFrame:
    """DataFrame with a mix of valid and invalid User records."""
    return pd.DataFrame(
        [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},     # good
            {"id": 2, "name": "Bob",   "email": "not-an-email"},          # bad email
            {"id": -1, "name": "Bad",  "email": "bad@test.com"},          # bad id
            {"id": 3, "name": "",      "email": "empty@test.com"},        # bad name
            {"id": 4, "name": "Eve",   "email": "eve@domain.org"},        # good
        ]
    )


@pytest.fixture()
def tmp_json_file(tmp_path: Path) -> Path:
    """Write a small JSON fixture to a temp file and return its path."""
    data = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob",   "email": "bob@test.com"},
    ]
    path = tmp_path / "users.json"
    path.write_text(json.dumps(data))
    return path
