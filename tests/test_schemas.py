"""Tests for Pydantic schema models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from data_extractor.schemas.todo import TodoItem
from data_extractor.schemas.user import User


# -- TodoItem ---------------------------------------------------------------

class TestTodoItem:
    def test_valid_todo(self):
        item = TodoItem(userId=1, id=1, title="Buy milk", completed=False)
        assert item.userId == 1
        assert item.completed is False

    def test_invalid_userid_zero(self):
        with pytest.raises(ValidationError, match="greater than or equal to 1"):
            TodoItem(userId=0, id=1, title="x", completed=True)

    def test_empty_title_rejected(self):
        with pytest.raises(ValidationError, match="at least 1 character"):
            TodoItem(userId=1, id=1, title="", completed=True)

    def test_missing_required_field(self):
        with pytest.raises(ValidationError):
            TodoItem(userId=1, id=1, title="x")  # type: ignore[call-arg]


# -- User -------------------------------------------------------------------

class TestUser:
    def test_valid_user(self):
        user = User(id=1, name="Alice", email="alice@example.com")
        assert user.email == "alice@example.com"

    def test_bad_email_rejected(self):
        with pytest.raises(ValidationError, match="email"):
            User(id=1, name="Bob", email="not-an-email")

    def test_empty_email_rejected(self):
        with pytest.raises(ValidationError, match="email"):
            User(id=1, name="Bob", email="")

    def test_negative_id_rejected(self):
        with pytest.raises(ValidationError, match="greater than or equal to 1"):
            User(id=-1, name="Bad", email="ok@test.com")

    def test_empty_name_rejected(self):
        with pytest.raises(ValidationError, match="at least 1 character"):
            User(id=1, name="", email="ok@test.com")
