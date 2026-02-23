"""Pydantic model for JSONPlaceholder /todos records.

Referenced by dotted path in the pydantic_validation transformer config:
    model: "data_extractor.schemas.todo.TodoItem"
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class TodoItem(BaseModel):
    userId: int = Field(..., ge=1)
    id: int = Field(..., ge=1)
    title: str = Field(..., min_length=1)
    completed: bool
