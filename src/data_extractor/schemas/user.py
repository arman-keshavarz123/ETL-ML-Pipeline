"""Pydantic model for User records — used to demo validation of broken data.

Referenced by dotted path in transformer config:
    model: "data_extractor.schemas.user.User"
"""

from __future__ import annotations

from pydantic import BaseModel, EmailStr, Field


class User(BaseModel):
    id: int = Field(..., ge=1)
    name: str = Field(..., min_length=1)
    email: EmailStr
