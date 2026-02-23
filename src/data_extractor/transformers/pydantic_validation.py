"""Pydantic validation transformer — validate rows against a Pydantic model.

Bad records are dropped and logged at WARNING level; the pipeline never crashes
due to individual record failures.
"""

from __future__ import annotations

import importlib
import logging
from typing import Any

import pandas as pd
from pydantic import BaseModel, ValidationError

from data_extractor.registry import register_transformer
from data_extractor.transformers.base import BaseTransformer

logger = logging.getLogger(__name__)


def _import_model(dotted_path: str) -> type[BaseModel]:
    """Dynamically import a Pydantic model class from a dotted path.

    Example: ``"data_extractor.schemas.todo.TodoItem"``
    """
    module_path, _, class_name = dotted_path.rpartition(".")
    if not module_path:
        raise ImportError(f"Invalid model path: {dotted_path!r} (need module.Class)")
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    if not (isinstance(cls, type) and issubclass(cls, BaseModel)):
        raise TypeError(
            f"{dotted_path!r} resolved to {cls!r}, which is not a BaseModel subclass"
        )
    return cls


@register_transformer("pydantic_validation")
class PydanticValidationTransformer(BaseTransformer):
    """Validate each row against a Pydantic model, dropping failures."""

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        model_path: str = config["model"]
        self._model = _import_model(model_path)
        self._chunk_size: int = config.get("chunk_size", 1000)
        self._strict: bool = config.get("strict", False)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            logger.info("%s: input DataFrame is empty — nothing to validate", self.name)
            return df.copy()

        valid_rows: list[dict[str, Any]] = []
        total = len(df)
        failed = 0

        for start in range(0, total, self._chunk_size):
            chunk = df.iloc[start : start + self._chunk_size]
            for idx, row in chunk.iterrows():
                row_dict = row.to_dict()
                try:
                    self._model.model_validate(row_dict, strict=self._strict)
                    valid_rows.append(row_dict)
                except ValidationError as exc:
                    failed += 1
                    logger.warning(
                        "%s: row %s failed validation — %s", self.name, idx, exc
                    )

        passed = total - failed
        logger.info(
            "%s: %d/%d rows passed validation (%d dropped)",
            self.name,
            passed,
            total,
            failed,
        )

        if not valid_rows:
            return pd.DataFrame(columns=df.columns)

        return pd.DataFrame(valid_rows, columns=df.columns)
