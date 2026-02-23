"""Pass-through transformer — returns the DataFrame unchanged.

Useful as a placeholder or when no transforms are needed but the config
requires at least one transform step.
"""

from __future__ import annotations

import pandas as pd

from data_extractor.registry import register_transformer
from data_extractor.transformers.base import BaseTransformer


@register_transformer("pass_through")
class PassThroughTransformer(BaseTransformer):
    """Return a copy of the input DataFrame with no modifications."""

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.copy()
