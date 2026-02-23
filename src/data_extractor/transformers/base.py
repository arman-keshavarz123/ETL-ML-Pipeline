"""Base transformer interface.

Transformers are pure functions over DataFrames — no side effects, no I/O.
This makes them trivially testable and composable.
"""

from __future__ import annotations

import abc
from typing import Any

import pandas as pd


class BaseTransformer(abc.ABC):
    """Accept a DataFrame, apply a transformation, return a new DataFrame.

    Transformers are chained by the engine in the order defined in the
    pipeline config.  Each transformer receives the output of the previous one.

    Lifecycle:
        1. __init__(config)  — receive the validated transform config.
        2. validate(df)      — optional pre-condition check on the input.
        3. transform(df)     — apply the transformation (mandatory).
    """

    def __init__(self, config: Any) -> None:
        self._config = config

    @property
    def name(self) -> str:
        return self.__class__.__name__

    # -- hooks ---------------------------------------------------------------

    def validate(self, df: pd.DataFrame) -> None:
        """Raise ``ValueError`` if *df* does not meet pre-conditions.

        Override to assert required columns exist, dtypes match, etc.
        Default is a no-op — the transform is applied unconditionally.
        """

    @abc.abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a *new* DataFrame with the transformation applied.

        Implementations must **never** mutate *df* in place.
        """
        ...
