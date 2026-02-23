"""Base loader interface.

Loaders are the final stage — they persist a DataFrame to a destination
(local file, database, cloud storage, API push, etc.).
"""

from __future__ import annotations

import abc
from typing import Any

import pandas as pd


class BaseLoader(abc.ABC):
    """Write a DataFrame to an external destination.

    Lifecycle (called by the engine in this order):
        1. __init__(config)  — receive the validated loader config.
        2. connect()         — open connections / authenticate.
        3. load(df)          — write the data.
        4. disconnect()      — tear down resources (called even on failure).
    """

    def __init__(self, config: Any) -> None:
        self._config = config

    @property
    def name(self) -> str:
        return self.__class__.__name__

    # -- lifecycle hooks -----------------------------------------------------

    def connect(self) -> None:
        """Open connections or authenticate with the destination.

        Default is a no-op so file-based loaders can skip it.
        """

    @abc.abstractmethod
    def load(self, df: pd.DataFrame) -> None:
        """Persist *df* to the destination.

        This is the only *mandatory* method a concrete loader must implement.
        """
        ...

    def disconnect(self) -> None:
        """Release connections and clean up.

        Default is a no-op.
        """

    # -- context-manager support ---------------------------------------------

    def __enter__(self) -> BaseLoader:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        self.disconnect()
