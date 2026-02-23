"""Base extractor interface.

Every data source (API, scrape, DB, file) implements this contract.
The engine only ever talks to BaseExtractor — it never knows the concrete type.
"""

from __future__ import annotations

import abc
from typing import Any

import pandas as pd


class BaseExtractor(abc.ABC):
    """Pull raw data from an external source and return a Pandas DataFrame.

    Lifecycle (called by the engine in this order):
        1. __init__(config)  — receive the validated Pydantic source config.
        2. connect()         — open connections / authenticate.
        3. extract()         — fetch data, return a DataFrame.
        4. disconnect()      — tear down resources (called even on failure).
    """

    def __init__(self, config: Any) -> None:
        self._config = config

    @property
    def name(self) -> str:
        """Human-readable name used in logs and metrics."""
        return self.__class__.__name__

    # -- lifecycle hooks -----------------------------------------------------

    def connect(self) -> None:
        """Open connections, authenticate, acquire tokens.

        Override when the source requires a persistent session.
        Default is a no-op so simple sources can skip it.
        """

    @abc.abstractmethod
    def extract(self) -> pd.DataFrame:
        """Execute the extraction and return raw data as a Pandas DataFrame.

        This is the only *mandatory* method a concrete extractor must implement.
        """
        ...

    def disconnect(self) -> None:
        """Release connections, close sessions, clean up temp files.

        Override when the source holds resources that must be freed.
        Default is a no-op.
        """

    # -- context-manager support ---------------------------------------------

    def __enter__(self) -> BaseExtractor:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        self.disconnect()
