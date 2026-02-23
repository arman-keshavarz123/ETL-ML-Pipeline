"""JSON local file loader — writes a DataFrame to a JSON file on disk."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from data_extractor.loaders.base import BaseLoader
from data_extractor.registry import register_loader

logger = logging.getLogger(__name__)


@register_loader("json_local")
class JSONLocalLoader(BaseLoader):
    """Persist a DataFrame as a JSON file on the local filesystem."""

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._output_path: Path | None = None

    def connect(self) -> None:
        self._output_path = Path(self._config["output_path"])
        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Output directory ready: %s", self._output_path.parent)

    def load(self, df: pd.DataFrame) -> None:
        if self._output_path is None:
            self.connect()

        orient = self._config.get("orient", "records")
        indent = self._config.get("indent", 2)

        df.to_json(self._output_path, orient=orient, indent=indent)
        logger.info(
            "Wrote %d rows to %s (orient=%s)", len(df), self._output_path, orient
        )
