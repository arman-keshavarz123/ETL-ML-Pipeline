"""JSON file extractor — reads a local JSON file into a DataFrame.

Useful for testing pipelines with local fixtures or for ingesting
pre-downloaded datasets.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from data_extractor.extractors.base import BaseExtractor
from data_extractor.registry import register_extractor

logger = logging.getLogger(__name__)


@register_extractor("json_file")
class JSONFileExtractor(BaseExtractor):
    """Read a local JSON file and return its contents as a DataFrame."""

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._path = Path(config["file_path"])
        self._orient: str = config.get("orient", "records")

    def extract(self) -> pd.DataFrame:
        logger.info("Reading JSON from %s (orient=%s)", self._path, self._orient)
        df = pd.read_json(self._path, orient=self._orient)
        logger.info("Loaded %d rows, %d columns from %s", len(df), len(df.columns), self._path)
        return df
