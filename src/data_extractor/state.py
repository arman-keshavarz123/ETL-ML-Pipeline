"""State management for incremental pipeline loading.

Tracks a cursor (e.g., max ID or timestamp) per pipeline so only
new/changed data is extracted on subsequent runs.  Uses atomic
write-to-temp-then-rename to avoid corrupting state on crash.
"""

from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class StateManager:
    """Read and persist per-pipeline cursor values in a JSON file."""

    def __init__(self, state_file: str | Path = "state.json") -> None:
        self._path = Path(state_file)

    def get_cursor(self, pipeline_id: str) -> Any | None:
        """Return the stored cursor for *pipeline_id*, or ``None``."""
        state = self._read()
        return state.get(pipeline_id)

    def save_cursor(self, pipeline_id: str, cursor_value: Any) -> None:
        """Persist *cursor_value* atomically (temp-file then rename)."""
        state = self._read()
        # Convert numpy/pandas scalars to native Python types so
        # json.dump produces "20" (int) instead of a stringified object.
        state[pipeline_id] = self._to_native(cursor_value)

        # Ensure parent directories exist
        self._path.parent.mkdir(parents=True, exist_ok=True)

        # Atomic write: write to temp file in the same directory, then rename
        fd, tmp_path = tempfile.mkstemp(
            dir=self._path.parent, suffix=".tmp"
        )
        try:
            with open(fd, "w") as fh:
                json.dump(state, fh, indent=2, default=str)
            Path(tmp_path).replace(self._path)
        except BaseException:
            # Clean up temp file on failure
            Path(tmp_path).unlink(missing_ok=True)
            raise

        logger.info(
            "Saved cursor for pipeline %r: %s", pipeline_id, cursor_value
        )

    @staticmethod
    def _to_native(value: Any) -> Any:
        """Convert numpy/pandas scalars to native Python types."""
        if hasattr(value, "item"):  # numpy scalar
            return value.item()
        return value

    def _read(self) -> dict[str, Any]:
        """Load the state file; return ``{}`` on missing or corrupt file."""
        if not self._path.exists():
            return {}
        try:
            text = self._path.read_text()
            data = json.loads(text)
            if not isinstance(data, dict):
                logger.warning("State file is not a JSON object — resetting")
                return {}
            return data
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not read state file (%s) — resetting", exc)
            return {}
