"""Pydantic models for pipeline configuration validation.

The YAML config is parsed into these models at startup.  Invalid configs
fail fast with clear error messages before any I/O happens.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, model_validator


class RetrySettings(BaseModel):
    max_attempts: int = 3
    backoff_seconds: float = 2.0


class IncrementalConfig(BaseModel):
    """Cursor-based incremental extraction settings."""

    cursor_field: str
    cursor_param: str
    initial_value: Any = None


class PipelineSettings(BaseModel):
    log_level: str = "INFO"
    retry: RetrySettings = RetrySettings()
    on_failure: Literal["abort", "skip", "warn"] = "abort"
    state_file: str = "state.json"


class ExtractConfig(BaseModel):
    source: str
    config_file: str | None = None
    inline_config: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _require_some_config(self):
        if self.config_file is None and self.inline_config is None:
            raise ValueError(
                "Extract step must provide at least one of "
                "'config_file' or 'inline_config'"
            )
        return self


class TransformStepConfig(BaseModel):
    name: str
    config_file: str | None = None
    inline_config: dict[str, Any] | None = None


class LoadConfig(BaseModel):
    destination: str
    config_file: str | None = None
    inline_config: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _require_some_config(self):
        if self.config_file is None and self.inline_config is None:
            raise ValueError(
                "Load step must provide at least one of "
                "'config_file' or 'inline_config'"
            )
        return self


class PipelineDefinition(BaseModel):
    name: str
    description: str = ""
    extract: ExtractConfig
    transform: list[TransformStepConfig] = []
    load: LoadConfig
    incremental: IncrementalConfig | None = None


class PipelineConfig(BaseModel):
    """Root model — represents the entire pipeline YAML file."""

    version: str = "1.0"
    pipeline: PipelineDefinition
    settings: PipelineSettings = PipelineSettings()
