"""Pipeline engine — the orchestrator.

Reads a validated config, resolves string keys to concrete classes via the
registry, and executes the Extract → Transform → Load lifecycle.

The engine **never** imports a concrete extractor/transformer/loader class.
It relies entirely on the decorator-based registry for class resolution.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

# Importing the subpackages triggers @register_* decorators in their __init__.py
import data_extractor.extractors  # noqa: F401
import data_extractor.transformers  # noqa: F401
import data_extractor.loaders  # noqa: F401

from data_extractor.models import IncrementalConfig, PipelineConfig
from data_extractor.registry import get_extractor, get_loader, get_transformer
from data_extractor.state import StateManager

logger = logging.getLogger(__name__)


class PipelineEngine:
    """Load a pipeline config and execute Extract → Transform → Load."""

    def __init__(self, config_path: str | Path) -> None:
        self._config_path = Path(config_path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, *, full_refresh: bool = False) -> None:
        """Execute the full pipeline.

        Parameters
        ----------
        full_refresh:
            When *True* the stored cursor is ignored and extraction starts
            from scratch (using ``initial_value``).  The new cursor is still
            saved after a successful load.
        """
        raw = yaml.safe_load(self._config_path.read_text())
        config = PipelineConfig.model_validate(raw)

        logging.basicConfig(
            level=config.settings.log_level,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )

        logger.info("Pipeline %r started", config.pipeline.name)

        # ── Incremental setup ─────────────────────────────────────
        incremental = config.pipeline.incremental
        state_mgr: StateManager | None = None
        cursor_value: Any = None

        if incremental is not None:
            state_mgr = StateManager(config.settings.state_file)
            if full_refresh:
                cursor_value = incremental.initial_value
                logger.info("Full refresh requested — ignoring stored cursor")
            else:
                cursor_value = state_mgr.get_cursor(config.pipeline.name)
                if cursor_value is None:
                    cursor_value = incremental.initial_value
            logger.info(
                "Incremental mode: cursor_field=%r cursor_param=%r cursor=%r",
                incremental.cursor_field,
                incremental.cursor_param,
                cursor_value,
            )

        # ── Extract ───────────────────────────────────────────────
        logger.info(
            "Resolving extractor for source=%r", config.pipeline.extract.source
        )
        df = self._run_extract(
            config,
            incremental=incremental,
            cursor_value=cursor_value,
        )
        logger.info("Extract complete — %d rows, %d columns", len(df), len(df.columns))

        # ── Compute new cursor before transforms ──────────────────
        new_cursor: Any = None
        if incremental is not None and not df.empty:
            if incremental.cursor_field in df.columns:
                new_cursor = df[incremental.cursor_field].max()
                logger.info("New cursor value: %r", new_cursor)
            else:
                logger.warning(
                    "cursor_field %r not found in extracted columns — "
                    "skipping cursor update",
                    incremental.cursor_field,
                )

        # ── Transform chain ───────────────────────────────────────
        for step in config.pipeline.transform:
            step_config = self._resolve_step_config(step.config_file, step.inline_config)
            transformer_cls = get_transformer(step.name)
            logger.info(
                "Registry resolved %r → %s", step.name, transformer_cls.__name__
            )
            transformer = transformer_cls(step_config)
            logger.info("Running transformer %r", transformer.name)
            transformer.validate(df)
            df = transformer.transform(df)
            logger.info("Transform %r complete — %d rows", transformer.name, len(df))

        # ── Load ──────────────────────────────────────────────────
        logger.info(
            "Resolving loader for destination=%r", config.pipeline.load.destination
        )
        self._run_load(config, df)

        # ── Save cursor ONLY after successful load ────────────────
        if state_mgr is not None and new_cursor is not None:
            state_mgr.save_cursor(config.pipeline.name, new_cursor)

        logger.info("Pipeline %r finished successfully", config.pipeline.name)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_step_config(
        config_file: str | None,
        inline_config: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Merge config_file YAML with inline_config.  Inline wins."""
        merged: dict[str, Any] = {}
        if config_file is not None:
            merged.update(yaml.safe_load(Path(config_file).read_text()))
        if inline_config is not None:
            merged.update(inline_config)
        return merged

    def _run_extract(
        self,
        config: PipelineConfig,
        incremental: IncrementalConfig | None = None,
        cursor_value: Any = None,
    ) -> pd.DataFrame:
        ext_cfg = config.pipeline.extract
        step_config = self._resolve_step_config(ext_cfg.config_file, ext_cfg.inline_config)

        # Inject cursor into query_params so the extractor filters by it
        if incremental is not None and cursor_value is not None:
            step_config.setdefault("query_params", {})[
                incremental.cursor_param
            ] = cursor_value

        extractor_cls = get_extractor(ext_cfg.source)
        logger.info(
            "Registry resolved %r → %s", ext_cfg.source, extractor_cls.__name__
        )
        extractor = extractor_cls(step_config)

        return self._with_retry(
            func=lambda: self._do_extract(extractor),
            retry=config.settings.retry,
            label=extractor.name,
        )

    def _run_load(self, config: PipelineConfig, df: pd.DataFrame) -> None:
        ld_cfg = config.pipeline.load
        step_config = self._resolve_step_config(ld_cfg.config_file, ld_cfg.inline_config)
        loader_cls = get_loader(ld_cfg.destination)
        logger.info(
            "Registry resolved %r → %s", ld_cfg.destination, loader_cls.__name__
        )
        loader = loader_cls(step_config)

        self._with_retry(
            func=lambda: self._do_load(loader, df),
            retry=config.settings.retry,
            label=loader.name,
        )

    @staticmethod
    def _do_extract(extractor) -> pd.DataFrame:
        with extractor:
            return extractor.extract()

    @staticmethod
    def _do_load(loader, df: pd.DataFrame) -> None:
        with loader:
            loader.load(df)

    @staticmethod
    def _with_retry(func, retry, label: str):
        """Call *func* with exponential backoff on failure."""
        last_exc: Exception | None = None
        for attempt in range(1, retry.max_attempts + 1):
            try:
                return func()
            except Exception as exc:
                last_exc = exc
                if attempt < retry.max_attempts:
                    wait = retry.backoff_seconds * (2 ** (attempt - 1))
                    logger.warning(
                        "%s attempt %d/%d failed (%s), retrying in %.1fs…",
                        label, attempt, retry.max_attempts, exc, wait,
                    )
                    time.sleep(wait)
        logger.error("%s failed after %d attempts", label, retry.max_attempts)
        raise last_exc  # type: ignore[misc]
