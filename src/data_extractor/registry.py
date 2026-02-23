"""Decorator-based plugin registry for extractors, transformers, and loaders.

Concrete classes register themselves at import time via decorators like
``@register_extractor("rest_api")``.  The engine resolves string keys from
config to classes via ``get_extractor("rest_api")`` — it never imports a
concrete class directly.
"""

from __future__ import annotations

from typing import Any

_extractor_registry: dict[str, type] = {}
_transformer_registry: dict[str, type] = {}
_loader_registry: dict[str, type] = {}


# ---------------------------------------------------------------------------
# Decorator factories
# ---------------------------------------------------------------------------

def register_extractor(name: str):
    """Class decorator that registers an extractor under *name*."""

    def decorator(cls: type) -> type:
        if name in _extractor_registry:
            raise ValueError(
                f"Duplicate extractor registration: {name!r} is already "
                f"registered to {_extractor_registry[name].__name__}"
            )
        _extractor_registry[name] = cls
        return cls

    return decorator


def register_transformer(name: str):
    """Class decorator that registers a transformer under *name*."""

    def decorator(cls: type) -> type:
        if name in _transformer_registry:
            raise ValueError(
                f"Duplicate transformer registration: {name!r} is already "
                f"registered to {_transformer_registry[name].__name__}"
            )
        _transformer_registry[name] = cls
        return cls

    return decorator


def register_loader(name: str):
    """Class decorator that registers a loader under *name*."""

    def decorator(cls: type) -> type:
        if name in _loader_registry:
            raise ValueError(
                f"Duplicate loader registration: {name!r} is already "
                f"registered to {_loader_registry[name].__name__}"
            )
        _loader_registry[name] = cls
        return cls

    return decorator


# ---------------------------------------------------------------------------
# Getters — used by the engine to resolve config keys → classes
# ---------------------------------------------------------------------------

def get_extractor(name: str) -> type:
    """Return the extractor class registered under *name*."""
    try:
        return _extractor_registry[name]
    except KeyError:
        available = ", ".join(sorted(_extractor_registry)) or "(none)"
        raise KeyError(
            f"Unknown extractor {name!r}. Available: {available}"
        ) from None


def get_transformer(name: str) -> type:
    """Return the transformer class registered under *name*."""
    try:
        return _transformer_registry[name]
    except KeyError:
        available = ", ".join(sorted(_transformer_registry)) or "(none)"
        raise KeyError(
            f"Unknown transformer {name!r}. Available: {available}"
        ) from None


def get_loader(name: str) -> type:
    """Return the loader class registered under *name*."""
    try:
        return _loader_registry[name]
    except KeyError:
        available = ", ".join(sorted(_loader_registry)) or "(none)"
        raise KeyError(
            f"Unknown loader {name!r}. Available: {available}"
        ) from None


def list_registered() -> dict[str, dict[str, str]]:
    """Return all registered modules grouped by category.

    Returns a dict like::

        {
            "extractors":   {"rest_api": "RESTAPIExtractor", ...},
            "transformers": {"pass_through": "PassThroughTransformer", ...},
            "loaders":      {"json_local": "JSONLocalLoader", ...},
        }
    """
    return {
        "extractors": {k: v.__name__ for k, v in sorted(_extractor_registry.items())},
        "transformers": {k: v.__name__ for k, v in sorted(_transformer_registry.items())},
        "loaders": {k: v.__name__ for k, v in sorted(_loader_registry.items())},
    }
