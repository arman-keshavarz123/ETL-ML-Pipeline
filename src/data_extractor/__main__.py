"""CLI entry point — ``python -m data_extractor``."""

from __future__ import annotations

import argparse
import sys

from dotenv import load_dotenv

load_dotenv()

from data_extractor.engine import PipelineEngine
from data_extractor.registry import list_registered


def _print_modules() -> None:
    """Print all registered extractors, transformers, and loaders."""
    modules = list_registered()
    for category, entries in modules.items():
        print(f"\n{category.upper()}")
        print("-" * len(category))
        if not entries:
            print("  (none)")
        for key, class_name in entries.items():
            print(f"  {key:30s} {class_name}")
    print()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="data-extractor",
        description="Run a configuration-driven ETL pipeline.",
    )
    parser.add_argument(
        "-c", "--config",
        help="Path to the pipeline YAML config file.",
    )
    parser.add_argument(
        "-f", "--full-refresh",
        action="store_true",
        default=False,
        help=(
            "Ignore the stored cursor and extract everything from scratch. "
            "The new cursor is still saved after a successful load."
        ),
    )
    parser.add_argument(
        "-l", "--list-modules",
        action="store_true",
        default=False,
        help="List all registered extractors, transformers, and loaders, then exit.",
    )

    args = parser.parse_args(argv)

    if args.list_modules:
        _print_modules()
        return

    if args.config is None:
        parser.error("the following argument is required: -c/--config")

    engine = PipelineEngine(args.config)
    engine.run(full_refresh=args.full_refresh)


if __name__ == "__main__":
    main()
