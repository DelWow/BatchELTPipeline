"""Command-line interface for narrow pipeline entry points."""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Sequence
from pathlib import Path

from housing_elt.config import SettingsError, load_settings
from housing_elt.ingestion.registry import RegistryError, load_source_registry
from housing_elt.ingestion.statcan import IngestionError, StatCanIngestor


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser without reading configuration or changing state."""
    parser = argparse.ArgumentParser(
        prog="housing-elt",
        description="Canadian metropolitan housing batch ELT pipeline",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "show-config",
        help="print resolved non-secret local configuration as JSON",
    )
    ingest_parser = subparsers.add_parser(
        "ingest",
        help="download and validate immutable raw Statistics Canada snapshots",
    )
    ingest_parser.add_argument(
        "--profile",
        default="development",
        help="source profile from config/sources.toml (default: development)",
    )
    ingest_parser.add_argument(
        "--registry",
        type=Path,
        help="source registry path (default: <project-root>/config/sources.toml)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run a selected command and return a process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "show-config":
        try:
            settings = load_settings()
        except SettingsError as error:
            parser.error(str(error))

        print(json.dumps(settings.to_display_dict(), indent=2, sort_keys=True))
        return 0

    if args.command == "ingest":
        try:
            settings = load_settings()
        except SettingsError as error:
            parser.error(str(error))

        logging.basicConfig(
            level=getattr(logging, settings.log_level),
            format="%(asctime)s level=%(levelname)s %(name)s %(message)s",
        )
        # Keep output focused on pipeline decisions rather than one line per
        # successful HTTP exchange. Failures are still surfaced by ingestion.
        logging.getLogger("httpx").setLevel(logging.WARNING)
        registry_path = args.registry or settings.project_root / "config/sources.toml"
        if not registry_path.is_absolute():
            registry_path = settings.project_root / registry_path
        try:
            registry = load_source_registry(registry_path.resolve())
            with StatCanIngestor(registry, settings.raw_data_dir) as ingestor:
                results = ingestor.ingest_profile(args.profile)
        except (IngestionError, RegistryError) as error:
            logging.getLogger(__name__).error("ingestion_failed reason=%s", error)
            return 1

        for result in results:
            print(
                f"{result.source_id}: {result.status} "
                f"bytes={result.byte_count} sha256={result.sha256} "
                f"path={result.archive_path}"
            )
        return 0

    # argparse enforces the command choices, so reaching this branch would be a
    # programming error rather than invalid user input.
    raise AssertionError(f"Unhandled command: {args.command}")
