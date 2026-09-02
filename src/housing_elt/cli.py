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
    clean_parser = subparsers.add_parser(
        "clean",
        help="read, normalize, and count clean PySpark source observations",
    )
    clean_parser.add_argument(
        "--profile",
        default="development",
        help="source profile from config/sources.toml (default: development)",
    )
    clean_parser.add_argument(
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

    if args.command == "clean":
        # Imports are intentionally local: inspecting configuration or running
        # ingestion should not start a JVM or pay Spark import/startup costs.
        from pyspark.errors import PySparkException
        from pyspark.sql import functions as F

        from housing_elt.spark import create_local_spark
        from housing_elt.transformation.errors import TransformationError
        from housing_elt.transformation.pipeline import clean_profile

        try:
            settings = load_settings()
        except SettingsError as error:
            parser.error(str(error))

        logging.basicConfig(
            level=getattr(logging, settings.log_level),
            format="%(asctime)s level=%(levelname)s %(name)s %(message)s",
        )
        registry_path = args.registry or settings.project_root / "config/sources.toml"
        if not registry_path.is_absolute():
            registry_path = settings.project_root / registry_path

        spark = None
        try:
            registry = load_source_registry(registry_path.resolve())
            spark = create_local_spark("canadian-housing-clean")
            frames = clean_profile(
                spark,
                registry,
                args.profile,
                settings.raw_data_dir,
                settings.interim_data_dir,
            )
            for source_id, frame in frames.items():
                summary = frame.agg(
                    F.count("*").alias("clean_rows"),
                    F.sum(F.when(~F.col("is_publishable"), 1).otherwise(0)).alias(
                        "non_publishable_rows"
                    ),
                    F.min("reference_month").alias("reference_start"),
                    F.max("reference_month").alias("reference_end"),
                ).first()
                print(
                    f"{source_id}: clean_rows={summary.clean_rows} "
                    f"non_publishable_rows={summary.non_publishable_rows} "
                    f"reference_start={summary.reference_start} "
                    f"reference_end={summary.reference_end}"
                )
        except (PySparkException, RegistryError, TransformationError) as error:
            logging.getLogger(__name__).error("cleaning_failed reason=%s", error)
            return 1
        finally:
            if spark is not None:
                spark.stop()
        return 0

    # argparse enforces the command choices, so reaching this branch would be a
    # programming error rather than invalid user input.
    raise AssertionError(f"Unhandled command: {args.command}")
