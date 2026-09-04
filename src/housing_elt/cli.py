"""Command-line interface for narrow pipeline entry points."""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Sequence
from functools import partial
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
    aggregate_parser = subparsers.add_parser(
        "aggregate",
        help="build and write the partitioned analytics-ready housing fact",
    )
    aggregate_parser.add_argument(
        "--profile",
        default="development",
        help="source profile from config/sources.toml (default: development)",
    )
    aggregate_parser.add_argument(
        "--registry",
        type=Path,
        help="source registry path (default: <project-root>/config/sources.toml)",
    )
    aggregate_parser.add_argument(
        "--output",
        type=Path,
        help="Parquet dataset path (default: data/curated/housing_monthly)",
    )
    aggregate_parser.add_argument(
        "--validation-contract",
        type=Path,
        help="validation policy path (default: config/validation.toml)",
    )
    run_parser = subparsers.add_parser(
        "run",
        help="run ingestion through validated local Parquet publication",
    )
    run_parser.add_argument(
        "--profile",
        default="development",
        help="source/validation profile (default: development)",
    )
    run_parser.add_argument(
        "--registry",
        type=Path,
        help="source registry path (default: <project-root>/config/sources.toml)",
    )
    run_parser.add_argument(
        "--validation-contract",
        type=Path,
        help="validation policy path (default: config/validation.toml)",
    )
    run_parser.add_argument(
        "--output",
        type=Path,
        help="Parquet dataset path (default: data/curated/housing_monthly)",
    )
    run_parser.add_argument(
        "--skip-ingestion",
        action="store_true",
        help="use existing immutable raw snapshots without contacting StatsCan",
    )
    run_parser.add_argument(
        "--load-snowflake",
        action="store_true",
        help=(
            "after validation and local publication, load Snowflake using "
            "HOUSING_ELT_SNOWFLAKE_* environment variables"
        ),
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

    if args.command in {"aggregate", "run"}:
        from pyspark.errors import PySparkException

        from housing_elt.analytics.errors import AnalyticsError
        from housing_elt.pipeline import run_local_pipeline
        from housing_elt.snowflake.config import (
            SnowflakeConfigurationError,
            load_snowflake_settings,
        )
        from housing_elt.snowflake.errors import SnowflakeLoadError
        from housing_elt.snowflake.loader import load_analytics_fact
        from housing_elt.spark import create_local_spark
        from housing_elt.transformation.errors import TransformationError
        from housing_elt.validation.config import load_validation_contract
        from housing_elt.validation.errors import (
            DataValidationError,
            ValidationContractError,
        )

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
        validation_path = (
            args.validation_contract or settings.project_root / "config/validation.toml"
        )
        if not validation_path.is_absolute():
            validation_path = settings.project_root / validation_path
        output_path = args.output or settings.curated_data_dir / "housing_monthly"
        if not output_path.is_absolute():
            output_path = settings.project_root / output_path

        spark = None
        try:
            registry = load_source_registry(registry_path.resolve())
            validation_contract = load_validation_contract(validation_path.resolve())
            policy = validation_contract.profile(args.profile)
            snowflake_publisher = None
            if getattr(args, "load_snowflake", False):
                snowflake_settings = load_snowflake_settings()
                snowflake_publisher = partial(
                    load_analytics_fact, settings=snowflake_settings
                )

            spark = create_local_spark("canadian-housing-pipeline")
            result = run_local_pipeline(
                spark,
                registry,
                settings,
                args.profile,
                policy,
                output_path.resolve(),
                ingest=(args.command == "run" and not args.skip_ingestion),
                snowflake_publisher=snowflake_publisher,
            )
            for ingestion_result in result.ingestion_results:
                print(
                    f"{ingestion_result.source_id}: {ingestion_result.status} "
                    f"sha256={ingestion_result.sha256}"
                )
            metrics = result.validation_report.metrics
            print(
                f"validation=passed analytics_rows={metrics['row_count']} "
                f"years={metrics['distinct_years']} "
                f"anomalies={metrics['anomaly_rows']} "
                f"activity_only_rows={metrics['activity_only_rows']} "
                f"market_only_rows={metrics['market_only_rows']} "
                f"missing_price_rows={metrics['missing_price_rows']} "
                f"missing_permit_rows={metrics['missing_permit_rows']} "
                f"output={result.output_path}"
            )
            if result.snowflake_load_result is not None:
                load_result = result.snowflake_load_result
                print(
                    f"snowflake=published batch_id={load_result.batch_id} "
                    f"profile={load_result.validation_profile} "
                    f"reference_start={load_result.reference_start} "
                    f"reference_end={load_result.reference_end} "
                    f"rows={load_result.published_row_count}"
                )
        except (
            AnalyticsError,
            DataValidationError,
            IngestionError,
            PySparkException,
            RegistryError,
            SnowflakeConfigurationError,
            SnowflakeLoadError,
            TransformationError,
            ValidationContractError,
        ) as error:
            logging.getLogger(__name__).error("pipeline_failed reason=%s", error)
            return 1
        finally:
            if spark is not None:
                spark.stop()
        return 0

    # argparse enforces the command choices, so reaching this branch would be a
    # programming error rather than invalid user input.
    raise AssertionError(f"Unhandled command: {args.command}")
