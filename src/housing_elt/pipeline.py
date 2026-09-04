"""Local ingestion-to-curated orchestration with a mandatory validation gate."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from housing_elt.analytics import build_analytics_fact, write_analytics_fact
from housing_elt.config import PipelineSettings
from housing_elt.ingestion.registry import SourceRegistry
from housing_elt.ingestion.statcan import IngestionResult, StatCanIngestor
from housing_elt.snowflake.loader import SnowflakeLoadResult
from housing_elt.transformation.pipeline import clean_profile
from housing_elt.validation import ValidationPolicy, validate_analytics_fact
from housing_elt.validation.report import ValidationReport


@dataclass(frozen=True, slots=True)
class LocalPipelineResult:
    """Auditable outcome from one successful local pipeline execution."""

    output_path: Path
    validation_report: ValidationReport
    ingestion_results: tuple[IngestionResult, ...]
    snowflake_load_result: SnowflakeLoadResult | None


def validate_and_publish(
    fact: DataFrame,
    policy: ValidationPolicy,
    output_path: Path,
    *,
    writer: Callable[[DataFrame, Path], Path] = write_analytics_fact,
) -> ValidationReport:
    """Publish only after every validation check has passed."""
    report = validate_analytics_fact(fact, policy)
    writer(fact, output_path)
    return report


def run_local_pipeline(
    spark: SparkSession,
    registry: SourceRegistry,
    settings: PipelineSettings,
    profile_name: str,
    policy: ValidationPolicy,
    output_path: Path,
    *,
    ingest: bool,
    snowflake_publisher: (
        Callable[[DataFrame, ValidationReport], SnowflakeLoadResult] | None
    ) = None,
) -> LocalPipelineResult:
    """Run ingestion (optional offline), cleaning, aggregation, validation, output."""
    ingestion_results: tuple[IngestionResult, ...] = ()
    if ingest:
        with StatCanIngestor(registry, settings.raw_data_dir) as ingestor:
            ingestion_results = ingestor.ingest_profile(profile_name)

    clean_frames = clean_profile(
        spark,
        registry,
        profile_name,
        settings.raw_data_dir,
        settings.interim_data_dir,
    )
    fact = build_analytics_fact(clean_frames).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        report = validate_and_publish(fact, policy, output_path)
        # The callback receives the report produced for this exact persisted
        # DataFrame. It is intentionally unreachable when validation raises.
        snowflake_result = (
            snowflake_publisher(fact, report)
            if snowflake_publisher is not None
            else None
        )
    finally:
        fact.unpersist()
    return LocalPipelineResult(
        output_path=output_path,
        validation_report=report,
        ingestion_results=ingestion_results,
        snowflake_load_result=snowflake_result,
    )
