"""Narrow orchestration for the Phase 6 local cleaning flow."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from housing_elt.ingestion.registry import SourceRegistry
from housing_elt.transformation.cleaning import clean_source
from housing_elt.transformation.reader import read_raw_snapshots
from housing_elt.transformation.snapshots import discover_snapshots


def clean_profile(
    spark: SparkSession,
    registry: SourceRegistry,
    profile_name: str,
    raw_data_dir: Path,
    interim_data_dir: Path,
) -> dict[str, DataFrame]:
    """Build lazy clean DataFrames for every source in a registry profile."""
    profile = registry.profile(profile_name)
    cleaned: dict[str, DataFrame] = {}
    for source_id in profile.source_ids:
        source = registry.source(source_id)
        snapshots = discover_snapshots(raw_data_dir, source)
        raw = read_raw_snapshots(spark, snapshots, interim_data_dir)
        cleaned[source_id] = clean_source(raw, source_id, profile)
    return cleaned
