"""Spark reads for extracted native Statistics Canada CSV observations."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from housing_elt.transformation.errors import TransformationError
from housing_elt.transformation.schemas import RAW_SCHEMAS
from housing_elt.transformation.snapshots import SourceSnapshot, materialize_csv


def read_raw_snapshots(
    spark: SparkSession,
    snapshots: Sequence[SourceSnapshot],
    interim_data_dir: Path,
) -> DataFrame:
    """Read same-table snapshots and attach immutable release provenance."""
    if not snapshots:
        raise TransformationError("At least one snapshot is required")
    source_ids = {snapshot.source_id for snapshot in snapshots}
    if len(source_ids) != 1:
        raise TransformationError("Raw snapshots from different tables cannot be mixed")

    source_id = snapshots[0].source_id
    try:
        schema = RAW_SCHEMAS[source_id]
    except KeyError as error:
        raise TransformationError(f"No raw Spark schema for {source_id}") from error

    frames: list[DataFrame] = []
    for snapshot in snapshots:
        csv_path = materialize_csv(snapshot, interim_data_dir)
        frame = (
            spark.read.option("header", True)
            .option("encoding", "UTF-8")
            .option("mode", "FAILFAST")
            .schema(schema)
            .csv(str(csv_path))
            .withColumn("_source_id", F.lit(source_id))
            .withColumn(
                "_source_release_timestamp",
                F.lit(snapshot.release_timestamp.replace(tzinfo=None)),
            )
            .withColumn("_source_archive_sha256", F.lit(snapshot.sha256))
        )
        frames.append(frame)

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    return combined
