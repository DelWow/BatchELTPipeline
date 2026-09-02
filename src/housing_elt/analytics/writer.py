"""Partitioned Parquet publication for analytics-ready housing facts."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame

from housing_elt.analytics.errors import AnalyticsError


def write_analytics_fact(fact: DataFrame, output_path: Path) -> Path:
    """Replace one reproducible fact dataset, partitioned by reference year.

    Year is intentionally coarser than month: this fact has only one row per
    CMA/dwelling/month, so monthly partitions would create many tiny files.
    Repartitioning on year produces one writer task per year in this data size.
    """
    if "reference_year" not in fact.columns:
        raise AnalyticsError("Analytics fact must include reference_year")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    (
        fact.repartition("reference_year")
        .sortWithinPartitions("reference_month", "cma_code", "dwelling_type")
        .write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("reference_year")
        .parquet(str(output_path))
    )
    return output_path
