"""Shared Spark session configuration for deterministic local execution."""

from pyspark.sql import SparkSession


def create_local_spark(app_name: str) -> SparkSession:
    """Create a small local session with stable SQL/timezone behavior.

    Two worker threads demonstrate parallel execution without consuming every
    host CPU. Four shuffle partitions are enough for the development slice;
    production sizing belongs in deployment configuration, not transforms.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
