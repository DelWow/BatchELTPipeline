"""Backward-looking trend and anomaly features with no future leakage."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def add_trend_features(fact: DataFrame) -> DataFrame:
    """Add contiguous-period trends at CMA/dwelling-type grain.

    The anomaly baseline is the preceding 12 complete months and excludes the
    current observation. A z-score threshold of 2 is intentionally simple and
    interview-defensible; it is a screening flag, not a causal conclusion.
    """
    partition = Window.partitionBy("cma_code", "dwelling_type").orderBy(
        "reference_month"
    )
    trailing_three = partition.rowsBetween(-2, 0)
    prior_twelve = partition.rowsBetween(-12, -1)

    enriched = (
        fact.withColumn("_three_count", F.count("housing_starts").over(trailing_three))
        .withColumn("_three_first_month", F.min("reference_month").over(trailing_three))
        .withColumn("_prior_month", F.lag("reference_month", 1).over(partition))
        .withColumn(
            "_prior_under_construction",
            F.lag("housing_under_construction", 1).over(partition),
        )
        .withColumn("_year_ago_month", F.lag("reference_month", 12).over(partition))
        .withColumn("_year_ago_starts", F.lag("housing_starts", 12).over(partition))
        .withColumn("_baseline_count", F.count("housing_starts").over(prior_twelve))
        .withColumn(
            "_baseline_first_month", F.min("reference_month").over(prior_twelve)
        )
        .withColumn("_baseline_last_month", F.max("reference_month").over(prior_twelve))
        .withColumn("_baseline_avg", F.avg("housing_starts").over(prior_twelve))
        .withColumn(
            "_baseline_stddev", F.stddev_samp("housing_starts").over(prior_twelve)
        )
    )
    three_complete = (F.col("_three_count") == 3) & (
        F.col("_three_first_month") == F.add_months(F.col("reference_month"), -2)
    )
    previous_month_contiguous = F.col("_prior_month") == F.add_months(
        F.col("reference_month"), -1
    )
    year_contiguous = F.col("_year_ago_month") == F.add_months(
        F.col("reference_month"), -12
    )
    baseline_complete = (
        (F.col("_baseline_count") == 12)
        & (
            F.col("_baseline_first_month")
            == F.add_months(F.col("reference_month"), -12)
        )
        & (F.col("_baseline_last_month") == F.add_months(F.col("reference_month"), -1))
    )

    enriched = (
        enriched.withColumn(
            "starts_3_month_average",
            F.when(
                three_complete,
                F.round(F.avg("housing_starts").over(trailing_three), 4),
            ),
        )
        .withColumn(
            "starts_year_over_year_pct",
            F.when(
                year_contiguous & (F.col("_year_ago_starts") != 0),
                F.round(
                    (
                        (F.col("housing_starts") - F.col("_year_ago_starts"))
                        / F.col("_year_ago_starts")
                    )
                    * 100,
                    4,
                ),
            ),
        )
        .withColumn(
            "under_construction_month_change",
            F.when(
                previous_month_contiguous,
                F.col("housing_under_construction")
                - F.col("_prior_under_construction"),
            ),
        )
        .withColumn("has_12_month_anomaly_baseline", baseline_complete)
        .withColumn(
            "starts_prior_12_month_average",
            F.when(baseline_complete, F.round(F.col("_baseline_avg"), 4)),
        )
        .withColumn(
            "starts_prior_12_month_stddev",
            F.when(baseline_complete, F.round(F.col("_baseline_stddev"), 4)),
        )
        .withColumn(
            "starts_anomaly_zscore",
            F.when(
                baseline_complete & (F.col("_baseline_stddev") > 0),
                F.round(
                    (F.col("housing_starts") - F.col("_baseline_avg"))
                    / F.col("_baseline_stddev"),
                    4,
                ),
            ),
        )
    )
    return enriched.withColumn(
        "starts_anomaly_flag",
        F.when(
            F.col("starts_anomaly_zscore").isNotNull(),
            F.abs(F.col("starts_anomaly_zscore")) >= 2.0,
        ),
    ).drop(
        "_three_count",
        "_three_first_month",
        "_prior_month",
        "_prior_under_construction",
        "_year_ago_month",
        "_year_ago_starts",
        "_baseline_count",
        "_baseline_first_month",
        "_baseline_last_month",
        "_baseline_avg",
        "_baseline_stddev",
    )
