"""Source rollups and joins for the monthly CMA housing fact."""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from housing_elt.analytics.errors import AnalyticsError
from housing_elt.analytics.trends import add_trend_features

ACTIVITY_SOURCE_ID = "cmhc_housing_activity"
MARKET_SOURCE_ID = "cmhc_starts_by_market"
PERMIT_SOURCE_ID = "statcan_building_permits"
PRICE_SOURCE_ID = "statcan_new_housing_price_index"

_DWELLING_TYPES = {
    "Total units": "total",
    "Single-detached units": "single_detached",
    "Single units": "single_detached",
    "Semi-detached units": "semi_detached",
    "Row units": "row",
    "Apartment and other unit types": "apartment_and_other",
    "Apartment and other types of units": "apartment_and_other",
}


def canonical_dwelling_type(source_column: Column) -> Column:
    """Map equivalent CMHC labels while retaining unexpected labels visibly."""
    mapping_items = [item for pair in _DWELLING_TYPES.items() for item in pair]
    mapping = F.create_map(*[F.lit(item) for item in mapping_items])
    fallback = F.concat(
        F.lit("unmapped_"),
        F.regexp_replace(F.lower(source_column), r"[^a-z0-9]+", "_"),
    )
    return F.coalesce(mapping[source_column], fallback)


def aggregate_activity(activity: DataFrame) -> DataFrame:
    """Pivot CMHC activity measures at month/CMA/canonical-dwelling grain."""
    prepared = (
        activity.filter(
            (F.col("geography_level") == "cma")
            & F.col("cma_code").isNotNull()
            & F.col("is_publishable")
        )
        .withColumn("dwelling_type", canonical_dwelling_type(F.col("dwelling_type")))
        .withColumn("has_activity_data", F.lit(True))
    )
    dimensions = ["reference_month", "cma_code", "dwelling_type"]
    return prepared.groupBy(*dimensions).agg(
        F.max("geography").alias("activity_geography"),
        F.max(
            F.when(F.col("housing_measure") == "Housing starts", F.col("housing_count"))
        ).alias("housing_starts"),
        F.max(
            F.when(
                F.col("housing_measure") == "Housing completions",
                F.col("housing_count"),
            )
        ).alias("housing_completions"),
        F.max(
            F.when(
                F.col("housing_measure") == "Housing under construction",
                F.col("housing_count"),
            )
        ).alias("housing_under_construction"),
        F.countDistinct("housing_measure").alias("activity_measure_count"),
        F.max("has_activity_data").alias("has_activity_data"),
        F.max("source_release_timestamp").alias("activity_release_timestamp"),
        F.max("source_archive_sha256").alias("activity_archive_sha256"),
    )


def aggregate_market(market: DataFrame) -> DataFrame:
    """Pivot intended-market starts without mixing dwelling totals/components."""
    prepared = (
        market.filter(
            (F.col("geography_level") == "cma")
            & F.col("cma_code").isNotNull()
            & F.col("is_publishable")
        )
        .withColumn("dwelling_type", canonical_dwelling_type(F.col("dwelling_type")))
        .withColumn("has_market_data", F.lit(True))
    )
    dimensions = ["reference_month", "cma_code", "dwelling_type"]
    market_columns = {
        "Homeowner": "starts_homeowner",
        "Rental": "starts_rental",
        "Condo": "starts_condominium",
        "Co-op": "starts_cooperative",
        "Other market": "starts_other_market",
    }
    expressions = [
        F.max(
            F.when(
                F.col("intended_market") == source_label,
                F.col("housing_starts_count"),
            )
        ).alias(target_column)
        for source_label, target_column in market_columns.items()
    ]
    return prepared.groupBy(*dimensions).agg(
        F.max("geography").alias("market_geography"),
        *expressions,
        F.countDistinct("intended_market").alias("market_member_count"),
        F.max("has_market_data").alias("has_market_data"),
        F.max("source_release_timestamp").alias("market_release_timestamp"),
        F.max("source_archive_sha256").alias("market_archive_sha256"),
    )


def aggregate_price_index(price_index: DataFrame) -> DataFrame:
    """Pivot the three NHPI components to one CMA/month context row."""
    prepared = price_index.filter(
        (F.col("geography_level") == "cma")
        & F.col("cma_code").isNotNull()
        & F.col("is_publishable")
    )
    dimensions = ["reference_month", "cma_code"]
    return prepared.groupBy(*dimensions).agg(
        F.max(
            F.when(
                F.col("index_component") == "Total (house and land)",
                F.col("index_value"),
            )
        ).alias("new_housing_price_index"),
        F.max(
            F.when(F.col("index_component") == "House only", F.col("index_value"))
        ).alias("new_house_price_index"),
        F.max(
            F.when(F.col("index_component") == "Land only", F.col("index_value"))
        ).alias("new_land_price_index"),
        F.countDistinct("index_component").alias("price_index_component_count"),
        F.max("source_release_timestamp").alias("price_release_timestamp"),
        F.max("source_archive_sha256").alias("price_archive_sha256"),
    )


def aggregate_residential_permits(permits: DataFrame) -> DataFrame:
    """Select one non-overlapping residential permit context series."""
    selected = permits.filter(
        (F.col("geography_level") == "cma")
        & F.col("cma_code").isNotNull()
        & F.col("is_publishable")
        & (F.col("building_type") == "Total residential")
        & (F.col("work_type") == "Types of work, total")
        & (F.col("permit_variable") == "Value of permits")
        & (F.col("adjustment_type") == "Seasonally adjusted, current")
    )
    return selected.groupBy("reference_month", "cma_code").agg(
        F.max("permit_value").alias("residential_permit_value_dollars"),
        F.max("source_release_timestamp").alias("permit_release_timestamp"),
        F.max("source_archive_sha256").alias("permit_archive_sha256"),
    )


def _market_total() -> Column:
    market_columns = (
        "starts_homeowner",
        "starts_rental",
        "starts_condominium",
        "starts_cooperative",
        "starts_other_market",
    )
    total = F.lit(0).cast("long")
    for column_name in market_columns:
        total = total + F.coalesce(F.col(column_name), F.lit(0).cast("long"))
    return F.when(F.col("market_member_count") > 0, total)


def build_core_housing_fact(activity: DataFrame, market: DataFrame) -> DataFrame:
    """Full-join the two core facts so either-side gaps remain observable."""
    activity_rollup = aggregate_activity(activity)
    market_rollup = aggregate_market(market)
    keys = ["reference_month", "cma_code", "dwelling_type"]
    joined = activity_rollup.join(market_rollup, keys, "full")
    return (
        joined.withColumn(
            "geography",
            F.coalesce(F.col("activity_geography"), F.col("market_geography")),
        )
        .withColumn(
            "has_activity_data", F.coalesce(F.col("has_activity_data"), F.lit(False))
        )
        .withColumn(
            "has_complete_activity",
            F.coalesce(F.col("activity_measure_count") == 3, F.lit(False)),
        )
        .withColumn(
            "has_market_data", F.coalesce(F.col("has_market_data"), F.lit(False))
        )
        .withColumn(
            "has_complete_market_breakdown",
            F.coalesce(F.col("market_member_count") == 5, F.lit(False)),
        )
        .withColumn("market_starts_total", _market_total())
        .drop("activity_geography", "market_geography")
    )


def _join_price_context(fact: DataFrame, price_index: DataFrame | None) -> DataFrame:
    if price_index is None:
        return (
            fact.withColumn(
                "new_housing_price_index", F.lit(None).cast(DecimalType(18, 4))
            )
            .withColumn("new_house_price_index", F.lit(None).cast(DecimalType(18, 4)))
            .withColumn("new_land_price_index", F.lit(None).cast(DecimalType(18, 4)))
            .withColumn("price_index_component_count", F.lit(None).cast("long"))
            .withColumn("price_release_timestamp", F.lit(None).cast("timestamp"))
            .withColumn("price_archive_sha256", F.lit(None).cast("string"))
            .withColumn("has_price_index_data", F.lit(False))
            .withColumn("has_complete_price_index", F.lit(False))
        )

    joined = fact.join(
        aggregate_price_index(price_index),
        ["reference_month", "cma_code"],
        "left",
    )
    return joined.withColumn(
        "has_price_index_data", F.col("price_index_component_count").isNotNull()
    ).withColumn(
        "has_complete_price_index",
        F.coalesce(F.col("price_index_component_count") == 3, F.lit(False)),
    )


def _join_permit_context(fact: DataFrame, permits: DataFrame | None) -> DataFrame:
    if permits is None:
        return (
            fact.withColumn(
                "residential_permit_value_dollars",
                F.lit(None).cast(DecimalType(24, 4)),
            )
            .withColumn("permit_release_timestamp", F.lit(None).cast("timestamp"))
            .withColumn("permit_archive_sha256", F.lit(None).cast("string"))
            .withColumn("has_permit_data", F.lit(False))
        )

    joined = fact.join(
        aggregate_residential_permits(permits),
        ["reference_month", "cma_code"],
        "left",
    )
    return joined.withColumn(
        "has_permit_data", F.col("residential_permit_value_dollars").isNotNull()
    )


def build_analytics_fact(clean_frames: Mapping[str, DataFrame]) -> DataFrame:
    """Build the analytics-ready monthly CMA × dwelling-type fact."""
    missing = {ACTIVITY_SOURCE_ID, MARKET_SOURCE_ID} - set(clean_frames)
    if missing:
        names = ", ".join(sorted(missing))
        raise AnalyticsError(f"Missing required clean sources: {names}")

    fact = build_core_housing_fact(
        clean_frames[ACTIVITY_SOURCE_ID], clean_frames[MARKET_SOURCE_ID]
    )
    fact = _join_price_context(fact, clean_frames.get(PRICE_SOURCE_ID))
    fact = _join_permit_context(fact, clean_frames.get(PERMIT_SOURCE_ID))
    fact = fact.withColumn(
        "completion_to_start_ratio",
        F.when(
            F.col("housing_starts") > 0,
            F.round(F.col("housing_completions") / F.col("housing_starts"), 4),
        ),
    )
    return add_trend_features(fact).withColumn(
        "reference_year", F.year("reference_month")
    )
