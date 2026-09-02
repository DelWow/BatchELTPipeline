"""Small, composable PySpark transformations for housing observations."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from housing_elt.ingestion.registry import ProfileDefinition
from housing_elt.transformation.errors import TransformationError
from housing_elt.transformation.schemas import CLEAN_SCHEMAS, NATURAL_KEYS


def _clean_string(column: Column) -> Column:
    trimmed = F.trim(column)
    return F.when(F.length(trimmed) == 0, F.lit(None).cast("string")).otherwise(trimmed)


def _normalized_value(raw_value: Column, scalar_id: Column) -> Column:
    """Apply StatsCan's base-10 scalar without floating-point arithmetic."""
    normalized = F.lit(None).cast(DecimalType(30, 6))
    for exponent in (0, 3, 6, 9):
        normalized = F.when(
            scalar_id == exponent,
            raw_value * F.lit(Decimal(10**exponent)),
        ).otherwise(normalized)
    return normalized


def normalize_common_columns(raw: DataFrame) -> DataFrame:
    """Normalize shared types and status semantics without dropping rows."""
    common_raw_columns = {
        "REF_DATE",
        "GEO",
        "DGUID",
        "UOM",
        "UOM_ID",
        "SCALAR_FACTOR",
        "SCALAR_ID",
        "VECTOR",
        "COORDINATE",
        "VALUE",
        "STATUS",
        "SYMBOL",
        "TERMINATED",
        "DECIMALS",
        "_source_id",
        "_source_release_timestamp",
        "_source_archive_sha256",
    }
    # Raw common fields are replaced by normalized aliases below. Keeping both
    # would be ambiguous because Spark SQL resolves names case-insensitively.
    dimension_columns = [
        F.col(f"`{name}`") for name in raw.columns if name not in common_raw_columns
    ]
    geography = _clean_string(F.col("GEO"))
    geography_dguid = _clean_string(F.col("DGUID"))
    cma_code_raw = F.regexp_extract(geography_dguid, r"S0503([0-9]{3})$", 1)
    cma_code = F.when(F.length(cma_code_raw) > 0, cma_code_raw)
    geography_level = (
        F.when(cma_code.isNotNull(), F.lit("cma"))
        .when(
            geography.rlike(
                r"^(Census metropolitan areas|Total census metropolitan areas|All )"
            ),
            F.lit("aggregate"),
        )
        .otherwise(F.lit("other"))
    )
    geography_key = (
        F.when(cma_code.isNotNull(), F.concat(F.lit("cma:"), cma_code))
        .when(
            geography_dguid.isNotNull(),
            F.concat(F.lit("dguid:"), geography_dguid),
        )
        .otherwise(F.concat(F.lit("name:"), geography))
    )

    status_code = F.upper(_clean_string(F.col("STATUS")))
    symbol_code = F.upper(_clean_string(F.col("SYMBOL")))
    terminated_code = F.upper(_clean_string(F.col("TERMINATED")))
    raw_numeric = F.expr("try_cast(VALUE as decimal(24, 6))")
    scalar_id = F.expr("try_cast(SCALAR_ID as int)")
    normalized_value = _normalized_value(raw_numeric, scalar_id)
    is_suppressed = F.coalesce(
        (status_code == "X") | (symbol_code == "X"), F.lit(False)
    )
    is_unavailable = F.coalesce(
        status_code.isin("..", "...", "F") | symbol_code.isin("..", "...", "F"),
        F.lit(False),
    )

    return raw.select(
        _clean_string(F.col("_source_id")).alias("source_id"),
        F.to_date(_clean_string(F.col("REF_DATE")), "yyyy-MM").alias("reference_month"),
        geography.alias("geography"),
        geography_dguid.alias("geography_dguid"),
        geography_level.alias("geography_level"),
        cma_code.alias("cma_code"),
        geography_key.alias("geography_key"),
        _clean_string(F.col("UOM")).alias("unit"),
        F.expr("try_cast(UOM_ID as int)").alias("unit_id"),
        _clean_string(F.col("SCALAR_FACTOR")).alias("scalar_factor"),
        scalar_id.alias("scalar_id"),
        status_code.alias("status_code"),
        symbol_code.alias("symbol_code"),
        F.coalesce((status_code == "R") | (symbol_code == "R"), F.lit(False)).alias(
            "is_revised"
        ),
        F.coalesce((status_code == "P") | (symbol_code == "P"), F.lit(False)).alias(
            "is_preliminary"
        ),
        F.coalesce(
            (terminated_code == "T") | (status_code == "T") | (symbol_code == "T"),
            F.lit(False),
        ).alias("is_terminated"),
        is_suppressed.alias("is_suppressed"),
        (normalized_value.isNotNull() & ~is_suppressed & ~is_unavailable).alias(
            "is_publishable"
        ),
        _clean_string(F.col("VECTOR")).alias("source_vector"),
        _clean_string(F.col("COORDINATE")).alias("source_coordinate"),
        F.expr("try_cast(DECIMALS as int)").alias("source_decimals"),
        F.col("_source_release_timestamp")
        .cast("timestamp")
        .alias("source_release_timestamp"),
        _clean_string(F.col("_source_archive_sha256")).alias("source_archive_sha256"),
        normalized_value.alias("_normalized_value"),
        *dimension_columns,
    )


def drop_rows_missing_required(
    frame: DataFrame, required_columns: tuple[str, ...]
) -> DataFrame:
    """Drop only rows whose natural key or provenance cannot be identified.

    A null observation value is not dropped: StatsCan uses null plus a status
    symbol to represent legitimate unavailable/suppressed observations.
    """
    condition = F.lit(True)
    for column_name in required_columns:
        condition = condition & F.col(column_name).isNotNull()
    return frame.filter(condition)


def deduplicate_observations(
    frame: DataFrame, natural_key: tuple[str, ...]
) -> DataFrame:
    """Select one deterministic observation across overlapping snapshots.

    The newest immutable release is authoritative. Within one release, an
    active series wins over a terminated replacement, then an explicitly
    revised/publishable row wins. The vector is the stable final tie-breaker.
    """
    rank_window = Window.partitionBy(*natural_key).orderBy(
        F.col("source_release_timestamp").desc_nulls_last(),
        F.col("is_terminated").asc(),
        F.col("is_revised").desc(),
        F.col("is_publishable").desc(),
        F.col("source_vector").desc_nulls_last(),
    )
    return (
        frame.withColumn("_deduplication_rank", F.row_number().over(rank_window))
        .filter(F.col("_deduplication_rank") == 1)
        .drop("_deduplication_rank")
    )


def _integral_count(value: Column) -> Column:
    return F.when(
        (value >= 0) & (value == F.floor(value)), value.cast("long")
    ).otherwise(F.lit(None).cast("long"))


def _activity(common: DataFrame) -> DataFrame:
    measure = _clean_string(F.col("Housing estimates"))
    dwelling_type = _clean_string(F.col("Type of unit"))
    housing_count = _integral_count(F.col("_normalized_value"))
    prepared = common.withColumn(
        "is_publishable", F.col("is_publishable") & housing_count.isNotNull()
    )
    return prepared.select(
        "*",
        measure.alias("housing_measure"),
        dwelling_type.alias("dwelling_type"),
        housing_count.alias("housing_count"),
        F.when(measure == "Housing under construction", F.lit("stock"))
        .when(measure.isin("Housing starts", "Housing completions"), F.lit("flow"))
        .otherwise(F.lit("unknown"))
        .alias("measure_semantics"),
        (dwelling_type == "Total units").alias("is_total_dwelling_type"),
    )


def _starts_by_market(common: DataFrame) -> DataFrame:
    dwelling_type = _clean_string(F.col("Type of dwelling unit"))
    intended_market = _clean_string(F.col("Type of market"))
    housing_count = _integral_count(F.col("_normalized_value"))
    prepared = common.withColumn(
        "is_publishable", F.col("is_publishable") & housing_count.isNotNull()
    )
    return prepared.select(
        "*",
        dwelling_type.alias("dwelling_type"),
        intended_market.alias("intended_market"),
        housing_count.alias("housing_starts_count"),
        F.lit("flow").alias("measure_semantics"),
        (dwelling_type == "Total units").alias("is_total_dwelling_type"),
    )


def _building_permits(common: DataFrame) -> DataFrame:
    building_type = _clean_string(F.col("Type of building"))
    permit_value = F.col("_normalized_value").cast(DecimalType(24, 4))
    prepared = common.withColumn(
        "is_publishable", F.col("is_publishable") & permit_value.isNotNull()
    )
    return prepared.select(
        "*",
        building_type.alias("building_type"),
        _clean_string(F.col("Type of work")).alias("work_type"),
        _clean_string(F.col("Variables")).alias("permit_variable"),
        _clean_string(F.col("Seasonal adjustment, value type")).alias(
            "adjustment_type"
        ),
        permit_value.alias("permit_value"),
        F.lower(building_type).startswith("total").alias("is_total_building_type"),
    )


def _price_index(common: DataFrame) -> DataFrame:
    component = _clean_string(F.col("New housing price indexes"))
    index_value = F.when(
        F.col("_normalized_value") > 0,
        F.col("_normalized_value").cast(DecimalType(18, 4)),
    )
    prepared = common.withColumn(
        "is_publishable", F.col("is_publishable") & index_value.isNotNull()
    )
    return prepared.select(
        "*",
        component.alias("index_component"),
        index_value.alias("index_value"),
        (component == "Total (house and land)").alias("is_total_index"),
    )


_SOURCE_TRANSFORMS = {
    "cmhc_housing_activity": _activity,
    "cmhc_starts_by_market": _starts_by_market,
    "statcan_building_permits": _building_permits,
    "statcan_new_housing_price_index": _price_index,
}


def clean_source(
    raw: DataFrame,
    source_id: str,
    profile: ProfileDefinition | None = None,
) -> DataFrame:
    """Clean and deduplicate one source while retaining audit columns."""
    try:
        source_transform = _SOURCE_TRANSFORMS[source_id]
        final_schema = CLEAN_SCHEMAS[source_id]
        natural_key = NATURAL_KEYS[source_id]
    except KeyError as error:
        raise TransformationError(f"No cleaning contract for {source_id}") from error

    source_frame = source_transform(normalize_common_columns(raw))
    required = natural_key + (
        "source_id",
        "geography",
        "source_release_timestamp",
        "source_archive_sha256",
    )
    filtered = drop_rows_missing_required(source_frame, required)
    # Apply the small reviewed profile before the window operation. CSV input
    # still remains native, but the deduplication shuffle is limited to the
    # relevant months/geographies during local development.
    if profile is not None:
        filtered = filter_profile(filtered, profile)
    deduplicated = deduplicate_observations(filtered, natural_key)
    return deduplicated.select(
        *[
            F.col(field.name).cast(field.dataType).alias(field.name)
            for field in final_schema.fields
        ]
    )


def filter_profile(frame: DataFrame, profile: ProfileDefinition) -> DataFrame:
    """Apply a reviewed month/CMA boundary after type normalization."""
    start = date.fromisoformat(f"{profile.reference_start}-01")
    end = date.fromisoformat(f"{profile.reference_end}-01")
    filtered = frame.filter(F.col("reference_month").between(start, end))
    if profile.cma_names:
        filtered = filtered.filter(F.col("geography").isin(*profile.cma_names))
    else:
        # The full profile is explicitly a CMA analytical scope, not Canada,
        # province, region, or publisher-provided aggregate rows.
        filtered = filtered.filter(F.col("geography_level") == "cma")
    return filtered
