"""Explicit source-specific raw and clean Spark schemas.

Statistics Canada CSV values are initially strings by design. Parsing numbers
inside the CSV reader would conflate a genuinely absent value with a value that
is intentionally unavailable (for example, ``..`` or ``x``). The cleaning
layer performs tolerant, auditable casts after retaining the status columns.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _string_schema(columns: tuple[str, ...]) -> StructType:
    return StructType([StructField(name, StringType(), True) for name in columns])


_COMMON_TRAILING_COLUMNS = (
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
)

RAW_SCHEMAS: dict[str, StructType] = {
    "cmhc_housing_activity": _string_schema(
        (
            "REF_DATE",
            "GEO",
            "DGUID",
            "Housing estimates",
            "Type of unit",
            *_COMMON_TRAILING_COLUMNS,
        )
    ),
    "cmhc_starts_by_market": _string_schema(
        (
            "REF_DATE",
            "GEO",
            "DGUID",
            "Type of dwelling unit",
            "Type of market",
            *_COMMON_TRAILING_COLUMNS,
        )
    ),
    "statcan_building_permits": _string_schema(
        (
            "REF_DATE",
            "GEO",
            "DGUID",
            "Type of building",
            "Type of work",
            "Variables",
            "Seasonal adjustment, value type",
            *_COMMON_TRAILING_COLUMNS,
        )
    ),
    "statcan_new_housing_price_index": _string_schema(
        (
            "REF_DATE",
            "GEO",
            "DGUID",
            "New housing price indexes",
            *_COMMON_TRAILING_COLUMNS,
        )
    ),
}


def _common_clean_fields() -> list[StructField]:
    return [
        StructField("source_id", StringType(), False),
        StructField("reference_month", DateType(), False),
        StructField("geography", StringType(), False),
        StructField("geography_dguid", StringType(), True),
        StructField("geography_level", StringType(), False),
        StructField("cma_code", StringType(), True),
    ]


def _audit_fields() -> list[StructField]:
    return [
        StructField("unit", StringType(), True),
        StructField("unit_id", IntegerType(), True),
        StructField("scalar_factor", StringType(), True),
        StructField("scalar_id", IntegerType(), True),
        StructField("status_code", StringType(), True),
        StructField("symbol_code", StringType(), True),
        StructField("is_revised", BooleanType(), False),
        StructField("is_preliminary", BooleanType(), False),
        StructField("is_terminated", BooleanType(), False),
        StructField("is_suppressed", BooleanType(), False),
        StructField("is_publishable", BooleanType(), False),
        StructField("source_vector", StringType(), True),
        StructField("source_coordinate", StringType(), True),
        StructField("source_decimals", IntegerType(), True),
        StructField("source_release_timestamp", TimestampType(), False),
        StructField("source_archive_sha256", StringType(), False),
    ]


CLEAN_SCHEMAS: dict[str, StructType] = {
    "cmhc_housing_activity": StructType(
        _common_clean_fields()
        + [
            StructField("housing_measure", StringType(), False),
            StructField("dwelling_type", StringType(), False),
            StructField("housing_count", LongType(), True),
            StructField("measure_semantics", StringType(), False),
            StructField("is_total_dwelling_type", BooleanType(), False),
        ]
        + _audit_fields()
    ),
    "cmhc_starts_by_market": StructType(
        _common_clean_fields()
        + [
            StructField("dwelling_type", StringType(), False),
            StructField("intended_market", StringType(), False),
            StructField("housing_starts_count", LongType(), True),
            StructField("measure_semantics", StringType(), False),
            StructField("is_total_dwelling_type", BooleanType(), False),
        ]
        + _audit_fields()
    ),
    "statcan_building_permits": StructType(
        _common_clean_fields()
        + [
            StructField("building_type", StringType(), False),
            StructField("work_type", StringType(), False),
            StructField("permit_variable", StringType(), False),
            StructField("adjustment_type", StringType(), False),
            StructField("permit_value", DecimalType(24, 4), True),
            StructField("is_total_building_type", BooleanType(), False),
        ]
        + _audit_fields()
    ),
    "statcan_new_housing_price_index": StructType(
        _common_clean_fields()
        + [
            StructField("index_component", StringType(), False),
            StructField("index_value", DecimalType(18, 4), True),
            StructField("is_total_index", BooleanType(), False),
        ]
        + _audit_fields()
    ),
}


NATURAL_KEYS: dict[str, tuple[str, ...]] = {
    "cmhc_housing_activity": (
        "reference_month",
        "geography_key",
        "housing_measure",
        "dwelling_type",
    ),
    "cmhc_starts_by_market": (
        "reference_month",
        "geography_key",
        "dwelling_type",
        "intended_market",
    ),
    "statcan_building_permits": (
        "reference_month",
        "geography_key",
        "building_type",
        "work_type",
        "permit_variable",
        "adjustment_type",
    ),
    "statcan_new_housing_price_index": (
        "reference_month",
        "geography_key",
        "index_component",
    ),
}
