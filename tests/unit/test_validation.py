from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from housing_elt.pipeline import validate_and_publish
from housing_elt.validation.checks import validate_analytics_fact
from housing_elt.validation.config import ValidationPolicy, load_validation_contract
from housing_elt.validation.errors import DataValidationError

VALIDATION_PATH = Path(__file__).resolve().parents[2] / "config/validation.toml"

_SCHEMA = StructType(
    [
        StructField("reference_month", DateType(), False),
        StructField("reference_year", IntegerType(), False),
        StructField("cma_code", StringType(), False),
        StructField("geography", StringType(), False),
        StructField("dwelling_type", StringType(), False),
        StructField("housing_starts", LongType(), False),
        StructField("housing_completions", LongType(), True),
        StructField("housing_under_construction", LongType(), False),
        StructField("market_starts_total", LongType(), False),
        StructField("new_housing_price_index", DecimalType(18, 4), False),
        StructField("residential_permit_value_dollars", DecimalType(24, 4), True),
        StructField("has_activity_data", BooleanType(), False),
        StructField("has_market_data", BooleanType(), False),
        StructField("has_price_index_data", BooleanType(), False),
        StructField("has_permit_data", BooleanType(), False),
        StructField("starts_anomaly_flag", BooleanType(), True),
    ]
)

_DWELLING_TYPES = (
    "total",
    "single_detached",
    "semi_detached",
    "row",
    "apartment_and_other",
)


def _policy(**overrides) -> ValidationPolicy:
    values = {
        "profile_name": "test",
        "min_rows": 5,
        "max_rows": 5,
        "key_columns": ("reference_month", "cma_code", "dwelling_type"),
        "allowed_dwelling_types": _DWELLING_TYPES,
        "require_complete_dwelling_set": True,
        "max_duplicate_rows": 0,
        "max_market_mismatch_fraction": 0.0,
        "max_component_mismatch_fraction": 0.0,
        "null_thresholds": {
            "housing_starts": 0.0,
            "housing_completions": 0.0,
            "housing_under_construction": 0.0,
            "market_starts_total": 0.0,
            "new_housing_price_index": 0.0,
            "residential_permit_value_dollars": 1.0,
        },
    }
    values.update(overrides)
    return ValidationPolicy(**values)


def _valid_fact(spark: SparkSession) -> DataFrame:
    starts = {
        "single_detached": 1,
        "semi_detached": 2,
        "row": 3,
        "apartment_and_other": 4,
        "total": 10,
    }
    completions = {
        "single_detached": 1,
        "semi_detached": 1,
        "row": 1,
        "apartment_and_other": 1,
        "total": 4,
    }
    under_construction = {
        "single_detached": 10,
        "semi_detached": 20,
        "row": 30,
        "apartment_and_other": 40,
        "total": 100,
    }
    rows = [
        (
            date(2024, 1, 1),
            2024,
            "535",
            "Toronto, Ontario",
            dwelling_type,
            starts[dwelling_type],
            completions[dwelling_type],
            under_construction[dwelling_type],
            starts[dwelling_type],
            Decimal("114.6000"),
            None,
            True,
            True,
            True,
            False,
            False,
        )
        for dwelling_type in _DWELLING_TYPES
    ]
    return spark.createDataFrame(rows, _SCHEMA)


def test_versioned_validation_contract_loads_reviewed_profiles() -> None:
    contract = load_validation_contract(VALIDATION_PATH)

    development = contract.profile("development")
    assert contract.contract_version == 1
    assert development.min_rows == 360
    assert development.max_rows == 360
    assert development.null_thresholds["residential_permit_value_dollars"] == 1.0
    assert contract.profile("full").min_rows == 15000


def test_valid_fact_returns_metrics_for_all_quality_gates(
    spark: SparkSession,
) -> None:
    report = validate_analytics_fact(_valid_fact(spark), _policy())

    assert report.passed is True
    assert report.row_count == 5
    assert report.metrics["duplicate_rows"] == 0
    assert report.metrics["market_reconciliation_mismatches"] == 0
    assert report.metrics["component_reconciliation_mismatches"] == 0
    assert report.metrics["incomplete_dwelling_groups"] == 0
    assert report.metrics["null_fraction.residential_permit_value_dollars"] == 1.0


def test_validation_collects_multiple_actionable_failures(
    spark: SparkSession,
) -> None:
    rows = _valid_fact(spark).collect()
    broken_total = rows[0].asDict()
    broken_total["housing_starts"] = 99
    broken_total["market_starts_total"] = 98
    broken_total["housing_completions"] = None
    broken = spark.createDataFrame(
        [broken_total, *(row.asDict() for row in rows[1:]), rows[1].asDict()],
        _SCHEMA,
    )

    with pytest.raises(DataValidationError) as raised:
        validate_analytics_fact(broken, _policy())

    message = str(raised.value)
    assert "row_count" in message
    assert "null_threshold" in message
    assert "duplicate_keys" in message
    assert "market_reconciliation" in message
    assert "component_reconciliation" in message


def test_schema_mismatch_fails_before_metric_queries(spark: SparkSession) -> None:
    wrong_schema = _valid_fact(spark).withColumn(
        "housing_starts", F.col("housing_starts").cast("string")
    )

    with pytest.raises(DataValidationError, match="expected 'bigint'") as raised:
        validate_analytics_fact(wrong_schema, _policy())

    assert raised.value.report.row_count is None


def test_failed_validation_never_calls_output_writer(
    spark: SparkSession, tmp_path: Path
) -> None:
    writer_calls: list[Path] = []

    def writer(_fact: DataFrame, output_path: Path) -> Path:
        writer_calls.append(output_path)
        return output_path

    invalid = _valid_fact(spark).limit(4)
    with pytest.raises(DataValidationError):
        validate_and_publish(
            invalid,
            _policy(),
            tmp_path / "must-not-exist",
            writer=writer,
        )

    assert writer_calls == []
    assert not (tmp_path / "must-not-exist").exists()
