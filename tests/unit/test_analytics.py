from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from housing_elt.analytics.aggregation import (
    ACTIVITY_SOURCE_ID,
    MARKET_SOURCE_ID,
    PERMIT_SOURCE_ID,
    PRICE_SOURCE_ID,
    build_analytics_fact,
)
from housing_elt.analytics.errors import AnalyticsError
from housing_elt.analytics.trends import add_trend_features
from housing_elt.analytics.writer import write_analytics_fact


def _activity(spark: SparkSession):
    rows = []
    measures = {
        "Housing starts": 10,
        "Housing completions": 8,
        "Housing under construction": 100,
    }
    for measure, value in measures.items():
        rows.append(
            {
                "reference_month": date(2024, 1, 1),
                "geography": "Toronto, Ontario",
                "geography_level": "cma",
                "cma_code": "535",
                "dwelling_type": "Single-detached units",
                "housing_measure": measure,
                "housing_count": value,
                "is_publishable": True,
                "source_release_timestamp": datetime(2026, 8, 20, 12, 30),
                "source_archive_sha256": "a" * 64,
            }
        )
    # This activity-only key verifies that the core full join does not discard
    # a housing measure merely because intended-market detail is absent.
    rows.append(
        {
            "reference_month": date(2024, 1, 1),
            "geography": "Toronto, Ontario",
            "geography_level": "cma",
            "cma_code": "535",
            "dwelling_type": "Row units",
            "housing_measure": "Housing starts",
            "housing_count": 4,
            "is_publishable": True,
            "source_release_timestamp": datetime(2026, 8, 20, 12, 30),
            "source_archive_sha256": "a" * 64,
        }
    )
    return spark.createDataFrame(rows)


def _market(spark: SparkSession):
    rows = []
    markets = {
        "Homeowner": 5,
        "Rental": 2,
        "Condo": 2,
        "Co-op": 0,
        "Other market": 1,
    }
    for market, value in markets.items():
        rows.append(
            {
                "reference_month": date(2024, 1, 1),
                "geography": "Toronto, Ontario",
                "geography_level": "cma",
                "cma_code": "535",
                "dwelling_type": "Single units",
                "intended_market": market,
                "housing_starts_count": value,
                "is_publishable": True,
                "source_release_timestamp": datetime(2026, 8, 20, 12, 30),
                "source_archive_sha256": "b" * 64,
            }
        )
    # This market-only key must survive with has_activity_data=false.
    rows.append(
        {
            "reference_month": date(2024, 1, 1),
            "geography": "Toronto, Ontario",
            "geography_level": "cma",
            "cma_code": "535",
            "dwelling_type": "Apartment and other types of units",
            "intended_market": "Rental",
            "housing_starts_count": 3,
            "is_publishable": True,
            "source_release_timestamp": datetime(2026, 8, 20, 12, 30),
            "source_archive_sha256": "b" * 64,
        }
    )
    return spark.createDataFrame(rows)


def _price_index(spark: SparkSession):
    components = {
        "Total (house and land)": Decimal("114.6"),
        "House only": Decimal("112.5"),
        "Land only": Decimal("117.7"),
    }
    return spark.createDataFrame(
        [
            {
                "reference_month": date(2024, 1, 1),
                "geography_level": "cma",
                "cma_code": "535",
                "index_component": component,
                "index_value": value,
                "is_publishable": True,
                "source_release_timestamp": datetime(2026, 8, 20, 12, 30),
                "source_archive_sha256": "c" * 64,
            }
            for component, value in components.items()
        ]
    )


def _permits(spark: SparkSession):
    return spark.createDataFrame(
        [
            {
                "reference_month": date(2024, 1, 1),
                "geography_level": "cma",
                "cma_code": "535",
                "building_type": "Total residential",
                "work_type": "Types of work, total",
                "permit_variable": "Value of permits",
                "adjustment_type": "Seasonally adjusted, current",
                "permit_value": Decimal("250000000.0000"),
                "is_publishable": True,
                "source_release_timestamp": datetime(2026, 8, 20, 12, 30),
                "source_archive_sha256": "d" * 64,
            }
        ]
    )


def test_rollups_join_context_and_preserve_core_mismatches(
    spark: SparkSession,
) -> None:
    fact = build_analytics_fact(
        {
            ACTIVITY_SOURCE_ID: _activity(spark),
            MARKET_SOURCE_ID: _market(spark),
            PRICE_SOURCE_ID: _price_index(spark),
            PERMIT_SOURCE_ID: _permits(spark),
        }
    )
    rows = {row.dwelling_type: row for row in fact.collect()}

    matched = rows["single_detached"]
    assert matched.housing_starts == 10
    assert matched.housing_completions == 8
    assert matched.housing_under_construction == 100
    assert matched.market_starts_total == 10
    assert matched.has_complete_activity is True
    assert matched.has_complete_market_breakdown is True
    assert matched.completion_to_start_ratio == 0.8
    assert matched.new_housing_price_index == Decimal("114.6000")
    assert matched.residential_permit_value_dollars == Decimal("250000000.0000")
    assert matched.has_complete_price_index is True
    assert matched.has_permit_data is True
    assert matched.reference_year == 2024

    assert rows["row"].has_activity_data is True
    assert rows["row"].has_market_data is False
    assert rows["apartment_and_other"].has_activity_data is False
    assert rows["apartment_and_other"].has_market_data is True


def test_missing_optional_permits_produce_typed_null_and_false_coverage(
    spark: SparkSession,
) -> None:
    fact = build_analytics_fact(
        {
            ACTIVITY_SOURCE_ID: _activity(spark),
            MARKET_SOURCE_ID: _market(spark),
            PRICE_SOURCE_ID: _price_index(spark),
        }
    )

    rows = (
        fact.select("residential_permit_value_dollars", "has_permit_data")
        .distinct()
        .collect()
    )
    assert len(rows) == 1
    assert rows[0].residential_permit_value_dollars is None
    assert rows[0].has_permit_data is False


def test_missing_required_core_source_fails_loudly(spark: SparkSession) -> None:
    with pytest.raises(AnalyticsError, match="cmhc_starts_by_market"):
        build_analytics_fact({ACTIVITY_SOURCE_ID: _activity(spark)})


def test_trends_use_contiguous_backward_looking_windows(spark: SparkSession) -> None:
    starts = list(range(1, 13)) + [100]
    rows = [
        {
            "reference_month": date(2024 + (index // 12), (index % 12) + 1, 1),
            "cma_code": "535",
            "dwelling_type": "total",
            "housing_starts": value,
            "housing_under_construction": 200 + index,
        }
        for index, value in enumerate(starts)
    ]

    latest = (
        add_trend_features(spark.createDataFrame(rows))
        .filter("reference_month = DATE '2025-01-01'")
        .first()
    )

    assert latest is not None
    assert latest.starts_3_month_average == pytest.approx(41.0)
    assert latest.starts_year_over_year_pct == pytest.approx(9900.0)
    assert latest.under_construction_month_change == 1
    assert latest.has_12_month_anomaly_baseline is True
    assert latest.starts_prior_12_month_average == pytest.approx(6.5)
    assert latest.starts_anomaly_zscore > 20
    assert latest.starts_anomaly_flag is True


def test_year_partitioned_parquet_layout(spark: SparkSession, tmp_path: Path) -> None:
    fact = spark.createDataFrame(
        [
            {
                "reference_month": date(2024, 1, 1),
                "reference_year": 2024,
                "cma_code": "535",
                "dwelling_type": "total",
                "housing_starts": 10,
            },
            {
                "reference_month": date(2025, 1, 1),
                "reference_year": 2025,
                "cma_code": "535",
                "dwelling_type": "total",
                "housing_starts": 12,
            },
        ]
    )
    output_path = tmp_path / "housing_monthly"

    write_analytics_fact(fact, output_path)

    partitions = {
        path.name
        for path in output_path.iterdir()
        if path.name.startswith("reference_year=")
    }
    assert partitions == {"reference_year=2024", "reference_year=2025"}
    assert spark.read.parquet(str(output_path)).count() == 2
