from __future__ import annotations

import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from housing_elt.ingestion.registry import ProfileDefinition
from housing_elt.transformation.cleaning import clean_source
from housing_elt.transformation.errors import TransformationError
from housing_elt.transformation.schemas import CLEAN_SCHEMAS, RAW_SCHEMAS
from housing_elt.transformation.snapshots import SourceSnapshot, materialize_csv

_COMMON_DEFAULTS = {
    "REF_DATE": "2024-01",
    "GEO": "Toronto, Ontario",
    "DGUID": "2011S0503535",
    "UOM": "Units",
    "UOM_ID": "300",
    "SCALAR_FACTOR": "units",
    "SCALAR_ID": "0",
    "VECTOR": "v1",
    "COORDINATE": "1.1",
    "VALUE": "10",
    "STATUS": "",
    "SYMBOL": "",
    "TERMINATED": "",
    "DECIMALS": "0",
}

_DIMENSION_DEFAULTS = {
    "cmhc_housing_activity": {
        "Housing estimates": "Housing starts",
        "Type of unit": "Single-detached units",
    },
    "cmhc_starts_by_market": {
        "Type of dwelling unit": "Single units",
        "Type of market": "Homeowner",
    },
    "statcan_building_permits": {
        "Type of building": "Residential buildings",
        "Type of work": "New construction",
        "Variables": "Value of permits",
        "Seasonal adjustment, value type": "Unadjusted",
    },
    "statcan_new_housing_price_index": {
        "New housing price indexes": "Total (house and land)",
    },
}


def _raw_frame(
    spark: SparkSession,
    source_id: str,
    rows: list[dict[str, str]],
    *,
    release: datetime = datetime(2026, 8, 20, 12, 30),
    sha256: str = "a" * 64,
) -> DataFrame:
    complete_rows = []
    for values in rows:
        row = _COMMON_DEFAULTS | _DIMENSION_DEFAULTS[source_id] | values
        complete_rows.append(row)
    return (
        spark.createDataFrame(complete_rows, RAW_SCHEMAS[source_id])
        .withColumn("_source_id", F.lit(source_id))
        .withColumn("_source_release_timestamp", F.lit(release))
        .withColumn("_source_archive_sha256", F.lit(sha256))
    )


def test_raw_schemas_preserve_source_lexical_values() -> None:
    assert set(RAW_SCHEMAS) == set(CLEAN_SCHEMAS)
    for schema in RAW_SCHEMAS.values():
        assert all(isinstance(field.dataType, StringType) for field in schema.fields)

    assert "Housing estimates" in RAW_SCHEMAS["cmhc_housing_activity"].fieldNames()
    assert "Type of market" in RAW_SCHEMAS["cmhc_starts_by_market"].fieldNames()
    assert "Variables" in RAW_SCHEMAS["statcan_building_permits"].fieldNames()
    assert (
        "New housing price indexes"
        in RAW_SCHEMAS["statcan_new_housing_price_index"].fieldNames()
    )


@pytest.mark.parametrize("source_id", sorted(RAW_SCHEMAS))
def test_each_source_emits_its_explicit_clean_schema(
    spark: SparkSession, source_id: str
) -> None:
    actual = clean_source(_raw_frame(spark, source_id, [{}]), source_id)

    assert actual.schema.names == CLEAN_SCHEMAS[source_id].names
    assert [field.dataType for field in actual.schema.fields] == [
        field.dataType for field in CLEAN_SCHEMAS[source_id].fields
    ]
    assert actual.count() == 1


def test_activity_applies_scalars_and_distinguishes_stock_from_flow(
    spark: SparkSession,
) -> None:
    raw = _raw_frame(
        spark,
        "cmhc_housing_activity",
        [
            {
                "Housing estimates": "Housing starts",
                "Type of unit": "Total units",
                "VALUE": "2",
                "SCALAR_FACTOR": "thousands",
                "SCALAR_ID": "3",
                "STATUS": "r",
            },
            {
                "Housing estimates": "Housing under construction",
                "VALUE": "25",
                "VECTOR": "v2",
                "COORDINATE": "1.2",
            },
            {
                "Housing estimates": "Housing completions",
                "VALUE": "-1",
                "VECTOR": "v3",
                "COORDINATE": "1.3",
            },
        ],
    )

    rows = {
        row.housing_measure: row
        for row in clean_source(raw, "cmhc_housing_activity").collect()
    }

    assert rows["Housing starts"].housing_count == 2_000
    assert rows["Housing starts"].measure_semantics == "flow"
    assert rows["Housing starts"].is_revised is True
    assert rows["Housing starts"].is_total_dwelling_type is True
    assert rows["Housing under construction"].measure_semantics == "stock"
    assert rows["Housing completions"].housing_count is None
    assert rows["Housing completions"].is_publishable is False


def test_required_key_null_is_dropped_but_coded_missing_value_is_retained(
    spark: SparkSession,
) -> None:
    raw = _raw_frame(
        spark,
        "cmhc_housing_activity",
        [
            {"GEO": "", "VALUE": "99"},
            {
                "REF_DATE": "2024-02",
                "VALUE": "",
                "SYMBOL": "..",
                "VECTOR": "v2",
            },
        ],
    )

    rows = clean_source(raw, "cmhc_housing_activity").collect()

    assert len(rows) == 1
    assert rows[0].housing_count is None
    assert rows[0].symbol_code == ".."
    assert rows[0].is_publishable is False


def test_newest_release_and_active_series_win_duplicate_key(
    spark: SparkSession,
) -> None:
    source_id = "cmhc_housing_activity"
    old = _raw_frame(
        spark,
        source_id,
        [{"VALUE": "9", "VECTOR": "old"}],
        release=datetime(2026, 7, 20, 12, 30),
        sha256="a" * 64,
    )
    newest_terminated = _raw_frame(
        spark,
        source_id,
        [{"VALUE": "10", "VECTOR": "terminated", "TERMINATED": "t"}],
        sha256="b" * 64,
    )
    newest_active = _raw_frame(
        spark,
        source_id,
        [{"VALUE": "11", "VECTOR": "active", "STATUS": "r"}],
        sha256="b" * 64,
    )

    row = clean_source(
        old.unionByName(newest_terminated).unionByName(newest_active), source_id
    ).first()

    assert row is not None
    assert row.housing_count == 11
    assert row.source_vector == "active"
    assert row.is_revised is True
    assert row.source_archive_sha256 == "b" * 64


def test_published_total_and_components_remain_distinct(
    spark: SparkSession,
) -> None:
    raw = _raw_frame(
        spark,
        "cmhc_starts_by_market",
        [
            {"Type of dwelling unit": "Total units", "VALUE": "12"},
            {
                "Type of dwelling unit": "Single units",
                "VALUE": "7",
                "VECTOR": "v2",
                "COORDINATE": "1.2",
            },
        ],
    )

    rows = clean_source(raw, "cmhc_starts_by_market").collect()

    assert len(rows) == 2
    assert {row.is_total_dwelling_type for row in rows} == {False, True}


def test_development_profile_filters_months_and_cmas(spark: SparkSession) -> None:
    profile = ProfileDefinition(
        name="development",
        source_ids=("cmhc_housing_activity",),
        reference_start="2024-01",
        reference_end="2025-12",
        cma_names=("Toronto, Ontario", "Calgary, Alberta"),
        cma_codes=("535", "825"),
    )
    raw = _raw_frame(
        spark,
        "cmhc_housing_activity",
        [
            {},
            {
                "GEO": "Vancouver, British Columbia",
                "DGUID": "2011S0503933",
                "VECTOR": "v2",
            },
            {"REF_DATE": "2023-12", "VECTOR": "v3"},
        ],
    )

    rows = clean_source(raw, "cmhc_housing_activity", profile).collect()

    assert len(rows) == 1
    assert rows[0].geography == "Toronto, Ontario"


def test_quality_flag_is_visible_and_suppression_is_not_publishable(
    spark: SparkSession,
) -> None:
    raw = _raw_frame(
        spark,
        "statcan_new_housing_price_index",
        [
            {"VALUE": "114.6", "STATUS": "E", "DECIMALS": "1"},
            {
                "New housing price indexes": "Land only",
                "VALUE": "",
                "SYMBOL": "x",
                "VECTOR": "v2",
                "COORDINATE": "1.2",
            },
        ],
    )

    rows = {
        row.index_component: row
        for row in clean_source(raw, "statcan_new_housing_price_index").collect()
    }

    assert str(rows["Total (house and land)"].index_value) == "114.6000"
    assert rows["Total (house and land)"].status_code == "E"
    assert rows["Total (house and land)"].is_publishable is True
    assert rows["Land only"].index_value is None
    assert rows["Land only"].is_suppressed is True
    assert rows["Land only"].is_publishable is False


def test_materialization_rejects_schema_drift_before_publication(
    tmp_path: Path,
) -> None:
    archive_path = tmp_path / "source.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("34100154.csv", "WRONG,HEADER\n1,2\n")
    snapshot = SourceSnapshot(
        source_id="cmhc_housing_activity",
        release_timestamp=datetime(2026, 8, 20, tzinfo=UTC),
        release_label="20260820T000000Z",
        sha256="c" * 64,
        archive_path=archive_path,
        data_member="34100154.csv",
    )

    with pytest.raises(TransformationError, match="schema drift"):
        materialize_csv(snapshot, tmp_path / "interim")

    extracted = tuple((tmp_path / "interim").rglob("34100154.csv"))
    partials = tuple((tmp_path / "interim").rglob("*.partial-*"))
    assert extracted == ()
    assert partials == ()
