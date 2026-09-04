import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DDL_PATH = PROJECT_ROOT / "sql/001_create_housing_analytics.sql"
PUBLISH_PATH = PROJECT_ROOT / "sql/002_publish_housing_monthly.sql"

ANALYTICS_COLUMNS = (
    "REFERENCE_MONTH",
    "CMA_CODE",
    "DWELLING_TYPE",
    "HOUSING_STARTS",
    "HOUSING_COMPLETIONS",
    "HOUSING_UNDER_CONSTRUCTION",
    "ACTIVITY_MEASURE_COUNT",
    "HAS_ACTIVITY_DATA",
    "ACTIVITY_RELEASE_TIMESTAMP",
    "ACTIVITY_ARCHIVE_SHA256",
    "STARTS_HOMEOWNER",
    "STARTS_RENTAL",
    "STARTS_CONDOMINIUM",
    "STARTS_COOPERATIVE",
    "STARTS_OTHER_MARKET",
    "MARKET_MEMBER_COUNT",
    "HAS_MARKET_DATA",
    "MARKET_RELEASE_TIMESTAMP",
    "MARKET_ARCHIVE_SHA256",
    "GEOGRAPHY",
    "HAS_COMPLETE_ACTIVITY",
    "HAS_COMPLETE_MARKET_BREAKDOWN",
    "MARKET_STARTS_TOTAL",
    "NEW_HOUSING_PRICE_INDEX",
    "NEW_HOUSE_PRICE_INDEX",
    "NEW_LAND_PRICE_INDEX",
    "PRICE_INDEX_COMPONENT_COUNT",
    "PRICE_RELEASE_TIMESTAMP",
    "PRICE_ARCHIVE_SHA256",
    "HAS_PRICE_INDEX_DATA",
    "HAS_COMPLETE_PRICE_INDEX",
    "RESIDENTIAL_PERMIT_VALUE_DOLLARS",
    "PERMIT_RELEASE_TIMESTAMP",
    "PERMIT_ARCHIVE_SHA256",
    "HAS_PERMIT_DATA",
    "COMPLETION_TO_START_RATIO",
    "STARTS_3_MONTH_AVERAGE",
    "STARTS_YEAR_OVER_YEAR_PCT",
    "UNDER_CONSTRUCTION_MONTH_CHANGE",
    "HAS_12_MONTH_ANOMALY_BASELINE",
    "STARTS_PRIOR_12_MONTH_AVERAGE",
    "STARTS_PRIOR_12_MONTH_STDDEV",
    "STARTS_ANOMALY_ZSCORE",
    "STARTS_ANOMALY_FLAG",
    "REFERENCE_YEAR",
)


def _table_columns(sql: str, table_name: str) -> tuple[str, ...]:
    marker = f"{table_name} ("
    block = sql.split(marker, maxsplit=1)[1].split("\n)", maxsplit=1)[0]
    columns = []
    for line in block.splitlines():
        match = re.match(r"\s{4}([A-Z][A-Z0-9_]*)\s+", line)
        if match:
            columns.append(match.group(1))
    return tuple(columns)


def _publish_insert_columns(sql: str) -> tuple[str, ...]:
    block = sql.split(
        "INSERT INTO HOUSING_ANALYTICS.FCT_HOUSING_MONTHLY (", maxsplit=1
    )[1].split("\n)\nSELECT", maxsplit=1)[0]
    return tuple(
        line.strip().removesuffix(",") for line in block.splitlines() if line.strip()
    )


def _publish_select_expressions(sql: str) -> tuple[str, ...]:
    block = sql.split("\nSELECT\n", maxsplit=1)[1].split(
        "\nFROM HOUSING_ANALYTICS.STG_HOUSING_MONTHLY", maxsplit=1
    )[0]
    return tuple(
        line.strip().removesuffix(",") for line in block.splitlines() if line.strip()
    )


def _without_comments(sql: str) -> str:
    return "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )


def test_ddl_maps_every_analytics_column_to_staging_and_final() -> None:
    ddl = DDL_PATH.read_text(encoding="utf-8")

    staging_columns = _table_columns(ddl, "STG_HOUSING_MONTHLY")
    final_columns = _table_columns(ddl, "FCT_HOUSING_MONTHLY")

    assert staging_columns == (
        "LOAD_BATCH_ID",
        "VALIDATION_PROFILE",
        "LOADED_AT_UTC",
        *ANALYTICS_COLUMNS,
    )
    assert final_columns == (
        *ANALYTICS_COLUMNS,
        "SOURCE_LOAD_BATCH_ID",
        "SOURCE_VALIDATION_PROFILE",
        "CREATED_AT_UTC",
        "UPDATED_AT_UTC",
    )


def test_publish_insert_column_order_matches_final_contract() -> None:
    publish_sql = PUBLISH_PATH.read_text(encoding="utf-8")

    assert _publish_insert_columns(publish_sql) == (
        *ANALYTICS_COLUMNS,
        "SOURCE_LOAD_BATCH_ID",
        "SOURCE_VALIDATION_PROFILE",
        "CREATED_AT_UTC",
        "UPDATED_AT_UTC",
    )
    assert _publish_select_expressions(publish_sql) == (
        *ANALYTICS_COLUMNS,
        "LOAD_BATCH_ID",
        "VALIDATION_PROFILE",
        "CURRENT_TIMESTAMP()",
        "CURRENT_TIMESTAMP()",
    )
    statement_order = (
        "BEGIN TRANSACTION",
        "DELETE FROM HOUSING_ANALYTICS.FCT_HOUSING_MONTHLY",
        "INSERT INTO HOUSING_ANALYTICS.FCT_HOUSING_MONTHLY",
        "UPDATE HOUSING_ANALYTICS.ELT_LOAD_AUDIT",
        "COMMIT",
    )
    positions = tuple(publish_sql.index(statement) for statement in statement_order)
    assert positions == tuple(sorted(positions))


def test_ddl_avoids_costly_or_environment_owned_objects() -> None:
    ddl = _without_comments(DDL_PATH.read_text(encoding="utf-8")).upper()

    assert "CREATE WAREHOUSE" not in ddl
    assert "CREATE DATABASE" not in ddl
    assert "CLUSTER BY" not in ddl
    assert "CREATE TRANSIENT TABLE IF NOT EXISTS STG_HOUSING_MONTHLY" in ddl
