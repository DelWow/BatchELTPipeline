"""Bounded, audited, transactional publication of a validated Spark fact."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any
from uuid import uuid4

from pyspark.sql import DataFrame

from housing_elt.snowflake.config import SnowflakeSettings
from housing_elt.snowflake.errors import SnowflakeLoadError
from housing_elt.validation.report import ValidationReport

# This order is the shared boundary between Spark and the reviewed Phase 9 DDL.
# Keeping it explicit prevents a harmless Spark column reorder from corrupting a
# positional executemany() insert.
ANALYTICS_COLUMNS = (
    "reference_month",
    "cma_code",
    "dwelling_type",
    "housing_starts",
    "housing_completions",
    "housing_under_construction",
    "activity_measure_count",
    "has_activity_data",
    "activity_release_timestamp",
    "activity_archive_sha256",
    "starts_homeowner",
    "starts_rental",
    "starts_condominium",
    "starts_cooperative",
    "starts_other_market",
    "market_member_count",
    "has_market_data",
    "market_release_timestamp",
    "market_archive_sha256",
    "geography",
    "has_complete_activity",
    "has_complete_market_breakdown",
    "market_starts_total",
    "new_housing_price_index",
    "new_house_price_index",
    "new_land_price_index",
    "price_index_component_count",
    "price_release_timestamp",
    "price_archive_sha256",
    "has_price_index_data",
    "has_complete_price_index",
    "residential_permit_value_dollars",
    "permit_release_timestamp",
    "permit_archive_sha256",
    "has_permit_data",
    "completion_to_start_ratio",
    "starts_3_month_average",
    "starts_year_over_year_pct",
    "under_construction_month_change",
    "has_12_month_anomaly_baseline",
    "starts_prior_12_month_average",
    "starts_prior_12_month_stddev",
    "starts_anomaly_zscore",
    "starts_anomaly_flag",
    "reference_year",
)
_TIMESTAMP_NTZ_COLUMNS = frozenset(
    {
        "activity_release_timestamp",
        "market_release_timestamp",
        "price_release_timestamp",
        "permit_release_timestamp",
    }
)


@dataclass(frozen=True, slots=True)
class SnowflakeLoadResult:
    """Auditable identifiers and counts from one successful publication."""

    batch_id: str
    validation_profile: str
    reference_start: date
    reference_end: date
    staged_row_count: int
    published_row_count: int


def _default_connect(**parameters: Any) -> Any:
    # Import lazily so configuration inspection and local-only pipeline commands
    # do not import the connector or initialize its logging/network stack.
    import snowflake.connector

    return snowflake.connector.connect(**parameters)


def _qualified(settings: SnowflakeSettings, table: str) -> str:
    return f"{settings.namespace}.{table}"


def _chunks(rows: Iterator[Sequence[Any]], size: int) -> Iterator[list[Sequence[Any]]]:
    batch: list[Sequence[Any]] = []
    for row in rows:
        batch.append(row)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def _bound_rows(
    fact: DataFrame,
    batch_id: str,
    profile: str,
    loaded_at_utc: datetime,
) -> Iterator[tuple[Any, ...]]:
    ordered = fact.select(*ANALYTICS_COLUMNS)
    for row in ordered.toLocalIterator(prefetchPartitions=False):
        values: list[Any] = [
            batch_id,
            profile,
            ("TIMESTAMP_TZ", loaded_at_utc),
        ]
        for column in ANALYTICS_COLUMNS:
            value = row[column]
            # Spark timestamps were normalized in a UTC session. Binding them
            # explicitly as NTZ preserves the reviewed UTC-without-zone contract.
            if value is not None and column in _TIMESTAMP_NTZ_COLUMNS:
                value = ("TIMESTAMP_NTZ", value)
            values.append(value)
        yield tuple(values)


def _staging_insert_sql(settings: SnowflakeSettings) -> str:
    columns = ",\n    ".join(
        (
            "LOAD_BATCH_ID",
            "VALIDATION_PROFILE",
            "LOADED_AT_UTC",
            *map(str.upper, ANALYTICS_COLUMNS),
        )
    )
    placeholders = ", ".join("?" for _ in range(3 + len(ANALYTICS_COLUMNS)))
    return f"""INSERT INTO {_qualified(settings, "STG_HOUSING_MONTHLY")} (
    {columns}
)
VALUES ({placeholders})"""


def _failure_message(error: BaseException) -> str:
    message = f"{type(error).__name__}: {error}"
    return message[:4000]


def load_analytics_fact(
    fact: DataFrame,
    report: ValidationReport,
    settings: SnowflakeSettings,
    *,
    batch_size: int = 1_000,
    connect: Callable[..., Any] = _default_connect,
) -> SnowflakeLoadResult:
    """Publish a fact only when its validation report passed.

    The serving table is replaced only for the complete month window in this
    batch. A corrected release can therefore remove stale natural keys, and a
    retry converges on the same business rows even though it receives a new
    load batch ID.
    """
    if not report.passed or report.row_count is None:
        raise SnowflakeLoadError("Refusing Snowflake load without passed validation")
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    missing = tuple(
        column for column in ANALYTICS_COLUMNS if column not in fact.columns
    )
    if missing:
        raise SnowflakeLoadError(
            "Analytics fact is missing Snowflake columns: " + ", ".join(missing)
        )

    batch_id = str(uuid4())
    # One aware timestamp across the batch makes the audit lineage deterministic
    # and binds explicitly to the staging TIMESTAMP_TZ contract.
    loaded_at_utc = datetime.now(UTC)
    profile = report.profile_name
    metrics_json = json.dumps(report.metrics, sort_keys=True, separators=(",", ":"))
    audit = _qualified(settings, "ELT_LOAD_AUDIT")
    staging = _qualified(settings, "STG_HOUSING_MONTHLY")
    final = _qualified(settings, "FCT_HOUSING_MONTHLY")
    connection = None
    cursor = None
    audit_started = False
    transaction_started = False

    try:
        connection = connect(**settings.connection_parameters())
        cursor = connection.cursor()

        cursor.execute(
            f"""SELECT COUNT(*)
FROM {audit}
WHERE VALIDATION_PROFILE = ? AND STATUS = 'STARTED'""",
            (profile,),
        )
        active_count = int(cursor.fetchone()[0])
        if active_count:
            raise SnowflakeLoadError(
                f"Refusing concurrent load: profile {profile!r} already has "
                f"{active_count} STARTED batch(es)"
            )

        cursor.execute(
            f"""INSERT INTO {audit} (
    LOAD_BATCH_ID, VALIDATION_PROFILE, STATUS, VALIDATED_ROW_COUNT,
    VALIDATION_METRICS, STARTED_AT_UTC
)
VALUES (?, ?, 'STARTED', ?, PARSE_JSON(?), CURRENT_TIMESTAMP())""",
            (batch_id, profile, report.row_count, metrics_json),
        )
        audit_started = True

        # UUIDs should be unique; clearing the batch address also makes a
        # deliberately replayed ID safe during tests or incident recovery.
        cursor.execute(f"DELETE FROM {staging} WHERE LOAD_BATCH_ID = ?", (batch_id,))
        rows = _bound_rows(fact, batch_id, profile, loaded_at_utc)
        for batch in _chunks(rows, batch_size):
            cursor.executemany(_staging_insert_sql(settings), batch)

        cursor.execute(
            f"""SELECT
    COUNT(*),
    COUNT(DISTINCT REFERENCE_MONTH, CMA_CODE, DWELLING_TYPE),
    MIN(REFERENCE_MONTH),
    MAX(REFERENCE_MONTH)
FROM {staging}
WHERE LOAD_BATCH_ID = ?""",
            (batch_id,),
        )
        staged_count, distinct_keys, reference_start, reference_end = cursor.fetchone()
        staged_count = int(staged_count)
        distinct_keys = int(distinct_keys)
        if (
            staged_count != report.row_count
            or distinct_keys != staged_count
            or reference_start is None
            or reference_end is None
        ):
            raise SnowflakeLoadError(
                "Staging reconciliation failed: "
                f"validated={report.row_count}, staged={staged_count}, "
                f"distinct_keys={distinct_keys}"
            )

        cursor.execute(
            f"""UPDATE {audit}
SET REFERENCE_START = ?, REFERENCE_END = ?, STAGED_ROW_COUNT = ?
WHERE LOAD_BATCH_ID = ?""",
            (reference_start, reference_end, staged_count, batch_id),
        )

        cursor.execute("BEGIN TRANSACTION")
        transaction_started = True
        cursor.execute(
            f"""DELETE FROM {final}
WHERE REFERENCE_MONTH BETWEEN
    (SELECT MIN(REFERENCE_MONTH) FROM {staging} WHERE LOAD_BATCH_ID = ?)
    AND
    (SELECT MAX(REFERENCE_MONTH) FROM {staging} WHERE LOAD_BATCH_ID = ?)""",
            (batch_id, batch_id),
        )

        analytics_columns = ",\n    ".join(map(str.upper, ANALYTICS_COLUMNS))
        cursor.execute(
            f"""INSERT INTO {final} (
    {analytics_columns},
    SOURCE_LOAD_BATCH_ID,
    SOURCE_VALIDATION_PROFILE,
    CREATED_AT_UTC,
    UPDATED_AT_UTC
)
SELECT
    {analytics_columns},
    LOAD_BATCH_ID,
    VALIDATION_PROFILE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM {staging}
WHERE LOAD_BATCH_ID = ?""",
            (batch_id,),
        )
        cursor.execute(
            f"""UPDATE {audit}
SET STATUS = 'SUCCEEDED',
    PUBLISHED_ROW_COUNT = (
        SELECT COUNT(*) FROM {staging} WHERE LOAD_BATCH_ID = ?
    ),
    COMPLETED_AT_UTC = CURRENT_TIMESTAMP()
WHERE LOAD_BATCH_ID = ?""",
            (batch_id, batch_id),
        )
        cursor.execute("COMMIT")
        transaction_started = False
        return SnowflakeLoadResult(
            batch_id=batch_id,
            validation_profile=profile,
            reference_start=reference_start,
            reference_end=reference_end,
            staged_row_count=staged_count,
            published_row_count=staged_count,
        )
    except Exception as error:
        if cursor is not None and transaction_started:
            try:
                cursor.execute("ROLLBACK")
            except Exception:
                # Preserve the publication error; the FAILED audit update below
                # still provides a second signal if the connection remains usable.
                pass
        if cursor is not None and audit_started:
            try:
                cursor.execute(
                    f"""UPDATE {audit}
SET STATUS = 'FAILED', COMPLETED_AT_UTC = CURRENT_TIMESTAMP(), ERROR_MESSAGE = ?
WHERE LOAD_BATCH_ID = ?""",
                    (_failure_message(error), batch_id),
                )
            except Exception:
                pass
        if isinstance(error, SnowflakeLoadError):
            raise
        raise SnowflakeLoadError(f"Snowflake load failed: {error}") from error
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
