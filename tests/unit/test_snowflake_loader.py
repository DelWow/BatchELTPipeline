from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pytest

from housing_elt.snowflake.config import SnowflakeSettings
from housing_elt.snowflake.errors import SnowflakeLoadError
from housing_elt.snowflake.loader import ANALYTICS_COLUMNS, load_analytics_fact
from housing_elt.validation.report import ValidationIssue, ValidationReport


class FakeFact:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.columns = list(ANALYTICS_COLUMNS)
        self.selected_columns: tuple[str, ...] | None = None

    def select(self, *columns: str) -> FakeFact:
        self.selected_columns = columns
        return self

    def toLocalIterator(self, *, prefetchPartitions: bool):  # noqa: N802
        assert prefetchPartitions is False
        yield from self.rows


class FakeCursor:
    def __init__(
        self,
        fetches: list[tuple[Any, ...]],
        *,
        fail_on: str | None = None,
    ) -> None:
        self.fetches = iter(fetches)
        self.fail_on = fail_on
        self.executed: list[tuple[str, tuple[Any, ...] | None]] = []
        self.many: list[tuple[str, list[tuple[Any, ...]]]] = []
        self.closed = False

    def execute(self, sql: str, parameters: tuple[Any, ...] | None = None):
        normalized = " ".join(sql.split())
        self.executed.append((normalized, parameters))
        if self.fail_on is not None and self.fail_on in normalized:
            self.fail_on = None
            raise RuntimeError("simulated statement failure")
        return self

    def executemany(self, sql: str, rows: list[tuple[Any, ...]]):
        self.many.append((" ".join(sql.split()), rows))
        return self

    def fetchone(self) -> tuple[Any, ...]:
        return next(self.fetches)

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self.fake_cursor = cursor
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self.fake_cursor

    def close(self) -> None:
        self.closed = True


def _settings() -> SnowflakeSettings:
    return SnowflakeSettings(
        account="organization-account",
        user="loader",
        password="secret",
        warehouse="HOUSING_WH",
        database="HOUSING_DEV",
        schema="HOUSING_ANALYTICS",
        role="HOUSING_LOADER",
    )


def _report(*, passed: bool = True) -> ValidationReport:
    issues = () if passed else (ValidationIssue("row_count", "too few rows"),)
    return ValidationReport(
        profile_name="development",
        row_count=2,
        metrics={"row_count": 2, "duplicate_rows": 0},
        issues=issues,
    )


def _row(reference_month: date, cma_code: str) -> dict[str, Any]:
    row = dict.fromkeys(ANALYTICS_COLUMNS)
    row.update(
        {
            "reference_month": reference_month,
            "reference_year": reference_month.year,
            "cma_code": cma_code,
            "dwelling_type": "total",
            "geography": "Example CMA",
            "has_activity_data": True,
            "has_market_data": True,
            "has_complete_activity": True,
            "has_complete_market_breakdown": True,
            "has_price_index_data": False,
            "has_complete_price_index": False,
            "has_permit_data": False,
            "has_12_month_anomaly_baseline": False,
            "activity_release_timestamp": datetime(2026, 1, 1, 12, 30),
        }
    )
    return row


def _fact() -> FakeFact:
    return FakeFact(
        [
            _row(date(2024, 1, 1), "001"),
            _row(date(2024, 2, 1), "001"),
        ]
    )


def _connection(*, fail_on: str | None = None, staged_count: int = 2) -> FakeConnection:
    return FakeConnection(
        FakeCursor(
            [
                (0,),
                (staged_count, staged_count, date(2024, 1, 1), date(2024, 2, 1)),
            ],
            fail_on=fail_on,
        )
    )


def test_loads_in_bounded_batches_and_commits_scope_replacement() -> None:
    fact = _fact()
    connection = _connection()
    connection_parameters: dict[str, Any] = {}

    def connect(**parameters: Any) -> FakeConnection:
        connection_parameters.update(parameters)
        return connection

    result = load_analytics_fact(
        fact,  # type: ignore[arg-type]
        _report(),
        _settings(),
        batch_size=1,
        connect=connect,
    )

    cursor = connection.fake_cursor
    assert fact.selected_columns == ANALYTICS_COLUMNS
    assert len(cursor.many) == 2
    assert all(len(rows) == 1 for _, rows in cursor.many)
    assert cursor.many[0][1][0][2][0] == "TIMESTAMP_TZ"
    timestamp_position = 3 + ANALYTICS_COLUMNS.index("activity_release_timestamp")
    assert cursor.many[0][1][0][timestamp_position] == (
        "TIMESTAMP_NTZ",
        datetime(2026, 1, 1, 12, 30),
    )
    statements = [sql for sql, _ in cursor.executed]
    begin = statements.index("BEGIN TRANSACTION")
    delete = next(
        index
        for index, sql in enumerate(statements)
        if "DELETE FROM HOUSING_DEV.HOUSING_ANALYTICS.FCT_HOUSING_MONTHLY" in sql
    )
    insert = next(
        index
        for index, sql in enumerate(statements)
        if "INSERT INTO HOUSING_DEV.HOUSING_ANALYTICS.FCT_HOUSING_MONTHLY" in sql
    )
    commit = statements.index("COMMIT")
    assert begin < delete < insert < commit
    assert result.staged_row_count == result.published_row_count == 2
    assert connection_parameters["paramstyle"] == "qmark"
    assert connection_parameters["autocommit"] is True
    assert cursor.closed and connection.closed


def test_retry_repeats_delete_before_insert_for_idempotent_snapshot() -> None:
    statement_runs: list[list[str]] = []
    for _ in range(2):
        connection = _connection()
        load_analytics_fact(
            _fact(),  # type: ignore[arg-type]
            _report(),
            _settings(),
            connect=lambda _connection=connection, **_parameters: _connection,
        )
        statement_runs.append([sql for sql, _ in connection.fake_cursor.executed])

    for statements in statement_runs:
        final_delete = next(
            i
            for i, sql in enumerate(statements)
            if sql.startswith(
                "DELETE FROM HOUSING_DEV.HOUSING_ANALYTICS.FCT_HOUSING_MONTHLY"
            )
        )
        final_insert = next(
            i
            for i, sql in enumerate(statements)
            if sql.startswith(
                "INSERT INTO HOUSING_DEV.HOUSING_ANALYTICS.FCT_HOUSING_MONTHLY"
            )
        )
        assert final_delete < final_insert


def test_failed_validation_never_creates_connection() -> None:
    calls = 0

    def connect(**_parameters: Any) -> FakeConnection:
        nonlocal calls
        calls += 1
        return _connection()

    with pytest.raises(SnowflakeLoadError, match="without passed validation"):
        load_analytics_fact(
            _fact(),  # type: ignore[arg-type]
            _report(passed=False),
            _settings(),
            connect=connect,
        )

    assert calls == 0


def test_staging_reconciliation_failure_never_starts_publication() -> None:
    connection = _connection(staged_count=1)

    with pytest.raises(SnowflakeLoadError, match="reconciliation failed"):
        load_analytics_fact(
            _fact(),  # type: ignore[arg-type]
            _report(),
            _settings(),
            connect=lambda **_parameters: connection,
        )

    statements = [sql for sql, _ in connection.fake_cursor.executed]
    assert "BEGIN TRANSACTION" not in statements
    assert any("SET STATUS = 'FAILED'" in sql for sql in statements)


def test_publication_error_rolls_back_then_records_failed_audit() -> None:
    connection = _connection(fail_on="INSERT INTO HOUSING_DEV.HOUSING_ANALYTICS.FCT")

    with pytest.raises(SnowflakeLoadError, match="simulated statement failure"):
        load_analytics_fact(
            _fact(),  # type: ignore[arg-type]
            _report(),
            _settings(),
            connect=lambda **_parameters: connection,
        )

    statements = [sql for sql, _ in connection.fake_cursor.executed]
    rollback = statements.index("ROLLBACK")
    failed_audit = next(
        index for index, sql in enumerate(statements) if "SET STATUS = 'FAILED'" in sql
    )
    assert rollback < failed_audit
