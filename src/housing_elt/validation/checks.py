"""Fail-closed schema, completeness, uniqueness, and reconciliation checks."""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from housing_elt.validation.config import ValidationPolicy
from housing_elt.validation.errors import DataValidationError
from housing_elt.validation.report import ValidationIssue, ValidationReport

# Essential contract columns are type-checked exactly. Additional analytics
# columns may be added compatibly without forcing an unrelated contract update.
EXPECTED_COLUMN_TYPES: Mapping[str, str] = {
    "reference_month": "date",
    "reference_year": "int",
    "cma_code": "string",
    "geography": "string",
    "dwelling_type": "string",
    "housing_starts": "bigint",
    "housing_completions": "bigint",
    "housing_under_construction": "bigint",
    "market_starts_total": "bigint",
    "new_housing_price_index": "decimal(18,4)",
    "residential_permit_value_dollars": "decimal(24,4)",
    "has_activity_data": "boolean",
    "has_market_data": "boolean",
    "has_price_index_data": "boolean",
    "has_permit_data": "boolean",
    "starts_anomaly_flag": "boolean",
}


def _schema_issues(fact: DataFrame, policy: ValidationPolicy) -> list[ValidationIssue]:
    actual = {field.name: field.dataType.simpleString() for field in fact.schema.fields}
    required_columns = (
        set(EXPECTED_COLUMN_TYPES)
        | set(policy.key_columns)
        | set(policy.null_thresholds)
    )
    issues: list[ValidationIssue] = []
    for column_name in sorted(required_columns):
        if column_name not in actual:
            issues.append(
                ValidationIssue("schema", f"missing required column {column_name!r}")
            )
            continue
        expected_type = EXPECTED_COLUMN_TYPES.get(column_name)
        if expected_type is not None and actual[column_name] != expected_type:
            issues.append(
                ValidationIssue(
                    "schema",
                    f"column {column_name!r} has type {actual[column_name]!r}; "
                    f"expected {expected_type!r}",
                )
            )
    return issues


def _fraction(numerator: int, denominator: int) -> float:
    return 0.0 if denominator == 0 else numerator / denominator


def _base_metrics(
    fact: DataFrame, policy: ValidationPolicy
) -> tuple[dict[str, int | float], list[ValidationIssue]]:
    null_columns = tuple(dict.fromkeys((*policy.key_columns, *policy.null_thresholds)))
    allowed = policy.allowed_dwelling_types
    expressions = [
        F.count("*").alias("row_count"),
        F.countDistinct("reference_year").alias("distinct_years"),
    ]
    expressions.extend(
        F.sum(F.when(F.col(column_name).isNull(), 1).otherwise(0)).alias(
            f"null__{column_name}"
        )
        for column_name in null_columns
    )
    expressions.extend(
        [
            F.sum(
                F.when(
                    F.col("housing_starts").isNotNull()
                    & F.col("market_starts_total").isNotNull(),
                    1,
                ).otherwise(0)
            ).alias("market_eligible"),
            F.sum(
                F.when(
                    F.col("housing_starts").isNotNull()
                    & F.col("market_starts_total").isNotNull()
                    & (F.col("housing_starts") != F.col("market_starts_total")),
                    1,
                ).otherwise(0)
            ).alias("market_mismatches"),
            F.sum(
                F.when(
                    F.col("reference_year") != F.year("reference_month"), 1
                ).otherwise(0)
            ).alias("reference_year_mismatches"),
            F.sum(F.when(~F.col("dwelling_type").isin(*allowed), 1).otherwise(0)).alias(
                "unexpected_dwelling_rows"
            ),
            F.sum(
                F.when(F.col("starts_anomaly_flag") == F.lit(True), 1).otherwise(0)
            ).alias("anomaly_rows"),
            F.sum(
                F.when(
                    F.col("has_activity_data") & ~F.col("has_market_data"), 1
                ).otherwise(0)
            ).alias("activity_only_rows"),
            F.sum(
                F.when(
                    ~F.col("has_activity_data") & F.col("has_market_data"), 1
                ).otherwise(0)
            ).alias("market_only_rows"),
            F.sum(F.when(~F.col("has_price_index_data"), 1).otherwise(0)).alias(
                "missing_price_rows"
            ),
            F.sum(F.when(~F.col("has_permit_data"), 1).otherwise(0)).alias(
                "missing_permit_rows"
            ),
        ]
    )
    result = fact.agg(*expressions).first()
    row_count = int(result.row_count)
    metrics: dict[str, int | float] = {
        "row_count": row_count,
        "distinct_years": int(result.distinct_years),
    }
    issues: list[ValidationIssue] = []

    if not policy.min_rows <= row_count <= policy.max_rows:
        issues.append(
            ValidationIssue(
                "row_count",
                f"observed {row_count}; expected [{policy.min_rows}, {policy.max_rows}]",
            )
        )

    for column_name in policy.key_columns:
        null_count = int(result[f"null__{column_name}"])
        metrics[f"null_count.{column_name}"] = null_count
        if null_count:
            issues.append(
                ValidationIssue(
                    "key_nulls",
                    f"key column {column_name!r} contains {null_count} null row(s)",
                )
            )

    for column_name, threshold in policy.null_thresholds.items():
        null_count = int(result[f"null__{column_name}"])
        null_fraction = _fraction(null_count, row_count)
        metrics[f"null_fraction.{column_name}"] = null_fraction
        if null_fraction > threshold:
            issues.append(
                ValidationIssue(
                    "null_threshold",
                    f"column {column_name!r} null fraction {null_fraction:.6f} "
                    f"exceeds {threshold:.6f}",
                )
            )

    market_eligible = int(result.market_eligible)
    market_mismatches = int(result.market_mismatches)
    market_fraction = _fraction(market_mismatches, market_eligible)
    metrics["market_reconciliation_eligible_rows"] = market_eligible
    metrics["market_reconciliation_mismatches"] = market_mismatches
    metrics["market_reconciliation_mismatch_fraction"] = market_fraction
    if row_count and market_eligible == 0:
        issues.append(
            ValidationIssue(
                "market_reconciliation",
                "no rows contain both activity and intended-market starts",
            )
        )
    elif market_fraction > policy.max_market_mismatch_fraction:
        issues.append(
            ValidationIssue(
                "market_reconciliation",
                f"mismatch fraction {market_fraction:.6f} exceeds "
                f"{policy.max_market_mismatch_fraction:.6f}",
            )
        )

    year_mismatches = int(result.reference_year_mismatches)
    metrics["reference_year_mismatches"] = year_mismatches
    if year_mismatches:
        issues.append(
            ValidationIssue(
                "reference_year",
                f"{year_mismatches} row(s) disagree with reference_month",
            )
        )

    unexpected = int(result.unexpected_dwelling_rows)
    metrics["unexpected_dwelling_rows"] = unexpected
    if unexpected:
        issues.append(
            ValidationIssue(
                "dwelling_domain",
                f"{unexpected} row(s) have an unreviewed dwelling type",
            )
        )
    metrics.update(
        {
            "anomaly_rows": int(result.anomaly_rows),
            "activity_only_rows": int(result.activity_only_rows),
            "market_only_rows": int(result.market_only_rows),
            "missing_price_rows": int(result.missing_price_rows),
            "missing_permit_rows": int(result.missing_permit_rows),
        }
    )
    return metrics, issues


def _duplicate_metrics(
    fact: DataFrame, policy: ValidationPolicy
) -> tuple[dict[str, int], list[ValidationIssue]]:
    duplicate_result = (
        fact.groupBy(*policy.key_columns)
        .count()
        .filter(F.col("count") > 1)
        .agg(F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("excess"))
        .first()
    )
    duplicate_rows = int(duplicate_result.excess)
    issues = []
    if duplicate_rows > policy.max_duplicate_rows:
        issues.append(
            ValidationIssue(
                "duplicate_keys",
                f"observed {duplicate_rows} excess duplicate row(s); "
                f"maximum is {policy.max_duplicate_rows}",
            )
        )
    return {"duplicate_rows": duplicate_rows}, issues


def _dwelling_completeness_metrics(
    fact: DataFrame, policy: ValidationPolicy
) -> tuple[dict[str, int], list[ValidationIssue]]:
    if not policy.require_complete_dwelling_set:
        return {"incomplete_dwelling_groups": 0}, []
    expected_count = len(policy.allowed_dwelling_types)
    incomplete_groups = (
        fact.groupBy("reference_month", "cma_code")
        .agg(F.countDistinct("dwelling_type").alias("dwelling_count"))
        .filter(F.col("dwelling_count") != expected_count)
        .count()
    )
    issues = []
    if incomplete_groups:
        issues.append(
            ValidationIssue(
                "dwelling_completeness",
                f"{incomplete_groups} CMA/month group(s) do not contain exactly "
                f"{expected_count} dwelling types",
            )
        )
    return {"incomplete_dwelling_groups": incomplete_groups}, issues


def _component_reconciliation_metrics(
    fact: DataFrame, policy: ValidationPolicy
) -> tuple[dict[str, int | float], list[ValidationIssue]]:
    component_types = tuple(
        value for value in policy.allowed_dwelling_types if value != "total"
    )
    measure_columns = (
        "housing_starts",
        "housing_completions",
        "housing_under_construction",
    )
    expressions = [
        F.countDistinct(
            F.when(
                F.col("dwelling_type").isin(*component_types),
                F.col("dwelling_type"),
            )
        ).alias("component_count")
    ]
    for measure in measure_columns:
        expressions.extend(
            [
                F.max(F.when(F.col("dwelling_type") == "total", F.col(measure))).alias(
                    f"total__{measure}"
                ),
                F.sum(
                    F.when(
                        F.col("dwelling_type").isin(*component_types),
                        F.col(measure),
                    )
                ).alias(f"components__{measure}"),
                F.count(
                    F.when(
                        F.col("dwelling_type").isin(*component_types),
                        F.col(measure),
                    )
                ).alias(f"component_values__{measure}"),
            ]
        )
    groups = fact.groupBy("reference_month", "cma_code").agg(*expressions)
    eligible = F.lit(0)
    mismatches = F.lit(0)
    for measure in measure_columns:
        is_eligible = (
            (F.col("component_count") == len(component_types))
            & (F.col(f"component_values__{measure}") == len(component_types))
            & F.col(f"total__{measure}").isNotNull()
        )
        eligible = eligible + F.when(is_eligible, 1).otherwise(0)
        mismatches = mismatches + F.when(
            is_eligible
            & (F.col(f"total__{measure}") != F.col(f"components__{measure}")),
            1,
        ).otherwise(0)
    result = groups.agg(
        F.sum(eligible).alias("eligible"), F.sum(mismatches).alias("mismatches")
    ).first()
    eligible_count = int(result.eligible or 0)
    mismatch_count = int(result.mismatches or 0)
    mismatch_fraction = _fraction(mismatch_count, eligible_count)
    metrics: dict[str, int | float] = {
        "component_reconciliation_eligible_comparisons": eligible_count,
        "component_reconciliation_mismatches": mismatch_count,
        "component_reconciliation_mismatch_fraction": mismatch_fraction,
    }
    issues = []
    if fact.take(1) and eligible_count == 0:
        issues.append(
            ValidationIssue(
                "component_reconciliation",
                "no complete total-versus-component groups were eligible",
            )
        )
    elif mismatch_fraction > policy.max_component_mismatch_fraction:
        issues.append(
            ValidationIssue(
                "component_reconciliation",
                f"mismatch fraction {mismatch_fraction:.6f} exceeds "
                f"{policy.max_component_mismatch_fraction:.6f}",
            )
        )
    return metrics, issues


def collect_validation_report(
    fact: DataFrame, policy: ValidationPolicy
) -> ValidationReport:
    """Execute all applicable checks and return every failure together."""
    schema_issues = _schema_issues(fact, policy)
    if schema_issues:
        return ValidationReport(
            profile_name=policy.profile_name,
            row_count=None,
            metrics={},
            issues=tuple(schema_issues),
        )

    metrics, issues = _base_metrics(fact, policy)
    for check in (
        _duplicate_metrics,
        _dwelling_completeness_metrics,
        _component_reconciliation_metrics,
    ):
        additional_metrics, additional_issues = check(fact, policy)
        metrics.update(additional_metrics)
        issues.extend(additional_issues)
    return ValidationReport(
        profile_name=policy.profile_name,
        row_count=int(metrics["row_count"]),
        metrics=metrics,
        issues=tuple(issues),
    )


def validate_analytics_fact(
    fact: DataFrame, policy: ValidationPolicy
) -> ValidationReport:
    """Return a passing report or raise one error containing all failures."""
    report = collect_validation_report(fact, policy)
    if not report.passed:
        raise DataValidationError(report)
    return report
