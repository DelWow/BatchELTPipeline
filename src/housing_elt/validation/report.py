"""Serializable validation results returned to orchestration and tests."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    """One failed quality gate with an actionable explanation."""

    check: str
    message: str


@dataclass(frozen=True, slots=True)
class ValidationReport:
    """All validation metrics and failures for one analytics fact."""

    profile_name: str
    row_count: int | None
    metrics: dict[str, int | float]
    issues: tuple[ValidationIssue, ...]

    @property
    def passed(self) -> bool:
        return not self.issues
