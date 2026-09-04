"""Validation-specific exceptions."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from housing_elt.validation.report import ValidationReport


class ValidationContractError(ValueError):
    """Raised when the versioned validation policy is malformed."""


class DataValidationError(RuntimeError):
    """Raised when an analytics fact fails one or more quality gates."""

    def __init__(self, report: ValidationReport) -> None:
        self.report = report
        details = "; ".join(
            f"{issue.check}: {issue.message}" for issue in report.issues
        )
        super().__init__(
            f"Analytics validation failed with {len(report.issues)} issue(s): {details}"
        )
