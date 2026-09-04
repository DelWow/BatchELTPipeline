"""Analytics data-quality policies, reports, and fail-closed validation."""

from housing_elt.validation.checks import validate_analytics_fact
from housing_elt.validation.config import ValidationPolicy, load_validation_contract
from housing_elt.validation.errors import DataValidationError

__all__ = [
    "DataValidationError",
    "ValidationPolicy",
    "load_validation_contract",
    "validate_analytics_fact",
]
