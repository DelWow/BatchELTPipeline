"""Typed loading for the versioned analytics validation contract."""

from __future__ import annotations

import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from housing_elt.validation.errors import ValidationContractError


@dataclass(frozen=True, slots=True)
class ValidationPolicy:
    """Quality thresholds for one reviewed execution profile."""

    profile_name: str
    min_rows: int
    max_rows: int
    key_columns: tuple[str, ...]
    allowed_dwelling_types: tuple[str, ...]
    require_complete_dwelling_set: bool
    max_duplicate_rows: int
    max_market_mismatch_fraction: float
    max_component_mismatch_fraction: float
    null_thresholds: Mapping[str, float]


@dataclass(frozen=True, slots=True)
class ValidationContract:
    """All named policies loaded from one reviewed contract file."""

    contract_version: int
    profiles: tuple[ValidationPolicy, ...]

    def profile(self, name: str) -> ValidationPolicy:
        for profile in self.profiles:
            if profile.profile_name == name:
                return profile
        available = ", ".join(sorted(item.profile_name for item in self.profiles))
        raise ValidationContractError(
            f"Unknown validation profile {name!r}; expected one of: {available}"
        )


def _required(mapping: Mapping[str, Any], key: str, *, context: str) -> Any:
    try:
        return mapping[key]
    except KeyError as error:
        raise ValidationContractError(f"Missing {context}.{key}") from error


def _fraction(value: Any, *, field: str) -> float:
    parsed = float(value)
    if not 0.0 <= parsed <= 1.0:
        raise ValidationContractError(f"{field} must be between 0 and 1")
    return parsed


def _load_policy(name: str, raw: Mapping[str, Any]) -> ValidationPolicy:
    context = f"profiles.{name}"
    min_rows = int(_required(raw, "min_rows", context=context))
    max_rows = int(_required(raw, "max_rows", context=context))
    key_columns = tuple(
        str(value) for value in _required(raw, "key_columns", context=context)
    )
    dwelling_types = tuple(
        str(value)
        for value in _required(raw, "allowed_dwelling_types", context=context)
    )
    nulls_raw = _required(raw, "null_thresholds", context=context)
    if not isinstance(nulls_raw, Mapping):
        raise ValidationContractError(f"{context}.null_thresholds must be a table")
    null_thresholds = {
        str(column): _fraction(value, field=f"{context}.null_thresholds.{column}")
        for column, value in nulls_raw.items()
    }
    policy = ValidationPolicy(
        profile_name=name,
        min_rows=min_rows,
        max_rows=max_rows,
        key_columns=key_columns,
        allowed_dwelling_types=dwelling_types,
        require_complete_dwelling_set=bool(
            _required(raw, "require_complete_dwelling_set", context=context)
        ),
        max_duplicate_rows=int(_required(raw, "max_duplicate_rows", context=context)),
        max_market_mismatch_fraction=_fraction(
            _required(raw, "max_market_mismatch_fraction", context=context),
            field=f"{context}.max_market_mismatch_fraction",
        ),
        max_component_mismatch_fraction=_fraction(
            _required(raw, "max_component_mismatch_fraction", context=context),
            field=f"{context}.max_component_mismatch_fraction",
        ),
        null_thresholds=null_thresholds,
    )
    if min_rows < 0 or max_rows < min_rows:
        raise ValidationContractError(f"{context} has an invalid row-count range")
    if policy.max_duplicate_rows < 0:
        raise ValidationContractError(
            f"{context}.max_duplicate_rows must be non-negative"
        )
    if not key_columns or len(key_columns) != len(set(key_columns)):
        raise ValidationContractError(
            f"{context}.key_columns must be unique and non-empty"
        )
    if not dwelling_types or len(dwelling_types) != len(set(dwelling_types)):
        raise ValidationContractError(
            f"{context}.allowed_dwelling_types must be unique and non-empty"
        )
    return policy


def load_validation_contract(path: Path) -> ValidationContract:
    """Load and validate a versioned TOML quality contract."""
    try:
        with path.open("rb") as contract_file:
            raw = tomllib.load(contract_file)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ValidationContractError(
            f"Could not load validation contract {path}: {error}"
        ) from error

    profiles_raw = _required(raw, "profiles", context="contract")
    if not isinstance(profiles_raw, Mapping):
        raise ValidationContractError("contract.profiles must be a table")
    profiles = tuple(
        _load_policy(str(name), values) for name, values in profiles_raw.items()
    )
    return ValidationContract(
        contract_version=int(_required(raw, "contract_version", context="contract")),
        profiles=profiles,
    )
