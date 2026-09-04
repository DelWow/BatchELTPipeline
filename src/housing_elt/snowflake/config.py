"""Snowflake connection settings loaded only from process environment variables."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field

SNOWFLAKE_ENV_PREFIX = "HOUSING_ELT_SNOWFLAKE_"
_UNQUOTED_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


class SnowflakeConfigurationError(ValueError):
    """Raised before connection when Snowflake settings are missing or unsafe."""


@dataclass(frozen=True, slots=True)
class SnowflakeSettings:
    """One explicit Snowflake target and its login credentials.

    Password authentication keeps this portfolio implementation easy to run in
    a trial account. The password field is excluded from ``repr`` so an error or
    debug statement cannot disclose it accidentally. Kubernetes will inject
    these same variables from a Secret in Phase 12.
    """

    account: str
    user: str
    password: str = field(repr=False)
    warehouse: str
    database: str
    schema: str
    role: str | None = None

    @property
    def namespace(self) -> str:
        """Return the validated, fully qualified SQL namespace."""
        return f"{self.database}.{self.schema}"

    def connection_parameters(self) -> dict[str, str | bool]:
        """Build connector parameters without logging or serializing them."""
        parameters: dict[str, str | bool] = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
            # qmark keeps values out of SQL text and enables server-side batch
            # binding. Explicit SQL transactions are used with autocommit on.
            "paramstyle": "qmark",
            "autocommit": True,
            "application": "CANADIAN_HOUSING_ELT",
        }
        if self.role is not None:
            parameters["role"] = self.role
        return parameters

    def safe_summary(self) -> dict[str, str | None]:
        """Return connection metadata that is safe to print; omit all secrets."""
        return {
            "account": self.account,
            "user": self.user,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
            "role": self.role,
        }


def _required(values: Mapping[str, str], suffix: str) -> str:
    variable = f"{SNOWFLAKE_ENV_PREFIX}{suffix}"
    value = values.get(variable)
    if value is None or not value.strip():
        raise SnowflakeConfigurationError(
            f"Missing required Snowflake environment variable {variable}"
        )
    # Passwords can intentionally contain leading or trailing whitespace.
    return value if suffix == "PASSWORD" else value.strip()


def _identifier(variable: str, value: str) -> str:
    """Restrict composed object names to ordinary unquoted identifiers.

    Values cannot be bound where SQL expects an identifier. Rejecting quoted or
    punctuated names makes fully qualified table composition injection-safe and
    keeps the target naming convention easy to explain.
    """
    if not _UNQUOTED_IDENTIFIER.fullmatch(value):
        raise SnowflakeConfigurationError(
            f"{variable} must be an unquoted Snowflake identifier; got {value!r}"
        )
    return value.upper()


def load_snowflake_settings(
    environ: Mapping[str, str] | None = None,
) -> SnowflakeSettings:
    """Load a complete target from environment variables without connecting."""
    values = os.environ if environ is None else environ
    account = _required(values, "ACCOUNT")
    user = _required(values, "USER")
    password = _required(values, "PASSWORD")
    warehouse = _identifier(
        f"{SNOWFLAKE_ENV_PREFIX}WAREHOUSE", _required(values, "WAREHOUSE")
    )
    database = _identifier(
        f"{SNOWFLAKE_ENV_PREFIX}DATABASE", _required(values, "DATABASE")
    )
    schema = _identifier(f"{SNOWFLAKE_ENV_PREFIX}SCHEMA", _required(values, "SCHEMA"))
    role_value = values.get(f"{SNOWFLAKE_ENV_PREFIX}ROLE")
    role = None
    if role_value is not None and role_value.strip():
        role = _identifier(f"{SNOWFLAKE_ENV_PREFIX}ROLE", role_value.strip())

    return SnowflakeSettings(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
    )
