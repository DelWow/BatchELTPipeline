from dataclasses import FrozenInstanceError

import pytest

from housing_elt.snowflake.config import (
    SnowflakeConfigurationError,
    load_snowflake_settings,
)


def _environment() -> dict[str, str]:
    return {
        "HOUSING_ELT_SNOWFLAKE_ACCOUNT": "organization-account",
        "HOUSING_ELT_SNOWFLAKE_USER": "pipeline_user",
        "HOUSING_ELT_SNOWFLAKE_PASSWORD": "never-print-this",
        "HOUSING_ELT_SNOWFLAKE_WAREHOUSE": "housing_wh",
        "HOUSING_ELT_SNOWFLAKE_DATABASE": "housing_dev",
        "HOUSING_ELT_SNOWFLAKE_SCHEMA": "housing_analytics",
        "HOUSING_ELT_SNOWFLAKE_ROLE": "housing_loader",
    }


def test_loads_complete_environment_and_normalizes_identifiers() -> None:
    settings = load_snowflake_settings(_environment())

    assert settings.account == "organization-account"
    assert settings.namespace == "HOUSING_DEV.HOUSING_ANALYTICS"
    assert settings.role == "HOUSING_LOADER"
    assert settings.connection_parameters()["paramstyle"] == "qmark"
    assert settings.connection_parameters()["autocommit"] is True


def test_password_is_omitted_from_representations_and_safe_summary() -> None:
    settings = load_snowflake_settings(_environment())

    assert "never-print-this" not in repr(settings)
    assert "password" not in settings.safe_summary()
    assert "never-print-this" not in str(settings.safe_summary())


def test_missing_required_variable_fails_before_connection() -> None:
    environment = _environment()
    del environment["HOUSING_ELT_SNOWFLAKE_PASSWORD"]

    with pytest.raises(
        SnowflakeConfigurationError,
        match="HOUSING_ELT_SNOWFLAKE_PASSWORD",
    ):
        load_snowflake_settings(environment)


def test_rejects_identifier_that_would_require_sql_quoting() -> None:
    environment = _environment()
    environment["HOUSING_ELT_SNOWFLAKE_SCHEMA"] = "dev; DROP SCHEMA prod"

    with pytest.raises(SnowflakeConfigurationError, match="unquoted"):
        load_snowflake_settings(environment)


def test_settings_are_immutable() -> None:
    settings = load_snowflake_settings(_environment())

    with pytest.raises(FrozenInstanceError):
        settings.schema = "OTHER"  # type: ignore[misc]
