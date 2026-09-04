"""Snowflake loading errors surfaced to orchestration and the CLI."""


class SnowflakeLoadError(RuntimeError):
    """Raised when a validated fact cannot be safely published to Snowflake."""
