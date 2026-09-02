"""Errors raised while building or writing analytics-ready facts."""


class AnalyticsError(RuntimeError):
    """Raised when clean inputs cannot satisfy the analytics contract."""
