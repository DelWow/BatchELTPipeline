"""Errors raised while discovering, reading, or cleaning source observations."""


class TransformationError(RuntimeError):
    """Raised when raw data cannot satisfy the transformation contract."""
