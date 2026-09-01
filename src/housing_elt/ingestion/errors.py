"""Domain errors shared by ingestion components."""


class IngestionError(RuntimeError):
    """Raised when a source cannot be ingested without violating the contract."""


class ArchiveIntegrityError(IngestionError):
    """An integrity failure that may be caused by a truncated transfer."""
