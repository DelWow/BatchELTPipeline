"""Raw-source ingestion for Statistics Canada housing tables."""

from housing_elt.ingestion.errors import IngestionError
from housing_elt.ingestion.registry import (
    HttpPolicy,
    ProfileDefinition,
    RegistryError,
    SourceDefinition,
    SourceRegistry,
    load_source_registry,
)
from housing_elt.ingestion.statcan import (
    IngestionResult,
    StatCanIngestor,
)

__all__ = [
    "HttpPolicy",
    "IngestionError",
    "IngestionResult",
    "ProfileDefinition",
    "RegistryError",
    "SourceDefinition",
    "SourceRegistry",
    "StatCanIngestor",
    "load_source_registry",
]
