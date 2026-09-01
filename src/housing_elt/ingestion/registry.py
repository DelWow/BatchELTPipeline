"""Typed loading for the versioned Statistics Canada source registry."""

from __future__ import annotations

import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class RegistryError(ValueError):
    """Raised when the source registry violates its expected contract."""


@dataclass(frozen=True, slots=True)
class HttpPolicy:
    """Bounded HTTP behavior shared by metadata and archive requests."""

    connect_timeout_seconds: float
    read_timeout_seconds: float
    download_deadline_seconds: float
    max_attempts: int
    backoff_base_seconds: float
    backoff_cap_seconds: float
    retry_after_cap_seconds: float
    retryable_status_codes: frozenset[int]


@dataclass(frozen=True, slots=True)
class ProfileDefinition:
    """A reviewed group of sources and downstream filtering boundaries."""

    name: str
    source_ids: tuple[str, ...]
    reference_start: str
    reference_end: str
    cma_names: tuple[str, ...] = ()
    cma_codes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class SourceDefinition:
    """Stable identifiers and integrity expectations for one source table."""

    id: str
    role: str
    source_organization: str
    distributor: str
    table_id: str
    product_id: str
    title: str
    table_url: str
    download_api_url: str
    expected_download_url: str
    frequency: str
    frequency_code: int
    availability_start: str
    baseline_start: str
    baseline_end: str
    dimensions: tuple[str, ...]
    native_format: str
    data_member: str
    metadata_member: str
    max_archive_bytes: int
    licence_urls: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SourceRegistry:
    """Validated registry used by ingestion rather than hard-coded URLs."""

    contract_version: int
    provider_id: str
    language: str
    raw_subdirectory: str
    cube_metadata_api_url: str
    http: HttpPolicy
    profiles: tuple[ProfileDefinition, ...]
    sources: tuple[SourceDefinition, ...]

    def profile(self, name: str) -> ProfileDefinition:
        """Return a named profile or raise an actionable registry error."""
        for profile in self.profiles:
            if profile.name == name:
                return profile
        available = ", ".join(sorted(item.name for item in self.profiles))
        raise RegistryError(f"Unknown profile {name!r}; expected one of: {available}")

    def source(self, source_id: str) -> SourceDefinition:
        """Return a source definition by its stable source ID."""
        for source in self.sources:
            if source.id == source_id:
                return source
        raise RegistryError(f"Profile references unknown source {source_id!r}")


def _required(mapping: Mapping[str, Any], key: str) -> Any:
    try:
        return mapping[key]
    except KeyError as error:
        raise RegistryError(f"Missing required registry field: {key}") from error


def _load_http_policy(raw: Mapping[str, Any]) -> HttpPolicy:
    policy = HttpPolicy(
        connect_timeout_seconds=float(_required(raw, "connect_timeout_seconds")),
        read_timeout_seconds=float(_required(raw, "read_timeout_seconds")),
        download_deadline_seconds=float(_required(raw, "download_deadline_seconds")),
        max_attempts=int(_required(raw, "max_attempts")),
        backoff_base_seconds=float(_required(raw, "backoff_base_seconds")),
        backoff_cap_seconds=float(_required(raw, "backoff_cap_seconds")),
        retry_after_cap_seconds=float(_required(raw, "retry_after_cap_seconds")),
        retryable_status_codes=frozenset(
            int(code) for code in _required(raw, "retryable_status_codes")
        ),
    )
    positive_values = (
        policy.connect_timeout_seconds,
        policy.read_timeout_seconds,
        policy.download_deadline_seconds,
        policy.max_attempts,
        policy.backoff_base_seconds,
        policy.backoff_cap_seconds,
        policy.retry_after_cap_seconds,
    )
    if any(value <= 0 for value in positive_values):
        raise RegistryError("HTTP policy values must all be positive")
    return policy


def _load_profile(name: str, raw: Mapping[str, Any]) -> ProfileDefinition:
    source_ids = tuple(str(value) for value in _required(raw, "source_ids"))
    if not source_ids:
        raise RegistryError(f"Profile {name!r} must select at least one source")
    return ProfileDefinition(
        name=name,
        source_ids=source_ids,
        reference_start=str(_required(raw, "reference_start")),
        reference_end=str(_required(raw, "reference_end")),
        cma_names=tuple(str(value) for value in raw.get("cma_names", ())),
        cma_codes=tuple(str(value) for value in raw.get("cma_codes", ())),
    )


def _load_source(raw: Mapping[str, Any]) -> SourceDefinition:
    source = SourceDefinition(
        id=str(_required(raw, "id")),
        role=str(_required(raw, "role")),
        source_organization=str(_required(raw, "source_organization")),
        distributor=str(_required(raw, "distributor")),
        table_id=str(_required(raw, "table_id")),
        product_id=str(_required(raw, "product_id")),
        title=str(_required(raw, "title")),
        table_url=str(_required(raw, "table_url")),
        download_api_url=str(_required(raw, "download_api_url")),
        expected_download_url=str(_required(raw, "expected_download_url")),
        frequency=str(_required(raw, "frequency")),
        frequency_code=int(_required(raw, "frequency_code")),
        availability_start=str(_required(raw, "availability_start")),
        baseline_start=str(_required(raw, "baseline_start")),
        baseline_end=str(_required(raw, "baseline_end")),
        dimensions=tuple(str(value) for value in _required(raw, "dimensions")),
        native_format=str(_required(raw, "native_format")),
        data_member=str(_required(raw, "data_member")),
        metadata_member=str(_required(raw, "metadata_member")),
        max_archive_bytes=int(_required(raw, "max_archive_bytes")),
        licence_urls=tuple(str(value) for value in _required(raw, "licence_urls")),
    )
    if not source.product_id.isdigit():
        raise RegistryError(f"Source {source.id!r} has a non-numeric product ID")
    if source.max_archive_bytes <= 0:
        raise RegistryError(f"Source {source.id!r} has an invalid archive size cap")
    if source.baseline_start > source.baseline_end:
        raise RegistryError(f"Source {source.id!r} has an inverted baseline window")
    return source


def load_source_registry(path: Path) -> SourceRegistry:
    """Load and validate the TOML registry at ``path``."""
    try:
        with path.open("rb") as registry_file:
            raw = tomllib.load(registry_file)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise RegistryError(
            f"Could not load source registry {path}: {error}"
        ) from error

    profiles_raw = _required(raw, "profiles")
    sources_raw = _required(raw, "sources")
    if not isinstance(profiles_raw, Mapping) or not isinstance(sources_raw, list):
        raise RegistryError("Registry profiles or sources have the wrong type")

    profiles = tuple(
        _load_profile(str(name), values) for name, values in profiles_raw.items()
    )
    sources = tuple(_load_source(values) for values in sources_raw)

    source_ids = [source.id for source in sources]
    product_ids = [source.product_id for source in sources]
    if len(source_ids) != len(set(source_ids)):
        raise RegistryError("Source IDs must be unique")
    if len(product_ids) != len(set(product_ids)):
        raise RegistryError("Product IDs must be unique")

    known_source_ids = set(source_ids)
    for profile in profiles:
        unknown = set(profile.source_ids) - known_source_ids
        if unknown:
            names = ", ".join(sorted(unknown))
            raise RegistryError(
                f"Profile {profile.name!r} has unknown sources: {names}"
            )

    return SourceRegistry(
        contract_version=int(_required(raw, "contract_version")),
        provider_id=str(_required(raw, "provider_id")),
        language=str(_required(raw, "language")),
        raw_subdirectory=str(_required(raw, "raw_subdirectory")),
        cube_metadata_api_url=str(_required(raw, "cube_metadata_api_url")),
        http=_load_http_policy(_required(raw, "http")),
        profiles=profiles,
        sources=sources,
    )
