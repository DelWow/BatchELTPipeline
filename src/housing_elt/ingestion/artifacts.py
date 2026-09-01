"""Archive identity, integrity, and manifest helpers."""

from __future__ import annotations

import hashlib
import json
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path, PurePosixPath
from typing import Any
from zoneinfo import ZoneInfo

from housing_elt.ingestion.errors import ArchiveIntegrityError, IngestionError
from housing_elt.ingestion.registry import ProfileDefinition, SourceDefinition

_CHUNK_SIZE = 1024 * 1024


@dataclass(frozen=True, slots=True)
class DownloadedArchive:
    """Verified temporary archive plus the HTTP facts used in its manifest."""

    path: Path
    byte_count: int
    sha256: str
    members: tuple[str, ...]
    http_status: int
    content_type: str
    content_length: int | None
    etag: str | None
    last_modified: str | None
    final_url: str


def content_length(raw_value: str | None) -> int | None:
    """Parse an optional non-negative Content-Length header."""
    if raw_value is None:
        return None
    try:
        value = int(raw_value)
    except ValueError as error:
        raise IngestionError(f"Invalid Content-Length header: {raw_value!r}") from error
    if value < 0:
        raise IngestionError(f"Invalid Content-Length header: {raw_value!r}")
    return value


def normalize_release_time(raw_value: str) -> datetime:
    """Interpret timezone-free WDS release times as Toronto local time."""
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError as error:
        raise IngestionError(f"Invalid source release time: {raw_value!r}") from error
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("America/Toronto"))
    return parsed.astimezone(UTC)


def isoformat_utc(value: datetime) -> str:
    """Format an aware timestamp in a stable UTC representation."""
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_retry_after(raw_value: str | None, *, now: datetime) -> float | None:
    """Parse Retry-After seconds or an HTTP date into a non-negative delay."""
    if raw_value is None:
        return None
    try:
        return max(0.0, float(raw_value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(raw_value)
        except (TypeError, ValueError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        return max(0.0, (retry_at.astimezone(UTC) - now).total_seconds())


def validate_zip(path: Path, source: SourceDefinition) -> tuple[str, ...]:
    """Require a safe, CRC-valid ZIP containing both configured CSV members."""
    try:
        with zipfile.ZipFile(path) as archive:
            members = tuple(info.filename for info in archive.infolist())
            for member in members:
                normalized = member.replace("\\", "/")
                member_path = PurePosixPath(normalized)
                if member_path.is_absolute() or ".." in member_path.parts:
                    raise IngestionError(
                        f"Unsafe ZIP member {member!r} for {source.id}"
                    )
            bad_member = archive.testzip()
    except (OSError, zipfile.BadZipFile) as error:
        raise ArchiveIntegrityError(f"Invalid ZIP for {source.id}") from error
    if bad_member is not None:
        raise ArchiveIntegrityError(
            f"ZIP CRC failed for member {bad_member!r} in {source.id}"
        )
    missing = {source.data_member, source.metadata_member} - set(members)
    if missing:
        names = ", ".join(sorted(missing))
        raise IngestionError(f"ZIP members missing for {source.id}: {names}")
    return members


def hash_file(path: Path) -> tuple[int, str]:
    """Return the byte count and SHA-256 of an existing raw artifact."""
    digest = hashlib.sha256()
    byte_count = 0
    try:
        with path.open("rb") as source_file:
            while chunk := source_file.read(_CHUNK_SIZE):
                byte_count += len(chunk)
                digest.update(chunk)
    except OSError as error:
        raise IngestionError(f"Could not read existing snapshot: {path}") from error
    return byte_count, digest.hexdigest()


def load_manifest(path: Path) -> dict[str, Any]:
    """Load a generated manifest without accepting a non-object root."""
    try:
        with path.open(encoding="utf-8") as manifest_file:
            manifest = json.load(manifest_file)
    except (OSError, json.JSONDecodeError) as error:
        raise IngestionError(f"Could not read manifest: {path}") from error
    if not isinstance(manifest, dict):
        raise IngestionError(f"Manifest root must be an object: {path}")
    return manifest


def revision_hints_match(
    manifest: dict[str, Any],
    current: dict[str, str | int | None],
) -> bool:
    """Compare every HTTP revision hint supplied by the current response."""
    manifest_http = manifest.get("http")
    if not isinstance(manifest_http, dict):
        return False
    available_keys = [key for key, value in current.items() if value is not None]
    return bool(available_keys) and all(
        manifest_http.get(key) == current[key] for key in available_keys
    )


def build_manifest(
    *,
    contract_version: int,
    source: SourceDefinition,
    profile: ProfileDefinition,
    metadata: dict[str, Any],
    wds_status: str,
    release_raw: str,
    release_utc: datetime,
    started_at: datetime,
    completed_at: datetime,
    downloaded: DownloadedArchive,
    filename: str,
) -> dict[str, Any]:
    """Build the stable audit manifest for a completed snapshot."""
    corrections = metadata.get("correction")
    if not isinstance(corrections, list):
        corrections = []
    return {
        "artifact": {
            "byte_count": downloaded.byte_count,
            "content_type": downloaded.content_type,
            "filename": filename,
            "sha256": downloaded.sha256,
            "zip_crc_valid": True,
            "zip_members": list(downloaded.members),
        },
        "contract_version": contract_version,
        "downloader": {
            "name": "canadian-housing-elt",
            "version": "0.1.0",
        },
        "http": {
            "content_length": downloaded.content_length,
            "etag": downloaded.etag,
            "final_url": downloaded.final_url,
            "last_modified": downloaded.last_modified,
            "request_url": source.download_api_url,
            "resolved_download_url": source.expected_download_url,
            "status": downloaded.http_status,
            "wds_status": wds_status,
        },
        "retrieval": {
            "completed_at_utc": isoformat_utc(completed_at),
            "profile": profile.name,
            "started_at_utc": isoformat_utc(started_at),
        },
        "source": {
            "archive_status": metadata.get("archiveStatusEn"),
            "corrections": [
                {
                    "date": item.get("correctionDate"),
                    "id": item.get("correctionId"),
                }
                for item in corrections
                if isinstance(item, dict)
            ],
            "cube_end_date": metadata.get("cubeEndDate"),
            "cube_start_date": metadata.get("cubeStartDate"),
            "id": source.id,
            "issue_date": metadata.get("issueDate"),
            "number_of_datapoints": metadata.get("nbDatapointsCube"),
            "number_of_series": metadata.get("nbSeriesCube"),
            "product_id": source.product_id,
            "source_release_time_raw": release_raw,
            "source_release_time_utc": isoformat_utc(release_utc),
            "table_id": source.table_id,
        },
    }
