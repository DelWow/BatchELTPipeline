"""Contract-driven, idempotent ingestion from Statistics Canada."""

from __future__ import annotations

import hashlib
import json
import logging
import random
import shutil
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

import httpx

from housing_elt.ingestion.artifacts import (
    DownloadedArchive,
    build_manifest,
    content_length,
    hash_file,
    load_manifest,
    normalize_release_time,
    parse_retry_after,
    revision_hints_match,
    validate_zip,
)
from housing_elt.ingestion.errors import ArchiveIntegrityError, IngestionError
from housing_elt.ingestion.registry import (
    ProfileDefinition,
    SourceDefinition,
    SourceRegistry,
)

LOGGER = logging.getLogger(__name__)
_ALLOWED_ARCHIVE_CONTENT_TYPES = frozenset(
    {"application/octet-stream", "application/x-zip-compressed", "application/zip"}
)
_CHUNK_SIZE = 1024 * 1024


class _RetryableStatusError(IngestionError):
    def __init__(self, status_code: int, response: httpx.Response) -> None:
        super().__init__(f"retryable HTTP status {status_code}")
        self.response = response


@dataclass(frozen=True, slots=True)
class IngestionResult:
    """Observable outcome for one source ingestion."""

    source_id: str
    status: str
    archive_path: Path
    byte_count: int
    sha256: str


class StatCanIngestor:
    """Download immutable Statistics Canada snapshots from a source registry."""

    def __init__(
        self,
        registry: SourceRegistry,
        raw_data_dir: Path,
        *,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        now_utc: Callable[[], datetime] | None = None,
        random_uniform: Callable[[float, float], float] = random.uniform,
    ) -> None:
        self._registry = registry
        self._raw_data_dir = raw_data_dir
        self._sleep = sleep
        self._monotonic = monotonic
        self._now_utc = now_utc or (lambda: datetime.now(UTC))
        self._random_uniform = random_uniform
        self._timeout = httpx.Timeout(
            registry.http.read_timeout_seconds,
            connect=registry.http.connect_timeout_seconds,
        )
        self._owns_client = client is None
        self._client = client or httpx.Client(
            follow_redirects=True,
            headers={"User-Agent": "canadian-housing-elt/0.1"},
        )

    def __enter__(self) -> StatCanIngestor:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        """Close only clients created by this ingestor."""
        if self._owns_client:
            self._client.close()

    def ingest_profile(self, profile_name: str) -> tuple[IngestionResult, ...]:
        """Ingest every source selected by a reviewed profile."""
        profile = self._registry.profile(profile_name)
        return tuple(
            self.ingest_source(self._registry.source(source_id), profile)
            for source_id in profile.source_ids
        )

    def ingest_source(
        self,
        source: SourceDefinition,
        profile: ProfileDefinition,
    ) -> IngestionResult:
        """Ingest one source or return a verified idempotent no-op."""
        LOGGER.info("source=%s step=metadata status=started", source.id)
        metadata_response = self._request(
            "POST",
            self._registry.cube_metadata_api_url,
            json_body=[{"productId": int(source.product_id)}],
        )
        metadata_bytes = metadata_response.content
        metadata = self._parse_and_validate_metadata(
            metadata_response,
            source,
            profile,
        )

        link_response = self._request("GET", source.download_api_url)
        download_url, wds_status = self._parse_and_validate_download_link(
            link_response,
            source,
        )
        release_raw = str(metadata["releaseTime"])
        release_utc = normalize_release_time(release_raw)
        release_token = release_utc.strftime("%Y%m%dT%H%M%SZ")

        revision_hints = self._head_revision_hints(download_url, source)
        existing = self._find_verified_existing(
            source,
            release_token,
            revision_hints,
        )
        if existing is not None:
            LOGGER.info(
                "source=%s status=already_present sha256=%s bytes=%d",
                source.id,
                existing.sha256,
                existing.byte_count,
            )
            return existing

        started_at = self._now_utc()
        downloaded = self._download_archive(download_url, source)
        completed_at = self._now_utc()
        result = self._publish_snapshot(
            source=source,
            profile=profile,
            metadata=metadata,
            metadata_bytes=metadata_bytes,
            wds_status=wds_status,
            release_raw=release_raw,
            release_utc=release_utc,
            release_token=release_token,
            started_at=started_at,
            completed_at=completed_at,
            downloaded=downloaded,
        )
        LOGGER.info(
            "source=%s status=%s sha256=%s bytes=%d path=%s",
            source.id,
            result.status,
            result.sha256,
            result.byte_count,
            result.archive_path,
        )
        return result

    def _request(
        self,
        method: str,
        url: str,
        *,
        json_body: object | None = None,
        accepted_statuses: frozenset[int] = frozenset({200}),
    ) -> httpx.Response:
        last_error: Exception | None = None
        for attempt in range(1, self._registry.http.max_attempts + 1):
            response: httpx.Response | None = None
            try:
                response = self._client.request(
                    method,
                    url,
                    json=json_body,
                    timeout=self._timeout,
                )
                if response.status_code in accepted_statuses:
                    return response
                if response.status_code in self._registry.http.retryable_status_codes:
                    raise _RetryableStatusError(response.status_code, response)
                raise IngestionError(
                    f"{method} {url} returned non-retryable HTTP {response.status_code}"
                )
            except (httpx.RequestError, _RetryableStatusError) as error:
                last_error = error
                if attempt == self._registry.http.max_attempts:
                    break
                delay = self._retry_delay(attempt, response)
                LOGGER.warning(
                    "request=%s url=%s attempt=%d status=retrying delay=%.2fs "
                    "reason=%s",
                    method,
                    url,
                    attempt,
                    delay,
                    type(error).__name__,
                )
                self._sleep(delay)

        raise IngestionError(
            f"{method} {url} failed after {self._registry.http.max_attempts} attempts: "
            f"{last_error}"
        ) from last_error

    def _parse_and_validate_metadata(
        self,
        response: httpx.Response,
        source: SourceDefinition,
        profile: ProfileDefinition,
    ) -> dict[str, Any]:
        payload = _json_response(response, context=f"metadata for {source.id}")
        if not isinstance(payload, list) or len(payload) != 1:
            raise IngestionError(f"Metadata for {source.id} has an unexpected shape")
        wrapper = payload[0]
        if not isinstance(wrapper, dict) or wrapper.get("status") != "SUCCESS":
            raise IngestionError(f"Metadata lookup failed for {source.id}")
        metadata = wrapper.get("object")
        if not isinstance(metadata, dict):
            raise IngestionError(f"Metadata object is missing for {source.id}")
        if str(metadata.get("productId")) != source.product_id:
            raise IngestionError(f"Metadata PID mismatch for {source.id}")
        if int(metadata.get("frequencyCode", -1)) != source.frequency_code:
            raise IngestionError(f"Metadata frequency mismatch for {source.id}")
        if not str(metadata.get("archiveStatusEn", "")).startswith("CURRENT"):
            raise IngestionError(f"Source table is not current for {source.id}")

        raw_dimensions = metadata.get("dimension")
        if not isinstance(raw_dimensions, list):
            raise IngestionError(f"Metadata dimensions are missing for {source.id}")
        dimension_names = {
            str(dimension.get("dimensionNameEn"))
            for dimension in raw_dimensions
            if isinstance(dimension, dict)
        }
        missing_dimensions = set(source.dimensions) - dimension_names
        if missing_dimensions:
            missing = ", ".join(sorted(missing_dimensions))
            raise IngestionError(
                f"Metadata dimensions missing for {source.id}: {missing}"
            )

        start = str(metadata.get("cubeStartDate", ""))[:7]
        end = str(metadata.get("cubeEndDate", ""))[:7]
        requested_start = max(source.baseline_start, profile.reference_start)
        requested_end = min(source.baseline_end, profile.reference_end)
        if not start or not end or start > requested_end or end < requested_start:
            raise IngestionError(
                f"Metadata coverage {start}..{end} does not intersect "
                f"{requested_start}..{requested_end} for {source.id}"
            )
        if not metadata.get("releaseTime"):
            raise IngestionError(f"Metadata release time is missing for {source.id}")
        return metadata

    def _parse_and_validate_download_link(
        self,
        response: httpx.Response,
        source: SourceDefinition,
    ) -> tuple[str, str]:
        payload = _json_response(response, context=f"download link for {source.id}")
        if not isinstance(payload, dict) or payload.get("status") != "SUCCESS":
            raise IngestionError(f"Download-link lookup failed for {source.id}")
        download_url = payload.get("object")
        if not isinstance(download_url, str):
            raise IngestionError(f"Download URL is missing for {source.id}")

        parsed = urlparse(download_url)
        expected_filename = f"{source.product_id}-eng.zip"
        if (
            parsed.scheme != "https"
            or parsed.hostname != "www150.statcan.gc.ca"
            or PurePosixPath(parsed.path).name != expected_filename
            or download_url != source.expected_download_url
        ):
            raise IngestionError(f"Unexpected download URL for {source.id}")
        return download_url, str(payload["status"])

    def _head_revision_hints(
        self,
        download_url: str,
        source: SourceDefinition,
    ) -> dict[str, str | int | None]:
        response = self._request(
            "HEAD",
            download_url,
            accepted_statuses=frozenset({200, 405, 501}),
        )
        if response.status_code != 200:
            LOGGER.info("source=%s step=head status=unsupported", source.id)
            return {"content_length": None, "etag": None, "last_modified": None}
        response_length = content_length(response.headers.get("content-length"))
        if response_length is not None and response_length > source.max_archive_bytes:
            raise IngestionError(
                f"HEAD content length {response_length} exceeds the "
                f"{source.max_archive_bytes} byte cap for {source.id}"
            )
        return {
            "content_length": response_length,
            "etag": response.headers.get("etag"),
            "last_modified": response.headers.get("last-modified"),
        }

    def _find_verified_existing(
        self,
        source: SourceDefinition,
        release_token: str,
        revision_hints: dict[str, str | int | None],
    ) -> IngestionResult | None:
        release_dir = self._release_dir(source, release_token)
        for manifest_path in sorted(release_dir.glob("sha256=*/manifest.json")):
            manifest = load_manifest(manifest_path)
            if not revision_hints_match(manifest, revision_hints):
                continue
            return self._verify_existing_snapshot(source, manifest_path, manifest)
        return None

    def _verify_existing_snapshot(
        self,
        source: SourceDefinition,
        manifest_path: Path,
        manifest: dict[str, Any] | None = None,
    ) -> IngestionResult:
        manifest = manifest or load_manifest(manifest_path)
        try:
            artifact = manifest["artifact"]
            filename = str(artifact["filename"])
            expected_sha = str(artifact["sha256"])
            expected_bytes = int(artifact["byte_count"])
        except (KeyError, TypeError, ValueError) as error:
            raise IngestionError(f"Malformed manifest: {manifest_path}") from error
        if filename != f"{source.product_id}-eng.zip":
            raise IngestionError(f"Manifest filename mismatch: {manifest_path}")

        archive_path = manifest_path.parent / filename
        byte_count, digest = hash_file(archive_path)
        if byte_count != expected_bytes or digest != expected_sha:
            raise IngestionError(
                f"Existing raw snapshot failed checksum validation: {archive_path}"
            )
        validate_zip(archive_path, source)
        return IngestionResult(
            source_id=source.id,
            status="already_present",
            archive_path=archive_path,
            byte_count=byte_count,
            sha256=digest,
        )

    def _download_archive(
        self,
        download_url: str,
        source: SourceDefinition,
    ) -> DownloadedArchive:
        partial_root = self._raw_data_dir / ".partial"
        partial_root.mkdir(parents=True, exist_ok=True)
        overall_started = self._monotonic()
        integrity_failures = 0
        last_error: Exception | None = None

        for attempt in range(1, self._registry.http.max_attempts + 1):
            partial_path = partial_root / f"{source.id}-{uuid.uuid4()}.zip.part"
            response_for_delay: httpx.Response | None = None
            successful_attempt = False
            try:
                with self._client.stream(
                    "GET",
                    download_url,
                    timeout=self._timeout,
                ) as response:
                    response_for_delay = response
                    if (
                        response.status_code
                        in self._registry.http.retryable_status_codes
                    ):
                        raise _RetryableStatusError(response.status_code, response)
                    if response.status_code != 200:
                        raise IngestionError(
                            f"Archive GET returned non-retryable HTTP "
                            f"{response.status_code} for {source.id}"
                        )
                    downloaded = self._stream_response(
                        response,
                        partial_path,
                        source,
                        overall_started,
                    )
                members = validate_zip(partial_path, source)
                successful_attempt = True
                return DownloadedArchive(
                    path=partial_path,
                    members=members,
                    **downloaded,
                )
            except ArchiveIntegrityError as error:
                last_error = error
                integrity_failures += 1
                if (
                    integrity_failures >= 2
                    or attempt == self._registry.http.max_attempts
                ):
                    break
                self._log_and_sleep_for_retry(
                    source,
                    attempt,
                    error,
                    response_for_delay,
                )
            except (httpx.RequestError, _RetryableStatusError) as error:
                last_error = error
                if attempt == self._registry.http.max_attempts:
                    break
                self._log_and_sleep_for_retry(
                    source,
                    attempt,
                    error,
                    response_for_delay,
                )
            finally:
                if partial_path.exists() and not successful_attempt:
                    partial_path.unlink()

        raise IngestionError(
            f"Archive download failed for {source.id} after {attempt} attempts: "
            f"{last_error}"
        ) from last_error

    def _stream_response(
        self,
        response: httpx.Response,
        partial_path: Path,
        source: SourceDefinition,
        overall_started: float,
    ) -> dict[str, Any]:
        content_type = response.headers.get("content-type", "").split(";", 1)[0]
        if content_type not in _ALLOWED_ARCHIVE_CONTENT_TYPES:
            raise IngestionError(
                f"Unexpected archive content type {content_type!r} for {source.id}"
            )
        expected_length = content_length(response.headers.get("content-length"))
        if expected_length is not None and expected_length > source.max_archive_bytes:
            raise IngestionError(
                f"Archive exceeds the {source.max_archive_bytes} byte cap for {source.id}"
            )

        digest = hashlib.sha256()
        byte_count = 0
        with partial_path.open("xb") as output_file:
            for chunk in response.iter_bytes(chunk_size=_CHUNK_SIZE):
                if self._monotonic() - overall_started > (
                    self._registry.http.download_deadline_seconds
                ):
                    raise IngestionError(f"Archive deadline exceeded for {source.id}")
                byte_count += len(chunk)
                if byte_count > source.max_archive_bytes:
                    raise IngestionError(
                        f"Archive exceeds the {source.max_archive_bytes} byte cap "
                        f"for {source.id}"
                    )
                digest.update(chunk)
                output_file.write(chunk)

        if byte_count <= 0:
            raise ArchiveIntegrityError(f"Archive is empty for {source.id}")
        if expected_length is not None and byte_count != expected_length:
            raise ArchiveIntegrityError(
                f"Archive length mismatch for {source.id}: expected "
                f"{expected_length}, received {byte_count}"
            )
        return {
            "byte_count": byte_count,
            "sha256": digest.hexdigest(),
            "http_status": response.status_code,
            "content_type": content_type,
            "content_length": expected_length,
            "etag": response.headers.get("etag"),
            "last_modified": response.headers.get("last-modified"),
            "final_url": str(response.url),
        }

    def _publish_snapshot(
        self,
        *,
        source: SourceDefinition,
        profile: ProfileDefinition,
        metadata: dict[str, Any],
        metadata_bytes: bytes,
        wds_status: str,
        release_raw: str,
        release_utc: datetime,
        release_token: str,
        started_at: datetime,
        completed_at: datetime,
        downloaded: DownloadedArchive,
    ) -> IngestionResult:
        release_dir = self._release_dir(source, release_token)
        final_dir = release_dir / f"sha256={downloaded.sha256}"
        filename = f"{source.product_id}-eng.zip"
        if final_dir.exists():
            downloaded.path.unlink(missing_ok=True)
            return self._verify_existing_snapshot(source, final_dir / "manifest.json")

        partial_root = self._raw_data_dir / ".partial"
        staging_dir = partial_root / f"{source.id}-{uuid.uuid4()}.bundle.part"
        staging_dir.mkdir(parents=False)
        try:
            archive_path = staging_dir / filename
            downloaded.path.replace(archive_path)
            (staging_dir / "cube-metadata.json").write_bytes(metadata_bytes)
            manifest = build_manifest(
                contract_version=self._registry.contract_version,
                source=source,
                profile=profile,
                metadata=metadata,
                wds_status=wds_status,
                release_raw=release_raw,
                release_utc=release_utc,
                started_at=started_at,
                completed_at=completed_at,
                downloaded=downloaded,
                filename=filename,
            )
            (staging_dir / "manifest.json").write_text(
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            release_dir.mkdir(parents=True, exist_ok=True)
            try:
                staging_dir.rename(final_dir)
            except FileExistsError:
                shutil.rmtree(staging_dir)
                return self._verify_existing_snapshot(
                    source,
                    final_dir / "manifest.json",
                )
        finally:
            downloaded.path.unlink(missing_ok=True)
            if staging_dir.exists():
                shutil.rmtree(staging_dir)

        return IngestionResult(
            source_id=source.id,
            status="downloaded",
            archive_path=final_dir / filename,
            byte_count=downloaded.byte_count,
            sha256=downloaded.sha256,
        )

    def _release_dir(self, source: SourceDefinition, release_token: str) -> Path:
        return (
            self._raw_data_dir
            / self._registry.raw_subdirectory
            / source.id
            / f"release={release_token}"
        )

    def _log_and_sleep_for_retry(
        self,
        source: SourceDefinition,
        attempt: int,
        error: Exception,
        response: httpx.Response | None,
    ) -> None:
        delay = self._retry_delay(attempt, response)
        LOGGER.warning(
            "source=%s step=archive attempt=%d status=retrying delay=%.2fs reason=%s",
            source.id,
            attempt,
            delay,
            type(error).__name__,
        )
        self._sleep(delay)

    def _retry_delay(
        self,
        attempt: int,
        response: httpx.Response | None,
    ) -> float:
        if response is not None:
            retry_after = parse_retry_after(
                response.headers.get("retry-after"),
                now=self._now_utc(),
            )
            if retry_after is not None:
                return min(retry_after, self._registry.http.retry_after_cap_seconds)
        ceiling = min(
            self._registry.http.backoff_cap_seconds,
            self._registry.http.backoff_base_seconds * (2 ** (attempt - 1)),
        )
        return self._random_uniform(0, ceiling)


def _json_response(response: httpx.Response, *, context: str) -> object:
    try:
        return response.json()
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise IngestionError(f"Invalid JSON response for {context}") from error
