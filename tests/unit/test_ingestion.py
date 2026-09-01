from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import httpx
import pytest

from housing_elt.ingestion.registry import (
    SourceDefinition,
    SourceRegistry,
    load_source_registry,
)
from housing_elt.ingestion.statcan import IngestionError, StatCanIngestor

REGISTRY_PATH = Path(__file__).resolve().parents[2] / "config/sources.toml"


def make_zip(source: SourceDefinition) -> bytes:
    output = BytesIO()
    with ZipFile(output, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr(source.data_member, "REF_DATE,GEO,VALUE\n2025-01,Test,1\n")
        archive.writestr(source.metadata_member, "metadata\n")
    return output.getvalue()


class MockStatCanServer:
    def __init__(
        self,
        registry: SourceRegistry,
        source: SourceDefinition,
        archive: bytes,
        *,
        archive_failures: int = 0,
        download_url: str | None = None,
    ) -> None:
        self.registry = registry
        self.source = source
        self.archive = archive
        self.archive_failures = archive_failures
        self.download_url = download_url or source.expected_download_url
        self.calls: Counter[str] = Counter()

    def __call__(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if request.method == "POST" and url == self.registry.cube_metadata_api_url:
            self.calls["metadata"] += 1
            return httpx.Response(
                200,
                json=[
                    {
                        "status": "SUCCESS",
                        "object": {
                            "archiveStatusEn": "CURRENT - test fixture",
                            "correction": [],
                            "cubeEndDate": "2026-07-01",
                            "cubeStartDate": "1972-01-01",
                            "dimension": [
                                {"dimensionNameEn": name}
                                for name in self.source.dimensions
                            ],
                            "frequencyCode": self.source.frequency_code,
                            "issueDate": "2018-06-27",
                            "nbDatapointsCube": 10,
                            "nbSeriesCube": 2,
                            "productId": self.source.product_id,
                            "releaseTime": "2026-08-19T08:30",
                        },
                    }
                ],
                request=request,
            )
        if request.method == "GET" and url == self.source.download_api_url:
            self.calls["download_link"] += 1
            return httpx.Response(
                200,
                json={"status": "SUCCESS", "object": self.download_url},
                request=request,
            )
        if request.method == "HEAD" and url == self.source.expected_download_url:
            self.calls["archive_head"] += 1
            return httpx.Response(
                200,
                headers=self.archive_headers,
                request=request,
            )
        if request.method == "GET" and url == self.source.expected_download_url:
            self.calls["archive_get"] += 1
            if self.calls["archive_get"] <= self.archive_failures:
                return httpx.Response(
                    503,
                    headers={"Retry-After": "0"},
                    request=request,
                )
            return httpx.Response(
                200,
                content=self.archive,
                headers=self.archive_headers,
                request=request,
            )
        raise AssertionError(f"Unexpected request: {request.method} {url}")

    @property
    def archive_headers(self) -> dict[str, str]:
        return {
            "Content-Length": str(len(self.archive)),
            "Content-Type": "application/zip",
            "ETag": '"fixture-etag"',
            "Last-Modified": "Wed, 19 Aug 2026 12:31:07 GMT",
        }


@pytest.fixture
def registry() -> SourceRegistry:
    return load_source_registry(REGISTRY_PATH)


def run_ingestion(
    registry: SourceRegistry,
    server: MockStatCanServer,
    raw_data_dir: Path,
    *,
    sleeps: list[float] | None = None,
) -> tuple[StatCanIngestor, httpx.Client]:
    client = httpx.Client(
        transport=httpx.MockTransport(server),
        follow_redirects=True,
    )
    ingestor = StatCanIngestor(
        registry,
        raw_data_dir,
        client=client,
        sleep=(sleeps if sleeps is not None else []).append,
        now_utc=lambda: datetime(2026, 8, 31, 18, 0, tzinfo=UTC),
        random_uniform=lambda _start, _end: 0,
    )
    return ingestor, client


def test_ingestion_publishes_manifest_and_rerun_is_idempotent(
    tmp_path: Path,
    registry: SourceRegistry,
) -> None:
    source = registry.source("cmhc_housing_activity")
    archive = make_zip(source)
    server = MockStatCanServer(registry, source, archive)
    ingestor, client = run_ingestion(registry, server, tmp_path / "raw")

    with client:
        first = ingestor.ingest_source(source, registry.profile("development"))
        second = ingestor.ingest_source(source, registry.profile("development"))

    assert first.status == "downloaded"
    assert second.status == "already_present"
    assert first.archive_path == second.archive_path
    assert first.archive_path.read_bytes() == archive
    assert server.calls["archive_get"] == 1

    manifest = json.loads((first.archive_path.parent / "manifest.json").read_text())
    metadata = json.loads(
        (first.archive_path.parent / "cube-metadata.json").read_text()
    )
    assert manifest["artifact"]["sha256"] == first.sha256
    assert manifest["artifact"]["zip_crc_valid"] is True
    assert manifest["retrieval"]["profile"] == "development"
    assert manifest["source"]["source_release_time_utc"] == "2026-08-19T12:30:00Z"
    assert metadata[0]["object"]["productId"] == source.product_id
    assert not list((tmp_path / "raw/.partial").glob("*.part"))


def test_ingestion_retries_transient_archive_status(
    tmp_path: Path,
    registry: SourceRegistry,
) -> None:
    source = registry.source("cmhc_housing_activity")
    server = MockStatCanServer(
        registry,
        source,
        make_zip(source),
        archive_failures=1,
    )
    sleeps: list[float] = []
    ingestor, client = run_ingestion(
        registry,
        server,
        tmp_path / "raw",
        sleeps=sleeps,
    )

    with client:
        result = ingestor.ingest_source(source, registry.profile("development"))

    assert result.status == "downloaded"
    assert server.calls["archive_get"] == 2
    assert sleeps == [0]


def test_ingestion_cleans_partial_files_after_invalid_zip(
    tmp_path: Path,
    registry: SourceRegistry,
) -> None:
    source = registry.source("cmhc_housing_activity")
    server = MockStatCanServer(registry, source, b"not a zip archive")
    ingestor, client = run_ingestion(registry, server, tmp_path / "raw")

    with client, pytest.raises(IngestionError, match="Archive download failed"):
        ingestor.ingest_source(source, registry.profile("development"))

    assert server.calls["archive_get"] == 2
    assert not list((tmp_path / "raw/.partial").glob("*.part"))
    assert not list((tmp_path / "raw").glob("statcan/**/manifest.json"))


def test_ingestion_rejects_download_link_on_unexpected_host(
    tmp_path: Path,
    registry: SourceRegistry,
) -> None:
    source = registry.source("cmhc_housing_activity")
    server = MockStatCanServer(
        registry,
        source,
        make_zip(source),
        download_url="https://example.com/34100154-eng.zip",
    )
    ingestor, client = run_ingestion(registry, server, tmp_path / "raw")

    with client, pytest.raises(IngestionError, match="Unexpected download URL"):
        ingestor.ingest_source(source, registry.profile("development"))

    assert server.calls["archive_head"] == 0
    assert server.calls["archive_get"] == 0


def test_ingestion_refuses_to_overwrite_corrupted_existing_snapshot(
    tmp_path: Path,
    registry: SourceRegistry,
) -> None:
    source = registry.source("cmhc_housing_activity")
    server = MockStatCanServer(registry, source, make_zip(source))
    ingestor, client = run_ingestion(registry, server, tmp_path / "raw")

    with client:
        first = ingestor.ingest_source(source, registry.profile("development"))
        first.archive_path.write_bytes(b"corrupted locally")

        with pytest.raises(IngestionError, match="checksum validation"):
            ingestor.ingest_source(source, registry.profile("development"))

    assert first.archive_path.read_bytes() == b"corrupted locally"
    assert server.calls["archive_get"] == 1
