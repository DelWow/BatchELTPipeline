"""Discover immutable archives and materialize their CSV members for Spark."""

from __future__ import annotations

import csv
import json
import os
import shutil
import uuid
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from housing_elt.ingestion.registry import SourceDefinition
from housing_elt.transformation.errors import TransformationError
from housing_elt.transformation.schemas import RAW_SCHEMAS


@dataclass(frozen=True, slots=True)
class SourceSnapshot:
    """One immutable raw archive and its source-release identity."""

    source_id: str
    release_timestamp: datetime
    release_label: str
    sha256: str
    archive_path: Path
    data_member: str


def _load_manifest(path: Path) -> dict:
    try:
        with path.open(encoding="utf-8") as manifest_file:
            value = json.load(manifest_file)
    except (OSError, json.JSONDecodeError) as error:
        raise TransformationError(
            f"Cannot read raw manifest {path}: {error}"
        ) from error
    if not isinstance(value, dict):
        raise TransformationError(f"Raw manifest must contain an object: {path}")
    return value


def discover_snapshots(
    raw_data_dir: Path, source: SourceDefinition
) -> tuple[SourceSnapshot, ...]:
    """Return every verified raw snapshot for a source in release order."""
    source_root = raw_data_dir / "statcan" / source.id
    snapshots: list[SourceSnapshot] = []
    for manifest_path in sorted(source_root.glob("release=*/sha256=*/manifest.json")):
        manifest = _load_manifest(manifest_path)
        source_manifest = manifest.get("source", {})
        artifact = manifest.get("artifact", {})
        if source_manifest.get("id") != source.id:
            raise TransformationError(
                f"Manifest source mismatch for {manifest_path}: expected {source.id!r}"
            )

        release_label = manifest_path.parents[1].name.removeprefix("release=")
        sha256 = manifest_path.parent.name.removeprefix("sha256=")
        if artifact.get("sha256") != sha256:
            raise TransformationError(f"Manifest SHA/path mismatch: {manifest_path}")
        try:
            release_timestamp = datetime.strptime(
                release_label, "%Y%m%dT%H%M%SZ"
            ).replace(tzinfo=UTC)
        except ValueError as error:
            raise TransformationError(
                f"Invalid release directory in {manifest_path}"
            ) from error

        archive_path = manifest_path.parent / str(artifact.get("filename", ""))
        if not archive_path.is_file():
            raise TransformationError(f"Raw archive is missing: {archive_path}")
        snapshots.append(
            SourceSnapshot(
                source_id=source.id,
                release_timestamp=release_timestamp,
                release_label=release_label,
                sha256=sha256,
                archive_path=archive_path,
                data_member=source.data_member,
            )
        )

    if not snapshots:
        raise TransformationError(
            f"No ingested snapshots found for {source.id} under {source_root}"
        )
    return tuple(snapshots)


def _validate_header(csv_path: Path, source_id: str) -> None:
    expected = RAW_SCHEMAS[source_id].fieldNames()
    try:
        with csv_path.open(encoding="utf-8-sig", newline="") as csv_file:
            actual = next(csv.reader(csv_file))
    except (OSError, StopIteration, csv.Error) as error:
        raise TransformationError(f"Cannot read CSV header from {csv_path}") from error
    if actual != expected:
        raise TransformationError(
            f"CSV schema drift for {source_id}: expected {expected!r}, got {actual!r}"
        )


def materialize_csv(snapshot: SourceSnapshot, interim_data_dir: Path) -> Path:
    """Idempotently extract one native CSV member for Spark's CSV reader.

    Spark does not natively split/read CSV members inside ZIP archives. The ZIP
    remains the immutable raw artifact; this byte-for-byte extraction is a
    replaceable interim cache, not an early format conversion.
    """
    destination = (
        interim_data_dir
        / "extracted"
        / snapshot.source_id
        / f"release={snapshot.release_label}"
        / f"sha256={snapshot.sha256}"
        / snapshot.data_member
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(snapshot.archive_path) as archive:
            info = archive.getinfo(snapshot.data_member)
            if destination.exists():
                if destination.stat().st_size != info.file_size:
                    raise TransformationError(
                        f"Existing extracted CSV has the wrong size: {destination}"
                    )
                _validate_header(destination, snapshot.source_id)
                return destination

            partial = destination.with_name(
                f".{destination.name}.partial-{uuid.uuid4().hex}"
            )
            try:
                with (
                    archive.open(info) as source_file,
                    partial.open("xb") as output_file,
                ):
                    shutil.copyfileobj(source_file, output_file, length=1024 * 1024)
                if partial.stat().st_size != info.file_size:
                    raise TransformationError(
                        f"Extracted CSV size mismatch for {snapshot.source_id}"
                    )
                _validate_header(partial, snapshot.source_id)
                os.replace(partial, destination)
            finally:
                partial.unlink(missing_ok=True)
    except (OSError, KeyError, zipfile.BadZipFile) as error:
        raise TransformationError(
            f"Cannot extract {snapshot.data_member} from {snapshot.archive_path}"
        ) from error
    return destination
