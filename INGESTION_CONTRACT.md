# Statistics Canada Ingestion Contract

## Purpose

This document freezes the behavior expected from the Phase 5 downloader before
implementation. The contract covers acquisition and preservation of official
source snapshots only. Filtering, schema normalization, annual unions, and
analytics belong to later PySpark phases.

The versioned machine-readable registry is `config/sources.toml`. Source URLs,
PIDs, expected archive members, size safety limits, and execution profiles must
come from that registry rather than being duplicated in downloader code.

## Source transport and native format

All four sources use Statistics Canada's `getFullTableDownloadCSV` method. The
method returns a small JSON response containing the current static ZIP URL; the
pipeline must validate `status == "SUCCESS"` and then download the returned
HTTPS URL. It must not assume that a successful HTTP response implies a
successful WDS response.

Statistics Canada's full-table format is an English ZIP containing a comma-
delimited data CSV and a corresponding metadata CSV. The data contains standard
fields such as `REF_DATE`, `GEO`, `DGUID`, `UOM`, `SCALAR_FACTOR`, `VECTOR`,
`VALUE`, and status fields. The metadata contains the table period, dimensions,
members, notes, symbols, and corrections. Both files are part of the source
artifact; neither may be discarded or converted in the raw zone.

The API and current resolved links were verified on 2026-08-31:

| Source ID | Table / PID | Source availability | Full-profile window | Observed ZIP size | Observed cube datapoints |
| --- | --- | --- | --- | ---: | ---: |
| `cmhc_housing_activity` | 34-10-0154-01 / 34100154 | 1972-01 onward | 2016-01–2025-12 | 4,262,413 bytes | 342,900 |
| `cmhc_starts_by_market` | 34-10-0148-01 / 34100148 | 1988-06 onward | 2016-01–2025-12 | 7,618,340 bytes | 799,600 |
| `statcan_building_permits` | 34-10-0292-01 / 34100292 | 2018-01 onward | 2018-01–2025-12 | 364,827,497 bytes | 38,338,128 |
| `statcan_new_housing_price_index` | 18-10-0205-01 / 18100205 | 1981-01 onward | 2016-01–2025-12 | 355,086 bytes | 65,640 |

These counts describe complete published cubes before geography, period,
aggregate-level, and variable filters. They are volatile source metadata, not
fixed validation thresholds and not counts of building-level events.

All tables report monthly frequency code `6`. Releases and corrections replace
the static ZIP at the same URL, so URL identity alone is not version identity.

## Execution profiles

### Development profile

The development profile downloads these manageable full-table archives:

- CMHC housing activity;
- CMHC starts by intended market; and
- the New Housing Price Index.

Their current compressed size is approximately 12 MB combined. After raw
ingestion, later phases will filter to January 2024 through December 2025 for:

- Toronto, Ontario (CMA code 535);
- Vancouver, British Columbia (CMA code 933); and
- Calgary, Alberta (CMA code 825).

The full-table endpoint does not offer a server-side date/geography subset, so
the raw archives still contain complete history. The 365 MB permits archive is
excluded from fast development runs. Phase 5 must cover its downloader behavior
with mocked HTTP tests; it is exercised against real data in the full profile.

### Full profile

The full profile downloads all four current snapshots. PySpark will retain all
eligible CMA rows for the fixed benchmark ending December 2025. The three
long-running sources use January 2016 as their start; building permits use
January 2018 because Table 34-10-0292-01 contains no earlier observations.

The fixed end date makes portfolio runs reproducible. A separately requested
incremental run may use later releases, but must create new snapshots rather
than altering the benchmark archives.

## Retrieval sequence

For each selected source, Phase 5 must perform these steps in order:

1. POST the PID to `getCubeMetadata` and preserve the exact JSON response.
2. Require a successful metadata response with the configured PID, monthly
   frequency, current archive status, release time, and expected dimensions.
3. GET the configured `getFullTableDownloadCSV/{PID}/en` endpoint.
4. Require `status == "SUCCESS"`; validate that the returned URL is HTTPS, is
   hosted by `www150.statcan.gc.ca`, and matches the configured PID filename.
5. Stream the ZIP to a run-owned `.part` file while calculating SHA-256 and
   counting bytes. The entire response must not be held in memory.
6. Validate HTTP metadata and ZIP integrity before making the artifact visible.
7. Write the source metadata and generated manifest beside the archive, then
   atomically publish the completed snapshot directory.

No step extracts, edits, recompresses, or converts the downloaded ZIP in
`data/raw/`.

## Deterministic raw layout

Each immutable snapshot is identified by the source's normalized release time
and the downloaded bytes, not by a mutable `latest` filename:

```text
data/raw/statcan/<source_id>/
└── release=<YYYYMMDDTHHMMSSZ>/
    └── sha256=<64-lowercase-hex-digest>/
        ├── <PID>-eng.zip
        ├── cube-metadata.json
        └── manifest.json
```

WDS release times are published as Eastern time with daylight-saving behavior.
The manifest preserves the raw value and stores the normalized UTC value used
in the directory name. Temporary files use:

```text
data/raw/.partial/<source_id>-<run-uuid>.zip.part
```

The UUID prevents concurrent processes from sharing a partial file. A process
may delete only its own partial file when it handles an error. It must not
remove another run's partial file or any completed snapshot.

## Manifest contract

`manifest.json` is UTF-8 JSON with sorted keys and a trailing newline. It must
contain at least:

```json
{
  "artifact": {
    "byte_count": 4262413,
    "content_type": "application/zip",
    "filename": "34100154-eng.zip",
    "sha256": "<64 lowercase hexadecimal characters>",
    "zip_crc_valid": true,
    "zip_members": ["34100154.csv", "34100154_MetaData.csv"]
  },
  "contract_version": 1,
  "http": {
    "content_length": 4262413,
    "etag": "<value or null>",
    "last_modified": "<value or null>",
    "resolved_download_url": "<validated HTTPS URL>"
  },
  "retrieval": {
    "completed_at_utc": "<ISO-8601 timestamp>",
    "started_at_utc": "<ISO-8601 timestamp>"
  },
  "source": {
    "id": "cmhc_housing_activity",
    "product_id": "34100154",
    "source_release_time_raw": "2026-08-19T08:30",
    "source_release_time_utc": "2026-08-19T12:30:00Z",
    "table_id": "34-10-0154-01"
  }
}
```

The real manifest also records the request URL, WDS response status, HTTP
status, redirect target, source issue date, source coverage dates, frequency,
archive status, series/datapoint counts, correction IDs/dates, downloader
version, and selected profile. Null HTTP headers are recorded explicitly rather
than invented.

`cube-metadata.json` preserves the exact per-PID WDS response used for the run.
The generated manifest summarizes the fields needed for discovery and audit;
it is not a replacement for source metadata.

## Integrity checks

A snapshot is publishable only when all checks pass:

1. WDS responses are valid JSON, report success, and identify the requested PID.
2. The archive response is HTTP 200 with an allowed content type and a positive
   byte count below the source-specific `max_archive_bytes` safety cap.
3. If `Content-Length` is present, it equals the number of streamed bytes.
4. A SHA-256 digest is calculated from the exact downloaded bytes and recorded.
5. The file is a readable ZIP, every member passes its CRC check, and no member
   uses an absolute path or `..` traversal segment.
6. The configured data and metadata CSV member names are both present. Extra
   members are recorded rather than silently discarded.
7. Metadata reports monthly frequency, a current table, the configured
   dimensions, and coverage intersecting the requested profile window.

Statistics Canada does not publish a checksum through this endpoint. SHA-256 is
therefore a local identity/integrity guarantee, not proof against a separately
compromised upstream file. `ETag`, `Last-Modified`, and `Content-Length` are
recorded as revision hints but never treated as substitutes for SHA-256.

## Retry, timeout, and failure behavior

- Use a 10-second connection timeout and a 120-second per-read timeout.
- Enforce a 1,800-second overall deadline per archive so the large permits file
  has a bounded but realistic window.
- Allow four total attempts with exponential full-jitter backoff starting at
  one second and capped at 30 seconds.
- Retry connection failures, timeouts, HTTP 408, 409, 425, 429, 500, 502, 503,
  and 504. Statistics Canada documents 409 while tables are locked for updates.
- Honor `Retry-After` when present, capped at 300 seconds.
- Retry a truncated response or failed ZIP/CRC validation once within the same
  four-attempt budget after removing only that attempt's partial file.
- Do not retry authentication errors, other non-retryable 4xx responses, an
  unexpected host/PID, malformed successful metadata, or contract/schema drift.
- On exhaustion, log the source ID, PID, attempt count, failure category, and
  actionable reason; exit non-zero and publish no manifest or final directory.

Logs must never contain response bodies beyond a short sanitized error message,
environment dumps, or future credentials.

## Idempotency and revision handling

Before downloading, inspect completed manifests for the same PID and normalized
release time. If a manifest's `ETag`, `Last-Modified`, and `Content-Length` still
match the current response, recalculate the local file's SHA-256 and ZIP CRC:

- if they match the manifest, report `already present` and skip;
- if they do not match, fail loudly without overwriting or deleting the file.

If revision hints changed—or are unavailable for a release not yet present—use
a new partial download and compute its digest. If the final digest directory
already exists and verifies, discard only the run-owned partial and report a
successful idempotent no-op. A new digest creates a new immutable sibling
snapshot, even when the source release timestamp is unchanged.

Historical full-table snapshots must never be unioned: each contains overlapping
history and may revise earlier values. Downstream processing selects exactly one
explicit snapshot per source, filters it to source-specific benchmark dates,
then unions compatible annual/geographic partitions after normalization.

## Licensing and cost boundary

Statistics Canada tables are reused under the Statistics Canada Open Licence.
The two CMHC-origin tables also retain CMHC attribution and no-endorsement
requirements. Retrieval timestamps are recorded because both licences state
that the terms in force when information is accessed apply.

These public endpoints require no account and incur no cloud charge. The full
permits archive does consume material local bandwidth and disk space, so it is
excluded from the development profile and clearly flagged before full runs.

