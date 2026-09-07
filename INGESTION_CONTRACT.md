# Ingestion contract

Ingestion downloads and preserves official Statistics Canada snapshots. It does
not filter or normalize observations; Spark handles that later.

`config/sources.toml` is the source of truth for product IDs, URLs, expected ZIP
members, size limits and profile membership. The downloader does not maintain a
second list in Python.

## Transport and source format

For each product ID, the pipeline calls Statistics Canada's cube-metadata and
`getFullTableDownloadCSV` endpoints. The latter returns JSON containing the
current ZIP URL. Both the HTTP response and the WDS-level status must succeed.

Every source arrives as an English ZIP containing a data CSV and metadata CSV.
The entire ZIP is kept in `data/raw`; neither member is discarded or converted
there.

The source sizes observed on 2026-08-31 were:

| Source | PID | ZIP size | Cube datapoints |
| --- | --- | ---: | ---: |
| CMHC housing activity | 34100154 | 4,262,413 bytes | 342,900 |
| CMHC starts by market | 34100148 | 7,618,340 bytes | 799,600 |
| Building permits | 34100292 | 364,827,497 bytes | 38,338,128 |
| New Housing Price Index | 18100205 | 355,086 bytes | 65,640 |

These are publisher cube counts, not counts of buildings or transactions, and
they can change with later releases.

## Download sequence

For each selected source, the ingestor:

1. fetches cube metadata and saves the exact response;
2. checks the product ID, monthly frequency, archive status, release time and
   configured dimensions;
3. resolves the full-table ZIP endpoint;
4. checks that the returned URL is HTTPS, belongs to
   `www150.statcan.gc.ca` and contains the expected PID filename;
5. streams the response to a run-owned partial file while counting bytes and
   calculating SHA-256;
6. validates the response metadata and ZIP; and
7. writes the manifest and atomically renames the completed snapshot directory.

No raw ZIP is extracted, recompressed or edited.

## Raw layout

Release time and file content identify a snapshot:

```text
data/raw/statcan/<source_id>/
└── release=<YYYYMMDDTHHMMSSZ>/
    └── sha256=<digest>/
        ├── <PID>-eng.zip
        ├── cube-metadata.json
        └── manifest.json
```

Statistics Canada publishes release times in Eastern time. The raw timestamp is
kept in the manifest and a normalized UTC timestamp is used in the path.

Partial downloads live at:

```text
data/raw/.partial/<source_id>-<run-uuid>.zip.part
```

A run removes only its own partial file after an error. It never deletes a
completed snapshot or another process's partial download.

## Manifest

`manifest.json` is sorted UTF-8 JSON with a trailing newline. Its main sections
are:

```json
{
  "artifact": {
    "byte_count": 4262413,
    "filename": "34100154-eng.zip",
    "sha256": "<64 lowercase hex characters>",
    "zip_crc_valid": true,
    "zip_members": ["34100154.csv", "34100154_MetaData.csv"]
  },
  "http": {
    "content_length": 4262413,
    "etag": "<value or null>",
    "last_modified": "<value or null>",
    "resolved_download_url": "<validated HTTPS URL>"
  },
  "retrieval": {
    "started_at_utc": "<timestamp>",
    "completed_at_utc": "<timestamp>"
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

The full manifest also records request/redirect URLs, statuses, coverage dates,
series and datapoint counts, corrections, downloader version and profile. A
missing HTTP header is recorded as null. `cube-metadata.json` remains the exact
source response; the manifest is only a useful summary.

## Publish checks

A snapshot is accepted only if:

- WDS JSON reports success and the requested PID;
- the archive response is HTTP 200 with an allowed content type;
- its byte count is positive and below the configured cap;
- `Content-Length`, when supplied, matches the streamed bytes;
- the SHA-256 digest is recorded;
- the ZIP opens and every member passes CRC validation;
- no ZIP member is absolute or contains a `..` segment;
- both configured CSV members exist; and
- metadata has the expected frequency, dimensions, active status and date
  overlap with the profile.

Statistics Canada does not provide a checksum for this endpoint. The local
SHA-256 identifies the downloaded bytes; it is not an independent signature
from the publisher. `ETag`, `Last-Modified` and `Content-Length` are revision
hints, not substitutes for the digest.

## Timeouts and retries

- connect timeout: 10 seconds
- per-read timeout: 120 seconds
- overall archive deadline: 1,800 seconds
- attempts: 4
- exponential full-jitter backoff: 1 to 30 seconds
- `Retry-After` cap: 300 seconds

Connection errors, timeouts and HTTP 408, 409, 425, 429, 500, 502, 503 and 504
are retryable. A truncated response or failed ZIP/CRC check can be retried once
within the same four-attempt budget.

Authentication errors, other non-retryable 4xx responses, unexpected hosts or
PIDs, malformed success responses and metadata/schema drift fail immediately.
After the last attempt, the command exits non-zero and leaves no completed
snapshot.

Logs include the source, PID, attempt and short failure reason. They do not dump
response bodies, environment variables or credentials.

## Reruns and revisions

Before downloading, the ingestor compares a release's saved HTTP hints and
revalidates the local SHA-256 and ZIP CRC. Matching bytes produce an
`already_present` result. A mismatch fails rather than overwriting the file.

If the upstream bytes change, the new digest is published as a sibling snapshot.
If that digest already exists and verifies, the partial download is discarded
as an idempotent no-op.

Complete table releases overlap and can revise history. Downstream code selects
one snapshot per source; it never unions several full-history releases.
