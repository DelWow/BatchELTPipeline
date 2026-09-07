# Container notes

The image runs the same local-mode Spark process used during development. The
driver and two worker threads share one container; this is not a packaged Spark
cluster.

## Image contents

The runtime starts from `python:3.11.15-slim-bookworm`, pinned by digest. Debian's
headless Java runtime supplies Java 17+, and `tini` forwards termination signals
to Python and the Spark JVM.

Python dependencies are installed from `uv.lock` in a builder stage. The final
stage does not contain `uv`, build caches, Ruff or pytest. It runs as the fixed
non-root user `10001:10001`.

The `.dockerignore` file is an allowlist. Local data, credentials, Git history,
tests and documentation never enter the build context. The image contains only
the application, its runtime dependencies and versioned configuration.

Debian packages are not fixed to package-level versions. The base image digest
pins the starting filesystem, while a rebuild can still receive repository
security updates.

## Build

```bash
docker build --tag canadian-housing-elt:local .
```

The image is local; no registry is needed for the Docker or `kind` examples.

## Run

The image ships without data. Mount raw input read-only and provide writable
intermediate and curated directories:

```bash
docker run --rm \
  --mount type=bind,source="$PWD/data/raw",target=/app/data/raw,readonly \
  --mount type=bind,source="$PWD/data/interim",target=/app/data/interim \
  --mount type=bind,source="$PWD/data/curated",target=/app/data/curated \
  canadian-housing-elt:local \
  run --profile development --skip-ingestion
```

Docker Desktop handles ownership for these shared directories. On native Linux,
UID/GID 10001 needs write access to `data/interim` and `data/curated`.

A verified development run returns exit code 0 and reports 360 rows, two years
and 15 anomaly flags.

## Failure smoke test

The fixture below changes the expected row count from 360 to 361. The process
should exit 1 before writing output or opening a Snowflake connection.

```bash
docker run --rm \
  --mount type=bind,source="$PWD/data/raw",target=/app/data/raw,readonly \
  --mount type=bind,source="$PWD/data/interim",target=/app/data/interim \
  --mount type=bind,source="$PWD/data/curated",target=/app/data/curated \
  --mount type=bind,source="$PWD/tests/fixtures/validation_row_count_failure.toml",target=/tmp/validation.toml,readonly \
  canadian-housing-elt:local \
  run --profile development --skip-ingestion \
  --validation-contract /tmp/validation.toml
```

Expected error:

```text
row_count: observed 360; expected [361, 361]
```

## Inspect

```bash
docker image inspect canadian-housing-elt:local \
  --format 'size_bytes={{.Size}} user={{.Config.User}} entrypoint={{json .Config.Entrypoint}}'
```

The current arm64 image is about 609 MB (581 MiB). Most of that is PySpark and
the Java runtime. Removing either would make the image smaller but would also
remove the workload it is meant to run.
