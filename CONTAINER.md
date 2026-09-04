# Local Container Runbook

Phase 11 packages the existing local-mode Spark application as one container.
It does not create a Spark cluster: the driver and two local worker threads run
in one process tree, which is proportional to this portfolio pipeline's serving
fact and keeps the later Kubernetes CronJob understandable.

## Image choices

- `python:3.11.15-slim-bookworm` matches `.python-version` and is pinned to its
  immutable multi-platform digest. The slim Debian base retains broad glibc
  wheel compatibility without the tools included in a full Python image.
- `default-jre-headless` provides Java 17 on Debian Bookworm. PySpark 4.2 needs
  Java 17 or newer; no JDK/compiler or standalone Spark distribution is needed
  because the locked `pyspark` package supplies Spark.
- The builder installs pinned `uv` 0.12.5 and performs `uv sync --locked
  --no-dev --no-editable`. Only the resulting virtual environment enters the
  final image, excluding uv, build caches, Ruff, and pytest.
- `tini` is the PID 1 process so termination signals reach both Python and the
  Java child cleanly. The application runs as fixed non-root UID/GID 10001.
- `.dockerignore` is an allowlist. Raw/interim/curated data, `.env` files,
  credentials, Git history, local environments, tests, and documentation never
  enter the build context.

OS packages are intentionally not version-pinned: Debian's repository supplies
compatible security updates for the digest-pinned Bookworm base. Python
application dependencies are exact through `uv.lock`.

## Build

From the repository root:

```bash
docker build --tag canadian-housing-elt:local .
```

The tag is local only; no registry login or push is required.

## Run the development pipeline offline

The image intentionally contains no datasets. Bind mounts preserve the raw
landing zone as read-only while allowing reproducible extraction and curated
output. Keep the image's fixed user: Spark derives its Ivy home from that
user's `/etc/passwd` entry. Docker Desktop permits this user to write the shared
directories; on native Linux, ensure UID/GID 10001 can write `data/interim` and
`data/curated` before running:

```bash
docker run --rm \
  --mount type=bind,source="$PWD/data/raw",target=/app/data/raw,readonly \
  --mount type=bind,source="$PWD/data/interim",target=/app/data/interim \
  --mount type=bind,source="$PWD/data/curated",target=/app/data/curated \
  canadian-housing-elt:local \
  run --profile development --skip-ingestion
```

Do not add `--load-snowflake` for this smoke test. Snowflake remains a separate
approval gate and the image contains no credentials.

## Verify a fail-closed exit

The test fixture changes only the expected development row count from 360 to
361. It proves a valid container invocation returns non-zero on data validation
failure before local or Snowflake publication:

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

Expected exit code: `1`, with a `row_count` validation error reporting 360
observed rows and an expected range of `[361, 361]`.

The verified successful development run exits `0` and reports 360 analytics
rows, two reference years, and 15 anomaly flags.

## Inspect the image

```bash
docker image inspect canadian-housing-elt:local \
  --format 'size_bytes={{.Size}} user={{.Config.User}} entrypoint={{json .Config.Entrypoint}}'
```

The image is larger than a typical Python service because the PySpark wheel and
Java runtime are intrinsically substantial. The relevant lean-image controls
are removing the JDK, build tools, package caches, uv, test dependencies, and
all data—not obscuring that unavoidable runtime cost.
