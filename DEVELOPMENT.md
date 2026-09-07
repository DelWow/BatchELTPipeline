# Local development

## Requirements

- Python 3.11
- Java 17 or newer
- `uv` 0.12 or a compatible later 0.x release

Python is pinned in `.python-version`. Dependencies and tool settings live in
`pyproject.toml`; `uv.lock` fixes the transitive versions used by local runs and
the container.

Set up the environment from the repository root:

```bash
UV_CACHE_DIR=.uv-cache UV_PYTHON_INSTALL_DIR=.python uv sync --locked
```

This creates `.venv/` inside the project. Activation is optional because every
command can be run through `uv`.

```bash
uv run python --version
uv run housing-elt --help
uv run pytest -q
```

## Configuration

`housing-elt show-config` prints the resolved non-secret path settings:

```bash
uv run housing-elt show-config
```

The application reads `HOUSING_ELT_` environment variables but does not load a
`.env` file automatically. Relative path overrides are resolved from
`HOUSING_ELT_PROJECT_ROOT`; absolute paths are useful for containers and volume
mounts.

Source definitions and profiles are in `config/sources.toml`. Validation limits
are in `config/validation.toml`.

## Development data

Download the three sources used by the development profile:

```bash
uv run housing-elt ingest --profile development
```

The archives are stored under `data/raw/` by source release and SHA-256. A
rerun verifies matching local files and prints `already_present` instead of
downloading or overwriting them.

The full profile adds the building-permits archive:

```bash
uv run housing-elt ingest --profile full
```

That ZIP was about 365 MB when the source contract was last checked. The public
downloads are free, but the full run uses considerably more bandwidth, disk and
Spark time than the development run.

## Running individual stages

Clean the selected source observations and print per-source row counts:

```bash
uv run housing-elt clean --profile development
```

Build and validate the analytics table:

```bash
uv run housing-elt aggregate --profile development
```

The aggregate command writes `data/curated/housing_monthly/` only after all
quality checks pass.

Run the whole local workflow, including the source check/download:

```bash
uv run housing-elt run --profile development
```

For offline work after ingestion:

```bash
uv run housing-elt run --profile development --skip-ingestion
```

The development result is 360 rows: 24 months × 3 CMAs × 5 dwelling types.
Output is Snappy Parquet partitioned by `reference_year`.

## Quality checks

```bash
uv lock --check
uv run ruff check .
uv run ruff format --check .
uv run pytest -q
```

The Spark tests start a local JVM. HTTP and Snowflake calls are mocked, so the
test suite does not need network access or cloud credentials.

## Dependency changes

Add or update direct dependencies in `pyproject.toml`, then run `uv lock` and
review both files. Use `uv sync --locked` in repeatable environments so an
out-of-date lock file fails instead of being refreshed silently.

Downloaded data, generated output, virtual environments, tool caches, logs and
credential files are excluded by `.gitignore`.
