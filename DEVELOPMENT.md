# Local Development Environment

## Tooling choice

This project uses `pyproject.toml` rather than `requirements.txt` because one
standard file can describe Python compatibility, direct dependencies, and tool
configuration. `uv.lock` records the exact transitive versions resolved from
those direct constraints, giving repeatable environments without manually
maintaining separate frozen requirements files.

Python 3.11 is pinned in `.python-version`. Using one Python minor version in
development, tests, and the future container reduces the risk of driver and
executor differences in PySpark. Runtime packages such as PySpark and the
Snowflake client will be added only when their designs are reviewed in later
phases.

`ruff` provides both formatting and linting in one fast tool. `pytest` is used
because fixtures and parametrization make data-transformation edge cases easy
to express without coupling tests to the pipeline entry point.

## Prerequisites

- `uv` 0.12 or a compatible later 0.x release
- Java 17 or later once PySpark is introduced

No global Python packages or shell configuration changes are required. If
Python 3.11 is not already installed, `uv` can place its managed interpreter in
the project-local ignored `.python/` directory by using the environment
variables shown below.

## Create or update the environment

Run these commands from the repository root:

```bash
UV_CACHE_DIR=.uv-cache UV_PYTHON_INSTALL_DIR=.python uv sync --locked
```

This creates the ignored `.venv/` environment. Commands can be run through
`uv`, so activating the environment is optional:

```bash
uv run python --version
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

To activate it for an interactive shell session instead:

```bash
source .venv/bin/activate
```

## Ingest the development sources

The development profile downloads and validates the two CMHC-origin tables and
the New Housing Price Index. It intentionally excludes the much larger building
permits archive so local iteration stays fast:

```bash
uv run housing-elt ingest --profile development
```

Every source is streamed into `data/raw/`, checked against its WDS metadata,
size cap, SHA-256 digest, ZIP CRC, safe member paths, and expected CSV members,
then published under an immutable release/digest path. Rerunning the command
revalidates the local bytes and reports `already_present` when the source
revision hints still match; it does not append or overwrite the snapshot.

The full profile includes the building-permits archive, which was approximately
365 MB compressed when the ingestion contract was reviewed:

```bash
uv run housing-elt ingest --profile full
```

Use the full profile intentionally because it consumes materially more local
bandwidth, disk, and validation time. Both profiles use public endpoints and do
not require credentials or paid cloud resources.

Do not install project dependencies into the system Python. When dependencies
change, run `uv lock` intentionally and review the resulting `uv.lock` diff.
CI and reproducible local runs should use `uv sync --locked`, which fails when
the lock file and project metadata disagree rather than silently re-resolving.

## Git boundary

The repository was already initialized when this phase began. This phase does
not stage, commit, push, switch branches, or otherwise change Git history or
remote state. `.venv/`, managed Python files, caches, downloaded source data,
generated output, logs, and common secret formats are excluded by `.gitignore`.
