# Project layout and paths

```text
.
├── config/                 source registry and validation policy
├── data/
│   ├── raw/                immutable downloaded snapshots
│   ├── interim/            extracted and cleaned working data
│   ├── curated/            partitioned analytics output
│   └── checkpoints/        runtime checkpoint directory
├── k8s/                    kind and Kubernetes manifests
├── sql/                    Snowflake DDL and publication SQL
├── src/housing_elt/        application package
└── tests/                  unit tests and fixtures
```

The package uses a `src` layout so tests import the installed application rather
than a same-named directory from the repository root.

## Data directories

`data/raw/` is append-only from the pipeline's point of view. Ingestion can add
a release/checksum-addressed snapshot or recognize one already present, but
later stages do not rewrite source archives.

The interim, curated and checkpoint directories are generated. Their contents
can be rebuilt from raw snapshots and versioned configuration. All four data
directories are ignored by Git except for their `.gitkeep` files.

Configuration, SQL and Kubernetes manifests are tracked. `.env` files, private
keys, downloaded data, generated output, logs, caches and local tools are not.

## Runtime settings

The application reads the following environment variables. It does not load a
`.env` file on its own.

| Variable | Default | Use |
| --- | --- | --- |
| `HOUSING_ELT_PROJECT_ROOT` | current directory | Base for relative paths |
| `HOUSING_ELT_RAW_DATA_DIR` | `data/raw` | Source snapshots |
| `HOUSING_ELT_INTERIM_DATA_DIR` | `data/interim` | Extracted and clean working data |
| `HOUSING_ELT_CURATED_DATA_DIR` | `data/curated` | Parquet output |
| `HOUSING_ELT_CHECKPOINT_DIR` | `data/checkpoints` | Checkpoint state |
| `HOUSING_ELT_LOG_LEVEL` | `INFO` | Python log level |

Relative overrides use the project root. Absolute overrides are useful for
Docker and Kubernetes mounts.

Inspect the resolved values with either entry point:

```bash
uv run housing-elt show-config
uv run python -m housing_elt show-config
```
