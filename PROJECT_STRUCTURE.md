# Project Structure and Path Contract

The repository uses a `src` layout so tests import the installed package rather
than accidentally importing Python files from the repository root. Empty
directories are reserved with `.gitkeep` files; implementation is added only in
the phase that owns it.

```text
.
├── config/                 # Versioned, non-secret pipeline configuration
├── data/
│   ├── raw/                # Unmodified downloaded source artifacts
│   ├── interim/            # Normalized and cleaned intermediate output
│   ├── curated/            # Analytics-ready partitioned output
│   └── checkpoints/        # Runtime state for recoverable processing
├── k8s/                    # Kubernetes manifests (Phase 12)
├── sql/                    # Reviewed Snowflake DDL and DML (Phases 9–10)
├── src/housing_elt/        # Installable application package
└── tests/
    ├── unit/               # Fast tests with no network or cloud dependency
    └── integration/        # Bounded component-boundary tests
```

## Data ownership

`data/raw/` is the landing zone and is treated as immutable input. Ingestion may
add a new versioned artifact or safely recognize an existing one, but later
stages must not rewrite the downloaded source file.

`data/interim/`, `data/curated/`, and `data/checkpoints/` are generated output.
They can be rebuilt from raw inputs and reviewed configuration. Their contents
are ignored by Git to keep source control free of large data, machine-specific
state, and accidental sensitive exports. Only `.gitkeep` placeholders are
tracked.

`config/`, `sql/`, and `k8s/` are intended for reviewed, non-secret files and
will be tracked. Real credentials, private keys, `.env` files, downloaded data,
generated outputs, and logs must remain untracked.

## Local configuration

`housing_elt.config.load_settings` reads only the following environment
variables. No `.env` file is loaded implicitly, which keeps configuration
sources visible in local shells, containers, CI, and Kubernetes manifests.

| Environment variable | Default relative to project root | Purpose |
| --- | --- | --- |
| `HOUSING_ELT_PROJECT_ROOT` | current working directory | Anchor for relative paths |
| `HOUSING_ELT_RAW_DATA_DIR` | `data/raw` | Native landing-zone input |
| `HOUSING_ELT_INTERIM_DATA_DIR` | `data/interim` | Normalized intermediate output |
| `HOUSING_ELT_CURATED_DATA_DIR` | `data/curated` | Analytics-ready output |
| `HOUSING_ELT_CHECKPOINT_DIR` | `data/checkpoints` | Recoverable runtime state |
| `HOUSING_ELT_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL` |

Relative overrides resolve from the project root. Absolute overrides support
container volumes and Kubernetes mounts without changing application code.
Settings are immutable after loading, and loading them has no filesystem or
network side effects.

The resolved non-secret values can be inspected with either entry point:

```bash
uv run housing-elt show-config
uv run python -m housing_elt show-config
```

