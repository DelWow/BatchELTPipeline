# Snowflake Loader Runbook

Phase 10 implements the load path but does not execute it against a real
Snowflake account. Creating or using an account, warehouse, database, or schema
can consume credits and remains an explicit decision gate.

## Required environment variables

The loader reads credentials only from the process environment. It does not
read a `.env` file, accept secrets as CLI arguments, or print the password.

| Variable | Purpose |
| --- | --- |
| `HOUSING_ELT_SNOWFLAKE_ACCOUNT` | Account identifier, without the Snowflake hostname suffix |
| `HOUSING_ELT_SNOWFLAKE_USER` | Login name |
| `HOUSING_ELT_SNOWFLAKE_PASSWORD` | Password, injected at runtime |
| `HOUSING_ELT_SNOWFLAKE_WAREHOUSE` | Existing warehouse used by the load |
| `HOUSING_ELT_SNOWFLAKE_DATABASE` | Existing target database |
| `HOUSING_ELT_SNOWFLAKE_SCHEMA` | Existing schema containing the Phase 9 tables |
| `HOUSING_ELT_SNOWFLAKE_ROLE` | Optional role override |

Database, schema, warehouse, and role names are limited to normal unquoted
Snowflake identifiers because SQL identifiers cannot be parameter-bound. This
allows safe fully qualified SQL without inventing a quoting mini-language.

## Explicit invocation

After an approved operator applies `sql/001_create_housing_analytics.sql` to the
intended account, the end-to-end command is:

```bash
UV_CACHE_DIR=.uv-cache UV_PYTHON_INSTALL_DIR=.python uv run housing-elt run \
  --profile development \
  --skip-ingestion \
  --load-snowflake
```

Omitting `--load-snowflake` preserves the existing fully local behavior. With
the flag, the same persisted fact is validated, written to local Parquet, and
then passed to Snowflake. Validation failure occurs before connection creation.

## Load guarantees

- Rows stream from Spark with `toLocalIterator()` and are inserted in bounded
  batches; the modeled fact is never collected into one driver-side list.
- A `STARTED` audit row is durable before staging begins. Partial staging data
  remains batch-addressable for investigation.
- Staged row count and distinct natural-key count must match the validation
  report before final-table DML starts.
- Final publication deletes and replaces the batch's complete month window in
  one explicit transaction. This removes stale keys after source revisions and
  makes a retry converge on the same business rows.
- A publication error causes an explicit rollback followed by a bounded
  `FAILED` audit message.
- A second `STARTED` batch for the same profile is rejected. Phase 12 will add
  Kubernetes `concurrencyPolicy: Forbid` as an outer scheduling guard.

Development and full-profile loads must target separate schemas (or separate
databases containing the same schema name); alternating their windows in one
target is unsupported.
