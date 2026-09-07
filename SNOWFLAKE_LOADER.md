# Snowflake loader

Live loading is optional and can consume Snowflake credits. The code has been
tested with mocked connections, not a real account.

## Required settings

The loader reads credentials from the process environment. It does not read a
`.env` file, accept a password on the command line or print secrets.

| Variable | Value |
| --- | --- |
| `HOUSING_ELT_SNOWFLAKE_ACCOUNT` | Account identifier without hostname suffix |
| `HOUSING_ELT_SNOWFLAKE_USER` | Login name |
| `HOUSING_ELT_SNOWFLAKE_PASSWORD` | Password |
| `HOUSING_ELT_SNOWFLAKE_WAREHOUSE` | Existing warehouse |
| `HOUSING_ELT_SNOWFLAKE_DATABASE` | Existing database |
| `HOUSING_ELT_SNOWFLAKE_SCHEMA` | Existing schema containing the project tables |
| `HOUSING_ELT_SNOWFLAKE_ROLE` | Optional role |

Object names are restricted to ordinary unquoted Snowflake identifiers. Values
are parameter-bound, but object identifiers cannot be.

## Run

First review and apply `sql/001_create_housing_analytics.sql` in the intended
account. Then export the variables above and run:

```bash
UV_CACHE_DIR=.uv-cache UV_PYTHON_INSTALL_DIR=.python uv run housing-elt run \
  --profile development \
  --skip-ingestion \
  --load-snowflake
```

Without `--load-snowflake`, the pipeline remains fully local. With the flag, it
validates and writes Parquet before calling the loader. A validation failure
occurs before connection creation.

## Guarantees

- Spark rows are streamed with `toLocalIterator()` in batches of 1,000.
- A durable `STARTED` audit row exists before staging begins.
- Staged row count and distinct natural-key count must match validation.
- The complete month window is replaced in one transaction.
- Publication errors trigger a rollback and a `FAILED` audit update.
- A second active batch for the same profile is rejected.

Development and full loads should use separate schemas or databases. Their date
windows are different, so alternating them in one target is unsupported.

See [SNOWFLAKE_DESIGN.md](SNOWFLAKE_DESIGN.md) for table definitions and the
connector trade-off.
