# Snowflake Load and Schema Design

Phase 9 is a design-only phase. The SQL in `sql/` is reviewed but has not been
executed, and no Snowflake account, warehouse, database, schema, or other paid
resource has been contacted or created.

## Connector decision

The loader will use `snowflake-connector-python` with server-side `qmark`
binding and bounded `executemany()` batches.

### Why the Python connector fits this pipeline

- PySpark performs the expensive work: reading the source cubes, resolving
  revisions, joining, and calculating windows. The resulting serving fact is
  compact—360 rows in development and expected to be only tens of thousands at
  the full ten-year/CMA scope.
- Phase 10 can stream rows from Spark with `DataFrame.toLocalIterator()` and
  form bounded batches. It will not call `collect()` on an unbounded frame.
- Server-side parameter binding keeps values separate from SQL text. Snowflake
  documents that `executemany()` can optimize sufficiently large server-bound
  batches through a temporary stage when the session has a current database
  and schema.
- The Python connector adds one Python dependency and no Spark package, JDBC
  driver, or Scala binary to the later container image.
- Explicit staging and publish SQL make transaction boundaries, reconciliation,
  and idempotency visible instead of hiding them behind `DataFrame.write`.

### Trade-off against the Spark–Snowflake connector

The Spark connector is optimized for high-volume, bidirectional transfer and
can push eligible Spark operations into Snowflake. It is the stronger choice
when the DataFrame being transferred is genuinely large or when Snowflake is a
Spark data source as well as a target.

It also introduces a Spark plugin, Snowflake JDBC driver, Scala compatibility,
and Spark-package resolution. Snowflake's current documentation lists Spark
Connector 3.x support through Spark 4.1, while this project uses PySpark 4.2.
Downgrading Spark or relying on an undocumented combination solely to use the
connector would add risk without a scale benefit for this modeled fact.

Revisit this decision if the output reaches millions of rows per batch, direct
Spark-to-Snowflake transfer becomes a measured bottleneck, or Snowflake
publishes a connector version verified with the project's Spark/Scala versions.
A staged Parquet `PUT`/`COPY INTO` path is another future bulk-load option.

Official references:

- [Snowflake Connector for Spark](https://docs.snowflake.com/en/user-guide/spark-connector)
- [Snowflake Python connector binding and batch inserts](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-example)
- [Python connector API](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api)

## Object model

The connection supplies the environment-specific database and warehouse. The
DDL creates only the `HOUSING_ANALYTICS` schema and its tables; it deliberately
does not create or resize a warehouse or create a database.

### `STG_HOUSING_MONTHLY`

This is a transient, batch-addressed copy of the validated Spark fact. Transient
storage persists across sessions for failure investigation but has no Fail-safe,
which is appropriate because staging is reproducible from immutable raw data.
Every row carries `LOAD_BATCH_ID`, `VALIDATION_PROFILE`, and `LOADED_AT_UTC`.

### `FCT_HOUSING_MONTHLY`

This permanent analytics table has the same business grain as the Parquet fact:

> one row per `REFERENCE_MONTH × CMA_CODE × DWELLING_TYPE`

It includes source-release/SHA lineage, coverage flags, and the batch/profile
that last published the row. Key fields and deterministic coverage flags are
`NOT NULL`; context and trend values remain nullable where their source or
history is unavailable.

The table intentionally has no declared primary key. Snowflake does not enforce
primary/unique constraints on standard tables, so an unenforced declaration
could imply protection that does not exist. The Phase 8 uniqueness check and
transactional scope replacement provide the actual guarantee. `NOT NULL`
constraints are retained because Snowflake enforces them.

### `ELT_LOAD_AUDIT`

One permanent row per attempted batch records status, profile/window, validation
metrics, staged/published counts, timestamps, and any bounded error message.
Phase 10 will use it for observable retries and troubleshooting, not as a source
of analytics data.

Official references:

- [Transient table storage behavior](https://docs.snowflake.com/en/user-guide/tables-storage-considerations)
- [Snowflake constraint enforcement](https://docs.snowflake.com/en/sql-reference/constraints-overview)

## Type mapping

| Spark/semantic value | Snowflake type | Reason |
| --- | --- | --- |
| Date/month | `DATE` | No time-of-day meaning. |
| Spark `long` counts | `NUMBER(38,0)` | Exact integer; Snowflake integer aliases map to this representation. |
| PySpark fixed decimals | matching `NUMBER(p,4)` | Preserves exact NHPI and permit values. |
| Ratios, averages, z-scores | `FLOAT` | Matches Spark double and their approximate analytical meaning. |
| Source release timestamps | `TIMESTAMP_NTZ(6)` | Spark values were normalized in a UTC session; the convention is UTC without conversion. |
| Loader/audit timestamps | `TIMESTAMP_TZ(6)` | Preserves the explicit instant produced by Snowflake. |
| SHA-256 | `VARCHAR(64)` | Fixed hexadecimal lineage identifier. |

Snowflake supports exact `NUMBER` values up to 38 digits and treats `FLOAT` as
64-bit double precision, matching the distinction already present in Spark.
See [Snowflake numeric data types](https://docs.snowflake.com/en/sql-reference/data-types-numeric).

## Idempotent loading and publication

Phase 10 will follow this sequence only after Phase 8 validation succeeds:

1. Generate a UUID `LOAD_BATCH_ID` and insert an audit row with `STARTED`.
2. Remove any staging rows for that same batch ID, then stream the validated
   DataFrame through bounded `executemany()` inserts using `qmark` parameters.
3. Compare staged count, distinct natural-key count, and validated row count.
   Any mismatch marks the audit row `FAILED` and stops before final publication.
4. Start an explicit DML transaction.
5. Delete final rows in the exact reference-month window represented by the
   staged batch.
6. Insert the complete staged batch into the final table and update the audit
   row to `SUCCEEDED` with the published count.
7. Commit. On any statement failure, explicitly roll back and record `FAILED`
   outside the rolled-back transaction.

This is a bounded snapshot replacement rather than a row-only `MERGE`. It is
idempotent and removes stale keys that disappeared from a corrected source
release. A retry creates the same final business rows even if it uses a new
batch ID. DDL is never executed inside the publication transaction because
Snowflake DDL implicitly commits active transactions.

One Snowflake schema represents one deployment/profile scope: a development
schema receives the development window and a production schema receives the
full window. Alternating development and full profiles in the same target
schema would make a window replacement semantically ambiguous and is not
supported. Kubernetes will later use `concurrencyPolicy: Forbid`; Phase 10 will
also refuse a second active audit batch for the same target/profile.

Snowflake recommends explicit transactions while leaving autocommit enabled;
statements within `BEGIN TRANSACTION`/`COMMIT` remain atomic. See
[Snowflake transaction semantics](https://docs.snowflake.com/en/sql-reference/transactions).

## Clustering decision

No clustering key is defined. The full fact is expected to remain far below the
large, multi-terabyte tables for which Snowflake recommends considering
clustering, and automatic clustering consumes compute credits. Natural
micro-partition pruning should be measured first.

If query profiles later show material scanning at much larger scale, test
`CLUSTER BY (REFERENCE_MONTH, CMA_CODE)` against representative date/CMA
filters before enabling it. See [Snowflake clustering-key guidance](https://docs.snowflake.com/en/user-guide/tables-clustering-keys).
