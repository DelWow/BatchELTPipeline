# Snowflake design

The loader and SQL are complete, but they have not been run against a live
Snowflake account. The DDL does not create a database, warehouse, user, role or
grants; those remain environment-specific and may incur charges.

## Connector choice

The project uses `snowflake-connector-python` with `qmark` binding and bounded
`executemany()` batches.

Spark does the expensive work before this boundary. The resulting table is 360
rows in development and should remain in the tens of thousands for the full
benchmark. The loader can therefore stream rows with
`DataFrame.toLocalIterator()` without collecting the whole DataFrame or adding
a Spark plugin, JDBC driver and Scala-version dependency to the image.

The Spark–Snowflake connector would make more sense for a much larger transfer
or when Snowflake is also a Spark source. A staged Parquet `PUT`/`COPY INTO`
load is another option if connector batches become a bottleneck.

References:

- [Snowflake Connector for Spark](https://docs.snowflake.com/en/user-guide/spark-connector)
- [Python connector batch inserts](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-example)
- [Python connector API](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api)

## Tables

The DDL in `sql/001_create_housing_analytics.sql` creates the
`HOUSING_ANALYTICS` schema and three tables.

### `STG_HOUSING_MONTHLY`

A transient, batch-addressed copy of the validated Spark output. Staging rows
include the load UUID, validation profile and load timestamp. Transient storage
is sufficient because staging can be rebuilt from the immutable source files.

### `FCT_HOUSING_MONTHLY`

The serving grain matches Parquet:

> one `REFERENCE_MONTH × CMA_CODE × DWELLING_TYPE` row

The table carries source release/checksum lineage, coverage flags and the batch
that last published each row. Optional context and trend fields are nullable.

There is no declared primary key. Snowflake does not enforce primary or unique
constraints on standard tables, so the pipeline's validation and staging
reconciliation provide the real uniqueness check. `NOT NULL` constraints are
used because Snowflake does enforce them.

### `ELT_LOAD_AUDIT`

One row per attempted batch records status, profile, date window, validation
metrics, row counts, timestamps and a bounded error message.

References:

- [Transient tables](https://docs.snowflake.com/en/user-guide/tables-storage-considerations)
- [Constraint enforcement](https://docs.snowflake.com/en/sql-reference/constraints-overview)

## Type mapping

| Spark value | Snowflake type | Notes |
| --- | --- | --- |
| reference month | `DATE` | No time component |
| counts | `NUMBER(38,0)` | Exact integers |
| indexes and permit values | matching `NUMBER(p,4)` | Keeps decimal precision |
| ratios, averages and z-scores | `FLOAT` | Approximate analytical values |
| source release timestamps | `TIMESTAMP_NTZ(6)` | Stored using the pipeline's UTC convention |
| load/audit timestamps | `TIMESTAMP_TZ(6)` | Preserves the instant |
| SHA-256 | `VARCHAR(64)` | Lowercase hexadecimal digest |

See [Snowflake numeric types](https://docs.snowflake.com/en/sql-reference/data-types-numeric)
for the `NUMBER` and `FLOAT` behavior.

## Publication transaction

The loader runs only after validation:

1. create a UUID and insert a `STARTED` audit row;
2. clear any staging rows for that UUID;
3. stream rows into staging in batches of 1,000;
4. compare staged count, distinct key count and validated count;
5. start a transaction;
6. delete final rows in the staging batch's month range;
7. insert the staged snapshot and mark the audit row `SUCCEEDED`; and
8. commit, or roll back and record `FAILED`.

Replacing the complete month range removes stale keys when a corrected source
release drops an observation. Retrying the same business snapshot converges on
the same final rows even though the load UUID changes.

One target schema should receive one profile. Mixing development and full
windows in the same schema would make date-range replacement ambiguous. The
loader rejects a second active batch for the same profile, while the Kubernetes
CronJob also prevents overlapping Jobs.

DDL stays outside the transaction because Snowflake DDL implicitly commits.
The transaction behavior is documented in
[Snowflake transactions](https://docs.snowflake.com/en/sql-reference/transactions).

## Clustering

No clustering key is configured. This table is far below the scale where
automatic clustering is likely to pay for itself. If query history eventually
shows poor pruning, `CLUSTER BY (REFERENCE_MONTH, CMA_CODE)` should be tested
against representative filters before enabling it.

See [Snowflake clustering keys](https://docs.snowflake.com/en/user-guide/tables-clustering-keys).
