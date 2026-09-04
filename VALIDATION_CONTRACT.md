# Analytics Validation Contract

Phase 8 introduces a fail-closed quality gate between analytics construction
and Parquet publication. The versioned thresholds live in
`config/validation.toml`; code defines how each metric is calculated. A failed
gate raises one `DataValidationError` containing all observed failures, and the
writer is not called.

## Checks

### Schema and required columns

The validator checks the presence and exact Spark type of essential key,
measure, context, coverage, and anomaly columns. Policy key and null-threshold
columns are also required. Schema checks run before metric queries so a renamed
or incorrectly typed field produces a direct contract error instead of a later
Spark analysis exception.

### Row count, keys, and domains

The development fact must contain exactly 360 rows:

```text
24 months × 3 CMAs × 5 canonical dwelling types = 360
```

The full profile uses a reviewed range because available CMAs can change with
source geography revisions. All profiles reject null natural-key fields and
duplicate month/CMA/dwelling keys. The dwelling domain is limited to the five
reviewed canonical types, and each CMA/month must contain the complete set.

### Null thresholds

Null fractions are measured as null rows divided by total fact rows. The
development slice requires complete activity, intended-market, and NHPI values.
Its residential permit-value null threshold is 1.0 because that profile
explicitly does not ingest the large permits archive. A zero permit value would mean a real
published zero; it must not be substituted for missing coverage.

The full profile allows bounded core gaps and wider NHPI/permit gaps because
those tables do not cover every CMA represented by the two CMHC sources. These
limits are initial reviewed operating thresholds and must be re-evaluated after
the first approved full-profile run.

### Reconciliation

Two independent reconciliations detect transformations that can pass simple
row-count checks while producing wrong measures:

1. Each activity `housing_starts` value must equal the sum of its five
   intended-market start categories (`market_starts_total`).
2. For starts, completions, and under-construction stock, the published `total`
   dwelling value must equal the sum of the four component dwelling types at
   the same CMA/month.

The validator also requires `reference_year` to agree with `reference_month`.
Eligible comparison counts and mismatch fractions are included in the returned
report so a passing result remains auditable.

## Execution order

The local pipeline order is:

```text
optional ingestion
  → source cleaning and revision resolution
  → analytics aggregation and feature windows
  → validation (all checks)
  → partitioned Parquet publication
```

`housing-elt run` includes idempotent ingestion by default. Passing
`--skip-ingestion` runs offline against existing immutable snapshots. The older
`housing-elt aggregate` command also uses the same mandatory validation gate;
it cannot bypass quality checks.

This gate will also sit before the Snowflake loader in Phase 10. Phase 8 does
not contact Snowflake or provision any paid resource.
