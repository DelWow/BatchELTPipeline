# Validation policy

Validation sits between analytics construction and publication. If any check
fails, the command returns exit code 1 and neither the Parquet writer nor the
Snowflake loader is called.

Thresholds are versioned in `config/validation.toml`. Development and full
profiles differ because their source coverage and expected row counts differ.

## Checks

### Schema

Required columns must exist with the expected Spark types. The contract checks
keys, measures, coverage flags and anomaly output. Extra analytical columns are
allowed so a compatible addition does not require a contract rewrite.

### Rows and nulls

The development profile expects exactly 360 rows. The full profile uses a range
because CMA coverage can change between source releases.

Key columns cannot be null. Measure null rates are compared with profile limits.
The development profile allows permits to be entirely null because that source
is not downloaded; its core housing and price measures require complete
coverage.

### Natural key and dwelling coverage

The natural key is `reference_month`, `cma_code`, `dwelling_type`. Duplicate
keys are counted as excess rows and are not allowed.

For each CMA/month, the development output must contain exactly these five
dwelling values:

```text
total
single_detached
semi_detached
row
apartment_and_other
```

The `reference_year` partition column must agree with `reference_month`.

### Reconciliation

When both sources are available, `housing_starts` must match the sum of the five
intended-market fields for that row.

For every complete CMA/month component set, the four non-total dwelling values
must add to the published total for starts, completions and units under
construction. Totals and components are compared here; they are not combined in
analytics.

The report also records activity-only keys, market-only keys, missing price
context, missing permit context and anomaly counts. These are coverage metrics,
not automatically failures unless the profile sets a limit for them.

## Failure output

All applicable checks run before the error is raised, so one run can report
several issues. Messages include the check name, observed value and expected
bound, for example:

```text
Analytics validation failed with 1 issue(s):
row_count: observed 359; expected [360, 360]
```

Schema failures return first because later metric queries may not be safe when
required columns or types are missing.

## Verified development result

- 360 rows
- 0 duplicate keys
- 0 key nulls
- 0 incomplete dwelling groups
- 0 mismatches across 360 market reconciliations
- 0 mismatches across 216 total/component comparisons
- 0 missing price rows
- 360 expected missing-permit rows
- 15 anomaly flags

Both the unit suite and the Docker/Kubernetes smoke tests include a forced
row-count failure to confirm that publication is skipped.
