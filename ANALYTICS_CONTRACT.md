# Analytics table

`data/curated/housing_monthly/` contains one row per:

> reference month × CMA × canonical dwelling type

It is built from aggregate monthly series, not building-level records.

## Dwelling types

The two CMHC tables use slightly different names for the same categories. They
map to:

| Canonical value | Housing activity | Intended market |
| --- | --- | --- |
| `total` | Total units | Total units |
| `single_detached` | Single-detached units | Single units |
| `semi_detached` | Semi-detached units | Semi-detached units |
| `row` | Row units | Row units |
| `apartment_and_other` | Apartment and other unit types | Apartment and other types of units |

An unknown label is kept with an `unmapped_` prefix so source drift is visible.
Published totals remain separate from components and must not be added to them.

## Core housing fields

The activity table is pivoted into `housing_starts`, `housing_completions` and
`housing_under_construction`. Starts and completions are monthly flows; under
construction is a month-end stock. `completion_to_start_ratio` is null when
starts are zero.

Intended-market starts are pivoted into homeowner, rental, condominium,
co-operative and other-market fields. `market_starts_total` is the sum of the
available market members. Completeness flags record whether all expected
activity measures and market members were present.

The two rollups are full-joined on month, CMA and dwelling type. One-sided keys
stay in the table with `has_activity_data` and `has_market_data` flags instead
of disappearing in an inner join.

## Context fields

The New Housing Price Index is pivoted into total, house-only and land-only
indexes at month/CMA grain, then repeated across the five dwelling rows. Price
coverage and component completeness have separate flags.

Building permits use one series to avoid overlapping totals:

- building type: `Total residential`
- work type: `Types of work, total`
- variable: `Value of permits`
- adjustment: `Seasonally adjusted, current`

The value is in dollars after cleaning applies Statistics Canada's scalar. The
development profile does not download permits, so it produces a typed null and
`has_permit_data = false`, not a zero.

## Time features

Windows are partitioned by CMA and dwelling type and ordered by month. A gap in
the monthly sequence invalidates calculations that depend on adjacency.

- `starts_3_month_average`: current month and previous two months
- `starts_year_over_year_pct`: current starts versus 12 months earlier
- `under_construction_month_change`: current stock minus previous month
- `starts_prior_12_month_average`: mean of the previous 12 months
- `starts_prior_12_month_stddev`: sample standard deviation of those months
- `starts_anomaly_zscore`: current starts relative to that prior baseline
- `starts_anomaly_flag`: absolute z-score of at least 2

The anomaly baseline excludes the current month and all future observations.
The flag is a screening aid, not an error classification.

## Output layout

Parquet is partitioned by `reference_year`. Monthly partitions would create too
many small files for this table, while yearly partitions still support common
date filters. Rows within a year are sorted by month, CMA and dwelling type.

The local writer replaces the generated dataset as one unit after validation.
Raw archives are never changed. On object storage, the equivalent design would
write a versioned dataset and promote a manifest rather than overwrite a path.
