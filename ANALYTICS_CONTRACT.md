# Analytics Fact Contract

Phase 7 publishes one analytics-ready Parquet dataset at
`data/curated/housing_monthly/`. Its grain is:

> one row per reference month × census metropolitan area (CMA) × canonical
> dwelling type

This table supports comparison across time and CMAs without claiming that the
source observations are individual housing records. It is an analysis cube
built from pre-aggregated official monthly series.

## Canonical dwelling types

The two CMHC tables use slightly different labels for equivalent categories.
They are mapped explicitly:

| Canonical value | Housing activity label | Intended-market label |
| --- | --- | --- |
| `total` | Total units | Total units |
| `single_detached` | Single-detached units | Single units |
| `semi_detached` | Semi-detached units | Semi-detached units |
| `row` | Row units | Row units |
| `apartment_and_other` | Apartment and other unit types | Apartment and other types of units |

An unexpected source label is retained with an `unmapped_` prefix instead of
being dropped or silently assigned to a known category. Published `total` rows
remain separate from components. Totals and components must never be summed
together.

## Housing measures and rollups

The activity source is pivoted into `housing_starts`, `housing_completions`, and
`housing_under_construction`. Starts and completions are monthly flows; under
construction is a month-end stock. `completion_to_start_ratio` is null when
starts are zero, avoiding undefined division.

The intended-market source is pivoted into homeowner, rental, condominium,
co-operative, and other-market starts. `market_starts_total` is their sum when
at least one market member is present. `has_complete_activity` requires all
three activity measures; `has_complete_market_breakdown` requires all five
market members.

Activity and intended-market rollups are full-joined on month, CMA code, and
canonical dwelling type. This deliberately retains one-sided keys and marks
them using `has_activity_data` and `has_market_data`; an inner join would hide a
coverage problem.

## Context indicators

New Housing Price Index (NHPI) rows are pivoted to total, house-only, and
land-only indexes at month/CMA grain. They are left-joined to the housing fact
and repeated across dwelling types. Coverage is explicit through
`has_price_index_data` and `has_complete_price_index`.

Building permits use one non-overlapping context series:

- building type: `Total residential`
- work type: `Types of work, total`
- variable: `Value of permits`
- adjustment: `Seasonally adjusted, current`

The cleaning layer has already applied Statistics Canada's scalar, so
`residential_permit_value_dollars` is expressed in dollars. Permit context is
also left-joined on month/CMA with `has_permit_data`. The development profile
does not ingest the large permit archive; it therefore writes typed null permit
values and `has_permit_data = false` rather than fabricating zeros.

## Trend and anomaly measures

All windows partition by CMA and canonical dwelling type and order by month.
They require contiguous months so missing periods are not treated as adjacent:

- `starts_3_month_average`: current and prior two months.
- `starts_year_over_year_pct`: current starts versus the same month one year
  earlier; null when the prior value is zero or unavailable.
- `under_construction_month_change`: current stock minus the immediately prior
  month's stock.
- `starts_prior_12_month_average` and `starts_prior_12_month_stddev`: the 12
  months before the current month, excluding the current observation.
- `starts_anomaly_zscore`: current starts relative to that prior-only baseline.
- `starts_anomaly_flag`: absolute z-score at least 2; null when no valid
  baseline exists.

Excluding the current and future observations from the anomaly baseline avoids
look-ahead leakage. The z-score is a transparent screening signal, not proof of
an error or a causal event.

## Partition and replacement policy

Parquet output is partitioned by `reference_year`, not month. At this fact's
compact grain, monthly partitions would produce many tiny files. Year
partitions still support date pruning while keeping a practical file layout.
Rows are sorted within year writer partitions by month, CMA, and dwelling type.

The dataset is reproducible from immutable raw snapshots and reviewed code, so
the Phase 7 writer replaces the generated dataset as one unit on rerun. Raw
archives are never modified. A production object-store implementation would
write to a versioned location and atomically promote a manifest instead of
depending on filesystem overwrite semantics.
