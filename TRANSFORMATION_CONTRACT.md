# PySpark Cleaning Contract

Phase 6 converts native Statistics Canada observations into separate clean fact
shapes. It does not aggregate or join them; those operations belong to Phase 7.
Keeping source grains separate avoids a wide table full of inapplicable nulls
and makes each metric's business meaning explicit.

## Source grains and natural keys

| Source | Clean grain / deduplication key | Value meaning |
| --- | --- | --- |
| CMHC housing activity | month × CMA × housing measure × dwelling type | Starts and completions are monthly flows; under construction is a month-end stock. |
| CMHC starts by market | month × CMA × dwelling type × intended market | Monthly housing-start flow. |
| Building permits | month × CMA × building type × work type × variable × adjustment type | Value meaning and unit depend on the selected variable. |
| New Housing Price Index | month × CMA × index component | Index level, December 2016 = 100 in the current table. |

The geography portion of a key uses the three-digit CMA code when the DGUID
identifies a CMA. This reconciles an overlapping legacy/current geography series
without confusing a publisher-provided regional total with a CMA. Other
geographies fall back to DGUID and then their published name.

## Revisions and duplicates

Raw archives are immutable complete snapshots. When more than one release is
present, the newest source release wins for a natural key. Within a release, an
active series wins over a terminated duplicate, followed by revised and
publishable observations. The Statistics Canada vector is the deterministic
final tie-breaker. This policy applies source corrections without mutating or
discarding the older raw evidence.

Totals and component rows are not duplicates. Flags such as
`is_total_dwelling_type` and `is_total_index` preserve both views while making
it possible for Phase 7 to select either the published total or additive
components. Adding a total to its components would double-count and is never an
allowed rollup.

## Nulls, statuses, units, and scalars

Every CSV field is first read as a string under an explicit per-source schema.
This preserves the distinction between a blank numeric value and Statistics
Canada status symbols such as `..` (unavailable) and `x` (suppressed). Rows are
dropped only when a natural-key or immutable-provenance field is missing. A
coded missing observation remains in the clean fact with a null measure and
`is_publishable = false`.

`STATUS`, `SYMBOL`, and `TERMINATED` are retained and normalized into explicit
revision, preliminary, termination, suppression, and publishability flags. An
`E` quality marker remains publishable but visible as `status_code = E`, so
analysts can include it with caution rather than silently losing it.

`SCALAR_ID` is applied as an exact base-10 multiplier before the value is cast
to its clean type. Count facts accept non-negative integral results only, and a
price index must be positive. Values that violate these domain rules remain as
auditable rows with a null clean measure and `is_publishable = false`; no
arbitrary percentile cutoff is presented as a business rule. The original
unit, unit ID, scalar label/ID, vector, coordinate, decimals, release timestamp,
and archive SHA-256 remain on every clean row for traceability.

## ZIP handling and profile scope

The native ZIP is always the immutable raw artifact. Spark's CSV reader cannot
split CSV members directly inside ZIP files, so the reader byte-copies the CSV
member into a release/SHA-addressed `data/interim/extracted/` cache. It validates
the exact header before Spark reads it and refuses to overwrite a differently
sized existing extraction.

The development profile filters the typed data to January 2024 through December
2025 and Calgary, Toronto, and Vancouver. The full profile keeps only rows whose
DGUID identifies a CMA, excluding Canada/province/region and published aggregate
rows. Phase 6 performs no network calls and writes no curated analytics output.
