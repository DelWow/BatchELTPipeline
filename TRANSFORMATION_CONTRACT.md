# Cleaning rules

Spark cleans each source into its own fact before any joins or analytics. This
keeps source-specific dimensions visible and avoids a wide intermediate table
full of fields that do not apply.

## Grains and keys

| Source | Clean grain | Measure |
| --- | --- | --- |
| CMHC housing activity | month × CMA × housing measure × dwelling type | starts/completions flow or month-end under-construction stock |
| CMHC starts by market | month × CMA × dwelling type × intended market | monthly starts |
| Building permits | month × CMA × building type × work type × variable × adjustment | value or units, depending on the variable |
| New Housing Price Index | month × CMA × index component | index level, December 2016 = 100 |

CMA identity uses the three-digit code when the DGUID represents a CMA. Other
geographies fall back to DGUID and then the published name. This lets current
and legacy CMA labels reconcile without confusing them with provincial or
national totals.

## Raw types

Every CSV field is first read as a string under a source-specific schema. That
matters because blank numeric fields and Statistics Canada symbols such as `..`
or `x` do not mean zero.

Rows are dropped only when a natural-key or provenance field is missing. A
coded missing value remains in the clean fact with a null measure and
`is_publishable = false`.

`STATUS`, `SYMBOL` and `TERMINATED` are normalized into revision, preliminary,
termination, suppression and publishability flags. An estimated value (`E`)
remains usable but keeps its status code.

## Values and units

`SCALAR_ID` is applied as an exact power-of-ten multiplier before casting.
Housing counts must be non-negative integers; price indexes must be positive.
An invalid value becomes a non-publishable null rather than being clipped or
silently removed.

Each clean row keeps its unit, scalar, vector, coordinate, decimals, release
timestamp and archive SHA-256.

## Duplicates and revisions

The natural keys above define duplicates. When several raw snapshots contain a
key, the newest source release wins. Within a release, the order is active over
terminated, revised over unrevised, publishable over non-publishable, then
Statistics Canada vector as the deterministic tie-breaker.

Publisher totals are not duplicates of their components. Both stay in the clean
facts with flags that let the analytics layer choose the correct level.

## ZIP extraction

Spark cannot read an individual CSV member directly from a ZIP. The reader
copies that member byte-for-byte into
`data/interim/extracted/<release>/<sha>/`, validates its header and refuses to
overwrite an existing extraction of a different size.

The development profile then filters to Calgary, Toronto and Vancouver from
January 2024 through December 2025. Cleaning performs no network calls and does
not write the curated dataset.
