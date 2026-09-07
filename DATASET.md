# Dataset notes

The core data comes from CMHC's Starts and Completions Survey. I use the
Statistics Canada distribution of those series because it provides stable table
IDs, bulk CSV downloads and machine-readable cube metadata. Two Statistics
Canada indicators add permit and price context.

These are aggregate monthly series. A row is not a home, permit application or
transaction.

## Tables

| Role | Table | Dimensions used |
| --- | --- | --- |
| Housing activity | [34-10-0154-01](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3410015401), sourced from CMHC | month, CMA, activity measure, dwelling type |
| Intended market | [34-10-0148-01](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3410014801), sourced from CMHC | month, CMA, dwelling type, market type |
| Building permits | [34-10-0292-01](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3410029201) | month, CMA, building type, work type, value type |
| New Housing Price Index | [18-10-0205-01](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810020501) | month, CMA, index component |

CMHC's [housing activity](https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data/data-tables/housing-market-data/starts-completions-units-under-construction-geography)
and [intended-market](https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data/data-tables/housing-market-data/starts-completions-intended-market-cities)
pages remain the methodology references. Direct CMHC exports are not appended
to the Statistics Canada versions because they describe the same observations.

## Profiles

The fixed benchmark window is January 2016 through December 2025. The current
building-permits table starts in January 2018, so that source uses 2018–2025.

The development profile keeps January 2024 through December 2025 for:

- Toronto (CMA 535)
- Calgary (CMA 825)
- Vancouver (CMA 933)

It downloads the two CMHC tables and the price index. The permits ZIP is about
365 MB, so it is reserved for the full profile. The full profile keeps all CMA
rows available in each source's benchmark window.

Statistics Canada's full-table endpoint cannot filter by date or geography.
Raw ZIPs therefore contain each cube's full published history even when the
development output is small. The three development archives currently contain
1,208,140 datapoints before filtering.

Raw files remain in the publisher's ZIP/CSV format. See
[INGESTION_CONTRACT.md](INGESTION_CONTRACT.md) for versioning and integrity
rules.

## Source grains

The clean layer keeps separate facts because the tables do not share one grain:

- housing activity: month × CMA × activity measure × dwelling type;
- intended-market starts: month × CMA × dwelling type × market type;
- building permits: month × CMA × building/work/value dimensions; and
- price index: month × CMA × index component.

Intended market applies only to starts. It is not inferred for completions or
units under construction.

Starts, completions and permit values are monthly flows. Units under
construction are a month-end stock, so summing that field through time would be
wrong. The price and permits tables cover fewer CMAs than the core housing
series; the joins retain nulls and explicit coverage flags.

Compatible rows are normalized within each fact before the sources are joined
on month and CMA. Full-history snapshots overlap and may revise earlier values,
so the pipeline selects one release per source rather than unioning releases.

## Analytics use

The serving table supports CMA and dwelling-type comparisons, intended-market
mix, rolling starts, year-over-year change, month-over-month changes in the
under-construction stock, and a prior-12-month anomaly score.

The anomaly flag is a prompt to inspect an observation. It is not an error label
or forecast. Price, permit and construction fields are shown together as
context; the pipeline does not make causal claims about them.

## Known source issues

The main risks are label/schema changes, overlapping geography totals, scalar
changes, suppression/status symbols, historical revisions and changing CMA
boundaries. Source geography names, codes, status fields and release lineage
remain available after cleaning so those cases can be investigated.

Published totals and components are both useful but cannot be added together.
Where a complete component set exists, validation checks it against the
publisher's total.

## Licence and attribution

Statistics Canada material is used under the
[Statistics Canada Open Licence](https://www.statcan.gc.ca/en/terms-conditions/open-licence).
The two CMHC-origin tables are also subject to the
[CMHC data licence](https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data/cmhc-licence-agreement-use-of-data).

Outputs should name the table IDs and reference period, credit Statistics
Canada and CMHC where applicable, identify adapted content and avoid implying
endorsement by either organization.
