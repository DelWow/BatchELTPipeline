# Dataset Decision: Canadian Metropolitan Housing Supply Pipeline

## Decision

The project will use monthly Canadian housing-supply data centered on the
Canada Mortgage and Housing Corporation (CMHC) Starts and Completions Survey,
enriched with a deliberately small set of related Statistics Canada indicators.

This is an analytical, pre-aggregated dataset. The portfolio claim will be
multi-source and multi-dimensional processing across years and geographies—not
millions of household- or building-level events that these sources do not
contain.

## Official sources

The initial source set is intentionally limited to four monthly tables:

| Role | Official table | Dimensions used | Why it is included |
| --- | --- | --- | --- |
| Core housing activity | [Statistics Canada 34-10-0154-01](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3410015401), sourced from CMHC | reference month, CMA, housing estimate, dwelling type | Provides starts, completions, and units under construction by dwelling type. |
| Core intended-market detail | [Statistics Canada 34-10-0148-01](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3410014801), sourced from CMHC | reference month, CMA, dwelling type, market type | Adds homeowner, rental, condominium, and co-operative detail for housing starts. |
| Leading indicator | [Statistics Canada 34-10-0292-01](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3410029201) | reference month, CMA, structure type, work type, value/units | Building permits provide context earlier in the supply pipeline. |
| Market context | [Statistics Canada 18-10-0205-01](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810020501) | reference month, CMA, price component | The New Housing Price Index provides price context without implying causality. |

CMHC's [housing starts data page](https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data/data-tables/housing-market-data/starts-completions-units-under-construction-geography)
and [intended-market data page](https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data/data-tables/housing-market-data/starts-completions-intended-market-cities)
remain the methodology references. The pipeline will use Statistics Canada's
official distribution of the CMHC series because stable table identifiers,
bulk downloads, and machine-readable metadata make backfills and reruns easier
to reproduce than workbook sheet layouts. Direct CMHC exports may be used for
reconciliation, but will not be appended to the same facts because that would
duplicate the CMHC observations.

## Scope and scale

- **Baseline window:** January 2016 through December 2025 (ten complete calendar
  years). Later monthly releases will be incremental pipeline runs, not part of
  the fixed benchmark window.
- **Geography:** retain CMA-level observations published by each source. Exclude
  Canada, province, census agglomeration, census subdivision, and CMA-part rows
  from the CMA analytical facts. Source coverage will be explicit because some
  tables contain only selected CMAs.
- **Development slice:** Toronto, Vancouver, and Calgary over 24 months. This is
  small enough for fast local tests while exercising multiple regions and time
  periods.
- **Full run:** all eligible CMA rows for the ten-year baseline across the four
  tables. The exact normalized row count will be measured after ingestion; no
  unsupported million-row claim will be made.
- **Native format:** Statistics Canada full-table ZIP downloads containing CSV
  data plus cube metadata. Raw files remain unchanged in the landing zone.

The full-table API is documented by Statistics Canada's
[Web Data Service](https://www.statcan.gc.ca/en/developers/wds) and
[CSV download guide](https://www.statcan.gc.ca/en/developers/csv/user-guide).
The future ingestion contract will pin each table ID, record the download and
release timestamps, retain the original ZIP, and calculate a SHA-256 checksum.

## Data model boundaries

The sources do not share one universal grain, so the clean layer will preserve
separate facts rather than manufacture unavailable dimensions:

1. **Housing activity fact:** CMA × reference month × dwelling type × activity
   measure, where the measure is starts, completions, or under construction.
2. **Starts by market fact:** CMA × reference month × dwelling type × intended
   market. This source measures starts; intended market will not be invented for
   completions or units under construction.
3. **Building permits fact:** CMA × reference month × structure/work dimensions.
4. **New-housing price fact:** CMA × reference month × index component.

Starts, completions, and permits are flows for a period. Under-construction
units are a stock at a point in time, so they may be compared month over month
but must not be summed across months. The New Housing Price Index covers fewer
CMAs than the core series; enrichment will therefore use a left join, retain a
coverage flag, and avoid fabricated values.

Compatible annual/geographic partitions within a fact will be unioned after
schema normalization. Different facts will be related through canonical
reference-month and CMA keys, not unioned together. A newly published full-table
snapshot may revise history, so it will replace the corresponding source
snapshot idempotently rather than being blindly appended.

## Analytics story

The analytics-ready layer will emphasize meaningful rollups and temporal
behavior:

- monthly, quarterly, and annual rollups by CMA and dwelling type;
- intended-market mix and market-share changes;
- starts-to-completions and starts-to-under-construction pipeline ratios, with
  stock/flow semantics clearly labelled;
- 3- and 12-month rolling measures, year-over-year change, and seasonal
  comparisons;
- lagged comparison with building permits and contextual comparison with the
  New Housing Price Index; and
- anomaly flags based on a documented robust seasonal or rolling baseline.

An anomaly is an observation requiring investigation, not proof of an error or
a forecast. Relationships among permits, construction activity, and prices are
descriptive and must not be presented as causal.

## Quality and revision risks

Validation will focus on the real risks in official aggregate data:

- schema or label drift in periodically republished tables;
- duplicate aggregate levels, totals, and overlapping geographies;
- unit/scalar mismatches, status symbols, suppression, and missing values;
- revised historical observations across publication snapshots;
- CMA name/code and census-boundary changes over time; and
- aggregation invariants, such as dwelling subtotals reconciling to totals when
  the source metadata says they should.

The pipeline will preserve the source geography label/code and a geography
definition/version field where available. It will not assume that identical CMA
names imply unchanged boundaries throughout the full period.

## Reuse terms and attribution

CMHC data is governed by the [CMHC data licence](https://www.cmhc-schl.gc.ca/professionals/housing-markets-data-and-research/housing-data/cmhc-licence-agreement-use-of-data),
which permits reuse with accurate reproduction, acknowledgment, and no implied
endorsement. Statistics Canada material is governed by the
[Statistics Canada Open Licence](https://www.statcan.gc.ca/en/terms-conditions/open-licence),
which has similar attribution and no-endorsement requirements.

Documentation and published outputs will identify the applicable table IDs and
reference dates, credit both Statistics Canada and CMHC for CMHC-sourced series,
state when content has been adapted, and avoid implying endorsement by either
organization.

## Assumptions to verify during ingestion

- Inspect the downloaded cube metadata before freezing raw schemas or dimension
  labels; table titles alone are not a schema contract.
- Confirm the earliest usable month and CMA coverage for every selected series
  in the 2016–2025 baseline.
- Measure actual row counts and missingness before choosing validation ranges.
- Confirm which observations are seasonally adjusted and never combine adjusted
  and unadjusted measures without an explicit dimension.
- Review source notes and correction metadata on each retrieval, because these
  tables can revise historical observations.
