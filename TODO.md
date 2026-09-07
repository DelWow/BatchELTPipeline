# Project status

## Complete

- [x] Select and document the CMHC/Statistics Canada sources.
- [x] Set up the Python 3.11 environment, dependency lock, linting and tests.
- [x] Implement streamed, immutable and idempotent source ingestion.
- [x] Add explicit PySpark schemas and source-specific cleaning rules.
- [x] Build the monthly CMA/dwelling analytics table and time features.
- [x] Write year-partitioned Parquet output.
- [x] Add schema, null, uniqueness, coverage and reconciliation checks.
- [x] Implement the local end-to-end command and forced-failure tests.
- [x] Design the Snowflake tables and transactional publication flow.
- [x] Implement and mock-test the Snowflake loader.
- [x] Build and test the non-root Docker image.
- [x] Run success and failure Jobs on a local `kind` cluster.
- [x] Document local, container, Kubernetes and Snowflake workflows.

## Still optional

- [ ] Run a bounded Snowflake integration test after approving account and
  warehouse usage.
- [ ] Run the full profile, including the large building-permits archive.
- [ ] Add CI for linting, tests and container checks.
- [ ] Add persistent/object storage and external secrets for a non-local
  deployment.

The verified development profile remains the supported demo path. The first two
items above use additional bandwidth or paid infrastructure and should not run
implicitly.
