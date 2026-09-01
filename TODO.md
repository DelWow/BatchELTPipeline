# Batch ELT Pipeline TODO

This checklist is intentionally split into small, reviewable units. Each checked implementation item should be suitable for one focused future commit; Codex will not create commits unless explicitly asked to "commit this".

## Phase 1 — Choose the dataset

- [x] Inspect the workspace baseline (currently empty and not initialized as a Git repository).
- [x] Compare 2–3 public datasets for scale, license, access, and analytical value.
- [x] **Decision gate:** project owner selected CMHC Starts and Completions Survey data plus related Statistics Canada housing indicators.
- [x] Record the selected source, license/terms, data window, expected scale, native file format, and analytical boundaries in `DATASET.md`.

## Phase 2 — Establish Python tooling and repository hygiene

- [x] Choose and document `pyproject.toml` plus `uv.lock` for direct constraints and reproducible dependency resolution.
- [x] Add a Python 3.11 project-local `.venv` workflow in `DEVELOPMENT.md`.
- [x] Add `.gitignore` rules for downloaded data, generated output, secrets, caches, and local tooling.
- [x] Configure Ruff for formatting/linting and pytest as the test runner.
- [x] Confirm Git was already initialized; do not initialize, stage, commit, push, or otherwise mutate Git state.
- [x] Verify the locked Python environment and development tools can run, then pause for review.

## Phase 3 — Create the project structure and configuration

- [x] Create the minimal source, test, configuration, SQL, Kubernetes, and data directory structure.
- [x] Add an installable `housing_elt` package and read-only `show-config` entry points without pipeline behavior.
- [x] Add immutable typed configuration with safe local defaults and `HOUSING_ELT_` environment-variable overrides.
- [x] Document which paths are inputs, generated outputs, versioned configuration, and intentionally excluded from Git.
- [x] Verify package imports, both CLI entry points, configuration overrides, linting, formatting, and unit tests, then pause for review.

## Phase 4 — Define the ingestion contract

- [x] Record official PIDs, URLs, licences, monthly frequency, dimensions, native ZIP/CSV format, and source-specific availability in `config/sources.toml`.
- [x] Define the 2024–2025 three-CMA development slice and source-specific full benchmark windows without treating cube datapoints as row-level events.
- [x] Define immutable release/SHA-addressed raw paths and the source metadata plus generated manifest stored with every archive.
- [x] Define WDS, HTTP, size, SHA-256, ZIP CRC/member, path-safety, metadata, and coverage integrity checks.
- [x] Define bounded retry/timeout behavior, run-owned partial cleanup, atomic publication, idempotent skips, and non-destructive revision handling.
- [x] Validate the source registry and ingestion contract, then pause for review.

## Phase 5 — Implement and verify raw ingestion

- [x] Implement registry-driven, streamed Statistics Canada downloads into `data/raw/` without changing the native ZIP format.
- [x] Add release/SHA-addressed immutable publication, manifest verification, and idempotent rerun behavior.
- [x] Add bounded timeout/retry behavior, actionable logs, run-owned partial cleanup, and atomic publication.
- [x] Add mocked ingestion tests for success, retries, corrupt ZIP cleanup, unexpected hosts, idempotency, and local corruption.
- [x] Run the development profile, independently verify all three ZIPs, and confirm the rerun reports `already_present` without archive downloads.
- [x] Pause for review.

## Phase 6 — Clean source observations with PySpark

- [ ] Define explicit raw and clean Spark schemas for each source-specific fact grain.
- [ ] Implement raw reads and type normalization as testable functions.
- [ ] Implement required-field and null handling as a separate transformation.
- [ ] Implement duplicate/overlapping-aggregate handling using documented dimension keys.
- [ ] Implement revision, status-symbol, geography, unit/scalar, and stock-versus-flow handling.
- [ ] Add focused unit tests for schema enforcement, nulls, duplicates, revisions, and semantic rules.
- [ ] Run the cleaning flow against the development sample, then pause for review.

## Phase 7 — Build the analytics aggregation and partitioned output

- [ ] Define the grain and business meaning of the analytics-ready table.
- [ ] Implement CMA × dwelling type × time rollups and trend/anomaly measures as testable transformations separate from cleaning.
- [ ] Add permits and price-index joins with explicit CMA/month keys, coverage flags, and unmatched-row handling.
- [ ] Write curated Parquet output with a justified date partition.
- [ ] Add aggregation and partition-layout tests.
- [ ] Inspect representative output and partition sizes, then pause for review.

## Phase 8 — Add validation and assemble the local pipeline

- [ ] Implement schema and required-column validation.
- [ ] Implement row-count and key-column null-threshold validation.
- [ ] Add reconciliation checks between clean input and aggregated output where meaningful.
- [ ] Make validation failures stop the pipeline before any load step and log actionable reasons.
- [ ] Add validation success and failure-path unit tests.
- [ ] Assemble the ingestion, cleaning, aggregation, output, and validation steps behind one local entry point.
- [ ] Run the local pipeline against the development sample and inspect representative output.
- [ ] Pause for review.

## Phase 9 — Design the Snowflake loading approach and schema

- [ ] Compare `snowflake-connector-python` with the Spark–Snowflake connector for this pipeline.
- [ ] Choose and document the loading approach.
- [ ] Design staging and final table schemas, column types, keys, and any clustering choice.
- [ ] Define the idempotent staging and final-table merge/replace strategy.
- [ ] Add reviewed, repeatable SQL DDL for tables and required objects.
- [ ] Review the design without contacting Snowflake, then pause.

## Phase 10 — Implement and verify the Snowflake loader

- [ ] Implement credential loading from environment variables; never store secrets in the repository.
- [ ] Implement the staging load and final-table merge/replace strategy from Phase 9.
- [ ] Ensure validation must succeed before the loader can run.
- [ ] Add mocked unit tests for load logic, idempotency, and failure behavior.
- [ ] **Decision gate:** obtain approval before connecting to or provisioning a real Snowflake account, warehouse, database, schema, stage, or other billable resource.
- [ ] If approved, run a bounded integration test and verify source/target counts.
- [ ] Pause for review.

## Phase 11 — Containerize the batch job

- [ ] Add a lean, pinned Dockerfile for a single-node Spark job and document the base-image choice.
- [ ] Add a `.dockerignore` so local data, output, secrets, and caches are excluded from the build context.
- [ ] Build the image locally.
- [ ] Run the small-sample pipeline in the container and verify output and failure codes.
- [ ] Pause for review.

## Phase 12 — Schedule and verify the job on local Kubernetes

- [ ] Confirm `kind` or `minikube` (default recommendation: `kind`, subject to review).
- [ ] Add a namespace and non-secret runtime configuration manifest.
- [ ] Add a documented Kubernetes Secret template without real credentials.
- [ ] Add a commented CronJob manifest with resource requests/limits, retry policy, concurrency policy, and history limits.
- [ ] Add local storage/data-mount handling appropriate to the selected local cluster.
- [ ] Document how to load the local Docker image into the selected cluster.
- [ ] Run the CronJob locally and verify pod completion, logs, outputs, and failed-validation behavior.
- [ ] Pause for review.

## Phase 13 — Portfolio documentation and final review

- [ ] Write the project overview and business/analytical use case.
- [ ] Add a Mermaid or ASCII architecture diagram.
- [ ] Document prerequisites and exact local end-to-end commands.
- [ ] Explain component, schema, partitioning, validation, connector, container, and Kubernetes choices.
- [ ] Document secrets handling, cost boundaries, known limitations, and production-scale improvements.
- [ ] Add a troubleshooting section and expected sample outputs.
- [ ] Run formatting, linting, unit tests, local sample execution, container execution, and Kubernetes smoke checks.
- [ ] Review the repository for secrets, generated data, stale instructions, and unclear interview talking points.
- [ ] Show final `git status`/diff if Git has been initialized; leave all changes uncommitted unless explicitly told "commit this".
- [ ] Pause for final owner review.
