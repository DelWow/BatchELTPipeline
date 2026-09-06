# Local Kubernetes Runbook

Phase 12 uses `kind` because its Kubernetes nodes are ordinary Docker
containers. It can load `canadian-housing-elt:local` without a registry, reuses
Docker Desktop already required by Phase 11, and avoids the VM and image-daemon
differences of minikube. No cloud cluster or paid resource is involved.

## Prerequisites

- Docker Desktop with at least 4 CPUs and 6 GiB memory available
- `kind` v0.32.0
- `kubectl` v1.36.1 (kept project-local below to match the cluster exactly)

This project keeps both tools local rather than installing them globally. For
macOS arm64, run from the repository root and verify the published checksums:

```bash
mkdir -p .tools
curl -Lo .tools/kind https://kind.sigs.k8s.io/dl/v0.32.0/kind-darwin-arm64
echo "dca67911095a110c2b5c36e26df6cac860c602033e456c0db47be498cdef1ebb  .tools/kind" | shasum -a 256 -c
chmod 0755 .tools/kind
curl -Lo .tools/kubectl https://dl.k8s.io/release/v1.36.1/bin/darwin/arm64/kubectl
echo "9092778abaef3079449da4cd70ded0e4be112480c93efcdeace3155968d1d133  .tools/kubectl" | shasum -a 256 -c
chmod 0755 .tools/kubectl
```

Use the appropriate official asset and checksum on another OS/architecture.
`.tools/` is ignored and no shell configuration is changed. Pinning the client
to the node version also keeps this runbook inside Kubernetes' supported
`kubectl` version-skew range, regardless of another client installed globally.

## Create the cluster

Run from the repository root because kind resolves `./data/raw` in the cluster
configuration against the current directory:

```bash
.tools/kind create cluster --config k8s/kind-cluster.yaml
.tools/kubectl cluster-info --context kind-housing-elt
```

The single-node cluster is pinned to Kubernetes 1.36.1. kind passes the local
raw landing directory into its node read-only; the Pod then mounts that node
path read-only. Intermediate, curated, checkpoint, Spark-home, and temporary
volumes are bounded `emptyDir` scratch space and disappear with each Pod.

## Load the local image

Kubernetes cannot see images stored only in Docker Desktop's host image store.
Load the Phase 11 image into the named kind node explicitly:

```bash
.tools/kind load docker-image canadian-housing-elt:local --name housing-elt
docker exec housing-elt-control-plane crictl images | grep canadian-housing-elt
```

The CronJob uses `imagePullPolicy: Never`, so a missing load fails visibly
instead of silently trying a public registry.

## Apply and inspect the scheduled workload

```bash
.tools/kubectl apply -k k8s
.tools/kubectl -n housing-elt get cronjob housing-elt-monthly
.tools/kubectl -n housing-elt describe cronjob housing-elt-monthly
```

The schedule is 11:00 UTC on the fifth day of each month. `Forbid` prevents
overlap, a six-hour deadline skips stale missed schedules, each Job can retry
twice, and 30 minutes bounds an attempt. History limits plus a one-day TTL keep
local cluster state bounded. The CronJob reads no Snowflake Secret and uses
`--skip-ingestion`; it demonstrates scheduled transformation/validation from
the already landed immutable snapshots.

## Run and verify immediately

Create a one-off Job from the exact CronJob template instead of waiting for the
monthly schedule:

```bash
.tools/kubectl -n housing-elt create job \
  --from=cronjob/housing-elt-monthly housing-elt-smoke
.tools/kubectl -n housing-elt wait \
  --for=condition=complete job/housing-elt-smoke --timeout=20m
.tools/kubectl -n housing-elt logs job/housing-elt-smoke
```

Expected log summary: validation passes with 360 rows, two years, 15 anomaly
flags, and 360 missing-permit rows by development-profile design.

## Verify fail-closed validation

The smoke overlay produces a separately named, suspended CronJob with a
test-only validation ConfigMap. It expects 361 rows, ensuring the actual
360-row fact fails before publication:

```bash
.tools/kubectl apply -k k8s/smoke-failure
.tools/kubectl -n housing-elt create job \
  --from=cronjob/housing-elt-monthly-failure housing-elt-validation-failure
.tools/kubectl -n housing-elt wait \
  --for=condition=failed job/housing-elt-validation-failure --timeout=20m
.tools/kubectl -n housing-elt logs job/housing-elt-validation-failure
```

Expected log: `row_count: observed 360; expected [361, 361]`. The overlay sets
`backoffLimit: 0` to avoid repeating a deterministic validation failure.

## Verified local result

The Phase 12 smoke run used kind v0.32.0, the project-local kubectl v1.36.1,
and the pinned Kubernetes 1.36.1 node:

- `housing-elt-smoke` completed in 18 seconds; its Pod phase was `Succeeded`
  with exit code 0 and no image pull.
- The success log reported 360 analytics rows, two years, 15 anomaly flags,
  and `/app/data/curated/housing_monthly`. That line is emitted only after the
  year-partitioned Parquet writer returns successfully.
- `housing-elt-validation-failure` reached the Job `Failed` condition with exit
  code 1, zero container restarts, and the expected 360-versus-361 row-count
  message.
- No Snowflake Secret was created and neither workload used
  `--load-snowflake`.

## Secrets and the Snowflake gate

`k8s/snowflake-secret.example.yaml` is documentation only and is excluded from
the main Kustomization. Do not edit it with real values or apply it. When a real
Snowflake integration is approved, create the Secret from a secure local source
or external secret manager, add `envFrom.secretRef`, and explicitly add
`--load-snowflake` to the workload. Kubernetes Secrets require separate
encryption-at-rest and rotation controls in production.

## Local cleanup

The commands below delete local smoke Jobs or the entire disposable cluster.
Run them only when their outputs are no longer needed:

```bash
.tools/kubectl -n housing-elt delete job housing-elt-smoke housing-elt-validation-failure
.tools/kind delete cluster --name housing-elt
```
