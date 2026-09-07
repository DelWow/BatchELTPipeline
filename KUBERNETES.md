# Running on local Kubernetes

This setup uses `kind`. Its nodes run as Docker containers, which makes it easy
to reuse the local image without setting up a registry. Nothing here creates a
cloud cluster.

## Requirements

- Docker Desktop with roughly 4 CPUs and 6 GiB available
- `kind` 0.32.0
- `kubectl` 1.36.1

The following macOS arm64 commands install both Kubernetes tools under the
ignored `.tools/` directory. The checksums are from their published releases.

```bash
mkdir -p .tools
curl -Lo .tools/kind https://kind.sigs.k8s.io/dl/v0.32.0/kind-darwin-arm64
echo "dca67911095a110c2b5c36e26df6cac860c602033e456c0db47be498cdef1ebb  .tools/kind" | shasum -a 256 -c
chmod 0755 .tools/kind

curl -Lo .tools/kubectl https://dl.k8s.io/release/v1.36.1/bin/darwin/arm64/kubectl
echo "9092778abaef3079449da4cd70ded0e4be112480c93efcdeace3155968d1d133  .tools/kubectl" | shasum -a 256 -c
chmod 0755 .tools/kubectl
```

Use the matching official binaries and checksums on another operating system or
architecture. Keeping `kubectl` at the node version also avoids client/server
version-skew problems.

## Create the cluster

Run this from the repository root. The cluster configuration resolves
`./data/raw` relative to the current directory.

```bash
.tools/kind create cluster --config k8s/kind-cluster.yaml
.tools/kubectl cluster-info --context kind-housing-elt
```

The cluster has one Kubernetes 1.36.1 control-plane node. `data/raw` is mounted
read-only into that node and then read-only into the workload Pod.

## Load the image

An image in Docker Desktop's host store is not automatically visible inside a
kind node. Load it explicitly:

```bash
.tools/kind load docker-image canadian-housing-elt:local --name housing-elt
docker exec housing-elt-control-plane crictl images | grep canadian-housing-elt
```

The CronJob uses `imagePullPolicy: Never`, so forgetting this step produces a
clear local-image error instead of an attempted public pull.

## Apply the CronJob

```bash
.tools/kubectl apply -k k8s
.tools/kubectl -n housing-elt get cronjob housing-elt-monthly
```

The schedule is 11:00 UTC on the fifth of each month. Overlapping runs are
forbidden. A Job may retry twice, has a 30-minute deadline and is removed after
one day. Requests are 500m CPU and 1 GiB memory; limits are 2 CPUs and 3 GiB.

The container runs as UID/GID 10001 with a read-only root filesystem, no added
capabilities and no service-account token. Intermediate, curated, checkpoint,
home and temporary paths use size-limited `emptyDir` volumes. Those outputs are
temporary and disappear with the Pod.

The scheduled command includes `--skip-ingestion`. Raw snapshots must already
exist on the host; the local CronJob is meant to exercise scheduling and
failure handling, not act as an unattended production pipeline.

## Run now

Create a one-off Job from the same Pod template instead of waiting for the
monthly schedule:

```bash
.tools/kubectl -n housing-elt create job \
  --from=cronjob/housing-elt-monthly housing-elt-smoke
.tools/kubectl -n housing-elt wait \
  --for=condition=complete job/housing-elt-smoke --timeout=20m
.tools/kubectl -n housing-elt logs job/housing-elt-smoke
```

The verified run completed with 360 rows, two years and 15 anomaly flags.

## Check the failure path

`k8s/smoke-failure` creates a second, suspended CronJob with a validation file
that requires 361 rows. Its Jobs have no retry because the failure is
deterministic.

```bash
.tools/kubectl apply -k k8s/smoke-failure
.tools/kubectl -n housing-elt create job \
  --from=cronjob/housing-elt-monthly-failure housing-elt-validation-failure
.tools/kubectl -n housing-elt wait \
  --for=condition=failed job/housing-elt-validation-failure --timeout=20m
.tools/kubectl -n housing-elt logs job/housing-elt-validation-failure
```

Expected error:

```text
row_count: observed 360; expected [361, 361]
```

## Snowflake credentials

`k8s/snowflake-secret.example.yaml` is a template and is not part of the main
Kustomization. Do not put real values in that tracked file. If the live load is
enabled later, create the Secret from a local secure source or an external
secret manager, reference it from the workload, and add `--load-snowflake`.

## Cleanup

These commands remove the smoke Jobs or the whole local cluster:

```bash
.tools/kubectl -n housing-elt delete job housing-elt-smoke housing-elt-validation-failure
.tools/kind delete cluster --name housing-elt
```
