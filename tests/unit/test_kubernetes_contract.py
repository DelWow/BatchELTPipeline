from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
K8S_DIR = PROJECT_ROOT / "k8s"


def _read(name: str) -> str:
    return (K8S_DIR / name).read_text(encoding="utf-8")


def test_kind_cluster_and_local_image_are_immutable_and_offline() -> None:
    cluster = _read("kind-cluster.yaml")
    cronjob = _read("base/cronjob.yaml")

    assert "kindest/node:v1.36.1@sha256:" in cluster
    assert "hostPath: ./data/raw" in cluster
    assert "readOnly: true" in cluster
    assert "image: canadian-housing-elt:local" in cronjob
    assert "imagePullPolicy: Never" in cronjob
    assert "--skip-ingestion" in cronjob
    assert "--load-snowflake" not in cronjob


def test_cronjob_has_schedule_retries_resources_and_security_boundaries() -> None:
    cronjob = _read("base/cronjob.yaml")

    required_fragments = (
        "apiVersion: batch/v1",
        'schedule: "0 11 5 * *"',
        "timeZone: Etc/UTC",
        "concurrencyPolicy: Forbid",
        "startingDeadlineSeconds: 21600",
        "successfulJobsHistoryLimit: 3",
        "failedJobsHistoryLimit: 3",
        "backoffLimit: 2",
        "activeDeadlineSeconds: 1800",
        "ttlSecondsAfterFinished: 86400",
        "automountServiceAccountToken: false",
        "runAsNonRoot: true",
        "readOnlyRootFilesystem: true",
        "allowPrivilegeEscalation: false",
        "sizeLimit: 1Gi",
    )
    for fragment in required_fragments:
        assert fragment in cronjob
    assert "requests:" in cronjob
    assert "limits:" in cronjob


def test_secret_template_is_never_applied_by_default() -> None:
    kustomization = _read("kustomization.yaml")
    secret_template = _read("snowflake-secret.example.yaml")

    assert "snowflake-secret.example.yaml" not in kustomization
    assert "TEMPLATE ONLY" in secret_template
    assert "REPLACE_ME" in secret_template
    assert "HOUSING_ELT_SNOWFLAKE_PASSWORD" in secret_template


def test_failure_overlay_is_isolated_and_does_not_retry() -> None:
    overlay = _read("smoke-failure/kustomization.yaml")
    validation = _read("smoke-failure/validation-configmap.yaml")

    assert "nameSuffix: -failure" in overlay
    assert "path: /spec/suspend" in overlay
    assert "path: /spec/jobTemplate/spec/backoffLimit" in overlay
    assert "value: 0" in overlay
    assert "--validation-contract" in overlay
    assert "min_rows = 361" in validation
    assert "max_rows = 361" in validation
