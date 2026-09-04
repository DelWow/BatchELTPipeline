from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = PROJECT_ROOT / "Dockerfile"
DOCKERIGNORE = PROJECT_ROOT / ".dockerignore"


def test_runtime_image_is_pinned_locked_and_non_root() -> None:
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    assert "python:3.11.15-slim-bookworm@sha256:" in dockerfile
    assert "uv==${UV_VERSION}" in dockerfile
    assert "uv sync --locked --no-dev --no-editable" in dockerfile
    assert "default-jre-headless tini" in dockerfile
    assert "USER 10001:10001" in dockerfile
    assert 'ENTRYPOINT ["/usr/bin/tini", "--", "housing-elt"]' in dockerfile


def test_dockerfile_never_copies_the_repository_or_data_wholesale() -> None:
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    assert "COPY . " not in dockerfile
    assert "COPY data" not in dockerfile
    assert "COPY tests" not in dockerfile
    assert "COPY --from=builder" in dockerfile
    assert "COPY --chown=10001:10001 config/ /app/config/" in dockerfile


def test_build_context_is_an_explicit_allowlist() -> None:
    rules = tuple(
        line.strip()
        for line in DOCKERIGNORE.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )

    assert rules[0] == "**"
    assert set(rules[1:]) == {
        "!pyproject.toml",
        "!uv.lock",
        "!.python-version",
        "!src/",
        "!src/**",
        "!config/",
        "!config/**",
    }
    assert not any("data" in rule for rule in rules[1:])
