# syntax=docker/dockerfile:1.7

# Pin the Docker Official Image by both human-readable patch tag and immutable
# multi-platform digest. Python 3.11 matches the project contract; Bookworm slim
# supplies glibc-compatible wheels without a full general-purpose OS image.
ARG PYTHON_IMAGE=python:3.11.15-slim-bookworm@sha256:d29f48a31a8b408ed19272ca1e7b10ebae13b240a27e862d3d4217c528e2e0c3

FROM ${PYTHON_IMAGE} AS builder

# uv is build-only. Pinning the installer and using uv.lock gives repeatable
# Python packages while keeping uv, its cache, and development tools out of the
# runtime image.
ARG UV_VERSION=0.12.5
RUN python -m pip install --no-cache-dir "uv==${UV_VERSION}"

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0
WORKDIR /app

# Separate dependency and application layers so ordinary source edits can reuse
# the expensive PySpark/Snowflake dependency layer.
COPY pyproject.toml uv.lock .python-version ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project --no-editable

COPY src/ ./src/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

FROM ${PYTHON_IMAGE} AS runtime

# PySpark 4.2 requires Java 17+. A JRE is sufficient because the container runs
# packaged Spark bytecode; a JDK would add compilers and size with no runtime use.
# tini forwards Kubernetes termination signals to Python and its Java child.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends default-jre-headless tini \
    && rm -rf /var/lib/apt/lists/*

ENV PATH=/app/.venv/bin:${PATH} \
    JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SPARK_LOCAL_IP=127.0.0.1 \
    HOUSING_ELT_PROJECT_ROOT=/app

WORKDIR /app

# Use a fixed non-root identity for Kubernetes securityContext compatibility.
# Runtime data directories are mount points; the image contains no datasets.
RUN groupadd --gid 10001 pipeline \
    && useradd --uid 10001 --gid pipeline --create-home \
        --home-dir /home/pipeline pipeline \
    && mkdir -p data/raw data/interim data/curated data/checkpoints \
    && chown -R 10001:10001 /app /home/pipeline

COPY --from=builder --chown=10001:10001 /app/.venv /app/.venv
COPY --chown=10001:10001 config/ /app/config/

USER 10001:10001

ENTRYPOINT ["/usr/bin/tini", "--", "housing-elt"]
CMD ["--help"]
