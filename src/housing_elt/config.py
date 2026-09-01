"""Typed, side-effect-free configuration for local pipeline execution."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

ENV_PREFIX = "HOUSING_ELT_"
_VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


class SettingsError(ValueError):
    """Raised when a configuration value is invalid."""


@dataclass(frozen=True, slots=True)
class PipelineSettings:
    """Resolved local paths and behavior shared by pipeline commands.

    The dataclass is immutable so one pipeline run cannot accidentally change
    its configuration halfway through execution. Paths are absolute after
    loading, which avoids ambiguity when Spark or a container changes its
    working directory.
    """

    project_root: Path
    raw_data_dir: Path
    interim_data_dir: Path
    curated_data_dir: Path
    checkpoint_dir: Path
    log_level: str

    def to_display_dict(self) -> dict[str, str]:
        """Return only known non-secret settings in a JSON-friendly form."""
        return {
            "checkpoint_dir": str(self.checkpoint_dir),
            "curated_data_dir": str(self.curated_data_dir),
            "interim_data_dir": str(self.interim_data_dir),
            "log_level": self.log_level,
            "project_root": str(self.project_root),
            "raw_data_dir": str(self.raw_data_dir),
        }


def _resolve_path(value: str, *, relative_to: Path) -> Path:
    """Resolve an absolute path or anchor a relative path to a known root."""
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = relative_to / candidate
    return candidate.resolve()


def load_settings(
    environ: Mapping[str, str] | None = None,
    *,
    working_dir: Path | None = None,
) -> PipelineSettings:
    """Load settings from explicit inputs, then safe local defaults.

    Passing an environment mapping and working directory makes the function
    deterministic in tests. Loading configuration does not create directories,
    read secrets, or contact any external service.
    """
    values = os.environ if environ is None else environ
    base_dir = (Path.cwd() if working_dir is None else working_dir).resolve()

    project_root = _resolve_path(
        values.get(f"{ENV_PREFIX}PROJECT_ROOT", str(base_dir)),
        relative_to=base_dir,
    )

    def data_path(variable_suffix: str, default: str) -> Path:
        return _resolve_path(
            values.get(f"{ENV_PREFIX}{variable_suffix}", default),
            relative_to=project_root,
        )

    log_level = values.get(f"{ENV_PREFIX}LOG_LEVEL", "INFO").strip().upper()
    if log_level not in _VALID_LOG_LEVELS:
        allowed = ", ".join(sorted(_VALID_LOG_LEVELS))
        raise SettingsError(
            f"{ENV_PREFIX}LOG_LEVEL must be one of {allowed}; got {log_level!r}"
        )

    return PipelineSettings(
        project_root=project_root,
        raw_data_dir=data_path("RAW_DATA_DIR", "data/raw"),
        interim_data_dir=data_path("INTERIM_DATA_DIR", "data/interim"),
        curated_data_dir=data_path("CURATED_DATA_DIR", "data/curated"),
        checkpoint_dir=data_path("CHECKPOINT_DIR", "data/checkpoints"),
        log_level=log_level,
    )
