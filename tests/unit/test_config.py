from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from housing_elt.config import SettingsError, load_settings


def test_load_settings_uses_project_local_defaults(tmp_path: Path) -> None:
    settings = load_settings({}, working_dir=tmp_path)

    assert settings.project_root == tmp_path
    assert settings.raw_data_dir == tmp_path / "data/raw"
    assert settings.interim_data_dir == tmp_path / "data/interim"
    assert settings.curated_data_dir == tmp_path / "data/curated"
    assert settings.checkpoint_dir == tmp_path / "data/checkpoints"
    assert settings.log_level == "INFO"


def test_load_settings_resolves_relative_overrides_from_project_root(
    tmp_path: Path,
) -> None:
    settings = load_settings(
        {
            "HOUSING_ELT_PROJECT_ROOT": "workspace",
            "HOUSING_ELT_RAW_DATA_DIR": "landing/raw",
            "HOUSING_ELT_LOG_LEVEL": "debug",
        },
        working_dir=tmp_path,
    )

    assert settings.project_root == tmp_path / "workspace"
    assert settings.raw_data_dir == tmp_path / "workspace/landing/raw"
    assert settings.log_level == "DEBUG"


def test_load_settings_preserves_absolute_path_override(tmp_path: Path) -> None:
    external_raw = tmp_path / "mounted-raw"

    settings = load_settings(
        {"HOUSING_ELT_RAW_DATA_DIR": str(external_raw)},
        working_dir=tmp_path / "working-directory",
    )

    assert settings.raw_data_dir == external_raw


def test_load_settings_rejects_unknown_log_level(tmp_path: Path) -> None:
    with pytest.raises(SettingsError, match="HOUSING_ELT_LOG_LEVEL"):
        load_settings(
            {"HOUSING_ELT_LOG_LEVEL": "verbose"},
            working_dir=tmp_path,
        )


def test_settings_are_immutable(tmp_path: Path) -> None:
    settings = load_settings({}, working_dir=tmp_path)

    with pytest.raises(FrozenInstanceError):
        settings.log_level = "DEBUG"  # type: ignore[misc]
