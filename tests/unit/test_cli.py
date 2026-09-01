import json
from pathlib import Path

import pytest

from housing_elt.cli import main


def test_show_config_prints_resolved_non_secret_settings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("HOUSING_ELT_PROJECT_ROOT", str(tmp_path))

    exit_code = main(["show-config"])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output == {
        "checkpoint_dir": str(tmp_path / "data/checkpoints"),
        "curated_data_dir": str(tmp_path / "data/curated"),
        "interim_data_dir": str(tmp_path / "data/interim"),
        "log_level": "INFO",
        "project_root": str(tmp_path),
        "raw_data_dir": str(tmp_path / "data/raw"),
    }


def test_show_config_exits_with_clear_error_for_invalid_settings(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("HOUSING_ELT_LOG_LEVEL", "verbose")

    with pytest.raises(SystemExit) as error:
        main(["show-config"])

    assert error.value.code == 2
    assert "HOUSING_ELT_LOG_LEVEL" in capsys.readouterr().err
