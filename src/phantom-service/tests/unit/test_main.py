"""Unit tests for ``phantom.__main__`` — particularly the ``--validate`` flag (§7.5)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from phantom.__main__ import main


def _write_valid_yaml(path: Path) -> None:
    """Drop a minimal-but-valid Phantom YAML at ``path``."""
    path.write_text(
        yaml.safe_dump(
            {
                "instances": [
                    {
                        "id": "primary",
                        "host_prefixes": ["upstream.example.com"],
                        "data_dir": "primary",
                        "routes": [
                            {
                                "name": "upstream-files",
                                "hosts": ["upstream.example.com"],
                                "auth_mode": "phantom_bearer",
                            }
                        ],
                    }
                ],
            },
        )
    )


def test_validate_flag_exits_zero_on_valid_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``--validate`` exits 0 and prints the resolved settings on success.

    The operator's CI runs this before deploy; exit 0 means the YAML
    parses against the current schema and would boot.
    """
    cfg = tmp_path / "phantom.yaml"
    _write_valid_yaml(cfg)
    monkeypatch.setattr("sys.argv", ["phantom", "-c", str(cfg), "--validate"])
    exit_code = main()
    assert exit_code == 0
    captured = capsys.readouterr()
    # stdout is the resolved settings as JSON.
    parsed = json.loads(captured.out)
    assert parsed["instances"][0]["id"] == "primary"
    # stderr should be empty on success.
    assert captured.err == ""


def test_validate_flag_exits_one_on_invalid_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``--validate`` exits 1 with the validation error on stderr."""
    cfg = tmp_path / "phantom.yaml"
    # `body_size_threshold_bytes: -1` is rejected (ge=0); trigger validation failure.
    cfg.write_text(
        yaml.safe_dump(
            {"storage": {"persist_trigger": {"body_size_threshold_bytes": -1}}},
        ),
    )
    monkeypatch.setattr("sys.argv", ["phantom", "-c", str(cfg), "--validate"])
    exit_code = main()
    assert exit_code == 1
    captured = capsys.readouterr()
    # Error message goes to stderr; stdout stays empty.
    assert "config validation failed" in captured.err
    assert captured.out == ""


def test_validate_flag_does_not_bind_server(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``--validate`` must not import or call uvicorn (§7.5 safety contract).

    Patch uvicorn.run to a sentinel that raises; if --validate triggers
    binding, the test fails loudly.
    """
    cfg = tmp_path / "phantom.yaml"
    _write_valid_yaml(cfg)
    monkeypatch.setattr("sys.argv", ["phantom", "-c", str(cfg), "--validate"])

    def _explode(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("uvicorn.run must not be called when --validate is set")

    import uvicorn

    monkeypatch.setattr(uvicorn, "run", _explode)
    exit_code = main()
    assert exit_code == 0
