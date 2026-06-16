"""Unit tests for :mod:`phantom_emulator.config`."""

from __future__ import annotations

from pathlib import Path

import pytest
from phantom_emulator.auth.modes import AuthMode
from phantom_emulator.config import (
    DEFAULT_BODY_MAX_BYTES,
    DEFAULT_EXPIRES_IN_SECONDS,
    DEFAULT_PORT,
    DEFAULT_PRESIGNED_TTL_SECONDS,
    AppConfig,
    load_config,
)


def test_defaults() -> None:
    cfg = load_config(None)
    assert isinstance(cfg, AppConfig)
    assert cfg.server.host == "0.0.0.0"
    assert cfg.server.port == DEFAULT_PORT
    assert cfg.auth.default_mode is AuthMode.OAUTH_CLIENT_CREDENTIALS
    assert cfg.auth.signing.mode == "HS256"
    assert cfg.auth.default_expires_in_seconds == DEFAULT_EXPIRES_IN_SECONDS
    assert cfg.upstream.body_max_bytes == DEFAULT_BODY_MAX_BYTES
    assert cfg.upstream.presigned_ttl_seconds == DEFAULT_PRESIGNED_TTL_SECONDS
    assert cfg.control.bind == "loopback"
    assert cfg.logging.level == "INFO"
    assert len(cfg.auth.clients) == 1
    assert cfg.auth.clients[0].client_id == "test-client"


def test_load_yaml(tmp_path: Path) -> None:
    yml = tmp_path / "cfg.yml"
    yml.write_text(
        "server:\n"
        "  port: 9001\n"
        "auth:\n"
        "  audience: 'api://override/.default'\n"
        "  signing:\n"
        "    mode: RS256\n"
        "upstream:\n"
        "  presigned_ttl_seconds: 7\n"
        "logging:\n"
        "  level: DEBUG\n",
        encoding="utf-8",
    )
    cfg = load_config(yml)
    assert cfg.server.port == 9001
    assert cfg.auth.audience == "api://override/.default"
    assert cfg.auth.signing.mode == "RS256"
    assert cfg.upstream.presigned_ttl_seconds == 7
    assert cfg.logging.level == "DEBUG"


def test_load_yaml_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "does-not-exist.yml")


def test_load_yaml_non_mapping_raises(tmp_path: Path) -> None:
    yml = tmp_path / "bad.yml"
    yml.write_text("- not\n- a mapping\n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_config(yml)


def test_env_overlay(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PHANTOM_EMULATOR_SERVER__PORT", "12345")
    monkeypatch.setenv("PHANTOM_EMULATOR_LOGGING__LEVEL", "DEBUG")
    cfg = load_config(None)
    assert cfg.server.port == 12345
    assert cfg.logging.level == "DEBUG"


def test_env_overlay_layered_over_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    yml = tmp_path / "cfg.yml"
    yml.write_text("server:\n  port: 9001\n", encoding="utf-8")
    monkeypatch.setenv("PHANTOM_EMULATOR_SERVER__PORT", "12345")
    cfg = load_config(yml)
    # Env wins over YAML.
    assert cfg.server.port == 12345


def test_extra_keys_rejected() -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        AppConfig.model_validate({"unexpected": 1})
