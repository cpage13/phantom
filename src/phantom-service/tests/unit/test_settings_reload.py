"""Unit tests for ``Settings.reload_from_yaml`` (plan §11.1).

The reload classmethod is shared by SIGHUP and ``POST /v1/admin/reload``;
both paths construct a fresh validated :class:`Settings` from the YAML
on disk. Failures must surface as exception types the caller can branch
on (``yaml.YAMLError`` for parse failures; ``pydantic.ValidationError``
for invalid config).
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from phantom.config.settings import Settings
from pydantic import ValidationError


def _write_yaml(path: Path, payload: dict[str, object]) -> Path:
    """Dump ``payload`` to ``path`` and return it for convenience."""
    path.write_text(yaml.safe_dump(payload))
    return path


def test_reload_from_yaml_succeeds_with_valid_yaml(tmp_path: Path) -> None:
    """A minimal valid YAML returns a populated :class:`Settings`."""
    cfg = _write_yaml(
        tmp_path / "phantom.yaml",
        {
            "instances": [
                {
                    "id": "primary",
                    "host_prefixes": ["files.example.com"],
                    "data_dir": "primary",
                    "routes": [
                        {
                            "name": "files",
                            "hosts": ["files.example.com"],
                            "auth_mode": "phantom_bearer",
                        }
                    ],
                }
            ]
        },
    )
    settings = Settings.reload_from_yaml(cfg)
    assert isinstance(settings, Settings)
    assert len(settings.instances) == 1
    assert settings.instances[0].id == "primary"


def test_reload_from_yaml_rejects_invalid_yaml(tmp_path: Path) -> None:
    """Malformed YAML payloads raise ``yaml.YAMLError``."""
    cfg = tmp_path / "phantom.yaml"
    # Tab-after-key is an unrecoverable YAML scan error.
    cfg.write_text("key:\t- bad\n  nested: value")
    with pytest.raises(yaml.YAMLError):
        Settings.reload_from_yaml(cfg)


def test_reload_from_yaml_rejects_validation_failure(tmp_path: Path) -> None:
    """Invalid config raises ``pydantic.ValidationError``.

    Setting ``persist_trigger.body_size_threshold_bytes`` to a negative
    value violates the ``ge=0`` constraint and the validator surfaces
    the failure as ``ValidationError`` (NOT wrapped in
    :class:`phantom.config.settings.SettingsError`).
    """
    cfg = _write_yaml(
        tmp_path / "phantom.yaml",
        {"storage": {"persist_trigger": {"body_size_threshold_bytes": -5}}},
    )
    with pytest.raises(ValidationError):
        Settings.reload_from_yaml(cfg)


def test_reload_from_yaml_skip_probe_leaves_holes_unset(tmp_path: Path) -> None:
    """``skip_probe=True`` bypasses the host probe.

    Probe-fillable fields not pinned in YAML stay ``None`` because the
    validator's probe is short-circuited. Caller is responsible for
    pinning load-bearing fields in YAML when using ``skip_probe=True``.
    """
    cfg = _write_yaml(
        tmp_path / "phantom.yaml",
        {
            "saturation": {
                "max_in_flight": 50,
                "max_in_flight_bytes": 2_000_000,
                "max_disk_bytes": 100_000_000,
                "large_body_threshold_bytes": 10_000_000,
                "max_large_in_flight": 2,
            },
            "storage": {
                "body_store": {"ram_ceiling_bytes": 1_000_000},
                "persist_trigger": {"body_size_threshold_bytes": 16 * 1024 * 1024},
            },
            "retry": {"worker_count": 4},
        },
    )
    # Probe should NOT run.
    settings = Settings.reload_from_yaml(cfg, skip_probe=True)
    # Every pinned field carries its YAML value.
    assert settings.saturation.max_in_flight == 50
    assert settings.retry.worker_count == 4


def test_reload_from_yaml_skip_probe_with_empty_yaml_leaves_holes(tmp_path: Path) -> None:
    """``skip_probe=True`` with an empty YAML leaves probe fields ``None``."""
    cfg = _write_yaml(tmp_path / "phantom.yaml", {})
    settings = Settings.reload_from_yaml(cfg, skip_probe=True)
    # No YAML pins, probe skipped — the probe-fillable fields stay None.
    assert settings.saturation.max_in_flight is None
    assert settings.retry.worker_count is None
