"""Unit tests for phantom.config.settings."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from phantom.config.settings import (
    RetentionCfg,
    Settings,
    SettingsError,
    load_settings,
)
from phantom.transport.httpx_client import HttpxUpstreamClient
from pydantic import ValidationError

# Every RetentionCfg window field bounded by the C11 hardening (ge=-1).
_RETENTION_WINDOW_FIELDS: tuple[str, ...] = (
    "succeeded_metadata_seconds",
    "succeeded_body_seconds",
    "failed_metadata_seconds",
    "failed_body_seconds",
    "cancelled_metadata_seconds",
    "cancelled_body_seconds",
    "corrupted_metadata_seconds",
    "corrupted_body_seconds",
    "stored_metadata_seconds",
    "stored_body_seconds",
    "auth_expired_metadata_seconds",
    "auth_expired_body_seconds",
    "expired_metadata_seconds",
    "expired_body_seconds",
)

# A value below the -1 sentinel: must be a loud validation error, never a
# silent "forever".
_BELOW_SENTINEL: int = -5


def test_server_defaults() -> None:
    """ServerCfg defaults: one listener bound to loopback by default.

    The deployment is same-machine-only, so the single listener
    (intake + admin + health) defaults to ``127.0.0.1:8080`` - that
    loopback bind is the admin access control (ADR-004).
    """
    settings = Settings()
    assert settings.server.bind_tcp == "127.0.0.1:8080"
    assert settings.server.bind_uds is None


def test_storage_defaults() -> None:
    """Non-probe StorageCfg defaults match plan §7.

    ``body_store.ram_ceiling_bytes`` and
    ``persist_trigger.body_size_threshold_bytes`` are probe-driven
    post-F6 (test_defaults covers the values directly); this test pins
    what stayed static (max_buffered_bytes, body_store deployment
    knobs, compression, sqlite). Phase 1 removed ``default_tier``
    (subsumed by ``body_store.mode``), ``in_memory_max_bytes`` (renamed
    to ``body_store.ram_ceiling_bytes``), ``persist_trigger.after_attempts``
    (subsumed by ``body_store.mode``), ``sqlite.autovacuum`` (hardcoded
    by code per SD-card-wear rule).
    """
    settings = Settings()
    assert settings.storage.max_buffered_bytes == 2_147_483_648
    assert settings.storage.compression.mode == "always"
    assert settings.storage.compression.algorithm == "zstd"
    # New (Phase 1) body_store defaults.
    assert settings.storage.body_store.mode == "hybrid"
    assert settings.storage.body_store.linger_seconds == 90
    assert settings.storage.body_store.ram_pressure_poll_seconds == 1.0
    assert settings.storage.body_store.body_orphan_sweep_seconds == 3600
    # New (Phase 1) sqlite narrowing — default synchronous flipped to NORMAL.
    assert settings.storage.sqlite.synchronous == "NORMAL"
    assert settings.storage.sqlite.journal_mode == "WAL"
    assert settings.storage.sqlite.journal_size_limit_bytes == 16_777_216


def test_retention_defaults() -> None:
    """RetentionCfg defaults are static hardcoded values (no probe input)."""
    settings = Settings()
    assert settings.retention.succeeded_body_seconds == 0
    assert settings.retention.succeeded_metadata_seconds == 180
    assert settings.retention.failed_metadata_seconds == 2_592_000
    assert settings.retention.failed_body_seconds == 2_592_000
    assert settings.retention.cancelled_metadata_seconds == 604_800
    assert settings.retention.cancelled_body_seconds == 604_800
    assert settings.retention.stored_metadata_seconds == -1
    assert settings.retention.stored_body_seconds == 15_552_000
    assert settings.retention.auth_expired_metadata_seconds == -1
    assert settings.retention.auth_expired_body_seconds == 15_552_000
    assert settings.retention.reaper_interval_seconds == 60


@pytest.mark.parametrize("field", _RETENTION_WINDOW_FIELDS)
def test_retention_window_below_sentinel_is_rejected(field: str) -> None:
    """A window below -1 is a validation error, not a silent forever (C11).

    The reaper's sweep guard (``>= 0``) treats any negative window as
    never-expire, which fails SAFE but turns an operator typo like -5
    into an unintended "keep forever". The ``ge=-1`` bound makes -1 the
    only sentinel; the boundary values -1 / 0 / positive all remain
    valid.
    """
    with pytest.raises(ValidationError):
        RetentionCfg(**{field: _BELOW_SENTINEL})
    for valid in (-1, 0, 1):
        cfg = RetentionCfg(**{field: valid})
        assert getattr(cfg, field) == valid


def test_retry_defaults() -> None:
    """RetryCfg non-probe defaults match plan §7.

    ``worker_count`` is probe-driven (covered in test_defaults); this
    test pins what stayed static (poll_interval_ms, default_strategy
    type).
    """
    settings = Settings()
    assert settings.retry.poll_interval_ms == 250
    assert settings.retry.default_strategy.type == "exponential_backoff"
    # Probe-filled: worker_count is in [2, 8] post-validator.
    assert settings.retry.worker_count is not None
    assert 2 <= settings.retry.worker_count <= 8


def test_observability_defaults() -> None:
    """ObservabilityCfg defaults match plan §7."""
    settings = Settings()
    assert settings.observability.log_level == "INFO"
    assert settings.observability.log_to_stdout is True
    assert settings.observability.log_to_file is None


def test_load_empty_yaml_warns(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """Empty YAML produces a zero-instance state with a startup warning."""
    yaml_path = tmp_path / "phantom.yaml"
    yaml_path.write_text("")
    with caplog.at_level("WARNING", logger="phantom.config.settings"):
        settings = load_settings(yaml_path)
    assert settings.instances == []
    assert any("empty" in record.message.lower() for record in caplog.records)


def test_load_missing_yaml_warns(tmp_path: Path) -> None:
    """A missing YAML file is tolerated."""
    settings = load_settings(tmp_path / "missing.yaml")
    assert isinstance(settings, Settings)
    assert settings.instances == []


def test_load_invalid_yaml_raises(tmp_path: Path) -> None:
    """Unparseable YAML raises :class:`SettingsError`."""
    bad = tmp_path / "bad.yaml"
    bad.write_text(":\n  : nope")
    with pytest.raises(SettingsError):
        load_settings(bad)


def test_load_non_mapping_yaml_raises(tmp_path: Path) -> None:
    """Top-level YAML must be a mapping."""
    p = tmp_path / "list.yaml"
    p.write_text("- a\n- b\n")
    with pytest.raises(SettingsError):
        load_settings(p)


def test_load_valid_yaml(tmp_path: Path) -> None:
    """A well-formed YAML loads with one configured instance."""
    p = tmp_path / "phantom.yaml"
    p.write_text(
        yaml.safe_dump(
            {
                "server": {"bind_tcp": "0.0.0.0:9000"},
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
    settings = load_settings(p)
    assert settings.server.bind_tcp == "0.0.0.0:9000"
    assert len(settings.instances) == 1
    assert settings.instances[0].id == "primary"


def test_env_overlay_scalar(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``PHANTOM_SERVER__BIND_TCP`` overrides the YAML value."""
    p = tmp_path / "phantom.yaml"
    p.write_text(yaml.safe_dump({"server": {"bind_tcp": "0.0.0.0:8080"}}))
    monkeypatch.setenv("PHANTOM_SERVER__BIND_TCP", "0.0.0.0:9090")
    settings = load_settings(p)
    assert settings.server.bind_tcp == "0.0.0.0:9090"


def test_env_overlay_nested_two_levels(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Double-underscore nesting goes two levels deep."""
    p = tmp_path / "phantom.yaml"
    p.write_text("")
    monkeypatch.setenv("PHANTOM_STORAGE__COMPRESSION__ALGORITHM", "gzip")
    settings = load_settings(p)
    assert settings.storage.compression.algorithm == "gzip"


def test_env_overlay_rejects_instances(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``PHANTOM_INSTANCES`` is rejected with :class:`SettingsError`."""
    p = tmp_path / "phantom.yaml"
    p.write_text("")
    monkeypatch.setenv("PHANTOM_INSTANCES", "garbage")
    with pytest.raises(SettingsError):
        load_settings(p)


def test_config_yaml_example_loads_cleanly() -> None:
    """``config/phantom.yaml.example`` boots without validation errors (§7.6).

    The example file is what operators copy as a starting config. It must
    parse against the current Pydantic schema and produce at least one
    configured instance (ADR-006 instance topology).
    """
    # Walk up from this test file: src/phantom-service/tests/unit/<file> -> repo root.
    repo_root = Path(__file__).resolve().parents[4]
    example_path = repo_root / "config" / "phantom.yaml.example"
    assert example_path.exists(), f"example config missing at {example_path}"
    settings = load_settings(example_path)
    assert len(settings.instances) == 1
    instance = settings.instances[0]
    assert instance.id == "upstream"
    assert len(instance.routes) == 2


def test_upstream_timeout_knob_parses_and_reaches_the_client(tmp_path: Path) -> None:
    """``upstream.timeout_seconds`` is configurable and reaches the transport (CL12).

    Objective: the knob the ``RouteCfg.timeout_seconds`` description has always
    promised operators now exists AND is the value the composition root hands
    ``HttpxUpstreamClient``. Success is both halves: a YAML value resolves onto
    ``Settings``, and constructing the client the way ``create_app`` does puts
    that same value on the httpx client it builds, and an omitted block still
    resolves to the documented 30 s. A knob that parsed but was ignored at the
    composition root would pass a parse-only assertion, which is why the reach
    half is asserted rather than assumed.
    """
    p = tmp_path / "phantom.yaml"
    p.write_text(yaml.safe_dump({"upstream": {"timeout_seconds": 5.0}}))

    settings = load_settings(p)
    assert settings.upstream.timeout_seconds == 5.0

    client = HttpxUpstreamClient(timeout_seconds=settings.upstream.timeout_seconds)
    assert client._timeout_seconds == 5.0

    # And an omitted block still yields the 30 s the composition root used to
    # carry as a literal, which is the number the exported description names.
    bare = tmp_path / "bare.yaml"
    bare.write_text("")
    assert load_settings(bare).upstream.timeout_seconds == 30.0
