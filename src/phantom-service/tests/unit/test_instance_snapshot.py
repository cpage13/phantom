"""Unit tests for phantom.instances.snapshot.InstanceSettingsSnapshot."""

from __future__ import annotations

import dataclasses

import pytest
from phantom.config.settings import (
    CompressionCfg,
    InstanceCfg,
    PersistTriggerCfg,
    RetentionCfg,
    RouteCfg,
    SaturationCfg,
    Settings,
)
from phantom.instances.snapshot import InstanceSettingsSnapshot, _build_snapshot

from .conftest import make_snapshot


def test_snapshot_is_frozen() -> None:
    """Every field on a built snapshot rejects mutation."""
    snapshot = make_snapshot()
    with pytest.raises(dataclasses.FrozenInstanceError):
        snapshot.capture_reexecution = True  # type: ignore[misc]


def test_snapshot_carries_hot_reloadable_fields() -> None:
    """``_build_snapshot`` populates every hot-reloadable field from Settings.

    Constructs a Settings with an instance that pins
    ``capture_reexecution=True``; asserts the built snapshot carries
    the top-level Settings sub-blocks by reference. Retry parameters
    and AD-mint knobs deliberately do NOT live on the snapshot (R5-2):
    retry reloads via the ``apply_reload`` strategy rebuild and every
    ``ad_mint`` change is restart-required (ADR-013).
    """
    instance_cfg = InstanceCfg(
        id="primary",
        host_prefixes=["files.example.com"],
        data_dir="primary",
        capture_reexecution=True,
        routes=[RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer")],
    )
    settings = Settings(instances=[instance_cfg])

    snapshot = _build_snapshot(settings, instance_cfg)

    assert isinstance(snapshot, InstanceSettingsSnapshot)
    # Top-level blocks must be present and the same instances by reference
    # (share-by-reference contract from _build_snapshot's docstring).
    assert isinstance(snapshot.persist_trigger, PersistTriggerCfg)
    assert isinstance(snapshot.retention, RetentionCfg)
    assert isinstance(snapshot.compression, CompressionCfg)
    assert isinstance(snapshot.saturation, SaturationCfg)
    assert snapshot.persist_trigger is settings.storage.persist_trigger
    assert snapshot.retention is settings.retention
    assert snapshot.compression is settings.storage.compression
    assert snapshot.saturation is settings.saturation
    # Per-instance fields project from the InstanceCfg.
    assert snapshot.capture_reexecution is True


def test_snapshot_capture_reexecution_false_projects() -> None:
    """``capture_reexecution=False`` projects through ``_build_snapshot``."""
    instance_cfg = InstanceCfg(
        id="primary",
        host_prefixes=["files.example.com"],
        data_dir="primary",
        capture_reexecution=False,
        routes=[RouteCfg(name="files", hosts=["files.example.com"], auth_mode="none")],
    )
    settings = Settings(instances=[instance_cfg])

    snapshot = _build_snapshot(settings, instance_cfg)

    assert snapshot.capture_reexecution is False
