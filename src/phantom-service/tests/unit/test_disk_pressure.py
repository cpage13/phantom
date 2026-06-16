"""Unit tests for phantom.workers.disk_pressure.

The probe wires `FileBodyStore.total_bytes()` into the gate's disk-usage
view so admit returns AdmissionRefusedDiskPressure when the
configured `max_disk_bytes` is reached (§2.3).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.workers.disk_pressure import DiskPressureProbe
from phantom.workers.saturation import SaturationGate

from .conftest import make_snapshot, snapshot_thunk, track_instance


async def _build(tmp_path: Path, *, max_disk_bytes: int = 100_000) -> InstanceContext:
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await store.start()
    await ram.start()
    await fbs.start()
    await tokens.start()
    cfg = InstanceCfg(
        id="primary",
        host_prefixes=["*"],
        data_dir="primary",
        routes=[RouteCfg(name="r", hosts=["*"], auth_mode="none")],
    )
    from phantom.storage.hybrid_body_store import HybridBodyStore

    body_store = HybridBodyStore(ram=ram, disk=fbs)
    await body_store.start()
    instance = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=None,
        token_cache=tokens,
        minter=None,
        retry_strategy=MagicMock(),
        upstream_client=MagicMock(),
        executor=MagicMock(),
        saturation=SaturationGate(
            max_in_flight=100,
            max_in_flight_bytes=10_000_000,
            max_disk_bytes=max_disk_bytes,
        ),
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(make_snapshot()),
    )
    return track_instance(instance)


@pytest.mark.asyncio
async def test_probe_updates_gate_disk_usage(tmp_path: Path) -> None:
    """A single probe tick reads file_body_store.total_bytes and updates the gate."""
    instance = await _build(tmp_path)
    # Drop a real body file so total_bytes is non-zero.
    payload = b"x" * 1024
    await instance.file_body_store.put(uuid4(), {"body": payload})
    probe = DiskPressureProbe(instance=instance, poll_interval_seconds=60.0)
    assert instance.saturation.disk_usage_bytes == 0
    await probe._probe_once()
    assert instance.saturation.disk_usage_bytes >= len(payload)


@pytest.mark.asyncio
async def test_probe_returns_immediately_when_max_disk_bytes_zero(tmp_path: Path) -> None:
    """``max_disk_bytes=0`` disables the probe entirely (loop exits)."""
    import asyncio

    instance = await _build(tmp_path, max_disk_bytes=0)
    probe = DiskPressureProbe(instance=instance, poll_interval_seconds=60.0)
    stop_event = asyncio.Event()
    # run() should exit immediately when max_disk_bytes <= 0.
    await asyncio.wait_for(probe.run(stop_event), timeout=1.0)
    # Gate's disk_usage_bytes stays at its initial zero.
    assert instance.saturation.disk_usage_bytes == 0
