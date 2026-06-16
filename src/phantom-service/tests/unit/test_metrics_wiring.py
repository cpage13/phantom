"""Plan § 4.2.2 metric-wiring tests.

Verifies that each worker class registers its expected metrics on the
:class:`MetricsRegistry` and emits at the documented sites.

Per the no-parallel-schema rule: a metric name added to the canonical
list in this file requires updating every worker that owns it; a metric
name removed requires deleting the corresponding registration in the
worker. The test is the contract.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from phantom.observability.metrics import MetricsRegistry
from phantom.storage.file_body_store import FileBodyStore
from phantom.storage.ram_body_store import RamBodyStore
from phantom.storage.sqlite_store import SqliteUploadStore
from phantom.workers.body_orphan_janitor import BodyOrphanJanitor
from phantom.workers.persist_controller import PersistController
from phantom.workers.reaper import Reaper
from phantom.workers.saturation import SaturationGate

from .conftest import make_snapshot, snapshot_thunk


@pytest.mark.asyncio
async def test_persist_controller_registers_canonical_metrics(tmp_path: Path) -> None:
    registry = MetricsRegistry()
    store = SqliteUploadStore(str(tmp_path / "uploads.db"), metrics_registry=registry)
    await store.start()
    ram = RamBodyStore()
    await ram.start()
    file_bs = FileBodyStore(tmp_path / "bodies", shard_prefix_chars=2)
    await file_bs.start()
    PersistController(
        store=store,
        ram_body_store=ram,
        file_body_store=file_bs,
        metrics_registry=registry,
    )
    assert "persist_total" in registry.counters
    assert "persist_controller_queue_depth" in registry.gauges
    await store.stop()


def test_saturation_gate_registers_canonical_metrics() -> None:
    registry = MetricsRegistry()
    SaturationGate(
        max_in_flight=10,
        max_in_flight_bytes=1000,
        max_disk_bytes=10000,
        metrics_registry=registry,
    )
    assert "saturation_balance" in registry.gauges


@pytest.mark.asyncio
async def test_saturation_admit_release_updates_balance_gauge() -> None:
    registry = MetricsRegistry()
    gate = SaturationGate(
        max_in_flight=10,
        max_in_flight_bytes=1000,
        max_disk_bytes=10000,
        metrics_registry=registry,
    )
    await gate.admit(declared_bytes=100)
    assert registry.gauges["saturation_balance"].snapshot()[""] == 100.0
    await gate.admit(declared_bytes=200)
    assert registry.gauges["saturation_balance"].snapshot()[""] == 300.0
    await gate.release(actual_bytes=100)
    assert registry.gauges["saturation_balance"].snapshot()[""] == 200.0


def test_body_orphan_janitor_registers_canonical_metrics() -> None:
    registry = MetricsRegistry()
    BodyOrphanJanitor(
        store=_StubUploadStore(),
        body_store=_StubBodyStore(),
        current_settings=snapshot_thunk(make_snapshot()),
        metrics_registry=registry,
    )
    assert "orphan_body_count_total" in registry.counters


def test_reaper_registers_canonical_metrics() -> None:
    registry = MetricsRegistry()
    Reaper(instances=[], metrics_registry=registry)
    assert "reaper_actions_total" in registry.counters


@pytest.mark.asyncio
async def test_body_orphan_janitor_bumps_counter_on_orphans_found() -> None:
    """Two confirmed orphans bump the counter by 2 (on the second sweep).

    R6-1 two-sweep confirmation: the first sweep only marks candidates,
    so the counter stays at zero until the second sweep actually
    removes them. The counter counts REMOVALS, not sightings.
    """
    from uuid import uuid4

    registry = MetricsRegistry()

    orphan_a = uuid4()
    orphan_b = uuid4()
    body_store = _StubBodyStore(orphans=[orphan_a, orphan_b])
    janitor = BodyOrphanJanitor(
        store=_StubUploadStore(),
        body_store=body_store,
        current_settings=snapshot_thunk(make_snapshot()),
        metrics_registry=registry,
    )
    counter = registry.counters["orphan_body_count_total"]
    await janitor._sweep_once()  # type: ignore[reportPrivateUsage]
    assert counter.snapshot().get("", 0) == 0, "first sighting must not count as removal"
    await janitor._sweep_once()  # type: ignore[reportPrivateUsage]
    assert counter.snapshot()[""] == 2


# --- Stubs ---------------------------------------------------------------


class _StubUploadStore:
    async def list_chain_ids(self) -> list:
        return []

    async def get(self, chain_id) -> None:
        # No live rows: every candidate passes the R6-1 live-row re-read.
        return None


class _StubBodyStore:
    def __init__(self, *, orphans: list | None = None) -> None:
        self._orphans = orphans or []

    async def list_orphans(self, known: set) -> list:
        return list(self._orphans)

    async def delete(self, chain_id) -> None:
        pass


@pytest.mark.asyncio
async def test_persist_controller_inc_persist_total_on_success(
    tmp_path: Path, make_upload_row
) -> None:
    """Migrate one body; assert persist_total{success} increments."""
    import contextlib

    registry = MetricsRegistry()
    store = SqliteUploadStore(str(tmp_path / "uploads.db"), metrics_registry=registry)
    await store.start()
    ram = RamBodyStore()
    await ram.start()
    file_bs = FileBodyStore(tmp_path / "bodies", shard_prefix_chars=2)
    await file_bs.start()
    controller = PersistController(
        store=store,
        ram_body_store=ram,
        file_body_store=file_bs,
        metrics_registry=registry,
    )
    row = make_upload_row(body_location="ram")
    await store.insert(row)
    await ram.put(row.chain_id, {"a": b"hello"})

    handle = await controller.enqueue(row.chain_id)
    task = asyncio.create_task(controller.run(asyncio.Event()))
    try:
        await asyncio.wait_for(handle, timeout=5.0)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    counter = registry.counters["persist_total"]
    snap = counter.snapshot()
    assert snap.get("success") == 1
    assert snap.get("failure", 0) == 0
    await store.stop()
