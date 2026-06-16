"""Unit tests for :class:`BodyOrphanJanitor` (plan § 2.3.14 / § 2.3.21 #3).

Closes invariant #4 (plan § 0.5): no :class:`FileBodyStore` ref set
exists without a corresponding ``uploads`` row. Each test exercises one
of the contract surfaces — the sweep removes orphans, leaves live rows
alone, treats an empty known-set as "all orphans," and the periodic
loop is driven by ``body_store.body_orphan_sweep_seconds``.

Slice 1.F authoring per the brief's Phase 1 acceptance plan.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path
from uuid import UUID

import pytest
from phantom.config.settings import BodyStoreCfg
from phantom.models.upload import UploadRow
from phantom.storage.file_body_store import FileBodyStore
from phantom.storage.sqlite_store import SqliteUploadStore
from phantom.workers.body_orphan_janitor import BodyOrphanJanitor

from .conftest import make_snapshot, snapshot_thunk, track_started

pytestmark = pytest.mark.asyncio

# Deadline for the cadence-loop test's removal poll: covers the startup
# sweep plus one 1-second cadence with generous headroom for a loaded
# host, far below the suite's patience.
_REMOVAL_DEADLINE_SECONDS = 10.0
_REMOVAL_POLL_SECONDS = 0.05


async def _stack(
    tmp_path: Path,
) -> tuple[SqliteUploadStore, FileBodyStore]:
    """Build a started store + file body store under ``tmp_path``."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await store.start()
    bodies = FileBodyStore(tmp_path / "bodies", shard_prefix_chars=2)
    await bodies.start()
    return track_started(store), track_started(bodies)


async def test_sweep_removes_orphans_not_in_known_set(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Bodies present on disk but absent from ``uploads`` are deleted.

    Two chains: one is live in ``uploads`` (kept), one is an orphan
    (deleted). Deletion requires two consecutive candidate sightings
    (R6-1 two-sweep confirmation), so the orphan is gone after the
    second sweep; the live body remains throughout.
    """
    store, bodies = await _stack(tmp_path)
    live_row = make_upload_row(body_location="file")
    await store.insert(live_row)
    await bodies.put(live_row.chain_id, {"a": b"live"})

    # Orphan: present on disk with no matching uploads row.
    orphan_chain_id = make_upload_row(body_location="file").chain_id
    await bodies.put(orphan_chain_id, {"a": b"orphan"})

    janitor = BodyOrphanJanitor(
        store=store, body_store=bodies, current_settings=snapshot_thunk(make_snapshot())
    )
    await janitor._sweep_once()
    await janitor._sweep_once()

    # Live body still present.
    assert await bodies.has_body_ref(live_row.chain_id, "a")
    # Orphan gone.
    assert not await bodies.has_body_ref(orphan_chain_id, "a")


async def test_sweep_preserves_bodies_in_known_set(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Live rows with bodies on disk survive every sweep — defensive guard.

    Even when no orphans exist, the sweep MUST NOT delete the live
    body. This locks the janitor as a no-op on healthy state.
    """
    store, bodies = await _stack(tmp_path)
    rows = [make_upload_row(body_location="file") for _ in range(3)]
    for row in rows:
        await store.insert(row)
        await bodies.put(row.chain_id, {"a": b"x"})

    janitor = BodyOrphanJanitor(
        store=store, body_store=bodies, current_settings=snapshot_thunk(make_snapshot())
    )
    await janitor._sweep_once()
    await janitor._sweep_once()

    for row in rows:
        assert await bodies.has_body_ref(row.chain_id, "a")


async def test_empty_known_set_treats_every_body_as_orphan(tmp_path: Path) -> None:
    """An empty ``uploads`` table means every body on disk is orphaned.

    Closes the edge case where a fresh-process boot finds bodies on
    disk left by a prior process but no surviving row metadata (e.g.,
    after quarantine). Collection lands on the second sweep (R6-1
    two-sweep confirmation).
    """
    store, bodies = await _stack(tmp_path)
    chain_ids: list[UUID] = []
    for _ in range(3):
        cid = UUID(int=0x1000 + len(chain_ids))
        await bodies.put(cid, {"a": b"orphaned"})
        chain_ids.append(cid)

    janitor = BodyOrphanJanitor(
        store=store, body_store=bodies, current_settings=snapshot_thunk(make_snapshot())
    )
    await janitor._sweep_once()
    await janitor._sweep_once()

    for cid in chain_ids:
        assert not await bodies.has_body_ref(cid, "a")


async def test_run_loop_sweeps_on_period_then_stops(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """The periodic loop sweeps on cadence and exits when ``stop_event`` is set.

    Exercises the cadence path (plan § 2.3.21 #3 "periodic-poll loop
    driven by ``body_orphan_sweep_seconds``"). Uses a short cadence so
    the test stays fast. Collection needs the SECOND sweep (R6-1
    two-sweep confirmation), so the loop must demonstrably re-fire on
    cadence for this test to pass; removal is polled with a deadline.
    """
    store, bodies = await _stack(tmp_path)
    orphan_cid = make_upload_row(body_location="file").chain_id
    await bodies.put(orphan_cid, {"a": b"orphan"})

    janitor = BodyOrphanJanitor(
        store=store,
        body_store=bodies,
        # 1-second cadence via the live snapshot (T1: cadence is read per
        # loop iteration, no longer pinned at construction).
        current_settings=snapshot_thunk(
            make_snapshot(body_store=BodyStoreCfg(mode="hybrid", body_orphan_sweep_seconds=1))
        ),
    )
    stop_event = asyncio.Event()
    task = asyncio.create_task(janitor.run(stop_event))
    try:
        # First sweep fires immediately; the second (collecting) sweep
        # fires one cadence later. Poll up to a generous deadline.
        async with asyncio.timeout(_REMOVAL_DEADLINE_SECONDS):
            while await bodies.has_body_ref(orphan_cid, "a"):
                await asyncio.sleep(_REMOVAL_POLL_SECONDS)
    finally:
        stop_event.set()
        await asyncio.wait_for(task, timeout=2.0)
