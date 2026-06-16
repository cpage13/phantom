"""Unit tests for :class:`PersistController` (plan § 2.3.11)."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from pathlib import Path

import pytest
from phantom.models.upload import UploadRow
from phantom.storage.file_body_store import FileBodyStore
from phantom.storage.ram_body_store import RamBodyStore
from phantom.storage.sqlite_store import SqliteUploadStore
from phantom.workers.persist_controller import PersistController

from .conftest import track_started


@pytest.fixture
async def fixture_stack(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> dict[str, object]:
    """Build a started SqliteUploadStore + Ram/File body stores.

    Returns the components plus a helper to insert a RAM-tier row.
    """
    store = track_started(SqliteUploadStore(str(tmp_path / "uploads.db")))
    await store.start()
    ram = track_started(RamBodyStore())
    await ram.start()
    file_bs = track_started(FileBodyStore(tmp_path / "bodies", shard_prefix_chars=2))
    await file_bs.start()
    return {
        "store": store,
        "ram": ram,
        "file": file_bs,
        "make_row": make_upload_row,
    }


async def test_enqueue_returns_future_resolving_after_migration(
    fixture_stack: dict[str, object],
) -> None:
    """:meth:`enqueue` -> migration -> future resolves with None.

    Happy path: a RAM-tier row gets its body moved to disk, body_location
    flips to 'file', RAM bytes are released, and the awaiting Future
    receives ``None``.
    """
    store = fixture_stack["store"]
    ram = fixture_stack["ram"]
    file_bs = fixture_stack["file"]
    make_row = fixture_stack["make_row"]
    assert isinstance(store, SqliteUploadStore)
    assert isinstance(ram, RamBodyStore)
    assert isinstance(file_bs, FileBodyStore)
    assert callable(make_row)
    # Insert a RAM-tier row + put its body bytes in RAM.
    row = make_row(body_location="ram")
    await store.insert(row)
    await ram.put(row.chain_id, {"a": b"hello"})

    controller = PersistController(
        store=store,
        ram_body_store=ram,
        file_body_store=file_bs,
    )
    handle = await controller.enqueue(row.chain_id)
    # Spawn the run loop until the handle resolves.
    task = asyncio.create_task(controller.run(asyncio.Event()))
    try:
        await asyncio.wait_for(handle, timeout=5.0)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    # Body is now on disk.
    assert await file_bs.has_body_ref(row.chain_id, "a")
    # RAM is empty for this chain_id.
    assert not await ram.has_body_ref(row.chain_id, "a")
    # body_location flipped to 'file'.
    fetched = await store.get(row.chain_id)
    assert fetched is not None
    assert fetched.body_location == "file"


async def test_enqueue_is_idempotent_returns_same_future(
    fixture_stack: dict[str, object],
) -> None:
    """Two :meth:`enqueue` calls for the same chain_id return the same Future.

    Idempotent contract (plan § 2.3.11): both callers see the same
    completion signal; both resolve together.
    """
    store = fixture_stack["store"]
    ram = fixture_stack["ram"]
    file_bs = fixture_stack["file"]
    make_row = fixture_stack["make_row"]
    assert isinstance(store, SqliteUploadStore)
    assert isinstance(ram, RamBodyStore)
    assert isinstance(file_bs, FileBodyStore)
    assert callable(make_row)
    row = make_row(body_location="ram")
    await store.insert(row)
    await ram.put(row.chain_id, {"a": b"x"})

    controller = PersistController(
        store=store,
        ram_body_store=ram,
        file_body_store=file_bs,
    )
    handle1 = await controller.enqueue(row.chain_id)
    handle2 = await controller.enqueue(row.chain_id)
    # Same object — collapsed to one in-flight Future per plan
    # § 2.3.11.
    assert handle1 is handle2

    task = asyncio.create_task(controller.run(asyncio.Event()))
    try:
        # Both awaiters resolve together.
        await asyncio.wait_for(asyncio.gather(handle1, handle2), timeout=5.0)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_enqueue_failure_sets_handle_exception_then_continues(
    fixture_stack: dict[str, object],
) -> None:
    """A migration failure on one chain doesn't kill the controller.

    Per plan § 2.3.11: ``run`` does NOT re-raise — the next chain in
    the queue still migrates. The failing handle gets ``set_exception``.
    """
    store = fixture_stack["store"]
    ram = fixture_stack["ram"]
    file_bs = fixture_stack["file"]
    make_row = fixture_stack["make_row"]
    assert isinstance(store, SqliteUploadStore)
    assert isinstance(ram, RamBodyStore)
    assert isinstance(file_bs, FileBodyStore)
    assert callable(make_row)
    # Bad chain: row exists, but RAM has no body for it. ``ram.get_all``
    # raises KeyError; the controller's except branch should set the
    # handle's exception.
    bad_row = make_row(body_location="ram")
    await store.insert(bad_row)
    # Good chain: row + body both present.
    good_row = make_row(body_location="ram")
    await store.insert(good_row)
    await ram.put(good_row.chain_id, {"a": b"ok"})

    controller = PersistController(
        store=store,
        ram_body_store=ram,
        file_body_store=file_bs,
    )
    bad_handle = await controller.enqueue(bad_row.chain_id)
    good_handle = await controller.enqueue(good_row.chain_id)

    task = asyncio.create_task(controller.run(asyncio.Event()))
    try:
        # Bad raises KeyError; good resolves to None.
        with pytest.raises(KeyError):
            await asyncio.wait_for(bad_handle, timeout=5.0)
        await asyncio.wait_for(good_handle, timeout=5.0)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    # Good chain's body is on disk.
    assert await file_bs.has_body_ref(good_row.chain_id, "a")
    # Good row's body_location flipped.
    good_fetched = await store.get(good_row.chain_id)
    assert good_fetched is not None
    assert good_fetched.body_location == "file"
    # Bad row's body_location stays 'ram'.
    bad_fetched = await store.get(bad_row.chain_id)
    assert bad_fetched is not None
    assert bad_fetched.body_location == "ram"


async def test_enqueue_after_migration_completes_returns_fresh_future(
    fixture_stack: dict[str, object],
) -> None:
    """A re-enqueue AFTER the first migration completes gets a new Future.

    The in-flight dict pops the chain on completion, so a subsequent
    enqueue for the same chain_id allocates a fresh future (re-running
    the migration on the now-empty RAM is a no-op for the body but
    re-flips body_location — a defensive no-op via the mark_persisted
    WHERE-guard).
    """
    store = fixture_stack["store"]
    ram = fixture_stack["ram"]
    file_bs = fixture_stack["file"]
    make_row = fixture_stack["make_row"]
    assert isinstance(store, SqliteUploadStore)
    assert isinstance(ram, RamBodyStore)
    assert isinstance(file_bs, FileBodyStore)
    assert callable(make_row)
    row = make_row(body_location="ram")
    await store.insert(row)
    await ram.put(row.chain_id, {"a": b"once"})

    controller = PersistController(
        store=store,
        ram_body_store=ram,
        file_body_store=file_bs,
    )
    first = await controller.enqueue(row.chain_id)
    task = asyncio.create_task(controller.run(asyncio.Event()))
    try:
        await asyncio.wait_for(first, timeout=5.0)
        # A second enqueue (after the first completed + popped) gets a
        # NEW Future, not the same as `first`. RAM is empty now; the
        # controller will raise KeyError on get_all and set the
        # exception.
        second = await controller.enqueue(row.chain_id)
        assert second is not first
        with pytest.raises(KeyError):
            await asyncio.wait_for(second, timeout=5.0)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
