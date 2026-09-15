"""The PersistController's dedupe map must empty on EVERY exit from a migration.

``_in_flight`` is the one piece of controller state that is supposed to be
self-clearing, and :meth:`PersistController.enqueue` reads it as the authority
on "this chain is already queued". An entry that outlives its migration is
therefore permanent: every later enqueue takes the ``existing is not None``
branch and hands back the dead future WITHOUT re-queueing, so the chain never
migrates again for the process lifetime and the admin surface counts the
phantom entry as pending work.

Covered here:

* **S8-6** - cancellation during ``_handle_one`` skipped the settlement
  entirely, because ``except Exception`` does not catch
  :class:`asyncio.CancelledError` and the ``finally`` only touched the gauge.
* **SW-4** - a row already off RAM was migrated anyway, so a migration that had
  ALREADY SUCCEEDED was logged at ERROR and counted ``persist_total{failure}``.
* **D7** - the failure settlement left an un-retrieved exception on a future
  every production caller discards, so a disk-full episode emitted a second
  ERROR record per chain from ``Future.__del__``, with no chain_id and timed to
  a garbage-collection pass.
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from phantom.models.upload import UploadRow
from phantom.observability.metrics import MetricsRegistry
from phantom.storage.file_body_store import FileBodyStore
from phantom.storage.ram_body_store import RamBodyStore
from phantom.storage.sqlite_store import SqliteUploadStore
from phantom.workers.persist_controller import PersistController

from .conftest import track_started

pytestmark = pytest.mark.asyncio

# How long a test waits on a signal that should arrive immediately. Generous
# enough to survive a loaded CI box, short enough that a genuinely stuck future
# fails the test rather than hanging the lane.
_WAIT_SECONDS = 5.0

# Poll interval for the one test that must observe settlement WITHOUT reading
# the future (reading marks its exception retrieved, which is what that test
# measures). Short enough to keep the test instant, long enough not to spin.
_POLL_SECONDS = 0.01


class _Stack:
    """A started store trio plus the controller under test."""

    def __init__(
        self,
        store: SqliteUploadStore,
        ram: RamBodyStore,
        file_bs: FileBodyStore,
        make_row: Callable[..., UploadRow],
    ) -> None:
        self.store = store
        self.ram = ram
        self.file = file_bs
        self.make_row = make_row
        self.metrics = MetricsRegistry()
        self.controller = PersistController(
            store=store,
            ram_body_store=ram,
            file_body_store=file_bs,
            metrics_registry=self.metrics,
        )

    def persist_outcomes(self) -> dict[str, int]:
        """Return the ``persist_total`` counter's label buckets."""
        return dict(self.metrics.counters["persist_total"].snapshot())


@pytest.fixture
async def stack(tmp_path: Path, make_upload_row: Callable[..., UploadRow]) -> _Stack:
    """Build a started SqliteUploadStore + Ram/File body stores + controller."""
    store = track_started(SqliteUploadStore(str(tmp_path / "uploads.db")))
    await store.start()
    ram = track_started(RamBodyStore())
    await ram.start()
    file_bs = track_started(FileBodyStore(tmp_path / "bodies", shard_prefix_chars=2))
    await file_bs.start()
    return _Stack(store, ram, file_bs, make_upload_row)


async def _drain(controller: PersistController, handle: asyncio.Future[None]) -> None:
    """Run the controller's loop until ``handle`` settles, then stop it."""
    task = asyncio.create_task(controller.run(asyncio.Event()))
    try:
        await asyncio.wait_for(asyncio.shield(handle), timeout=_WAIT_SECONDS)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_a_cancelled_migration_does_not_poison_the_chain_forever(
    stack: _Stack,
) -> None:
    """Objective: a cancel mid-migration must leave the chain re-enqueueable.

    The controller is suspended inside ``_migrate_one``'s very first await (the
    live-row read) and then cancelled, which is what a lifespan shutdown or a
    supervising TaskGroup does.

    Expected: the in-flight entry is released, so a later ``enqueue`` allocates
    a FRESH future and actually re-queues the chain, and a second run loop
    migrates it to disk. Before the fix the entry survived, every later enqueue
    returned the same never-resolving future without re-queueing, and the chain
    was excluded from migration for the process lifetime.
    """
    row = stack.make_row(body_location="ram")
    await stack.store.insert(row)
    await stack.ram.put(row.chain_id, {"a": b"hello"})

    entered = asyncio.Event()
    forever = asyncio.Event()
    real_get = stack.store.get

    async def blocking_get(chain_id: UUID) -> UploadRow | None:
        entered.set()
        await forever.wait()
        return await real_get(chain_id)

    stack.store.get = blocking_get  # type: ignore[method-assign]

    first = await stack.controller.enqueue(row.chain_id)
    task = asyncio.create_task(stack.controller.run(asyncio.Event()))
    await asyncio.wait_for(entered.wait(), timeout=_WAIT_SECONDS)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert first.done(), "the cancelled migration left its handle pending forever"
    assert first.cancelled()

    stack.store.get = real_get  # type: ignore[method-assign]
    second = await stack.controller.enqueue(row.chain_id)
    assert second is not first, "enqueue handed back the dead future without re-queueing"
    await _drain(stack.controller, second)

    assert await stack.file.has_body_ref(row.chain_id, "a")
    fetched = await stack.store.get(row.chain_id)
    assert fetched is not None
    assert fetched.body_location == "file"


async def test_a_cancelled_migration_re_raises_rather_than_swallowing(
    stack: _Stack,
) -> None:
    """Objective: cancellation must propagate out of the run loop.

    Expected: cancelling the task raises :class:`asyncio.CancelledError` out of
    ``run``. Swallowing it would leave the lifespan TaskGroup waiting on a task
    that acknowledged no cancel, which is why the sibling workers re-raise too.
    """
    row = stack.make_row(body_location="ram")
    await stack.store.insert(row)
    await stack.ram.put(row.chain_id, {"a": b"hello"})

    entered = asyncio.Event()
    forever = asyncio.Event()
    real_get = stack.store.get

    async def blocking_get(chain_id: UUID) -> UploadRow | None:
        entered.set()
        await forever.wait()
        return await real_get(chain_id)

    stack.store.get = blocking_get  # type: ignore[method-assign]
    await stack.controller.enqueue(row.chain_id)
    task = asyncio.create_task(stack.controller.run(asyncio.Event()))
    await asyncio.wait_for(entered.wait(), timeout=_WAIT_SECONDS)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


async def test_a_row_already_off_ram_is_a_clean_skip_not_a_failure(
    stack: _Stack,
) -> None:
    """Objective: re-enqueueing an already-migrated chain must not count a failure.

    This is the shape both healthy-traffic paths produce: the sender's
    retry-linger enqueue and admission's size-threshold enqueue both act on a
    CLAIM-TIME row snapshot that still reads 'ram' while the live row has
    already been flipped to 'file' by a previous migration, whose step 4
    deleted the RAM entry.

    Expected: the handle resolves to ``None`` and ``persist_total`` records a
    success with NO failure bucket. Before the fix the run read RAM anyway, hit
    ``KeyError``, logged ``PersistController migration failed`` at ERROR and
    counted a failure for a migration that had already succeeded.
    """
    row = stack.make_row(body_location="file")
    await stack.store.insert(row)
    # RAM deliberately holds nothing: the previous migration's step 4 took it.

    handle = await stack.controller.enqueue(row.chain_id)
    await _drain(stack.controller, handle)

    assert handle.result() is None
    outcomes = stack.persist_outcomes()
    assert outcomes.get("failure", 0) == 0, (
        f"an already-migrated chain counted a failure: {outcomes}"
    )
    assert outcomes.get("success", 0) == 1
    # The live row is untouched: this path writes nothing.
    fetched = await stack.store.get(row.chain_id)
    assert fetched is not None
    assert fetched.body_location == "file"


async def test_a_discarded_failing_handle_reports_one_error_not_two(
    stack: _Stack,
) -> None:
    """Objective: a fire-and-forget failure must not also log from the GC.

    All three production callers of ``enqueue`` discard the handle, which the
    contract sanctions, so a failed migration's future is collected un-awaited.

    Expected: no ``Future exception was never retrieved`` record reaches the
    loop's exception handler. Before the fix an ENOSPC or EIO episode emitted a
    second ERROR per chain from ``Future.__del__``, carrying no chain_id and
    timed to a garbage-collection pass rather than to the failure.

    Nothing here may read the handle's result or exception: both mark it
    retrieved, which is the very flag under test. Settlement is therefore
    observed with ``done()``, and that the exception still reaches a real
    awaiter is pinned separately by
    ``test_enqueue_failure_sets_handle_exception_then_continues``.
    """
    row = stack.make_row(body_location="ram")
    await stack.store.insert(row)
    # No RAM entry, so ``get_all`` raises the KeyError that stands in for the
    # disk-fault exceptions of the finding.

    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    seen: list[str] = []

    def record(_loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
        seen.append(str(context.get("message", "")))

    loop.set_exception_handler(record)
    try:
        handle = await stack.controller.enqueue(row.chain_id)
        task = asyncio.create_task(stack.controller.run(asyncio.Event()))
        try:
            deadline = loop.time() + _WAIT_SECONDS
            while not handle.done() and loop.time() < deadline:
                await asyncio.sleep(_POLL_SECONDS)
            assert handle.done(), "the failed migration never settled its handle"
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        del handle
        gc.collect()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous)

    assert not [m for m in seen if "never retrieved" in m], (
        f"a discarded handle logged a second error record: {seen}"
    )
