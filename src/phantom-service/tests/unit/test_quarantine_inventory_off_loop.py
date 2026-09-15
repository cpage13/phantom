"""SP-4. The quarantine inventory walk must not run on the event loop.

``list_quarantines`` is a synchronous ``rglob("*")`` plus a ``stat`` per
file over every quarantined body tree. A quarantined tree holds the
backlog for ``retention.stored_body_seconds`` (six months by default), so
it can be hundreds of thousands of files, and for the whole walk nothing
else on the loop runs: no admission, no sender attempt, no heartbeat, no
kicker tick. The identical walk was deliberately moved off the loop in
``FileBodyStore`` via ``asyncio.to_thread``; the inventory's sibling walk
was not.

The pin is the property, not the implementation: while the inventory is
being produced, the event loop must still be able to run another task.
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path

from phantom.storage.integrity import (
    list_quarantines,
    list_quarantines_off_loop,
    quarantine,
)

# How long the loop is given to prove it is still alive during the walk.
# A single tick is enough: the question is whether ANY other task can run,
# not how fast it runs.
_HEARTBEAT_TICK_SECONDS = 0.01
# Body files planted under the quarantined tree. Enough that the walk is a
# real walk; small enough to stay instant.
_PLANTED_BODY_COUNT = 12


def _populate_quarantine(data_root: Path) -> None:
    """Create a live DB + body tree and quarantine it, leaving artifacts."""
    db_path = data_root / "uploads.db"
    bodies_root = data_root / "bodies"
    db_path.write_bytes(b"not a real database, only an artifact to move")
    for index in range(_PLANTED_BODY_COUNT):
        chain_dir = bodies_root / "ab" / f"chain-{index}"
        chain_dir.mkdir(parents=True)
        (chain_dir / "body").write_bytes(b"x" * (index + 1))
    quarantine(db_path, bodies_root, reason="corrupted")


async def test_the_inventory_walk_leaves_the_loop_free(tmp_path: Path) -> None:
    """SP-4. Another coroutine still runs while the inventory is produced.

    Objective: the admin handler is async, so the walk must be offloaded.
    A heartbeat task ticks on the event loop while the inventory is
    awaited; if the walk ran inline, the heartbeat could not tick until
    the walk was over.

    Expected outcome: the inventory comes back with the quarantined
    backup, AND the heartbeat ticked at least once during the await.
    """
    _populate_quarantine(tmp_path)
    ticks = 0
    walking = asyncio.Event()

    async def _heartbeat() -> None:
        """Stand in for the sender, kicker and reaper ticks."""
        nonlocal ticks
        while not walking.is_set():
            await asyncio.sleep(_HEARTBEAT_TICK_SECONDS)
            ticks += 1

    async with asyncio.TaskGroup() as tg:
        beat = tg.create_task(_heartbeat(), name="heartbeat")
        entries = await list_quarantines_off_loop(tmp_path)
        # The walk itself is fast on a small tree, so give the loop the
        # same chance it would have during a long one.
        await asyncio.sleep(_HEARTBEAT_TICK_SECONDS * 2)
        walking.set()
        await beat

    assert entries, "the quarantined backup must appear in the inventory"
    assert ticks > 0, (
        "the event loop must keep running other tasks while the inventory walk is in progress"
    )


async def test_the_inventory_walk_runs_in_a_worker_thread(tmp_path: Path) -> None:
    """SP-4. The walk executes off the loop thread, like FileBodyStore's.

    Objective: pin the mechanism the property rests on, so a future
    refactor that quietly inlines the call fails here rather than in
    production latency. Expected outcome: the thread that runs the walk is
    not the thread running the event loop, and the result is identical to
    the synchronous call.
    """
    _populate_quarantine(tmp_path)
    loop_thread = threading.get_ident()
    walk_thread: list[int] = []
    real_list = list_quarantines

    def _recording_walk(data_root: Path) -> list[object]:
        walk_thread.append(threading.get_ident())
        return list(real_list(data_root))

    # Patch the name the off-loop wrapper resolves at call time.
    import phantom.storage.integrity as integrity_module

    original = integrity_module.list_quarantines
    integrity_module.list_quarantines = _recording_walk  # type: ignore[assignment]
    try:
        entries = await list_quarantines_off_loop(tmp_path)
    finally:
        integrity_module.list_quarantines = original  # type: ignore[assignment]

    assert walk_thread and walk_thread[0] != loop_thread, (
        "the filesystem walk must run in a worker thread, not on the loop"
    )
    assert [e.backup_id for e in entries] == [e.backup_id for e in real_list(tmp_path)]
