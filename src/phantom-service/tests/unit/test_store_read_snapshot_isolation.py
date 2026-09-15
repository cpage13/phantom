"""Read-snapshot isolation in the SQLite store (findings S2-1 and S2-3).

SQLite pins a connection's read snapshot while any statement on it is
active. The store splits reads from writes across two connections, so
which connection a read runs on decides what it is allowed to see.

* S2-1: a row walk holds its cursor open for the whole walk. Run on the
  shared point-read connection it froze every overlapping point read to
  the walk's own snapshot, which is why the invariant auditor's
  mid-sweep re-read echoed the walk instead of the live row and reported
  every delivery finishing inside the walk as ``missing_body_file``.
* S2-3: ``cancel`` and ``replay`` read their returned row back after the
  commit. Read on the point-read connection, that read could answer from
  a snapshot taken before the write, or miss the row entirely, and hand
  back a pre-write row or raise ``KeyError`` for an operation that had
  already committed.

These tests pin a file-backed store, because ``:memory:`` stores have
only one connection and therefore cannot express the defect at all,
which is exactly why the pre-fix suite never caught it.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from pathlib import Path

import pytest
from phantom.models.upload import UploadRow
from phantom.storage.sqlite_store import SqliteUploadStore

# Rows seeded before a walk starts. aiosqlite's cursor iteration fetches
# in chunks of ``iter_chunk_size`` (64), so a table larger than one chunk
# guarantees the walk's statement is still ACTIVE, and its snapshot still
# pinned, after the first row is consumed. A smaller table is drained by
# the first fetch and releases the snapshot before anything can observe
# it, which is the shape the pre-fix tests happened to use.
_ROWS_BEYOND_ONE_FETCH_CHUNK = 100


@pytest.fixture
async def file_store(tmp_path: Path) -> AsyncIterator[SqliteUploadStore]:
    """A started FILE-backed store: the read/write connection split only exists on disk."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await store.start()
    yield store
    await store.stop()


@pytest.mark.asyncio
async def test_point_read_during_a_walk_sees_a_write_committed_after_the_walk_began(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Objective: a walk must not pin the snapshot every point read answers from (S2-1).

    Seeds more rows than one cursor fetch chunk, starts a walk, consumes
    one row so the walk's cursor is open and suspended, then commits a
    state change and asks for that row by id.

    Expected outcome: the point read reports the NEW state. Before the
    fix the walk and the point read shared one connection, so the read
    was served from the walk's pre-write snapshot and reported the old
    state, which is the mechanism behind the auditor's false
    ``missing_body_file`` violations.
    """
    rows = [make_upload_row() for _ in range(_ROWS_BEYOND_ONE_FETCH_CHUNK)]
    for row in rows:
        await file_store.insert(row)
    target = rows[len(rows) // 2].chain_id

    walk = file_store.iter_rows()
    assert await anext(walk) is not None

    assert await file_store.update_state(target, new_state="cancelled") is True
    during_walk = await file_store.get(target)

    await walk.aclose()

    assert during_walk is not None
    assert during_walk.state == "cancelled"


@pytest.mark.asyncio
async def test_walk_does_not_hide_a_row_admitted_after_it_started(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Objective: a row admitted mid-walk stays visible to point reads (S2-1).

    Starts a walk over a table larger than one fetch chunk, then admits a
    brand new row and looks it up by id while the walk is still open.

    Expected outcome: the lookup finds the new row. Before the fix the
    walk's snapshot predated the admission and the shared point-read
    connection answered ``None``, which is the same absence that made
    ``find_by_chain_id_at_ingress`` miss a freshly admitted chain.
    """
    for _ in range(_ROWS_BEYOND_ONE_FETCH_CHUNK):
        await file_store.insert(make_upload_row())

    walk = file_store.iter_rows()
    assert await anext(walk) is not None

    admitted = make_upload_row()
    await file_store.insert(admitted)
    found = await file_store.get(admitted.chain_id)

    await walk.aclose()

    assert found is not None
    assert found.chain_id == admitted.chain_id


async def _pin_the_point_read_snapshot(store: SqliteUploadStore) -> AsyncIterator[None]:
    """Hold the point-read connection's snapshot open for the caller's block.

    Opens a raw ``SELECT`` on the store's read connection and leaves it
    suspended after one row, which is what any overlapping reader does to
    that connection. Used to put ``cancel`` and ``replay`` in the state a
    concurrent walk used to put them in.
    """
    reader = store._read_conn
    assert reader is not None, "the file-backed store must have a dedicated reader"
    cursor = await reader.execute("SELECT * FROM uploads")
    await cursor.fetchone()
    try:
        yield
    finally:
        await cursor.close()


@pytest.mark.asyncio
async def test_cancel_returns_its_own_committed_row_while_the_reader_is_pinned(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Objective: cancel reads its result back on the connection that wrote it (S2-3).

    Pins the point-read connection's snapshot, then admits a row the
    pinned snapshot cannot contain and cancels it.

    Expected outcome: a ``CancelOutcome`` reporting the committed
    ``cancelled`` row and a ``previous_state`` of ``queued``. Before the
    fix the post-commit read ran on the pinned point-read connection,
    found nothing, and raised ``KeyError`` for a cancel that had already
    committed, so the admin route returned 500 and never settled the
    gate, stranding the row's slot and bytes until restart.
    """
    for _ in range(_ROWS_BEYOND_ONE_FETCH_CHUNK):
        await file_store.insert(make_upload_row())

    pin = _pin_the_point_read_snapshot(file_store)
    await anext(pin)
    try:
        admitted = make_upload_row()
        await file_store.insert(admitted)
        outcome = await file_store.cancel(admitted.chain_id)
    finally:
        await pin.aclose()

    assert outcome.previous_state == "queued"
    assert outcome.row.chain_id == admitted.chain_id
    assert outcome.row.state == "cancelled"


@pytest.mark.asyncio
async def test_replay_returns_its_own_committed_row_while_the_reader_is_pinned(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Objective: replay reads its result back on the connection that wrote it (S2-3).

    Pins the point-read connection's snapshot, then admits and
    terminalizes a row the pinned snapshot cannot contain, and replays
    it.

    Expected outcome: a ``ReplayOutcome`` reporting the re-queued row,
    ``previous_state='failed'`` and ``rowcount=1``. Before the fix the
    post-commit read ran on the pinned connection and raised
    ``KeyError`` AFTER the re-queue UPDATE had committed; the route's
    handler then unwound its reservation and re-raised, so the gate
    under-counted a row that was genuinely back in flight and admitted
    past ``max_in_flight``.
    """
    for _ in range(_ROWS_BEYOND_ONE_FETCH_CHUNK):
        await file_store.insert(make_upload_row())

    pin = _pin_the_point_read_snapshot(file_store)
    await anext(pin)
    try:
        admitted = make_upload_row(state="failed")
        await file_store.insert(admitted)
        outcome = await file_store.replay(admitted.chain_id)
    finally:
        await pin.aclose()

    assert outcome is not None
    assert outcome.previous_state == "failed"
    assert outcome.rowcount == 1
    assert outcome.row.chain_id == admitted.chain_id
    assert outcome.row.state == "queued"
