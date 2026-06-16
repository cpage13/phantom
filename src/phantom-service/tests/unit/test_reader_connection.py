"""Cycle-7 task 4.1 acceptance: the dedicated read-only store connection.

The store opens a SECOND aiosqlite connection in read-only mode
(``file:...?mode=ro``) during ``start()`` and routes every read-only
``UploadStore`` method onto it; every write keeps the single serialized
writer connection and its lock. These tests pin the acceptance legs:

* reader-beside-writer: admin-style reads return correct committed
  snapshots DURING a sustained write storm and never error on writer
  activity;
* writes never wait on a read: a write completes promptly while a read
  cursor is open mid-iteration (the historical same-connection
  cursor-vs-checkpoint collision class is structurally gone);
* the reader is genuinely read-only (``mode=ro`` enforced by SQLite);
* ``:memory:`` stores fall back to the writer connection (a second
  connection to ``:memory:`` would be a different database);
* no lock-classifier drift: ``is_transient_lock_error`` (ADR-023) has
  exactly one definition and the reader path introduces no second
  classifier or fragment table.
"""

from __future__ import annotations

import ast
import asyncio
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest
from phantom.models.upload import UploadRow
from phantom.storage.sqlite_store import SqliteUploadStore, is_transient_lock_error

# How long the sustained write storm runs. Long enough that readers
# provably interleave with hundreds of commits; short enough for a unit
# suite.
_STORM_SECONDS = 0.5

# Bound on how long a single write may take while a read cursor is held
# open. Generous against CI jitter; the failure mode it guards (a write
# queueing behind an open read cursor) blocks far longer or deadlocks.
_WRITE_PROMPTNESS_TIMEOUT_SECONDS = 2.0


@pytest.fixture
async def file_store(tmp_path: Path) -> SqliteUploadStore:
    """A started FILE-backed store (the reader only exists on disk stores)."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await store.start()
    yield store
    await store.stop()


@pytest.mark.asyncio
async def test_reader_connection_opens_and_closes(tmp_path: Path) -> None:
    """start() opens the mode=ro reader; stop() closes both connections."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await store.start()
    assert store._read_conn is not None
    assert store._read_conn is not store._conn
    await store.stop()
    assert store._read_conn is None
    assert store._conn is None


@pytest.mark.asyncio
async def test_reader_is_genuinely_read_only(file_store: SqliteUploadStore) -> None:
    """A write attempted through the reader is refused by SQLite itself."""
    reader = file_store._read_conn
    assert reader is not None
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        await reader.execute("INSERT INTO uploads (chain_id) VALUES ('x')")


@pytest.mark.asyncio
async def test_memory_store_falls_back_to_writer(
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """``:memory:`` stores keep reads on the writer (no split-brain reader)."""
    store = SqliteUploadStore(":memory:")
    await store.start()
    try:
        assert store._read_conn is None
        row = make_upload_row()
        await store.insert(row)
        fetched = await store.get(row.chain_id)
        assert fetched is not None
        assert fetched.chain_id == row.chain_id
    finally:
        await store.stop()


@pytest.mark.asyncio
async def test_reads_see_committed_writes_on_the_reader(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Read-your-committed-write across the two connections (WAL snapshot)."""
    row = make_upload_row()
    await file_store.insert(row)
    fetched = await file_store.get(row.chain_id)
    assert fetched is not None
    assert fetched.chain_id == row.chain_id
    rows, _cursor = await file_store.list_uploads()
    assert [r.chain_id for r in rows] == [row.chain_id]


@pytest.mark.asyncio
async def test_reads_serve_correct_snapshots_during_write_storm(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Reader-beside-writer: reads stay correct and error-free under load.

    Writers insert rows and flip states continuously; readers poll
    ``get`` / ``list_uploads`` / ``counts_by_state`` the whole time. The
    acceptance is twofold: no read ever raises (no lock error surfaces
    on the reader), and every snapshot is correct (visibility floor and
    ceiling below; the final settled read sees every write).

    Snapshot semantics being asserted: overlapping reads on the ONE
    reader connection share its read transaction, which SQLite pins to
    the oldest still-active statement. A read therefore sees AT LEAST
    everything committed before the oldest read concurrently in flight
    began (the floor), and never a row that was not yet submitted (the
    ceiling).
    """
    stop = asyncio.Event()
    intents: list[UploadRow] = []
    committed: list[UploadRow] = []
    read_errors: list[BaseException] = []
    snapshots: list[int] = []
    # committed-count floor per in-flight read, keyed by a unique token.
    # min() over it is the oldest active read's floor: the visibility
    # bound a pinned snapshot can never drop below.
    active_read_floors: dict[object, int] = {}

    async def writer() -> None:
        while not stop.is_set():
            row = make_upload_row(state="queued")
            intents.append(row)
            await file_store.insert(row)
            committed.append(row)
            await file_store.update_state(
                row.chain_id, new_state="attempting", expected_state="queued"
            )
            await asyncio.sleep(0)

    async def reader() -> None:
        while not stop.is_set():
            token = object()
            # Capture-and-register atomically (no await in between) so the
            # min() below always covers every read that could pin ours.
            active_read_floors[token] = len(committed)
            min_floor = min(active_read_floors.values())
            try:
                tallies = await file_store.counts_by_state()
                total = sum(t.count for t in tallies.values())
                intent_ceiling = len(intents)
                assert min_floor <= total <= intent_ceiling
                snapshots.append(total)
                if min_floor >= 1:
                    probe = committed[0]
                    got = await file_store.get(probe.chain_id)
                    assert got is not None
                    rows, _ = await file_store.list_uploads(limit=5)
                    assert len(rows) >= 1
            except BaseException as exc:  # collected for the assertion below
                read_errors.append(exc)
                stop.set()
                raise
            finally:
                del active_read_floors[token]
            await asyncio.sleep(0)

    async with asyncio.TaskGroup() as tg:
        tg.create_task(writer())
        tg.create_task(writer())
        tg.create_task(reader())
        tg.create_task(reader())
        await asyncio.sleep(_STORM_SECONDS)
        stop.set()

    assert not read_errors
    assert len(committed) > 0
    assert len(snapshots) > 0
    # The settled read sees every committed write.
    final = await file_store.counts_by_state()
    assert sum(t.count for t in final.values()) == len(committed)


@pytest.mark.asyncio
async def test_write_completes_while_read_cursor_is_open(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Writes never wait on a read: an open iter_rows cursor blocks nothing.

    On the historical shared connection this exact interleaving was the
    V1/V2 cursor-vs-checkpoint hazard class; with the dedicated reader
    the write lands promptly while the read cursor is suspended
    mid-iteration on its own connection.
    """
    for _ in range(3):
        await file_store.insert(make_upload_row())

    walker = file_store.iter_rows()
    first = await anext(walker)
    assert first is not None

    # Cursor is now open and suspended. A write must complete promptly.
    new_row = make_upload_row()
    await asyncio.wait_for(
        file_store.insert(new_row),
        timeout=_WRITE_PROMPTNESS_TIMEOUT_SECONDS,
    )

    remaining = [row async for row in walker]
    # The walk drains without error; its snapshot may or may not include
    # the row committed mid-walk (it sees ONE consistent snapshot).
    assert len(remaining) >= 2
    fetched = await file_store.get(new_row.chain_id)
    assert fetched is not None


@pytest.mark.asyncio
async def test_concurrent_claims_and_reads_no_lock_errors(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """claim_due (write) and list_non_terminal (read) interleave cleanly."""
    now = datetime.now(tz=UTC)
    for _ in range(10):
        await file_store.insert(make_upload_row(next_attempt_at=now))
    results = await asyncio.gather(
        *(file_store.claim_due(datetime.now(tz=UTC), 1) for _ in range(5)),
        *(file_store.list_non_terminal() for _ in range(5)),
    )
    claimed = [r for batch in results[:5] for r in batch]
    assert len(claimed) == 5
    assert len({r.chain_id for r in claimed}) == 5


def test_no_second_lock_classifier_exists() -> None:
    """ADR-023: is_transient_lock_error stays the ONLY lock classifier.

    Walks every module under ``phantom.storage`` and asserts exactly one
    function definition named ``is_transient_lock_error`` and exactly
    one transient-fragment table exist; the reader path must consult the
    shared classifier rather than growing a parallel definition.
    """
    import phantom.storage as storage_pkg

    package_dir = Path(storage_pkg.__file__).parent
    classifier_defs = 0
    fragment_tables = 0
    for module_path in package_dir.rglob("*.py"):
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == "is_transient_lock_error"
            ):
                classifier_defs += 1
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and "TRANSIENT_LOCK" in target.id:
                        fragment_tables += 1
            if isinstance(node, ast.AnnAssign):
                target = node.target
                if isinstance(target, ast.Name) and "TRANSIENT_LOCK" in target.id:
                    fragment_tables += 1
    assert classifier_defs == 1
    assert fragment_tables == 1


def test_reader_lock_errors_classify_through_the_shared_function() -> None:
    """A lock error shaped like the reader's SQLITE_BUSY classifies True.

    The reader adds no wrapping: a contended read surfaces the raw
    ``sqlite3.OperationalError``, which the one shared classifier
    recognizes; a genuine fault (no such table) stays non-transient.
    """
    assert is_transient_lock_error(sqlite3.OperationalError("database is locked"))
    assert not is_transient_lock_error(sqlite3.OperationalError("no such table: uploads"))
    assert not is_transient_lock_error(ValueError("database is locked"))


@pytest.mark.asyncio
async def test_reader_survives_writer_checkpoint(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """A writer-side WAL checkpoint does not break subsequent reads."""
    row = make_upload_row()
    await file_store.insert(row)
    writer = file_store._require_conn()
    await writer.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    await writer.commit()
    fetched = await file_store.get(row.chain_id)
    assert fetched is not None
    assert fetched.chain_id == row.chain_id


def test_read_only_uri_percent_encodes_uri_syntax(tmp_path: Path) -> None:
    """Paths carrying URI metacharacters round-trip into a safe file: URI."""
    odd_dir = tmp_path / "data set#1"
    odd_dir.mkdir()
    store = SqliteUploadStore(str(odd_dir / "uploads.db"))
    uri = store._read_only_uri()
    assert uri.startswith("file:/")
    assert uri.endswith("?mode=ro")
    assert "#" not in uri.removesuffix("?mode=ro").removeprefix("file:") or "%23" in uri
    assert " " not in uri


@pytest.mark.asyncio
async def test_store_with_uri_metacharacter_path_round_trips(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """A store rooted under a '#'-bearing directory reads its own writes."""
    odd_dir = tmp_path / "data#1"
    odd_dir.mkdir()
    store = SqliteUploadStore(str(odd_dir / "uploads.db"))
    await store.start()
    try:
        row = make_upload_row()
        await store.insert(row)
        fetched = await store.get(row.chain_id)
        assert fetched is not None
    finally:
        await store.stop()
