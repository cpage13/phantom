"""Adopted regression tests for aggressor Round-7 findings R7-1 + R7-2.

Catalog-ref: B-13 / C-06 (fsync EIO on DB/WAL + body file) and C-01 / C-02
(ENOSPC mid-body-write + mid-SQLite-commit). Source: fsyncgate (2018) + CuttleFS
(ATC'20, which studied SQLite specifically) + "How SQLite Is Tested" §3.2.

Two property classes, both adopted as permanent coverage by Defender R8:

1. OPEN-TRANSACTION (was RED on pre-R8 code — findings R7-1-D / R7-2-B): a
   SQLITE_IOERR / SQLITE_FULL ``OperationalError`` on the admission commit must
   ROLL BACK, not leave the single shared aiosqlite connection in an open
   transaction. The pre-R8 ``insert_with_idempotency_claim`` caught only
   ``IntegrityError`` so an ``OperationalError`` from ``commit()`` propagated
   with no rollback → the next ``BEGIN`` raised "cannot start a transaction
   within a transaction" → the store wedged for every subsequent writer. The R8
   fix (``_write_txn`` + a broad rollback arm in ``insert_with_idempotency_claim``)
   makes the connection self-healing.

2. DURABILITY (GREEN — pins the R7 headline result): a body-fsync EIO / an
   ENOSPC write during the hybrid RAM→disk migration must keep
   ``body_location='ram'`` AND preserve the RAM copy — the commit-last-column
   ordering means a body that didn't fsync is never claimed durable. A future
   refactor that flips the column before a successful fsync (the H6/H7 ordering
   inversion class) or deletes the RAM copy behind a failed write goes RED.

The naked-500 → ADR-017 HTTP surface (R7-1-A/B / R7-2-A) is covered by the
``storage_unavailable`` registration + ``test_admission.py`` slot-release test.
"""

from __future__ import annotations

import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import aiosqlite
import pytest
from phantom.models.upload import CapturedValues, UploadRow
from phantom.storage.file_body_store import FileBodyStore
from phantom.storage.ram_body_store import RamBodyStore
from phantom.storage.sqlite_store import SqliteUploadStore
from phantom.workers.persist_controller import PersistController

ENOSPC = 28


def _minimal_row(*, body_location: str = "file") -> UploadRow:
    now = datetime.now(tz=UTC)
    cid = uuid4()
    return UploadRow(  # type: ignore[call-arg]
        chain_id=cid,
        instance_id="primary",
        group_id=cid,
        multifile_id=cid,
        send_order=0,
        route_name="r7",
        state="queued",
        body_location=body_location,
        next_attempt_at=now,
        received_at=now,
        updated_at=now,
        endpoint="https://files.example.com/v2/files",
        uid="00000000-0000-0000-0000-000000000001",
        chain_envelope_json="{}",
        captured_values=CapturedValues(),
        current_step_index=0,
        idempotency_key="r7-key",
        chain_id_at_ingress="r7-key",
        capture_reexecution_active=False,
        body_size_bytes=10,
        storage_encoding="original",
        body_hashes={},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "commit_error",
    [
        sqlite3.OperationalError("disk I/O error"),  # SQLITE_IOERR (R7-1-D)
        sqlite3.OperationalError("database or disk is full"),  # SQLITE_FULL (R7-2-B)
    ],
    ids=["sqlite_ioerr", "sqlite_full"],
)
async def test_failed_admission_commit_does_not_leak_open_transaction(
    commit_error: sqlite3.OperationalError,
) -> None:
    """A SQLITE_IOERR / SQLITE_FULL on the admission commit rolls back, not wedge.

    Pre-R8 RED: ``insert_with_idempotency_claim`` caught only ``IntegrityError``,
    so an ``OperationalError`` from ``commit()`` propagated with no rollback,
    leaving the connection's transaction open → the next ``BEGIN`` raised
    "cannot start a transaction within a transaction".
    """
    tmp = Path(tempfile.mkdtemp(prefix="test-r7-txn-"))
    store = SqliteUploadStore(str(tmp / "uploads.db"))
    await store.start()
    conn = store._require_conn()
    original_commit = conn.commit

    async def _failing_commit() -> None:
        conn.commit = original_commit  # type: ignore[method-assign]
        raise commit_error

    try:
        conn.commit = _failing_commit  # type: ignore[method-assign]
        with pytest.raises(sqlite3.OperationalError):
            await store.insert_with_idempotency_claim(_minimal_row(), "r7-key")
        conn.commit = original_commit  # type: ignore[method-assign]

        # DURABILITY: a fresh reader must see NO half-committed row.
        async with (
            aiosqlite.connect(str(tmp / "uploads.db")) as fresh,
            fresh.execute("SELECT COUNT(*) FROM uploads") as cur,
        ):
            fetched = await cur.fetchone()
            durable = fetched[0] if fetched is not None else -1
        assert durable == 0, "a failed commit left a DURABLE half-committed row"

        # OPEN-TRANSACTION: the connection must accept a fresh transaction.
        # On pre-R8 code this raised "cannot start a transaction within a
        # transaction".
        await conn.execute("BEGIN")
        await conn.execute("ROLLBACK")

        # And a real subsequent write must succeed (the store is not wedged).
        await store.insert(_minimal_row())
    finally:
        conn.commit = original_commit  # type: ignore[method-assign]
        await store.stop()


def _eio_raiser(*_a: object, **_k: object) -> None:
    raise OSError(5, "injected fsync EIO")


@pytest.mark.asyncio
async def test_hybrid_migration_keeps_ram_under_body_fsync_eio() -> None:
    """Durability HOLDS: a body-fsync EIO must NOT flip body_location to 'file'.

    GREEN — pins the commit-last-column behavior so a future refactor that flips
    the column before the fsync succeeds (the H6/H7 ordering inversion class)
    goes RED.
    """
    import phantom.storage.file_body_store as fbs

    tmp = Path(tempfile.mkdtemp(prefix="test-r7-mig-eio-"))
    store = SqliteUploadStore(str(tmp / "uploads.db"))
    await store.start()
    ram = RamBodyStore()
    await ram.start()
    file_store = FileBodyStore(tmp / "bodies", shard_prefix_chars=2)
    await file_store.start()

    row_ram = _minimal_row(body_location="ram")
    await ram.put(row_ram.chain_id, {"body": b"x" * 4096})
    await store.insert(row_ram)

    controller = PersistController(store=store, ram_body_store=ram, file_body_store=file_store)
    original = fbs._fsync_file
    try:
        fbs._fsync_file = _eio_raiser  # type: ignore[assignment]
        with pytest.raises(OSError):
            await controller._migrate_one(row_ram.chain_id)
    finally:
        fbs._fsync_file = original  # type: ignore[assignment]

    persisted = await store.get(row_ram.chain_id)
    assert persisted is not None
    assert persisted.body_location == "ram", (
        "body_location flipped to 'file' despite the body fsync failing — the "
        "commit-last-column ordering was violated (durability hazard)"
    )
    await store.stop()


@pytest.mark.asyncio
async def test_enospc_migration_keeps_ram_and_preserves_body() -> None:
    """Durability HOLDS: ENOSPC on the migration write keeps RAM + body_location='ram'.

    GREEN — pins that a failed RAM→disk migration write does NOT flip the
    commit-last column AND does NOT delete the RAM copy (the ``ram.delete``
    cleanup step is never reached because ``file.put`` raised first).
    """
    import phantom.storage.file_body_store as fbs

    tmp = Path(tempfile.mkdtemp(prefix="test-r7-mig-enospc-"))
    store = SqliteUploadStore(str(tmp / "uploads.db"))
    await store.start()
    ram = RamBodyStore()
    await ram.start()
    file_store = FileBodyStore(tmp / "bodies", shard_prefix_chars=2)
    await file_store.start()

    row_ram = _minimal_row(body_location="ram")
    await ram.put(row_ram.chain_id, {"body": b"x" * 4096})
    await store.insert(row_ram)

    controller = PersistController(store=store, ram_body_store=ram, file_body_store=file_store)
    original = fbs.FileBodyStore._put_one

    async def _enospc_put_one(self: object, cid: UUID, name: str, data: bytes) -> int:
        raise OSError(ENOSPC, "No space left on device (injected)")

    try:
        fbs.FileBodyStore._put_one = _enospc_put_one  # type: ignore[assignment,method-assign]
        with pytest.raises(OSError):
            await controller._migrate_one(row_ram.chain_id)
    finally:
        fbs.FileBodyStore._put_one = original  # type: ignore[method-assign]

    persisted = await store.get(row_ram.chain_id)
    assert persisted is not None
    assert persisted.body_location == "ram", "body_location flipped despite ENOSPC write"
    assert await ram.has_body_ref(row_ram.chain_id, "body"), (
        "RAM copy was deleted behind a failed disk write — the body is lost"
    )
    await store.stop()
