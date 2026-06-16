"""Persist-controller crash-window tests (WS-4 matrix rows 1-3).

The persist controller's ``_migrate_one`` is a four-step ordered
sequence (plan § 0.5 commit-last-column ordering):

1. ``ram.get_all`` — pull body bytes.
2. ``file.put`` — write body files + fsync (FileBodyStore does
   tmp + fsync + atomic rename for each ref + a parent-dir fsync).
3. ``store.mark_persisted`` — flip ``body_location='ram'`` → ``'file'``
   (the commit point — sole writer per single-writer invariant #6).
4. ``ram.delete`` — release RAM bytes (cleanup; not load-bearing).

A crash at each inter-step boundary leaves a specific on-disk
state. These tests drive each boundary, halt, then run the
canonical startup recovery sweep and assert the predicted
end state.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from phantom.models.upload import BodyHash, BodyHashes, StorageHash, UploadRow
from phantom.storage import FileBodyStore, RamBodyStore, SqliteUploadStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.workers.recovery import run_recovery

pytestmark = [pytest.mark.asyncio]


async def _fresh_store(tmp_path: Path) -> SqliteUploadStore:
    """Open a fresh SqliteUploadStore against ``tmp_path/uploads.db``."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await store.start()
    return store


async def _fresh_body_stores(
    tmp_path: Path,
) -> tuple[HybridBodyStore, RamBodyStore, FileBodyStore]:
    """Open fresh RAM + File body stores against ``tmp_path/bodies``."""
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    hybrid = HybridBodyStore(ram=ram, disk=fbs)
    await hybrid.start()
    return hybrid, ram, fbs


def _ram_row(chain_id, body_bytes: bytes) -> UploadRow:
    """Build a queued, RAM-located row with body_hashes matching ``body_bytes``."""
    digest = hashlib.sha256(body_bytes).hexdigest()
    now = datetime.now(tz=UTC)
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": chain_id,
            "multifile_id": chain_id,
            "send_order": 0,
            "route_name": "r",
            "state": "queued",
            "body_location": "ram",
            "received_at": now,
            "updated_at": now,
            "endpoint": "e",
            "uid": "u",
            "chain_envelope_json": "{}",
            "idempotency_key": f"k-{chain_id}",
            "capture_reexecution_active": False,
            "body_hashes": {
                "body": BodyHashes(
                    body_hash=BodyHash(digest),
                    storage_hash=StorageHash(digest),
                ),
            },
            "body_size_bytes": len(body_bytes),
        },
    )


async def test_crash_between_get_all_and_file_put(tmp_path: Path) -> None:
    """WS-4 row: Step 1 done, Step 2 not yet started.

    Pre-crash on-disk state: row at ``body_location='ram'``, body
    files NOT on disk, RAM intact. Crash simulated by simply not
    calling Step 2.

    Post-crash environment (fresh store + fresh body stores):
    RAM is empty (RAM tier is by-design ephemeral across restarts).

    Recovery expectation: row marked ``corrupted`` with
    ``ram_body_lost_on_restart`` reason.
    """
    body_bytes = b"phantom-crash-window-step-1"
    chain_id = uuid4()

    # Pre-crash: insert row + RAM body, then "crash" (do nothing).
    store = await _fresh_store(tmp_path)
    _, ram, _ = await _fresh_body_stores(tmp_path)
    await store.insert(_ram_row(chain_id, body_bytes))
    await ram.put(chain_id, {"body": body_bytes})

    # Verify intermediate state: row + RAM body, no disk file.
    fbs_check = FileBodyStore(tmp_path / "bodies")
    assert not await fbs_check.has_body_ref(chain_id, "body")

    # Simulate crash: drop in-memory state, re-open from disk.
    await store.stop()

    # Post-crash: fresh store + fresh hybrid body store (RAM empty).
    store2 = await _fresh_store(tmp_path)
    hybrid2, _, _ = await _fresh_body_stores(tmp_path)

    await run_recovery(store2, hybrid2)

    fresh = await store2.get(chain_id)
    assert fresh is not None
    assert fresh.state == "corrupted"
    assert fresh.last_error is not None
    assert fresh.last_error.startswith("ram_body_lost_on_restart:")


async def test_crash_between_file_put_and_mark_persisted(tmp_path: Path) -> None:
    """WS-4 row: Step 2 done (files on disk + fsynced), Step 3 not yet.

    Pre-crash on-disk state: row at ``body_location='ram'``, body
    files on disk (FileBodyStore.put has fsync + atomic rename
    semantics, so files are durable), RAM intact.

    Post-crash environment: RAM lost; on-disk SQLite says ``ram``;
    body files present on disk but the row claims they're in RAM.

    Recovery expectation: row preserved at ``queued`` — even though
    the commit-point did not flip ``body_location``, the durable
    on-disk state IS the bytes, and :class:`HybridBodyStore` falls
    through to disk when RAM is empty. The persist-handoff is
    crash-safe by ordering, not by atomic flip. The on-disk file is
    visible to the recovery integrity check via the hybrid store and
    the row passes.

    The leftover ``body_location='ram'`` claim is operational debt
    that the next attempt's PersistController invocation may or may
    not clean up (the row's body is already durable on disk, so the
    next migration is a no-op from a durability standpoint).
    """
    body_bytes = b"phantom-crash-window-step-2"
    chain_id = uuid4()

    store = await _fresh_store(tmp_path)
    _, ram, fbs = await _fresh_body_stores(tmp_path)
    await store.insert(_ram_row(chain_id, body_bytes))
    await ram.put(chain_id, {"body": body_bytes})

    # Simulate Step 2 completing: write body files (FileBodyStore.put
    # does the fsync internally), but DO NOT call mark_persisted.
    await fbs.put(chain_id, {"body": body_bytes})
    assert await fbs.has_body_ref(chain_id, "body")

    # "Crash."
    await store.stop()

    # Post-restart: RAM empty, file body present, body_location='ram'.
    # Recovery passes the row through unchanged because the body IS
    # present on disk (HybridBodyStore.has_body_ref falls through).
    store2 = await _fresh_store(tmp_path)
    hybrid2, _, _ = await _fresh_body_stores(tmp_path)
    await run_recovery(store2, hybrid2)

    fresh = await store2.get(chain_id)
    assert fresh is not None
    assert fresh.state == "queued"
    # body_location is still 'ram' (Phase 1 recovery does not
    # heal/re-sync this column; it only quarantines genuinely missing
    # bodies). This is intentional — the body's durability is
    # established by the disk file, and the sender's body-read
    # router (HybridBodyStore) reads from disk via fall-through.
    assert fresh.body_location == "ram"
    # The bytes ARE recoverable via the hybrid store's disk fall-through.
    fetched = await hybrid2.get(chain_id, "body")
    assert fetched == body_bytes


async def test_crash_between_mark_persisted_and_ram_delete(tmp_path: Path) -> None:
    """WS-4 row: Step 3 done (commit-point committed), Step 4 not yet.

    Pre-crash on-disk state: row at ``body_location='file'``, body
    files on disk + fsynced, RAM still holds the bytes (but RAM is
    lost on restart anyway).

    Post-crash environment: RAM empty; SQLite says ``file``; body
    files present on disk.

    Recovery expectation: row preserved at ``queued`` — the
    commit-last-column invariant holds because the durable state
    after Step 3 IS the post-migration state. Step 4 is a cleanup
    that does not affect durability.
    """
    body_bytes = b"phantom-crash-window-step-3"
    chain_id = uuid4()

    store = await _fresh_store(tmp_path)
    _, ram, fbs = await _fresh_body_stores(tmp_path)
    await store.insert(_ram_row(chain_id, body_bytes))
    await ram.put(chain_id, {"body": body_bytes})

    # Step 2 (file write + fsync) and Step 3 (mark_persisted) both
    # complete. Step 4 (ram.delete) does NOT run.
    await fbs.put(chain_id, {"body": body_bytes})
    await store.mark_persisted(chain_id)

    assert await fbs.has_body_ref(chain_id, "body")
    mid = await store.get(chain_id)
    assert mid is not None
    assert mid.body_location == "file"

    # "Crash."
    await store.stop()

    # Post-restart: RAM empty, file body present, body_location='file'.
    store2 = await _fresh_store(tmp_path)
    hybrid2, _, _ = await _fresh_body_stores(tmp_path)
    await run_recovery(store2, hybrid2)

    fresh = await store2.get(chain_id)
    assert fresh is not None
    # The row's state is unchanged ('queued') — recovery only resets
    # attempting → queued and quarantines bodyless rows; the file is
    # present so this row is fine.
    assert fresh.state == "queued"
    assert fresh.body_location == "file"
    # Body file still present and readable on the new FileBodyStore.
    fbs2 = FileBodyStore(tmp_path / "bodies")
    assert await fbs2.has_body_ref(chain_id, "body")
    assert await fbs2.get(chain_id, "body") == body_bytes
