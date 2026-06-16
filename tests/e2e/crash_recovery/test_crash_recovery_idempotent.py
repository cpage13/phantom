"""Recovery-rerun idempotency test (WS-4 matrix: recovery itself).

Recovery is invoked at startup BEFORE workers are scheduled (plan
§ 2.3.15). A crash mid-recovery must be handled by the next startup
running recovery again with the same effect.

Two specific properties tested:

1. Running recovery twice in a row yields the same end state as
   running it once — the operations (reset attempting → queued,
   mark_corrupted) are idempotent at the row level.
2. A crash partway through recovery (after some rows have been
   marked corrupted but before others have been visited) is
   recoverable: the next pass picks up the un-marked rows and the
   end state matches a single full pass.
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


def _row(
    chain_id,
    *,
    state: str = "queued",
    body_location: str = "ram",
    with_body: bool = True,
) -> UploadRow:
    """Build a minimal upload row."""
    now = datetime.now(tz=UTC)
    base: dict[str, object] = {
        "chain_id": chain_id,
        "instance_id": "primary",
        "group_id": chain_id,
        "multifile_id": chain_id,
        "send_order": 0,
        "route_name": "r",
        "state": state,
        "body_location": body_location,
        "received_at": now,
        "updated_at": now,
        "endpoint": "e",
        "uid": "u",
        "chain_envelope_json": "{}",
        "idempotency_key": f"k-{chain_id}",
        "capture_reexecution_active": False,
    }
    if with_body:
        body_bytes = f"body-{chain_id}".encode()
        digest = hashlib.sha256(body_bytes).hexdigest()
        base["body_hashes"] = {
            "body": BodyHashes(
                body_hash=BodyHash(digest),
                storage_hash=StorageHash(digest),
            ),
        }
        base["body_size_bytes"] = len(body_bytes)
    return UploadRow.model_validate(base)


async def test_recovery_runs_twice_idempotently(tmp_path: Path) -> None:
    """Running recovery twice gives the same end state as running it once.

    Three rows: one attempting, one with RAM body lost, one with file
    body present. First recovery pass:

    - attempting → queued
    - RAM-lost → corrupted
    - file-present → unchanged at queued

    Second recovery pass: no further changes (the operations are
    idempotent — attempting → queued no-ops on a queued row;
    mark_corrupted on an already-corrupted row updates state to
    'corrupted' which is still 'corrupted'; the file-present row
    still passes the integrity check).
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await store.start()
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    hybrid = HybridBodyStore(ram=ram, disk=fbs)
    await hybrid.start()

    attempting_id = uuid4()
    ram_lost_id = uuid4()
    file_present_id = uuid4()

    await store.insert(_row(attempting_id, state="attempting", with_body=False))
    await store.insert(_row(ram_lost_id))  # RAM body never populated → lost
    await store.insert(_row(file_present_id, body_location="file"))
    await fbs.put(file_present_id, {"body": f"body-{file_present_id}".encode()})

    # First recovery pass.
    await run_recovery(store, hybrid)
    states_pass1 = {
        attempting_id: (await store.get(attempting_id)).state,  # type: ignore[union-attr]
        ram_lost_id: (await store.get(ram_lost_id)).state,  # type: ignore[union-attr]
        file_present_id: (await store.get(file_present_id)).state,  # type: ignore[union-attr]
    }
    assert states_pass1[attempting_id] == "queued"
    assert states_pass1[ram_lost_id] == "corrupted"
    assert states_pass1[file_present_id] == "queued"

    # Second recovery pass — same end state.
    await run_recovery(store, hybrid)
    states_pass2 = {
        attempting_id: (await store.get(attempting_id)).state,  # type: ignore[union-attr]
        ram_lost_id: (await store.get(ram_lost_id)).state,  # type: ignore[union-attr]
        file_present_id: (await store.get(file_present_id)).state,  # type: ignore[union-attr]
    }
    assert states_pass2 == states_pass1


async def test_recovery_crash_partway_then_rerun(tmp_path: Path) -> None:
    """A crash mid-recovery is recoverable: the next pass finishes the job.

    Manually mark a subset of the rows as if a prior recovery pass
    had crashed partway through. The next pass observes a mixed
    population (some already corrected, some still original) and
    completes the sweep with the same end state as a single full
    pass would produce.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await store.start()
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    hybrid = HybridBodyStore(ram=ram, disk=fbs)
    await hybrid.start()

    # Three attempting rows. Simulate a partial first-pass: reset
    # only the first of the three to 'queued' before invoking
    # run_recovery.
    ids = [uuid4() for _ in range(3)]
    for cid in ids:
        await store.insert(_row(cid, state="attempting", with_body=False))

    # Simulate partial first-pass: manually reset only ids[0].
    await store.update_state(ids[0], new_state="queued", expected_state="attempting")
    states_partial = [(await store.get(cid)).state for cid in ids]  # type: ignore[union-attr]
    assert states_partial == ["queued", "attempting", "attempting"]

    # Resumed recovery sweep completes the un-finished work.
    await run_recovery(store, hybrid)
    final_states = [(await store.get(cid)).state for cid in ids]  # type: ignore[union-attr]
    assert final_states == ["queued", "queued", "queued"]
