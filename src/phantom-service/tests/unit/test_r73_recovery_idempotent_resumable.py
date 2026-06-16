"""Adopted EXPLORATORY regression test for aggressor Round-7 finding R7-3.

Adopted by Defender R8 as permanent coverage (the property held on current code;
this pins it so a future recovery refactor cannot silently regress it).

Catalog-ref: A-05 (SIGKILL during the recovery sweep — compound failure). Source:
"How SQLite Is Tested" §3.4 (an I/O error or OOM WHILE RECOVERING FROM A PRIOR
CRASH). Directly validates R6's two-pass cursor-drain ``run_recovery`` against a
re-crash mid-sweep.

GREEN on current code — pins the desirable property so a future refactor that
makes recovery non-idempotent (e.g. double-quarantine, or losing the resumable
prefix) goes RED. Two cases:

1. Recovery run twice (a re-crash AFTER a complete sweep) is IDEMPOTENT: the
   second run produces the identical state — no extra quarantines, no drift, no
   exception.

2. A kill DURING pass 2 (only a PREFIX of the targets quarantined) followed by a
   full re-run is CONVERGENT/RESUMABLE: the restart finishes quarantining the
   remainder, and re-quarantining an already-corrupted row is a harmless no-op
   (``mark_corrupted`` has no state guard).

Belongs under ``src/phantom-service/tests/unit/`` (the deterministic store-level proof).
The real-subprocess re-crash variant (kill the first restart mid-recovery, third
boot converges) lives in the reproducer and could be adopted under
``tests/e2e/crash_recovery/`` alongside the V1/V2 SIGKILL test.
"""

from __future__ import annotations

import tempfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from phantom.models.upload import (
    BodyHash,
    BodyHashes,
    CapturedValues,
    StorageHash,
    UploadRow,
)
from phantom.storage.ram_body_store import RamBodyStore
from phantom.storage.sqlite_store import SqliteUploadStore
from phantom.workers.recovery import run_recovery


def _ram_lost_row(state: str = "queued") -> UploadRow:
    """A body_location='ram' row with non-empty body_hashes and NO bytes in RAM."""
    now = datetime.now(tz=UTC)
    cid = uuid4()
    return UploadRow(  # type: ignore[call-arg]
        chain_id=cid,
        instance_id="primary",
        group_id=cid,
        multifile_id=cid,
        send_order=0,
        route_name="r7-3",
        state=state,
        body_location="ram",
        next_attempt_at=now,
        received_at=now,
        updated_at=now,
        endpoint="https://files.example.com/v2/files",
        uid="00000000-0000-0000-0000-000000000001",
        chain_envelope_json="{}",
        captured_values=CapturedValues(),
        current_step_index=0,
        idempotency_key=str(cid),
        chain_id_at_ingress=None,
        capture_reexecution_active=False,
        body_size_bytes=64,
        storage_encoding="original",
        body_hashes={
            "body": BodyHashes(body_hash=BodyHash("a" * 64), storage_hash=StorageHash("b" * 64))
        },
    )


async def _census(store: SqliteUploadStore) -> dict[str, int]:
    out: dict[str, int] = {}
    async for row in store.iter_rows():
        out[row.state] = out.get(row.state, 0) + 1
    return out


@pytest.mark.asyncio
async def test_recovery_is_idempotent_on_rerun() -> None:
    """Running recovery twice (re-crash after a full sweep) is a no-op the 2nd time."""
    tmp = Path(tempfile.mkdtemp(prefix="test-r7-3-1a-"))
    store = SqliteUploadStore(str(tmp / "uploads.db"))
    await store.start()
    ram = RamBodyStore()  # EMPTY → every body_location='ram' row is RAM-lost
    await ram.start()
    try:
        for _ in range(4):
            await store.insert(_ram_lost_row())
        for _ in range(2):
            await store.insert(_ram_lost_row(state="attempting"))

        await run_recovery(store, ram)
        first = await _census(store)
        await run_recovery(store, ram)  # re-crash: recovery runs again
        second = await _census(store)

        assert first == second, f"recovery not idempotent: {first} != {second}"
        assert second.get("corrupted", 0) == 6, f"expected 6 quarantined, got {second}"
        assert sum(second.values()) == 6, "a row was lost across the re-run"
    finally:
        await store.stop()


@pytest.mark.asyncio
async def test_recovery_resumes_after_mid_sweep_kill() -> None:
    """A kill mid-pass-2 (prefix quarantined) then a full re-run converges."""
    tmp = Path(tempfile.mkdtemp(prefix="test-r7-3-1b-"))
    store = SqliteUploadStore(str(tmp / "uploads.db"))
    await store.start()
    ram = RamBodyStore()
    await ram.start()
    try:
        rows = [_ram_lost_row() for _ in range(6)]
        for r in rows:
            await store.insert(r)

        # Simulate a SIGKILL during pass 2 after writing only the first 3
        # quarantines (recovery's pass 2 is a plain for-loop over collected
        # targets, so a kill leaves a corrupted prefix + a queued remainder).
        for r in rows[:3]:
            await store.mark_corrupted(r.chain_id, "ram_body_lost_on_restart: ['body'] (partial)")
        mid = await _census(store)
        assert mid == {"corrupted": 3, "queued": 3}, f"unexpected mid-kill state: {mid}"

        # The restart runs a FULL recovery: it must finish the remaining 3 and
        # re-walk the already-corrupted 3 harmlessly.
        await run_recovery(store, ram)
        final = await _census(store)
        assert final == {"corrupted": 6}, f"recovery did not converge: {final}"
    finally:
        await store.stop()
