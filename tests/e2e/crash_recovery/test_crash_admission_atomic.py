"""Admission atomic-transaction crash test (WS-4 matrix new row).

Plan § 2.3.17 collapsed admission into a single SQLite transaction
that atomically inserts the upload row AND the idempotency claim
(closes H7 structurally). The WS-4 matrix did not have a row for
this surface because admission was two transactions in the old shape;
Phase 5 adds the regression test.

A crash mid-transaction must leave neither the upload row nor the
idempotency claim — SQLite's ACID guarantees ensure the transaction
either commits both or neither. We simulate the crash by injecting
a failure between the two INSERTs via monkeypatch on the underlying
aiosqlite ``execute`` call, then assert post-restart that the row
and claim are both absent.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import pytest
from phantom.models.upload import BodyHash, BodyHashes, StorageHash, UploadRow
from phantom.storage import InsertClaimOutcome, SqliteUploadStore

pytestmark = [pytest.mark.asyncio]


def _build_row(chain_id) -> UploadRow:
    """Build a minimal queued, RAM-located row for admission."""
    body_bytes = b"phantom-atomic-test"
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


async def test_crash_during_admission_atomic_transaction(tmp_path: Path) -> None:
    """Crash between the two atomic INSERTs leaves neither persisted.

    The Phase 1 ``insert_with_idempotency_claim`` method wraps both
    inserts in a single ``BEGIN``/``commit`` block. Inject a failure
    after the first INSERT but before the commit; assert rollback.
    """
    chain_id = uuid4()
    row = _build_row(chain_id)
    idempotency_key = "atomic-crash-test"

    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await store.start()

    # Monkeypatch the underlying connection's execute to raise the
    # second time it's called within insert_with_idempotency_claim
    # (the first execute is BEGIN; the second is the uploads INSERT;
    # the third is the idempotency INSERT — fail at #3).
    conn = store._require_conn()  # type: ignore[reportPrivateUsage]
    original_execute = conn.execute
    call_count = {"n": 0}

    async def failing_execute(*args: Any, **kwargs: Any) -> Any:
        call_count["n"] += 1
        if call_count["n"] == 3:
            raise RuntimeError("simulated crash mid-transaction")
        return await original_execute(*args, **kwargs)

    with (
        patch.object(conn, "execute", side_effect=failing_execute),
        pytest.raises(RuntimeError, match="simulated crash"),
    ):
        await store.insert_with_idempotency_claim(row, idempotency_key)

    # Simulate restart: close + re-open. Recovery does NOT run yet —
    # we are asserting on the atomic-transaction property only.
    await store.stop()
    store2 = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await store2.start()

    # Neither side of the transaction persisted.
    fresh = await store2.get(chain_id)
    assert fresh is None, "uploads row leaked despite mid-transaction crash"

    # Idempotency claim also absent (re-claiming the same key now
    # succeeds — proof that the prior claim did not commit).
    second_attempt = await store2.insert_with_idempotency_claim(row, idempotency_key)
    assert second_attempt is InsertClaimOutcome.INSERTED, (
        "idempotency claim leaked despite mid-transaction crash; "
        "second insert should have succeeded fresh"
    )


async def test_crash_during_admission_after_first_insert_rollback(tmp_path: Path) -> None:
    """Verify the rollback path: explicit IntegrityError on the second INSERT.

    If the second INSERT raises an IntegrityError (e.g., idempotency
    collision), the first INSERT must roll back. Insert a row + key,
    then try to insert a *different* row with the same key — the
    method returns IDEMPOTENCY_COLLISION AND the second row's chain_id
    is absent.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await store.start()

    chain_id_a = uuid4()
    chain_id_b = uuid4()
    row_a = _build_row(chain_id_a)
    row_b = _build_row(chain_id_b)
    shared_key = "collision-test-key"

    # First insert succeeds.
    ok_a = await store.insert_with_idempotency_claim(row_a, shared_key)
    assert ok_a is InsertClaimOutcome.INSERTED

    # Second insert collides on the idempotency key; method returns
    # IDEMPOTENCY_COLLISION AND the second row's chain_id is NOT in uploads.
    ok_b = await store.insert_with_idempotency_claim(row_b, shared_key)
    assert ok_b is InsertClaimOutcome.IDEMPOTENCY_COLLISION

    fresh_b = await store.get(chain_id_b)
    assert fresh_b is None, "row B leaked despite idempotency-collision rollback"

    # Row A is still present.
    fresh_a = await store.get(chain_id_a)
    assert fresh_a is not None
    assert fresh_a.chain_id == chain_id_a
