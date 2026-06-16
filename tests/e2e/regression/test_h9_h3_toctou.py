"""H9 — TOCTOU race regression test for H3 closure.

Plan § 6.2.9 / strategy §5 Layer 1 H9.

The H3 audit identified a race window: the reaper / admin-cancel
could mutate a row's state while the sender pool held it in
``attempting``. The pre-Phase-1 race shape was:

    sender:   read row (state='attempting')
    sender:   <... do upstream work ...>
    reaper:   DELETE row WHERE state IN terminal
              (no row was terminal yet, no-op)
    sender:   UPDATE row SET state='succeeded'  ← OVERWRITE
    admin:    cancel: UPDATE state='cancelled'  ← LOST UPDATE

Phase 1 + Phase 2 § 3.2.8 closed this by guarding the sender's
terminal UPDATE with ``WHERE state = :expected_state`` (defaulting
to 'attempting'). The H9 regression test forces the race window
through deliberate sequencing and asserts:

1. The sender's ``record_attempt_result`` returns rowcount=0 when
   the row's state moved out of 'attempting' between claim and
   write.
2. The pre-empting state (set by the admin cancel) is preserved —
   not silently overwritten by the sender's terminal UPDATE.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from phantom.models.upload import BodyHash, BodyHashes, StorageHash, UploadRow
from phantom.storage import SqliteUploadStore

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]


def _row(chain_id) -> UploadRow:
    """Build an attempting row with body_hashes."""
    body_bytes = b"phantom-h9-h3-toctou-body"
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
            "state": "attempting",
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


async def test_h9_cancel_during_attempt_preserves_cancelled_state(tmp_path: Path) -> None:
    """Admin cancel mid-attempt: sender's UPDATE is rejected, cancel wins.

    Forces the race:
    1. Insert a row at state='attempting'.
    2. Simulate an admin cancel: UPDATE state='cancelled'.
    3. Simulate the sender's terminal UPDATE: record_attempt_result
       expecting state='attempting'.
    4. Assert (a) record_attempt_result returns 0 (no rows updated)
       AND (b) the row's persisted state is still 'cancelled' (not
       overwritten by the sender's path).
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await store.start()

    chain_id = uuid4()
    await store.insert(_row(chain_id))

    # Admin cancel lands first.
    ok = await store.update_state(
        chain_id,
        new_state="cancelled",
        expected_state="attempting",
    )
    assert ok is True

    # Sender's record_attempt_result fires next, expecting attempting.
    # The WHERE state='attempting' predicate filters it out.
    rowcount = await store.record_attempt_result(
        chain_id,
        new_state="succeeded",
        attempts=1,
        next_attempt_at=None,
        last_error=None,
        upstream_status=200,
        upstream_headers_json="{}",
        captured_values=None,
        current_step_index=1,
        last_step_completed="put_s3",
        expected_state="attempting",
    )
    assert rowcount == 0, (
        f"record_attempt_result must return 0 when row is no longer attempting; "
        f"got {rowcount} (the H3 race regression test would catch a regression "
        f"where the predicate is removed)"
    )

    # The persisted state survives as 'cancelled' — no silent overwrite.
    fresh = await store.get(chain_id)
    assert fresh is not None
    assert fresh.state == "cancelled"


async def test_h9_replay_during_attempt_preserves_queued_state(tmp_path: Path) -> None:
    """Admin replay mid-attempt: sender's UPDATE rejected, replay wins.

    Replay flips state attempting → queued. The sender's
    record_attempt_result expecting attempting must not overwrite
    the queued state.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await store.start()

    chain_id = uuid4()
    await store.insert(_row(chain_id))

    # Admin replay lands first.
    ok = await store.update_state(
        chain_id,
        new_state="queued",
        expected_state="attempting",
    )
    assert ok is True

    # Sender's record_attempt_result is rejected by the WHERE predicate.
    rowcount = await store.record_attempt_result(
        chain_id,
        new_state="failed",
        attempts=2,
        next_attempt_at=None,
        last_error="upstream 503",
        upstream_status=503,
        upstream_headers_json="{}",
        captured_values=None,
        current_step_index=1,
        last_step_completed="put_s3",
        expected_state="attempting",
    )
    assert rowcount == 0

    fresh = await store.get(chain_id)
    assert fresh is not None
    assert fresh.state == "queued"
