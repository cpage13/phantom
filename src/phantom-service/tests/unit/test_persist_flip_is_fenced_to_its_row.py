"""The migration's flip must apply to the row it read, not to the key.

Objective: pin the fencing token on ``mark_persisted``.

THE DEFECT THIS CLOSES (SW-6). ``PersistController._migrate_one`` reads a row,
writes its RAM bytes to disk, then flips ``body_location`` to ``'file'``. The
flip was guarded on ``chain_id``, ``body_location='ram'`` and
``body_discarded_at IS NULL``. All three are questions about STATE; none is a
question about IDENTITY.

A chain_id becomes reusable the instant its row is deleted, and admission may
legally re-admit one at any point inside the migration's await window. The new
row is admitted RAM-resident in hybrid mode, so it satisfies every guard the
old row did. The flip then landed on a row whose bytes the migration had never
written, marking it ``'file'`` while its bytes were still only in RAM, and the
migration went on to believe it had succeeded.

The damage is the flip rather than the delete, which is why a guard on the RAM
delete alone could not fix it and why the rowcount-zero undo arm already does
the right thing once the flip is refused.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from phantom.models.upload import UploadRow
from phantom.storage.sqlite_store import SqliteUploadStore

_BASE = datetime(2026, 9, 15, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
async def store():
    """A started in-memory store; stops after the test."""
    s = SqliteUploadStore(":memory:")
    await s.start()
    yield s
    await s.stop()


async def test_the_flip_refuses_a_re_admitted_row_at_the_same_chain_id(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: a row re-admitted mid-migration is not flipped.

    Expected: rowcount 0, and the new row still reads ``'ram'``. The migration
    read the OLD row and wrote the OLD bytes to disk; flipping the NEW row
    would claim its bytes are on disk when they are only in RAM, and the row
    would then be undeliverable after a restart.
    """
    chain_id = uuid4()
    original = make_upload_row(
        chain_id=chain_id, state="queued", body_location="ram", received_at=_BASE
    )
    await store.insert(original)

    # The migration snapshots the row it is about to move.
    snapshot_received_at = original.received_at

    # Mid-migration: the row is deleted and the SAME chain_id is re-admitted.
    await store.delete(chain_id)
    readmitted = make_upload_row(
        chain_id=chain_id,
        state="queued",
        body_location="ram",
        received_at=_BASE + timedelta(seconds=30),
    )
    await store.insert(readmitted)

    flipped = await store.mark_persisted(chain_id, received_at=snapshot_received_at)

    assert flipped == 0, (
        "the flip landed on a row the migration never read; the re-admitted "
        "upload is now marked as on-disk while its bytes are only in RAM"
    )
    live = await store.get(chain_id)
    assert live is not None
    assert live.body_location == "ram", (
        "the re-admitted row's body_location was overwritten by another row's "
        "migration, so a restart would look for bytes that were never written"
    )


async def test_the_flip_still_applies_to_the_row_it_read(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the ordinary migration is unaffected.

    Expected: rowcount 1 and the row reads ``'file'``. The fence must not turn
    every healthy migration into a refusal, which is the way this guard could
    fail closed and silently stop RAM ever reaching disk.
    """
    row = make_upload_row(state="queued", body_location="ram", received_at=_BASE)
    await store.insert(row)

    flipped = await store.mark_persisted(row.chain_id, received_at=row.received_at)

    assert flipped == 1
    live = await store.get(row.chain_id)
    assert live is not None
    assert live.body_location == "file"


async def test_the_discard_guard_still_refuses_a_stamped_row(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the H4 carve-out survives the new guard.

    Expected: rowcount 0. Adding an identity guard must not displace the state
    guard that stops a migration resurrecting policy-discarded bytes.
    """
    row = make_upload_row(state="succeeded", body_location="ram", received_at=_BASE)
    await store.insert(row)
    flip = await store.discard_body_and_zero_accounting(row.chain_id, expected_state="succeeded")
    assert flip.flipped is True

    assert await store.mark_persisted(row.chain_id, received_at=row.received_at) == 0
