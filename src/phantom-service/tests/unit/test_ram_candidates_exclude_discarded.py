"""RAM-pressure migration candidates must still have a body to migrate.

Objective: pin the deliverability filter on ``list_oldest_ram_bodies``.

THE DEFECT THIS CLOSES. ``body_location`` is written by exactly one statement
after INSERT, ``mark_persisted``'s flip to ``'file'``, and nothing ever moves it
back or clears it when a body is discarded. The candidate query filtered on that
column alone, so every row discarded while RAM-resident stayed a permanent
``'ram'`` candidate for as long as the row existed.

Those corpses are OLDER by ``received_at`` than anything still holding memory,
so they crowded the front of an oldest-first query. The dominant source is the
DEFAULT SUCCESS PATH: ``succeeded_body_seconds`` defaults to 0, so the sender
discards every delivered upload's bytes immediately while leaving
``body_location`` at ``'ram'``, and ``succeeded_metadata_seconds`` is 180.

The watcher then enqueued rows with no body, ``PersistController._migrate_one``
rejected each at its deliverability pre-check, nothing migrated, and the
watcher logged unresolved pressure at 1 Hz forever. With the ceiling lowered,
which is the documented reason the knob exists, RAM grew to the in-flight byte
cap and the process was OOM-killed, after which recovery quarantined every
RAM-resident row and the buffered uploads were lost. CONTEXT.md states the
ceiling "is an enforced bound, not a best-effort gauge"; it was not enforceable.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest
from phantom.models.upload import UploadRow
from phantom.storage.sqlite_store import SqliteUploadStore

_BASE = datetime(2026, 9, 8, 9, 0, 0, tzinfo=UTC)


@pytest.fixture
async def store():
    """A started in-memory store; stops after the test."""
    s = SqliteUploadStore(":memory:")
    await s.start()
    yield s
    await s.stop()


@pytest.mark.asyncio
async def test_discarded_rows_are_not_migration_candidates(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: a delivered-and-discarded row is not offered for migration.

    Expected: only the live RAM row comes back. The discarded row is older, so
    before the filter it sorted ahead of the live one and would have consumed
    the candidate slot while freeing nothing.
    """
    discarded = make_upload_row(
        state="succeeded",
        body_location="ram",
        received_at=_BASE,
        updated_at=_BASE,
    )
    await store.insert(discarded)
    flip = await store.discard_body_and_zero_accounting(
        discarded.chain_id, expected_state="succeeded"
    )
    assert flip.flipped is True

    live = make_upload_row(
        state="queued",
        body_location="ram",
        received_at=_BASE + timedelta(minutes=5),
        updated_at=_BASE + timedelta(minutes=5),
    )
    await store.insert(live)

    candidates = await store.list_oldest_ram_bodies(limit=64)

    assert candidates == [live.chain_id], (
        "a row whose body was already discarded was offered as a migration "
        "candidate; migrating it frees no RAM and the pressure never resolves"
    )


@pytest.mark.asyncio
async def test_corpses_do_not_crowd_out_live_rows(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the candidate window is not consumed by rows with no body.

    Expected: with more discarded rows than the window holds, every returned
    candidate is still a live one. This is the shape that made the ceiling
    unenforceable: at a modest upload rate the whole window filled with
    delivered-and-discarded rows, all older than anything actually holding
    memory, and every tick enqueued work the controller then rejected.
    """
    window = 8
    for minute in range(20):
        corpse = make_upload_row(
            state="succeeded",
            body_location="ram",
            received_at=_BASE + timedelta(minutes=minute),
            updated_at=_BASE + timedelta(minutes=minute),
        )
        await store.insert(corpse)
        flip = await store.discard_body_and_zero_accounting(
            corpse.chain_id, expected_state="succeeded"
        )
        assert flip.flipped is True

    live_ids = []
    for minute in range(3):
        live = make_upload_row(
            state="queued",
            body_location="ram",
            received_at=_BASE + timedelta(hours=1, minutes=minute),
            updated_at=_BASE + timedelta(hours=1, minutes=minute),
        )
        await store.insert(live)
        live_ids.append(live.chain_id)

    candidates = await store.list_oldest_ram_bodies(limit=window)

    assert candidates == live_ids, (
        f"expected only the {len(live_ids)} live RAM rows, got {len(candidates)} "
        "candidates; discarded rows are older so they crowd an oldest-first window"
    )


@pytest.mark.asyncio
async def test_a_row_already_moved_to_disk_is_not_a_candidate(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the ordinary post-migration exclusion still holds.

    Expected: a row flipped to ``'file'`` is absent. The new filter must not
    be the only thing excluding rows, so this pins that the original
    ``body_location`` predicate is intact.
    """
    migrated = make_upload_row(state="queued", body_location="ram", received_at=_BASE)
    await store.insert(migrated)
    await store.mark_persisted(migrated.chain_id)

    assert await store.list_oldest_ram_bodies(limit=64) == []
