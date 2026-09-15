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
    await store.mark_persisted(migrated.chain_id, received_at=migrated.received_at)

    assert await store.list_oldest_ram_bodies(limit=64) == []


@pytest.mark.asyncio
async def test_a_stamped_row_does_not_shield_its_leaked_files(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the orphan janitor's known-set excludes discarded rows.

    Expected: a stamped row is absent from the known-set, so files left behind
    by an interrupted stamp-then-delete become reclaimable orphans.

    The reaper stamps a row and deletes its bodies as two steps. A crash or a
    TaskGroup cancellation between them leaves a stamped row whose bytes are
    still on disk, and every reclaimer was then closed to it: the reaper's own
    body pass filters on an unstamped row so it never retries, recovery skips
    stamped rows, the invariant auditor walks deliverable rows only, and the
    janitor treated the surviving row as proof its files were wanted.

    The reaper's comment claims the metadata pass and the janitor converge. For
    ``stored`` that is weakly true, because the row-count cap eventually evicts
    the row and that path deletes its bodies. For ``auth_expired`` it is false
    outright: the state is deliberately excluded from the terminal set so the
    count-cap eviction can never take it, and its metadata retention defaults
    to never, so the row never leaves the table and its files stay on disk for
    the life of the deployment while still counting against the disk cap.
    """
    live = make_upload_row(state="auth_expired", body_discarded_at=None)
    await store.insert(live)
    leaked = make_upload_row(state="auth_expired", body_discarded_at=None)
    await store.insert(leaked)

    # The reaper stamps, and is then interrupted before deleting the files.
    flip = await store.discard_body_and_zero_accounting(
        leaked.chain_id, expected_state="auth_expired"
    )
    assert flip.flipped is True

    known = set(await store.list_chain_ids_with_bodies())

    assert live.chain_id in known, "a row that still has bytes must be protected"
    assert leaked.chain_id not in known, (
        "the stamped row is still in the janitor's known-set, so it shields "
        "the files the interrupted delete left on disk and nothing can reclaim "
        "them for the life of the deployment"
    )
    # The all-rows alias is unchanged: it feeds the idempotency-index preserve
    # set, which does want every row.
    assert leaked.chain_id in set(await store.list_chain_ids())
