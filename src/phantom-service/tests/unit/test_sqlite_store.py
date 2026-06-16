"""Unit tests for phantom.storage.sqlite_store."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from phantom.models.upload import UploadRow
from phantom.storage.interface import InsertClaimOutcome, StateTally
from phantom.storage.sqlite_store import SqliteUploadStore


@pytest.fixture
async def store():
    """A started in-memory store; stops after the test.

    Slice 1.B: ``tier=`` ctor param is gone (plan § 2.3.7); the
    store is rooted at ``":memory:"`` for the unit-test convenience
    even though production no longer uses an in-memory SQLite.
    """
    s = SqliteUploadStore(":memory:")
    await s.start()
    yield s
    await s.stop()


@pytest.mark.asyncio
async def test_start_creates_schema() -> None:
    """``start`` applies the schema cleanly."""
    s = SqliteUploadStore(":memory:")
    await s.start()
    await s.stop()


@pytest.mark.asyncio
async def test_insert_roundtrip(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``get`` returns the inserted row."""
    row = make_upload_row()
    await store.insert(row)
    fetched = await store.get(row.chain_id)
    assert fetched is not None
    assert fetched.chain_id == row.chain_id
    assert fetched.state == row.state
    assert fetched.endpoint == row.endpoint


@pytest.mark.asyncio
async def test_get_missing_returns_none(store: SqliteUploadStore) -> None:
    """Missing uid returns ``None``."""
    assert await store.get(uuid4()) is None


@pytest.mark.asyncio
async def test_claim_due_atomic(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """10 queued rows; 8 concurrent claim_due(now,1) call → each row claimed once."""
    rows = [make_upload_row(next_attempt_at=datetime.now(tz=UTC)) for _ in range(10)]
    for row in rows:
        await store.insert(row)
    results = await asyncio.gather(*(store.claim_due(datetime.now(tz=UTC), 1) for _ in range(8)))
    claimed_ids = [r.chain_id for batch in results for r in batch]
    assert len(claimed_ids) == 8
    assert len(set(claimed_ids)) == 8  # no duplicates


@pytest.mark.asyncio
async def test_claim_due_skips_future(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """A future ``next_attempt_at`` is not claimed."""
    future = datetime.now(tz=UTC) + timedelta(hours=1)
    await store.insert(make_upload_row(next_attempt_at=future))
    claimed = await store.claim_due(datetime.now(tz=UTC), 5)
    assert claimed == []


@pytest.mark.asyncio
async def test_record_attempt_result_idempotent(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``record_attempt_result`` updates the row consistently."""
    row = make_upload_row(state="attempting")
    await store.insert(row)
    await store.record_attempt_result(
        row.chain_id,
        new_state="succeeded",
        attempts=1,
        next_attempt_at=None,
        last_error=None,
        upstream_status=200,
        upstream_headers_json=None,
        captured_values=None,
        current_step_index=1,
        last_step_completed="create_file",
    )
    fetched = await store.get(row.chain_id)
    assert fetched is not None
    assert fetched.state == "succeeded"
    assert fetched.attempts == 1
    assert fetched.upstream_status_code == 200


@pytest.mark.asyncio
async def test_list_pagination(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Cursor-paginated list returns sequential batches."""
    base = datetime.now(tz=UTC)
    for i in range(7):
        await store.insert(
            make_upload_row(next_attempt_at=base + timedelta(seconds=i)),
        )
    first, cursor = await store.list_uploads(limit=3)
    assert len(first) == 3
    assert cursor is not None
    second, cursor2 = await store.list_uploads(limit=3, cursor=cursor)
    assert len(second) == 3
    third, cursor3 = await store.list_uploads(limit=3, cursor=cursor2)
    assert len(third) == 1
    assert cursor3 is None


@pytest.mark.asyncio
async def test_cycle7_columns_roundtrip(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """group_id / multifile_id / send_order / sent_at survive insert + get."""
    group_id = uuid4()
    multifile_id = uuid4()
    sent_at = datetime.now(tz=UTC)
    row = make_upload_row(
        group_id=group_id,
        multifile_id=multifile_id,
        send_order=3,
        sent_at=sent_at,
    )
    await store.insert(row)
    fetched = await store.get(row.chain_id)
    assert fetched is not None
    assert fetched.group_id == group_id
    assert fetched.multifile_id == multifile_id
    assert fetched.send_order == 3
    assert fetched.sent_at == sent_at


@pytest.mark.asyncio
async def test_cycle7_nullable_columns_roundtrip_none(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """A standalone, undelivered row keeps multifile_id and sent_at NULL."""
    row = make_upload_row(multifile_id=None, sent_at=None)
    await store.insert(row)
    fetched = await store.get(row.chain_id)
    assert fetched is not None
    assert fetched.multifile_id is None
    assert fetched.sent_at is None


@pytest.mark.asyncio
async def test_list_filter_by_multifile_orders_by_send_order(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """The multifile_id filter returns ONLY the set, ordered by send_order ASC."""
    multifile_id = uuid4()
    inserted_out_of_order = [2, 0, 1]
    by_send_order: dict[int, UploadRow] = {}
    for position in inserted_out_of_order:
        row = make_upload_row(multifile_id=multifile_id, send_order=position)
        by_send_order[position] = row
        await store.insert(row)
    # An unrelated standalone row must be excluded.
    await store.insert(make_upload_row(multifile_id=None))

    rows, next_cursor = await store.list_uploads(multifile_id=multifile_id)

    assert [r.send_order for r in rows] == [0, 1, 2]
    assert [r.chain_id for r in rows] == [by_send_order[i].chain_id for i in (0, 1, 2)]
    assert next_cursor is None


@pytest.mark.asyncio
async def test_list_filter_by_group_id(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """The group_id filter returns exactly the group's rows and still paginates."""
    group_id = uuid4()
    base = datetime.now(tz=UTC)
    members = [
        make_upload_row(group_id=group_id, received_at=base + timedelta(seconds=i))
        for i in range(2)
    ]
    for row in members:
        await store.insert(row)
    await store.insert(make_upload_row())  # different group: excluded

    rows, _ = await store.list_uploads(group_id=group_id)
    assert {r.chain_id for r in rows} == {m.chain_id for m in members}

    # The keyset cursor applies to the group filter exactly as today.
    first_page, cursor = await store.list_uploads(group_id=group_id, limit=1)
    assert len(first_page) == 1
    assert cursor is not None
    second_page, cursor2 = await store.list_uploads(group_id=group_id, limit=1, cursor=cursor)
    assert len(second_page) == 1
    assert cursor2 is None
    assert first_page[0].chain_id != second_page[0].chain_id


@pytest.mark.asyncio
async def test_multifile_filter_rejects_cursor(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Combining the multifile filter with a keyset cursor raises ValueError.

    The cursor's receipt-time keyset is incompatible with the
    send_order ordering; the store fails loudly rather than silently
    ignoring the pagination request.
    """
    row = make_upload_row()
    await store.insert(row)
    cursor = SqliteUploadStore.build_resume_cursor_for(row)
    with pytest.raises(ValueError, match="multifile_id"):
        await store.list_uploads(multifile_id=uuid4(), cursor=cursor)


@pytest.mark.asyncio
async def test_multifile_filter_never_emits_cursor(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """A truncated multifile listing still returns next_cursor=None."""
    multifile_id = uuid4()
    member_count = 4
    for position in range(member_count):
        await store.insert(make_upload_row(multifile_id=multifile_id, send_order=position))
    rows, next_cursor = await store.list_uploads(multifile_id=multifile_id, limit=2)
    assert len(rows) == 2
    assert [r.send_order for r in rows] == [0, 1]
    assert next_cursor is None


@pytest.mark.asyncio
async def test_kv_query_finds_phantom_local_uuid(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``list_by_key_value`` finds rows by metadata key-value-store match.

    The stored envelope JSON carries metadata in camelCase
    (``keyValueStore``) because the upstream client's camelCase alias
    generator drives the serialization on the wire.
    The search path must match the wire convention exactly.
    """
    envelope_json = (
        '{"steps":[{"name":"create_file","body":{"value":{"metadata":'
        '{"keyValueStore":{"phantom_local_uuid":"abc"}}}}}]}'
    )
    await store.insert(make_upload_row(chain_envelope_json=envelope_json))
    matches = await store.list_by_key_value("phantom_local_uuid", "abc")
    assert len(matches) == 1


# KVS keys are user-defined dynamic keys, so every legal string must be
# addressable. Pre-fix the unquoted json-path interpolation re-segmented
# the path at '.' and could not express ':' or '"' keys at all (round 2
# defender fix R2-3: the path's key segment is now a quoted JSON1 label).
_SPECIAL_KVS_KEYS: tuple[str, ...] = (
    "tag:env",
    "telemetry.v2",
    'q"uote',
    "back\\slash",
)


@pytest.mark.asyncio
@pytest.mark.parametrize("key", _SPECIAL_KVS_KEYS)
async def test_kv_query_addresses_special_character_keys(
    store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
    key: str,
) -> None:
    """``list_by_key_value`` addresses colon/dot/quote/backslash keys exactly.

    Each seeded row carries the special-character key plus a decoy pair
    whose key is the truncation an unquoted path would have read, so a
    regression to the broken interpolation surfaces as a wrong-row or
    empty result, not a silent pass.
    """
    kvs = {key: "wanted", "tag": "decoy", "telemetry": "decoy"}
    body_value = {"metadata": {"keyValueStore": kvs}}
    envelope_json = json.dumps({"steps": [{"name": "create_file", "body": {"value": body_value}}]})
    row = make_upload_row(chain_envelope_json=envelope_json)
    await store.insert(row)
    matches = await store.list_by_key_value(key, "wanted")
    assert [m.chain_id for m in matches] == [row.chain_id]
    assert await store.list_by_key_value(key, "decoy") == []


@pytest.mark.asyncio
async def test_recovery_resets_attempting(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``reset_attempting_to_queued`` flips state and returns count."""
    await store.insert(make_upload_row(state="attempting"))
    await store.insert(make_upload_row(state="queued"))
    count = await store.reset_attempting_to_queued()
    assert count == 1


@pytest.mark.asyncio
async def test_recovery_reset_preserves_grouping_and_leaves_sent_at_null(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """A hard-kill recovery sweep re-queues without disturbing the cycle-7 columns.

    Round 3 adversary: an ``attempting`` row killed mid-flight has
    ``sent_at`` NULL by construction (the stamp lands only on confirmed
    final delivery). The sweep must flip state to ``queued`` so the
    sender re-claims it, while leaving ``group_id`` / ``multifile_id``
    / ``send_order`` exactly as admitted (a re-queued member must stay
    in its group) and ``sent_at`` NULL (no delivery was confirmed). A
    regression that touched any of those columns in the bulk UPDATE, or
    stamped sent_at on the re-queue, would silently mis-report group
    membership or fabricate a delivery time; this pins it.
    """
    group_id = uuid4()
    multifile_id = uuid4()
    attempting = make_upload_row(
        state="attempting",
        group_id=group_id,
        multifile_id=multifile_id,
        send_order=3,
        sent_at=None,
    )
    await store.insert(attempting)
    # A sibling that already delivered: terminal, sent_at stamped. The
    # sweep targets only attempting rows, so this stamp must survive.
    delivered_stamp = datetime.now(tz=UTC)
    delivered = make_upload_row(
        state="succeeded",
        group_id=group_id,
        multifile_id=multifile_id,
        send_order=1,
        sent_at=delivered_stamp,
    )
    await store.insert(delivered)

    count = await store.reset_attempting_to_queued()
    assert count == 1, "only the attempting row is swept"

    requeued = await store.get(attempting.chain_id)
    assert requeued is not None
    assert requeued.state == "queued"
    assert requeued.group_id == group_id
    assert requeued.multifile_id == multifile_id
    assert requeued.send_order == 3
    assert requeued.sent_at is None, "the re-queue must not fabricate a delivery time"

    survivor = await store.get(delivered.chain_id)
    assert survivor is not None
    assert survivor.state == "succeeded", "a terminal row is untouched by the sweep"
    assert survivor.sent_at == delivered_stamp, "the delivered sibling keeps its stamp"


@pytest.mark.asyncio
async def test_mark_persisted_flips_body_location(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``mark_persisted`` flips ``body_location`` from 'ram' to 'file'.

    Replaces the pre-Slice-1.B ``mark_committed`` test. The persist
    controller (Slice 1.C) is the sole writer of this transition
    per plan § 0.5 invariant #6 — the store method is what backs it.
    """
    row = make_upload_row(body_location="ram")
    await store.insert(row)
    await store.mark_persisted(row.chain_id)
    fetched = await store.get(row.chain_id)
    assert fetched is not None
    assert fetched.body_location == "file"


@pytest.mark.asyncio
async def test_mark_persisted_is_noop_when_already_file(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``mark_persisted`` on a ``body_location='file'`` row is a defensive no-op.

    The ``WHERE body_location = 'ram'`` guard makes a duplicate call
    safe — useful for the persist controller's retry-on-spurious-wake
    discipline (Slice 1.C). Without the guard, a duplicate call would
    silently rewrite ``updated_at`` on an already-persisted row.
    """
    row = make_upload_row(body_location="file")
    await store.insert(row)
    pre = await store.get(row.chain_id)
    assert pre is not None
    pre_updated_at = pre.updated_at
    await store.mark_persisted(row.chain_id)
    post = await store.get(row.chain_id)
    assert post is not None
    assert post.body_location == "file"
    assert post.updated_at == pre_updated_at  # guard rejected the write


@pytest.mark.asyncio
async def test_list_oldest_ram_bodies_orders_by_received_at(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``list_oldest_ram_bodies`` returns RAM-tier chain_ids by ``received_at`` ASC."""
    base = datetime.now(tz=UTC)
    row_a = make_upload_row(body_location="ram", received_at=base)
    row_b = make_upload_row(body_location="ram", received_at=base + timedelta(seconds=1))
    row_file = make_upload_row(body_location="file", received_at=base - timedelta(seconds=10))
    await store.insert(row_a)
    await store.insert(row_b)
    await store.insert(row_file)
    oldest = await store.list_oldest_ram_bodies(limit=10)
    assert oldest == [row_a.chain_id, row_b.chain_id]


@pytest.mark.asyncio
async def test_iter_rows_streams_every_row(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``iter_rows`` yields every inserted row."""
    inserted = [make_upload_row() for _ in range(3)]
    for row in inserted:
        await store.insert(row)
    streamed = [row async for row in store.iter_rows()]
    assert {r.chain_id for r in streamed} == {r.chain_id for r in inserted}


@pytest.mark.asyncio
async def test_mark_corrupted_quarantines_row(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``mark_corrupted`` flips state to ``corrupted`` + records the reason."""
    row = make_upload_row(state="attempting", body_location="file")
    await store.insert(row)
    await store.mark_corrupted(row.chain_id, "body file missing: foo.bin")
    fetched = await store.get(row.chain_id)
    assert fetched is not None
    assert fetched.state == "corrupted"
    assert fetched.last_error == "body file missing: foo.bin"
    assert fetched.next_attempt_at is None


@pytest.mark.asyncio
async def test_list_chain_ids_returns_all_ids(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``list_chain_ids`` returns every chain_id (orphan-janitor input set)."""
    rows = [make_upload_row() for _ in range(3)]
    for row in rows:
        await store.insert(row)
    ids = await store.list_chain_ids()
    assert set(ids) == {r.chain_id for r in rows}


@pytest.mark.asyncio
async def test_insert_with_idempotency_claim_happy_path(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Atomic admission insert + idempotency claim succeeds when key is fresh."""
    row = make_upload_row()
    accepted = await store.insert_with_idempotency_claim(row, "ingress-key-A")
    assert accepted is InsertClaimOutcome.INSERTED
    fetched = await store.get(row.chain_id)
    assert fetched is not None


@pytest.mark.asyncio
async def test_insert_with_idempotency_claim_rejects_duplicate(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Atomic admission rejects a duplicate idempotency key.

    Returns ``IDEMPOTENCY_COLLISION`` and rolls back the upload-row
    INSERT — closes H7 structurally (plan § 2.3.17). The second row's
    chain_id must NOT be present in the store after the rollback.
    """
    first = make_upload_row()
    second = make_upload_row()
    first_ok = await store.insert_with_idempotency_claim(first, "ingress-key-shared")
    second_ok = await store.insert_with_idempotency_claim(second, "ingress-key-shared")
    assert first_ok is InsertClaimOutcome.INSERTED
    assert second_ok is InsertClaimOutcome.IDEMPOTENCY_COLLISION
    # First row landed.
    assert await store.get(first.chain_id) is not None
    # Second row was rolled back.
    assert await store.get(second.chain_id) is None


@pytest.mark.asyncio
async def test_insert_with_idempotency_claim_both_rows_visible_after_commit(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Successful claim leaves BOTH ``uploads`` and ``idempotency_index`` rows.

    Closes H7 visibility — the atomic transaction's commit point must
    make both INSERTs observable together (no partial state). Slice 1.F
    explicit acceptance per plan § 2.3.21 #4.
    """
    row = make_upload_row()
    accepted = await store.insert_with_idempotency_claim(row, "ingress-visibility-key")
    assert accepted is InsertClaimOutcome.INSERTED
    # uploads row visible.
    assert await store.get(row.chain_id) is not None
    # idempotency_index row visible — claim replay returns the original
    # chain_id rather than re-inserting under a new chain_id.
    other_chain_id = uuid4()
    replayed = await store.claim_idempotency("ingress-visibility-key", other_chain_id)
    assert replayed == row.chain_id


@pytest.mark.asyncio
async def test_insert_with_idempotency_claim_collision_leaves_no_row(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Collision rollback removes BOTH would-be rows — atomicity proof.

    After a rejected claim, neither the upload row nor the idempotency
    claim (under the loser's chain_id) is present. Subsequent reads see
    only the winner.
    """
    winner = make_upload_row()
    loser = make_upload_row()
    assert (
        await store.insert_with_idempotency_claim(winner, "race-key") is InsertClaimOutcome.INSERTED
    )
    assert (
        await store.insert_with_idempotency_claim(loser, "race-key")
        is InsertClaimOutcome.IDEMPOTENCY_COLLISION
    )
    # Loser's upload row absent.
    assert await store.get(loser.chain_id) is None
    # Idempotency key resolves to the winner — the rolled-back loser
    # did not leave a stale idempotency_index row pointing at its
    # chain_id.
    yet_another = uuid4()
    resolved = await store.claim_idempotency("race-key", yet_another)
    assert resolved == winner.chain_id


@pytest.mark.asyncio
async def test_insert_with_idempotency_claim_concurrent_race_one_winner(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Concurrent admitters on the same idempotency key — exactly one wins.

    The other gets ``IDEMPOTENCY_COLLISION``. The winner's row + claim
    land; the loser's would-be upload row is absent.
    ``async with self._write_lock`` serializes the contenders so the
    second never sees a half-state.
    """
    rows = [make_upload_row() for _ in range(8)]

    async def _admit(r: UploadRow) -> InsertClaimOutcome:
        return await store.insert_with_idempotency_claim(r, "concurrent-key")

    results = await asyncio.gather(*[_admit(r) for r in rows])
    accepted = [r for r, o in zip(rows, results, strict=True) if o is InsertClaimOutcome.INSERTED]
    rejected = [
        r for r, o in zip(rows, results, strict=True) if o is not InsertClaimOutcome.INSERTED
    ]
    assert len(accepted) == 1
    assert len(rejected) == len(rows) - 1
    winner = accepted[0]
    # Winner's row present; every loser's chain_id absent.
    assert await store.get(winner.chain_id) is not None
    for r in rejected:
        assert await store.get(r.chain_id) is None
    # Idempotency key resolves to the winner.
    other = uuid4()
    resolved = await store.claim_idempotency("concurrent-key", other)
    assert resolved == winner.chain_id


@pytest.mark.asyncio
async def test_idempotency_replay(store: SqliteUploadStore) -> None:
    """``claim_idempotency`` returns the existing upload_uid on replay."""
    first = uuid4()
    second = uuid4()
    a = await store.claim_idempotency("k1", first)
    b = await store.claim_idempotency("k1", second)
    assert a == first
    assert b == first  # second insert was a no-op


@pytest.mark.asyncio
async def test_bulk_delete_rejects_empty(store: SqliteUploadStore) -> None:
    """``bulk_delete`` raises on all-None filter (ADR-004)."""
    with pytest.raises(ValueError):
        await store.bulk_delete()


@pytest.mark.asyncio
async def test_bulk_delete_by_state(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``bulk_delete(state=...)`` deletes matching rows and returns their chain_ids."""
    failed_a = make_upload_row(state="failed")
    failed_b = make_upload_row(state="failed")
    succeeded = make_upload_row(state="succeeded")
    await store.insert(failed_a)
    await store.insert(failed_b)
    await store.insert(succeeded)
    deleted = await store.bulk_delete(state="failed")
    assert {entry.chain_id for entry in deleted} == {failed_a.chain_id, failed_b.chain_id}
    # R8-4: per-row accounting rides along, captured atomically.
    assert all(entry.state == "failed" for entry in deleted)
    # Surviving row's chain_id is not returned.
    assert succeeded.chain_id not in {entry.chain_id for entry in deleted}


@pytest.mark.asyncio
async def test_replay_resets_attempts(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``replay`` resets attempts and state for retry."""
    row = make_upload_row(state="failed", attempts=5)
    await store.insert(row)
    replay_outcome = await store.replay(row.chain_id)
    assert replay_outcome is not None
    replayed = replay_outcome.row
    assert replayed.state == "queued"
    assert replayed.attempts == 0


@pytest.mark.asyncio
async def test_cancel_idempotent(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``cancel`` transitions non-terminal → cancelled; terminal is a no-op."""
    queued = make_upload_row(state="queued")
    succeeded = make_upload_row(state="succeeded")
    await store.insert(queued)
    await store.insert(succeeded)
    cancelled = await store.cancel(queued.chain_id)
    untouched = await store.cancel(succeeded.chain_id)
    assert cancelled.row.state == "cancelled"
    # R8-4: the in-transaction precheck reports the state the row was
    # actually cancelled FROM (the route's release decision input).
    assert cancelled.previous_state == "queued"
    assert untouched.row.state == "succeeded"
    assert untouched.previous_state is None


@pytest.mark.asyncio
async def test_delete(store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]) -> None:
    """``delete`` removes a row."""
    row = make_upload_row()
    await store.insert(row)
    await store.delete(row.chain_id)
    assert await store.get(row.chain_id) is None


@pytest.mark.asyncio
async def test_list_non_terminal(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``list_non_terminal`` excludes terminal states."""
    await store.insert(make_upload_row(state="queued"))
    await store.insert(make_upload_row(state="attempting"))
    await store.insert(make_upload_row(state="succeeded"))
    await store.insert(make_upload_row(state="failed"))
    rows = await store.list_non_terminal()
    states = {r.state for r in rows}
    assert "queued" in states
    assert "attempting" in states
    assert "succeeded" not in states
    assert "failed" not in states


@pytest.mark.asyncio
async def test_busy_timeout_pragma_applied() -> None:
    """``start`` applies the configured busy_timeout PRAGMA.

    The default is 1000 ms (finding R9-V6-1). The store serializes every
    writer through one ``_write_lock`` on a single connection, so internal
    write-vs-write contention is impossible; the busy_timeout only governs
    EXTERNAL cross-process contention. 1 s rides out sub-second external blips
    while failing fast under a sustained external hold so a contended write
    returns a clean retryable signal quickly instead of monopolizing the
    single writer slot (the former 5 s amplified an external-lock burst into
    HTTP read timeouts; see ``SqliteCfg.busy_timeout_ms`` /
    ``_DEFAULT_BUSY_TIMEOUT_MS``).

    Asserts both paths: the no-Settings default constant AND a cfg-threaded
    non-default value (so the ``cfg.busy_timeout_ms`` → PRAGMA wiring is
    proven, not just the module default).
    """
    from phantom.config.settings import SqliteCfg
    from phantom.storage.sqlite_store import _DEFAULT_BUSY_TIMEOUT_MS

    assert _DEFAULT_BUSY_TIMEOUT_MS == 1000

    # No-Settings path: the module default constant applies.
    s = SqliteUploadStore(":memory:")
    await s.start()
    try:
        conn = s._conn
        assert conn is not None
        cursor = await conn.execute("PRAGMA busy_timeout;")
        try:
            row = await cursor.fetchone()
        finally:
            await cursor.close()
        assert row is not None
        assert row[0] == _DEFAULT_BUSY_TIMEOUT_MS == 1000
    finally:
        await s.stop()

    # Config-threaded path: an explicit non-default value reaches the PRAGMA.
    cfg = SqliteCfg(busy_timeout_ms=2500)
    s2 = SqliteUploadStore(":memory:", sqlite_cfg=cfg)
    await s2.start()
    try:
        conn = s2._conn
        assert conn is not None
        cursor = await conn.execute("PRAGMA busy_timeout;")
        try:
            row = await cursor.fetchone()
        finally:
            await cursor.close()
        assert row is not None
        assert row[0] == cfg.busy_timeout_ms == 2500
    finally:
        await s2.stop()


@pytest.mark.asyncio
async def test_synchronous_pragma_from_settings_default() -> None:
    """Default ``synchronous`` pragma is NORMAL (numeric 1) per Slice 1.B § 2.3.5."""
    s = SqliteUploadStore(":memory:")
    await s.start()
    try:
        conn = s._conn
        assert conn is not None
        async with conn.execute("PRAGMA synchronous") as cur:
            row = await cur.fetchone()
        assert row is not None
        assert row[0] == 1  # NORMAL
    finally:
        await s.stop()


@pytest.mark.asyncio
async def test_synchronous_pragma_from_settings_full() -> None:
    """When SqliteCfg overrides synchronous=FULL, the pragma reports 2."""
    from phantom.config.settings import SqliteCfg

    cfg = SqliteCfg(synchronous="FULL")
    s = SqliteUploadStore(":memory:", sqlite_cfg=cfg)
    await s.start()
    try:
        conn = s._conn
        assert conn is not None
        async with conn.execute("PRAGMA synchronous") as cur:
            row = await cur.fetchone()
        assert row is not None
        assert row[0] == 2  # FULL
    finally:
        await s.stop()


@pytest.mark.asyncio
async def test_journal_size_limit_pragma_applied() -> None:
    """Default ``journal_size_limit`` is 16 MiB per Slice 1.B § 2.3.5."""
    s = SqliteUploadStore(":memory:")
    await s.start()
    try:
        conn = s._conn
        assert conn is not None
        async with conn.execute("PRAGMA journal_size_limit") as cur:
            row = await cur.fetchone()
        assert row is not None
        assert row[0] == 16_777_216
    finally:
        await s.stop()


@pytest.mark.asyncio
async def test_auto_vacuum_pragma_hardcoded_none() -> None:
    """``auto_vacuum`` is HARDCODED NONE per § 0.3 SD-card-wear rule.

    Never configurable via Settings — the pragma must report 0
    regardless of what an operator puts in YAML.
    """
    s = SqliteUploadStore(":memory:")
    await s.start()
    try:
        conn = s._conn
        assert conn is not None
        async with conn.execute("PRAGMA auto_vacuum") as cur:
            row = await cur.fetchone()
        assert row is not None
        assert row[0] == 0  # NONE
    finally:
        await s.stop()


@pytest.mark.asyncio
async def test_counts_by_state_groups_count_and_bytes(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``counts_by_state`` returns per-state count + summed body bytes.

    Seeds rows across both terminal and non-terminal states (including
    two rows in one state, to prove the byte sum is per-state) and
    asserts each state's tally. The ``stored`` state is the load-bearing
    case: terminal, so invisible to ``list_non_terminal`` but counted
    here.
    """
    # Two ``stored`` rows so the SUM is exercised, plus single rows in a
    # spread of other states with distinct sizes.
    await store.insert(make_upload_row(state="stored", body_size_bytes=100))
    await store.insert(make_upload_row(state="stored", body_size_bytes=250))
    await store.insert(make_upload_row(state="auth_expired", body_size_bytes=10))
    await store.insert(make_upload_row(state="queued", body_size_bytes=5))
    await store.insert(make_upload_row(state="succeeded", body_size_bytes=7))

    tallies = await store.counts_by_state()

    assert tallies["stored"] == StateTally(count=2, bytes=350)
    assert tallies["auth_expired"] == StateTally(count=1, bytes=10)
    assert tallies["queued"] == StateTally(count=1, bytes=5)
    assert tallies["succeeded"] == StateTally(count=1, bytes=7)


@pytest.mark.asyncio
async def test_counts_by_state_omits_zero_row_states(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """States with zero rows are ABSENT from the mapping.

    Callers default a missing state to ``StateTally(0, 0)`` via
    ``dict.get`` — this asserts the store does not fabricate zero entries.
    """
    await store.insert(make_upload_row(state="stored", body_size_bytes=42))

    tallies = await store.counts_by_state()

    assert set(tallies) == {"stored"}
    # The eight-state vocabulary minus the one seeded state are all absent.
    for absent in ("queued", "attempting", "succeeded", "failed", "cancelled", "corrupted"):
        assert absent not in tallies
        assert tallies.get(absent, StateTally(count=0, bytes=0)) == StateTally(count=0, bytes=0)


@pytest.mark.asyncio
async def test_counts_by_state_empty_table_returns_empty(store: SqliteUploadStore) -> None:
    """An empty ``uploads`` table yields an empty mapping (no rows, no keys)."""
    tallies = await store.counts_by_state()
    assert tallies == {}
