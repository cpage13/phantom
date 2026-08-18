"""M-W4-F7 audit closure — expected_state predicates on UPDATE paths.

Phase 2 § 3.2.8. Before this fix:

- ``SqliteUploadStore.record_attempt_result`` had no state predicate;
  a sender call after an admin ``cancel`` clobbered the cancelled
  state with the sender's stale outcome.
- ``replay`` had no state predicate; a racing ``record_attempt_result``
  clobbered the replay.

Both now carry SQL-level state predicates. ``record_attempt_result``'s
default is ``state = 'attempting'`` (matching the sender invariant);
``replay``'s is ``state IN (everything-except-attempting)``.
``record_attempt_result`` callers observe the rowcount and handle 0 as
"row state changed under me".

Cycle-7 phase 7 pre-round defender fix adds replay's typed
body-accounting refusal: a row whose ``body_discarded_at`` is stamped
has no bytes left to send, so ``replay`` raises
``ReplayBodyDiscardedError`` up front instead of re-queuing a row the
sender could only land in ``corrupted``. The round 1 defender fix
(R1-1) promotes the attempting refusal to the same typed idiom:
``replay`` raises ``ReplayRefusedAttemptingError`` from the in-lock
state precheck, and ``None`` narrows to exactly "row missing". The
later sections pin both predicates.

This test file exercises the storage-layer contract directly so the
predicates are locked in independently of the workers' specific call
patterns.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from phantom.models.upload import UploadRow
from phantom.storage import SqliteUploadStore
from phantom.storage.errors import ReplayBodyDiscardedError, ReplayRefusedAttemptingError
from phantom.storage.interface import ReplayOutcome

from .conftest import track_started


@pytest.fixture
async def store(tmp_path: Path) -> SqliteUploadStore:
    """Live single-store fixture for the predicate contract."""
    s = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await s.start()
    return track_started(s)


# -----------------------------------------------------------------------------
# record_attempt_result — expected_state predicate.
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_record_attempt_result_default_predicate_matches_attempting(
    store: SqliteUploadStore, make_upload_row
) -> None:
    """The default ``expected_state='attempting'`` updates an attempting row."""
    row = make_upload_row(state="attempting")
    await store.insert(row)
    write = await store.record_attempt_result(
        row.chain_id,
        new_state="succeeded",
        attempts=1,
        next_attempt_at=None,
        last_error=None,
        upstream_status=200,
        upstream_headers_json=None,
        captured_values=None,
        current_step_index=None,
        last_step_completed=None,
    )
    assert write.rowcount == 1
    fresh = await store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "succeeded"


@pytest.mark.asyncio
async def test_record_attempt_result_default_predicate_rejects_cancelled(
    store: SqliteUploadStore, make_upload_row
) -> None:
    """A cancelled row is NOT clobbered by the sender's record_attempt_result.

    M-W4-F7 closure regression: pre-Phase-2 the sender would have
    overwritten the cancelled state with whatever new_state it had
    (succeeded, queued, etc.). Now the WHERE predicate finds no row
    matching state='attempting' and the UPDATE returns rowcount=0.
    """
    row = make_upload_row(state="cancelled")
    await store.insert(row)
    write = await store.record_attempt_result(
        row.chain_id,
        new_state="succeeded",
        attempts=1,
        next_attempt_at=None,
        last_error=None,
        upstream_status=200,
        upstream_headers_json=None,
        captured_values=None,
        current_step_index=None,
        last_step_completed=None,
    )
    assert write.rowcount == 0
    fresh = await store.get(row.chain_id)
    assert fresh is not None
    # The cancelled state survives — no clobber.
    assert fresh.state == "cancelled"


@pytest.mark.asyncio
async def test_record_attempt_result_explicit_predicate_for_auth_expired_wake(
    store: SqliteUploadStore, make_upload_row
) -> None:
    """``expected_state='auth_expired'`` lets the bearer kicker wake the row."""
    row = make_upload_row(state="auth_expired")
    await store.insert(row)
    write = await store.record_attempt_result(
        row.chain_id,
        new_state="queued",
        attempts=row.attempts,
        next_attempt_at=None,
        last_error=None,
        upstream_status=None,
        upstream_headers_json=None,
        captured_values=None,
        current_step_index=None,
        last_step_completed=None,
        expected_state="auth_expired",
    )
    assert write.rowcount == 1
    fresh = await store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "queued"


# -----------------------------------------------------------------------------
# replay — state predicate.
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replay_succeeded_returns_row(store: SqliteUploadStore, make_upload_row) -> None:
    """Replaying a ``succeeded`` row resets it to ``queued`` with fresh fields."""
    row = make_upload_row(state="succeeded", attempts=3)
    await store.insert(row)
    outcome = await store.replay(row.chain_id)
    assert outcome is not None
    # R9-4: replay reports the in-transaction pre-state for the route's
    # gate reconciliation, alongside the refreshed row.
    assert outcome.previous_state == "succeeded"
    assert outcome.row.state == "queued"
    assert outcome.row.attempts == 0
    assert outcome.row.current_step_index == 0


@pytest.mark.asyncio
async def test_replay_attempting_raises_typed_refusal(
    store: SqliteUploadStore, make_upload_row
) -> None:
    """Replaying an ``attempting`` row raises the typed refusal (R1-1).

    M-W4-F7 closure regression: pre-Phase-2 the replay would have
    clobbered the sender's in-flight work. The round 1 defender fix
    promotes the refusal from an ambiguous ``None`` return to
    ``ReplayRefusedAttemptingError``, raised from the in-lock state
    precheck and carrying the chain_id plus the row's own instance_id;
    the app-registered handler converts it into the canonical 409
    ``replay_refused_attempting`` envelope.
    """
    row = make_upload_row(state="attempting", attempts=2)
    await store.insert(row)
    with pytest.raises(ReplayRefusedAttemptingError) as excinfo:
        await store.replay(row.chain_id)
    assert excinfo.value.chain_id == row.chain_id
    assert excinfo.value.instance_id == row.instance_id
    # The row's state is untouched — sender's in-flight work survived.
    fresh = await store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "attempting"
    assert fresh.attempts == 2


@pytest.mark.asyncio
async def test_replay_missing_row_returns_none(store: SqliteUploadStore) -> None:
    """Replaying a nonexistent chain returns ``None`` (the 404 signal).

    Post-R1-1 the ``None`` return means exactly one thing: the row does
    not exist (the route answers 404). The attempting refusal no longer
    shares it.
    """
    assert await store.replay(uuid4()) is None


@pytest.mark.asyncio
async def test_replay_each_non_attempting_state_returns_row(
    store: SqliteUploadStore, make_upload_row
) -> None:
    """Every state EXCEPT ``attempting`` is replayable."""
    for src in (
        "succeeded",
        "failed",
        "corrupted",
        "cancelled",
        "queued",
        "auth_expired",
        "stored",
    ):
        row = make_upload_row(state=src)
        await store.insert(row)
        outcome = await store.replay(row.chain_id)
        assert outcome is not None, f"replay refused for state={src!r}"
        assert outcome.previous_state == src
        assert outcome.row.state == "queued"


# -----------------------------------------------------------------------------
# replay - body-accounting predicate (cycle-7 phase 7 pre-round defender fix).
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replay_body_discarded_raises_typed_error_and_touches_nothing(
    store: SqliteUploadStore, make_upload_row
) -> None:
    """A stamped ``body_discarded_at`` refuses replay with the typed error.

    The error carries the chain_id, the row's own instance_id, and the
    exact discard instant; the row stays field-for-field identical
    (state, sent_at, attempts, updated_at all untouched), so the
    refusal is a pure read.
    """
    discarded_at = datetime.now(tz=UTC)
    row = make_upload_row(
        state="succeeded",
        attempts=1,
        sent_at=discarded_at,
        body_discarded_at=discarded_at,
        body_size_bytes=0,
    )
    await store.insert(row)
    before = await store.get(row.chain_id)
    assert before is not None
    with pytest.raises(ReplayBodyDiscardedError) as excinfo:
        await store.replay(row.chain_id)
    assert excinfo.value.chain_id == row.chain_id
    assert excinfo.value.instance_id == row.instance_id
    assert excinfo.value.body_discarded_at == discarded_at
    after = await store.get(row.chain_id)
    assert after is not None
    assert after.model_dump() == before.model_dump()


@pytest.mark.asyncio
async def test_replay_refuses_a_stamped_row_in_every_state(
    store: SqliteUploadStore, make_upload_row
) -> None:
    """The body-accounting refusal precedes the state predicate everywhere.

    Any terminal state can carry the stamp (failed/auth_expired/stored
    bodies are reaped after their own retention windows; a pre-fix
    replay could even leave a stamped row in queued or attempting), and
    in none of them can a replay succeed without bytes.
    """
    for src in (
        "succeeded",
        "failed",
        "corrupted",
        "cancelled",
        "queued",
        "auth_expired",
        "stored",
        "attempting",
    ):
        row = make_upload_row(
            state=src,
            body_discarded_at=datetime.now(tz=UTC),
            body_size_bytes=0,
        )
        await store.insert(row)
        with pytest.raises(ReplayBodyDiscardedError):
            await store.replay(row.chain_id)
        fresh = await store.get(row.chain_id)
        assert fresh is not None
        assert fresh.state == src, f"refused replay must not move state={src!r}"


@pytest.mark.asyncio
async def test_replay_refuses_after_the_one_discard_op(
    store: SqliteUploadStore, make_upload_row
) -> None:
    """A row stamped by ``discard_body_and_zero_accounting`` refuses replay.

    Pins the integration with the ONE discard owner (cycle-7 task 4.7):
    whichever leg called it (sender immediate or reaper scheduled), the
    stamp it writes is exactly what replay's refusal reads.
    """
    row = make_upload_row(state="succeeded", body_size_bytes=512)
    await store.insert(row)
    # Stamp via the one owner, not a hand-rolled UPDATE.
    await store.discard_body_and_zero_accounting(row.chain_id, expected_state="succeeded")
    stamped = await store.get(row.chain_id)
    assert stamped is not None
    assert stamped.body_discarded_at is not None
    assert stamped.body_size_bytes == 0
    with pytest.raises(ReplayBodyDiscardedError) as excinfo:
        await store.replay(row.chain_id)
    assert excinfo.value.body_discarded_at == stamped.body_discarded_at


# -----------------------------------------------------------------------------
# Round 2 adversary probe: replay racing a sender claim (R1-1 fix substance).
# -----------------------------------------------------------------------------

# Claim horizon comfortably past any stamp the racing replay writes, so
# the claim leg is due-eligible regardless of which writer wins the lock.
_CLAIM_HORIZON = timedelta(hours=1)

# Batch size for the racing claim; only one row is in play.
_CLAIM_BATCH_LIMIT = 5


@pytest.mark.asyncio
async def test_replay_racing_claim_settles_to_one_coherent_outcome(
    store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """A replay racing ``claim_due`` serializes; no torn third outcome.

    Round 2 adversary probe on the R1-1 fix: the store couples the
    state precheck to the re-queue UPDATE inside one ``_write_txn``, so
    a concurrent sender claim either lands BEFORE the replay (the
    replay raises the typed refusal and the claim's row survives
    untouched) or AFTER it (the replay re-queues, then the claim picks
    the fresh row up). Both interleavings are exercised by starting the
    writers in both orders; in each, the row must settle in
    ``attempting`` with accounting matching exactly one winner, and a
    follow-up replay must raise the typed refusal.
    """
    for claim_starts_first in (True, False):
        row = make_upload_row(state="queued", attempts=1)
        await store.insert(row)
        horizon = datetime.now(tz=UTC) + _CLAIM_HORIZON
        claim_coro = store.claim_due(horizon, _CLAIM_BATCH_LIMIT)
        replay_coro = store.replay(row.chain_id)
        if claim_starts_first:
            claim_result, replay_result = await asyncio.gather(
                claim_coro, replay_coro, return_exceptions=True
            )
        else:
            replay_result, claim_result = await asyncio.gather(
                replay_coro, claim_coro, return_exceptions=True
            )
        assert not isinstance(claim_result, BaseException), claim_result
        claimed_ids = [claimed.chain_id for claimed in claim_result]
        assert claimed_ids == [row.chain_id], (
            "the claim leg must win the row exactly once (pre-replay or "
            f"post-requeue); claimed={claimed_ids!r}"
        )
        final = await store.get(row.chain_id)
        assert final is not None
        assert final.state == "attempting", (
            f"the race must settle in attempting (order claim_first="
            f"{claim_starts_first}); got {final.state!r}"
        )
        if isinstance(replay_result, ReplayRefusedAttemptingError):
            # Claim won the lock first: the replay refused and touched
            # nothing, so the pre-race attempt accounting survives.
            assert replay_result.chain_id == row.chain_id
            assert final.attempts == row.attempts
        else:
            # Replay won: it re-queued with reset accounting, and the
            # claim then picked up the fresh row (R9-4: replay returns
            # the outcome carrying the in-transaction pre-state).
            assert isinstance(replay_result, ReplayOutcome), replay_result
            assert replay_result.row.state == "queued"
            assert replay_result.row.attempts == 0
            assert final.attempts == 0
        with pytest.raises(ReplayRefusedAttemptingError):
            await store.replay(row.chain_id)
