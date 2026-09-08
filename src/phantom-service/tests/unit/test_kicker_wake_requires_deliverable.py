"""The kicker's wake CAS must not land on a row whose body was just discarded.

Objective: pin the deliverability guard on the wake write.

THE DEFECT THIS CLOSES. ``list_parked_candidates`` filters
``body_discarded_at IS NULL``, but several awaits sit between that scan and the
wake write: the credential freshness probe and the saturation admit. A reaper
body-discard landing in that window leaves the row's STATE guard satisfied
while the row is now bodyless and its ``body_size_bytes`` has been zeroed, and
the size the kicker already admitted is the PRE-DISCARD one.

The CAS then landed. The row went back to ``queued`` with no body and a live
charge for bytes it no longer had. The sender claimed it, raised
``BodyMissingError``, and ``_on_corrupted`` settled on the row's own now-zeroed
field, so ``release(0)`` returned the row count but could not identify the
large charge to decrement: the bytes and the large-class slot were stranded for
the process lifetime, and a fresh healthy large upload was then refused.

A full reproduction of that sequence against the real store and gate observed
the gate finish at in_flight 0, in_flight_bytes 1 GiB, large 1, and then refuse
a legitimate 1 GiB admission.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

import pytest
from phantom.models.upload import UploadRow
from phantom.storage.sqlite_store import SqliteUploadStore


@pytest.fixture
async def store():
    """A started in-memory store; stops after the test."""
    s = SqliteUploadStore(":memory:")
    await s.start()
    yield s
    await s.stop()


async def _parked_row(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> UploadRow:
    """Insert a parked row holding a large body, as the kicker would scan it."""
    row = make_upload_row(
        state="auth_expired",
        body_size_bytes=1_073_741_824,
        body_discarded_at=None,
        auth_blocked_host="files.upstream.example",
    )
    await store.insert(row)
    return row


@pytest.mark.asyncio
async def test_wake_lands_on_a_deliverable_row(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the ordinary wake still works with the guard on.

    Expected: rowcount 1 and the row is queued again. The guard must not cost
    the kicker its actual job, which is the whole reason it is opt-in rather
    than applied to every attempt write.
    """
    row = await _parked_row(store, make_upload_row)

    write = await store.record_attempt_result(
        row.chain_id,
        new_state="queued",
        attempts=row.attempts,
        next_attempt_at=datetime.now(tz=UTC),
        last_error=None,
        upstream_status=None,
        upstream_headers_json=None,
        captured_values=None,
        current_step_index=None,
        last_step_completed=None,
        expected_state="auth_expired",
        require_deliverable=True,
    )

    assert write.rowcount == 1
    woken = await store.get(row.chain_id)
    assert woken is not None
    assert woken.state == "queued"


@pytest.mark.asyncio
async def test_wake_is_a_no_op_once_the_body_is_discarded(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: a discard landing inside the wake window makes the CAS a no-op.

    Expected: rowcount 0 and the row still parked. The state guard alone is
    satisfied here, so before the deliverability guard this write LANDED and
    re-queued a row with no body, which is what stranded the charge.

    The zero rowcount is what the settlement layer needs: it reads the write as
    no crossing and unwinds the reservation the kicker took, so the admitted
    size goes back instead of becoming a permanent charge.
    """
    row = await _parked_row(store, make_upload_row)

    # The reaper's body pass, landing between the kicker's scan and its write.
    discard = await store.discard_body_and_zero_accounting(
        row.chain_id, expected_state="auth_expired"
    )
    assert discard.flipped is True

    write = await store.record_attempt_result(
        row.chain_id,
        new_state="queued",
        attempts=row.attempts,
        next_attempt_at=datetime.now(tz=UTC),
        last_error=None,
        upstream_status=None,
        upstream_headers_json=None,
        captured_values=None,
        current_step_index=None,
        last_step_completed=None,
        expected_state="auth_expired",
        require_deliverable=True,
    )

    assert write.rowcount == 0, (
        "the wake landed on a row whose body was already discarded; it would be "
        "re-queued with no bytes and a charge for its pre-discard size"
    )
    still_parked = await store.get(row.chain_id)
    assert still_parked is not None
    assert still_parked.state == "auth_expired"
    assert still_parked.body_discarded_at is not None

    # The behavioural contrast, on the SAME row, so the guard is provably what
    # makes the difference rather than some other property of the fixture.
    # This second call is the pre-fix code path: state guard only. It LANDS,
    # re-queueing a row that has no body. That is the defect, and asserting it
    # here keeps the witness behavioural rather than a signature error.
    unguarded = await store.record_attempt_result(
        row.chain_id,
        new_state="queued",
        attempts=row.attempts,
        next_attempt_at=datetime.now(tz=UTC),
        last_error=None,
        upstream_status=None,
        upstream_headers_json=None,
        captured_values=None,
        current_step_index=None,
        last_step_completed=None,
        expected_state="auth_expired",
    )
    assert unguarded.rowcount == 1, (
        "without the deliverability guard the CAS lands on a bodyless row, "
        "which is the pre-fix behaviour this guard exists to prevent"
    )
    woken_bodyless = await store.get(row.chain_id)
    assert woken_bodyless is not None
    assert woken_bodyless.state == "queued"
    assert woken_bodyless.body_discarded_at is not None, (
        "and the row it woke is deliverable-looking but carries no body"
    )


@pytest.mark.asyncio
async def test_sender_transitions_are_unaffected_by_the_guard(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the guard is opt-in, and the sender's stamped writes still land.

    Expected: a stamped row still transitions to a terminal state when the
    caller does NOT ask for deliverability. The sender legitimately terminalises
    rows whose body the reaper already discarded, so making this guard
    unconditional would strand those rows in a non-terminal state instead.
    """
    row = make_upload_row(state="attempting", body_size_bytes=4096)
    await store.insert(row)
    discard = await store.discard_body_and_zero_accounting(
        row.chain_id, expected_state="attempting"
    )
    assert discard.flipped is True

    write = await store.record_attempt_result(
        row.chain_id,
        new_state="failed",
        attempts=1,
        next_attempt_at=None,
        last_error="upstream gone",
        upstream_status=None,
        upstream_headers_json=None,
        captured_values=None,
        current_step_index=None,
        last_step_completed=None,
        expected_state="attempting",
    )

    assert write.rowcount == 1
    terminal = await store.get(row.chain_id)
    assert terminal is not None
    assert terminal.state == "failed"
