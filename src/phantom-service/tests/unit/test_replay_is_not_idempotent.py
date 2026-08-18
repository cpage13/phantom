"""F12: replay is deliberately not idempotent, and the endpoint says so.

Replay means "deliver this again", so a second call is a second instruction
rather than a repeat of the first. The CAS re-queues from seven states
INCLUDING ``succeeded``, and the ``idempotency_index`` is ingress-only: no
admin route touches it, and the replay handler consults no request key.

F12's harm was never that this is wrong; it was that the SDK retried the call
blind on a read timeout, which turned "the response was lost" into a second
delivery nobody asked for. The client half removed that automatic retry. This
file pins the SERVICE side of the contract the handler's docstring now states,
so nobody "fixes" the endpoint into silent idempotency later without seeing
which promise they are breaking.

Both tests are PASSING attacks: they document behaviour that is already
correct and unchanged by F12, and they pass on the pre-fix tree too.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from phantom.models.upload import UploadRow
from phantom.storage import SqliteUploadStore
from phantom.storage.errors import ReplayBodyDiscardedError
from phantom.storage.interface import ReplayOutcome

from .conftest import track_started

# The two states a replayed row moves between: the CAS accepts a terminal
# ``succeeded`` row and re-queues it, which is the whole non-idempotence.
SUCCEEDED_STATE: str = "succeeded"
QUEUED_STATE: str = "queued"


async def _started_store(tmp_path: Path) -> SqliteUploadStore:
    """Return a started, teardown-tracked upload store.

    Args:
        tmp_path: The test's temporary directory.

    Returns:
        The started store.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await store.start()
    track_started(store)
    return store


def _row(*, state: str, body_discarded: bool) -> UploadRow:
    """Build a row in ``state``, optionally with its body already discarded.

    Args:
        state: The row's persisted state.
        body_discarded: When True, stamp ``body_discarded_at``, which is the
            precheck replay refuses on.

    Returns:
        The row.
    """
    now = datetime.now(tz=UTC)
    chain_id = uuid4()
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": chain_id,
            "multifile_id": None,
            "send_order": 0,
            "route_name": "r",
            "state": state,
            "body_location": "ram",
            "received_at": now,
            "updated_at": now,
            "sent_at": now,
            "endpoint": "files.example.com",
            "uid": "user-1",
            "chain_envelope_json": "{}",
            "idempotency_key": "k",
            "capture_reexecution_active": False,
            "body_size_bytes": 100,
            "body_discarded_at": (now - timedelta(seconds=1)) if body_discarded else None,
        },
    )


@pytest.mark.asyncio
async def test_a_second_replay_of_a_succeeded_row_requeues_it_again(tmp_path: Path) -> None:
    """Replaying a succeeded row twice re-queues it twice.

    Objective: pin the contract the handler's docstring now states. This is the
    behaviour F12's interleaving exploits: the operator's replay lands, the row
    delivers and returns to ``succeeded``, and a blind retry re-queues it for a
    SECOND delivery. Removing the SDK's automatic retry is what closes that;
    the endpoint's meaning is unchanged and must stay visible.

    Success: both calls return a :class:`ReplayOutcome` and the row is
    ``queued`` after each. A future change that made the second call a no-op
    would turn this red, which is the point.
    """
    store = await _started_store(tmp_path)
    row = _row(state=SUCCEEDED_STATE, body_discarded=False)
    await store.insert(row)

    first = await store.replay(row.chain_id)
    assert isinstance(first, ReplayOutcome), "the first replay must re-queue the row"
    after_first = await store.get(row.chain_id)
    assert after_first is not None
    assert after_first.state == QUEUED_STATE

    # Simulate the interleaving's step 2: the row delivered and is terminal
    # again by the time the second request arrives.
    await store.record_attempt_result(
        row.chain_id,
        new_state=SUCCEEDED_STATE,
        attempts=1,
        next_attempt_at=None,
        last_error=None,
        upstream_status=200,
        upstream_headers_json=None,
        captured_values=None,
        current_step_index=None,
        last_step_completed=None,
        expected_state=QUEUED_STATE,
    )

    second = await store.replay(row.chain_id)
    assert isinstance(second, ReplayOutcome), (
        "replay is NOT idempotent: a second call re-queues a row that has since "
        "succeeded, which is a second delivery"
    )
    after_second = await store.get(row.chain_id)
    assert after_second is not None
    assert after_second.state == QUEUED_STATE, (
        f"the second replay must re-queue the row again; it is {after_second.state!r}"
    )


@pytest.mark.asyncio
async def test_a_replay_of_a_body_discarded_row_is_refused(tmp_path: Path) -> None:
    """The body-discard precheck refuses before any state change.

    Objective: the natural guard, and its retention dependence. At the default
    ``succeeded_body_seconds`` of 0 the body is gone the moment the row
    succeeds, so a blind retry 409s and the interleaving cannot complete. Any
    non-zero window, which is supported config and is what the e2e suite runs,
    leaves the door open, which is why the guard is not the fix.

    Success: :class:`ReplayBodyDiscardedError` is raised and the row is
    untouched.
    """
    store = await _started_store(tmp_path)
    row = _row(state=SUCCEEDED_STATE, body_discarded=True)
    await store.insert(row)

    with pytest.raises(ReplayBodyDiscardedError):
        await store.replay(row.chain_id)

    unchanged = await store.get(row.chain_id)
    assert unchanged is not None
    assert unchanged.state == SUCCEEDED_STATE, (
        f"the refusal must precede any state change; the row is {unchanged.state!r}"
    )
