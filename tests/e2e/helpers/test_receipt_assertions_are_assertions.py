"""The e2e receipt helper must ASSERT on what arrived, not filter it away.

Objective: pin the observing power of :func:`assert_emulator_received`. That
helper is part of the machinery which certifies delivery correctness for the
whole e2e tier, and every one of its 42 call sites passes ``body_size``, so
whatever the helper cannot see the e2e suite cannot see either.

THE DEFECT THIS CLOSES. ``body_size`` was part of the POLL PREDICATE. An entry
whose size did not match was skipped with ``continue`` and the poll kept
looking, so a body delivered with the wrong number of bytes was reported as a
body that never arrived at all. Two different failures, delivered-wrong and
not-delivered, collapsed into the second one. That is the less alarming of the
pair and it sends a reader looking at timing and liveness rather than at
correctness.

The silent case is worse. The poll returned on its first match and the helper
then returned ``matched[0]``, so when a wrong-size copy and a correct copy were
both present the wrong one was filtered out and the assertion passed.

A second entry for one ``phantom_local_uuid`` is a real defect rather than
noise. The emulator keys accepted bodies by upload token and projects the
latest per token, so a retry against the same token overwrites its own entry.
Two entries therefore mean two distinct upstream objects exist for one source
upload, which is a duplicate delivery, and the suite certified it as success.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from phantom_emulator.control_models import ReceivedEntry

from .assertions import assert_emulator_received
from .timing import TimeoutWaitingError

_LOCAL_UUID = "11111111-1111-4111-8111-111111111111"
_OTHER_UUID = "22222222-2222-4222-8222-222222222222"

# Short enough that a helper which polls instead of asserting fails the test
# quickly rather than stalling it for the default receive timeout.
_FAST_TIMEOUT_SECONDS = 0.2


class _FakeReceivedLog:
    """Stand-in for the emulator's received-log surface.

    Holds a fixed list of entries so a test can put the log into a shape the
    real emulator only reaches under a genuine delivery fault.
    """

    def __init__(self, entries: list[ReceivedEntry]) -> None:
        """Bind the fake to the entries it will report."""
        self._entries = entries

    def received(self) -> list[ReceivedEntry]:
        """Return the recorded entries, oldest first."""
        return list(self._entries)


def _entry(*, local_uuid: str, body_size: int) -> ReceivedEntry:
    """Build a received-log entry carrying ``local_uuid`` and ``body_size``."""
    return ReceivedEntry(
        upload_token=uuid4().hex,
        file_id=UUID(int=0),
        metadata_kvs={"phantom_local_uuid": local_uuid},
        x_amz_meta_headers={},
        body_size=body_size,
        body_hash="0" * 64,
        accepted_at=datetime.now(UTC),
    )


async def test_a_wrong_sized_body_is_reported_as_wrong_not_as_missing() -> None:
    """Objective: a delivered-but-wrong body fails as a size mismatch.

    Expected: an assertion naming the size actually received, and specifically
    NOT a timeout. The body reached the emulator, so a timeout saying nothing
    was received is a false account of what happened and points the reader at
    the wrong system.
    """
    log = _FakeReceivedLog([_entry(local_uuid=_LOCAL_UUID, body_size=512)])

    with pytest.raises(AssertionError) as caught:
        await assert_emulator_received(
            log,
            phantom_local_uuid=_LOCAL_UUID,
            body_size=1000,
            timeout_seconds=_FAST_TIMEOUT_SECONDS,
        )

    assert not isinstance(caught.value, TimeoutWaitingError), (
        "the helper waited out its timeout instead of asserting, so a body that "
        "was delivered with the wrong number of bytes is reported as a body "
        "that never arrived"
    )
    assert "512" in str(caught.value), (
        f"the failure does not name the size actually received, so a reader "
        f"cannot tell what went wrong: {caught.value}"
    )


async def test_a_correct_copy_does_not_mask_a_wrong_sized_copy() -> None:
    """Objective: a duplicate delivery cannot pass by containing one good copy.

    Expected: a failure. This is the silent case. Two accepted bodies carry the
    same ``phantom_local_uuid``, one of them the size the test expects, and the
    old predicate filtered the wrong one away and returned the right one, so
    Phantom delivering the same source upload twice, once corrupt, certified as
    a clean success.
    """
    log = _FakeReceivedLog(
        [
            _entry(local_uuid=_LOCAL_UUID, body_size=512),
            _entry(local_uuid=_LOCAL_UUID, body_size=1000),
        ]
    )

    with pytest.raises(AssertionError):
        await assert_emulator_received(
            log,
            phantom_local_uuid=_LOCAL_UUID,
            body_size=1000,
            timeout_seconds=_FAST_TIMEOUT_SECONDS,
        )


async def test_a_duplicate_of_the_expected_size_is_still_a_failure() -> None:
    """Objective: duplicate delivery fails even when both copies are correct.

    Expected: a failure naming the count. Each entry is a distinct upload
    token, so two entries mean two upstream objects exist for one source
    upload. Both being the right size makes the waste invisible, not acceptable.
    """
    log = _FakeReceivedLog(
        [
            _entry(local_uuid=_LOCAL_UUID, body_size=1000),
            _entry(local_uuid=_LOCAL_UUID, body_size=1000),
        ]
    )

    with pytest.raises(AssertionError) as caught:
        await assert_emulator_received(
            log,
            phantom_local_uuid=_LOCAL_UUID,
            body_size=1000,
            timeout_seconds=_FAST_TIMEOUT_SECONDS,
        )

    assert "2" in str(caught.value), (
        f"the failure does not say how many bodies were accepted: {caught.value}"
    )


async def test_the_matching_receipt_still_passes_and_is_returned() -> None:
    """Objective: the ordinary success path is intact.

    Expected: the matching entry comes back. The stricter checks must not turn
    a correct single delivery into a failure, and callers read fields off the
    returned entry, most often ``body_hash``, so the return value still matters.
    """
    wanted = _entry(local_uuid=_LOCAL_UUID, body_size=1000)
    log = _FakeReceivedLog([_entry(local_uuid=_OTHER_UUID, body_size=7), wanted])

    got = await assert_emulator_received(
        log,
        phantom_local_uuid=_LOCAL_UUID,
        body_size=1000,
        timeout_seconds=_FAST_TIMEOUT_SECONDS,
    )

    assert got.upload_token == wanted.upload_token


async def test_a_body_that_never_arrives_still_times_out() -> None:
    """Objective: the genuine not-delivered case keeps its timeout shape.

    Expected: :class:`TimeoutWaitingError`. Waiting is still correct when
    nothing matching the identity has arrived, because upstream delivery is
    asynchronous. Only the size check moved out of the wait.
    """
    log = _FakeReceivedLog([_entry(local_uuid=_OTHER_UUID, body_size=1000)])

    with pytest.raises(TimeoutWaitingError):
        await assert_emulator_received(
            log,
            phantom_local_uuid=_LOCAL_UUID,
            body_size=1000,
            timeout_seconds=_FAST_TIMEOUT_SECONDS,
        )
