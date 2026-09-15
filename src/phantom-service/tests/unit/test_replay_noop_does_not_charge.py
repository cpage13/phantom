"""A replay that matched no row must not consume a saturation reservation.

Objective: pin that ``replay`` reports its rowcount and the gate settles a
no-op as a no-op.

THE DEFECT THIS CLOSES. ``replay``'s in-lock precheck admits every
non-``attempting`` state and refuses a stamped row, and that was argued as
proof the re-queue UPDATE could not miss. It is not proof.
``expire_row`` commits its state change to ``expired`` and its body-discard
stamp SEPARATELY, so a row sits in ``expired`` with a NULL stamp between the
two commits. Both prechecks pass on it, and the UPDATE's own state list omits
``expired``, so the write matched nothing while the caller was told it had
succeeded.

The admin route reserves a slot before the write, and ``SlotDelta.from_replay``
hard-coded the after-state as ``queued``, so the gate read a charge for a row
that had not moved and consumed the reservation. The charge then sat on a
terminal row that can never release it. Repeated occurrences walk the gate to
its cap and 503 every fresh ingress while nothing is really in flight, and boot
recovery re-seeds the same charge from the persisted row, so a restart does not
clear it either.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

import pytest
from phantom.models.upload import UploadRow
from phantom.storage.sqlite_store import SqliteUploadStore
from phantom.workers.saturation import AdmissionGranted, SaturationGate, SlotDelta


@pytest.fixture
async def store():
    """A started in-memory store; stops after the test."""
    s = SqliteUploadStore(":memory:")
    await s.start()
    yield s
    await s.stop()


def _gate() -> SaturationGate:
    """A gate roomy enough that only the test's own settle moves it."""
    return SaturationGate(
        max_in_flight=100,
        max_in_flight_bytes=1 << 30,
        max_disk_bytes=1 << 30,
        large_body_threshold_bytes=1 << 20,
        max_large_in_flight=4,
    )


@pytest.mark.asyncio
async def test_replay_of_an_expired_unstamped_row_reports_a_miss(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the window between expire_row's two commits is reported honestly.

    Expected: rowcount 0 and the row still ``expired``. This is the exact state
    a row occupies between the CAS to ``expired`` and the body-discard stamp,
    and it satisfies both of replay's prechecks while missing the UPDATE.
    """
    row = make_upload_row(state="expired", body_discarded_at=None, body_size_bytes=4096)
    await store.insert(row)

    outcome = await store.replay(row.chain_id)

    assert outcome.rowcount == 0, "the re-queue matched no row and must say so"
    assert outcome.previous_state == "expired"
    unchanged = await store.get(row.chain_id)
    assert unchanged is not None
    assert unchanged.state == "expired"


@pytest.mark.asyncio
async def test_a_missed_replay_returns_the_reservation(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the gate must not charge for a row that did not move.

    Expected: the ledger returns to empty. The route reserves before the write,
    so a delta that reads as a charge CONSUMES that reservation, and the charge
    then rides a terminal row that can never release it.
    """
    row = make_upload_row(state="expired", body_discarded_at=None, body_size_bytes=4096)
    await store.insert(row)
    gate = _gate()
    granted = await gate.admit(declared_bytes=4096)
    assert isinstance(granted, AdmissionGranted)
    assert gate.in_flight == 1

    outcome = await store.replay(row.chain_id)
    await gate.settle(
        SlotDelta.from_replay(outcome, size_bytes=4096),
        consumes=granted.reservation,
    )

    assert gate.in_flight == 0, (
        "a replay that matched no row consumed its reservation; the charge is "
        "now stranded on a terminal row that can never release it"
    )
    assert gate.in_flight_bytes == 0


@pytest.mark.asyncio
async def test_a_landed_replay_still_consumes_its_reservation(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the honest-rowcount change must not break the real re-queue.

    Expected: a genuine replay of a released row keeps exactly one charge. The
    reservation IS the charge, so the landing write consumes it rather than
    adding a second, and the row is queued again.
    """
    row = make_upload_row(state="succeeded", body_discarded_at=None, body_size_bytes=4096)
    await store.insert(row)
    gate = _gate()
    granted = await gate.admit(declared_bytes=4096)
    assert isinstance(granted, AdmissionGranted)

    outcome = await store.replay(row.chain_id)
    assert outcome.rowcount == 1
    await gate.settle(
        SlotDelta.from_replay(outcome, size_bytes=4096),
        consumes=granted.reservation,
    )

    assert gate.in_flight == 1
    assert gate.in_flight_bytes == 4096
    requeued = await store.get(row.chain_id)
    assert requeued is not None
    assert requeued.state == "queued"
    assert requeued.next_attempt_at is not None
    assert requeued.next_attempt_at <= datetime.now(tz=UTC)


@pytest.mark.asyncio
async def test_cancel_carries_its_release_basis_from_the_write(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the cancel's release basis survives a racing body discard.

    Expected: the outcome carries the pre-cancel size, and settling on it
    returns the full charge even after the reaper has zeroed the row.

    The route used to take the basis from a POST-COMMIT read of the row. The
    reaper's body-discard pass can zero body_size_bytes in the window between
    the cancel's commit and that read, and a cancelled row is in the reaper's
    retention table and immediately eligible at the default
    cancelled_body_seconds. The cancel then released zero bytes: the row count
    came back while the bytes stayed charged for the process lifetime, and the
    reaper's own discard could not recover them either, because its
    previous_state is cancelled, which holds no slot.
    """
    row = make_upload_row(state="stored", body_size_bytes=4_194_304, body_discarded_at=None)
    await store.insert(row)
    gate = _gate()
    granted = await gate.admit(declared_bytes=4_194_304)
    assert isinstance(granted, AdmissionGranted)
    assert gate.in_flight_bytes == 4_194_304

    outcome = await store.cancel(row.chain_id)
    assert outcome.body_size_bytes == 4_194_304, (
        "the release basis must come from the cancel's own pre-image"
    )

    # The reaper lands between the cancel's commit and the settlement, zeroing
    # the row exactly as it does in production.
    discard = await store.discard_body_and_zero_accounting(row.chain_id, expected_state="cancelled")
    assert discard.flipped is True
    post_commit = await store.get(row.chain_id)
    assert post_commit is not None
    assert post_commit.body_size_bytes == 0, "the reaper zeroed the row, as it does"

    await gate.settle(SlotDelta.from_cancel(outcome, size_bytes=outcome.body_size_bytes))

    assert gate.in_flight_bytes == 0, (
        "the cancel released the reaper-zeroed size instead of the size it "
        "actually cancelled; those bytes can never be recovered"
    )
