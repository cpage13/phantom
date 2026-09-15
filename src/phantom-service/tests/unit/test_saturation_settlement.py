"""Direct tests for the ADR-036 settlement contract.

Objective: pin the crossing rule itself, at the layer that owns it.

WHY THIS FILE EXISTS. Before it, ``SlotDelta``, its five adapters, ``settle``
and ``unwind`` had NO direct test anywhere in the repository: a grep for
``SlotDelta``, ``.settle(`` and ``.unwind(`` across all four test roots matched
one file, and only for ``SlotReservation``. The layer was exercised only
indirectly, through caller tests that happened to drive a real gate, and the
one end-to-end assertion on the ledger polls until the balance reaches zero
without ever asserting it was non-zero, so a gate that charged nothing at all
would pass it on its first probe.

That matters because the settlement layer is the single mechanism through which
twenty-three former hand-written release sites now run, and a full-coverage
review found SIX independent paths that permanently corrupt the ledger, each
ending with the gate refusing all ingress while nothing is actually in flight.
Nothing in the suite could have caught any of them.

These tests drive the real ``SaturationGate`` and assert on its observable
counters. They are deliberately about the RULE rather than about any one
caller, so a caller that later derives a delta differently still meets a
contract that is written down.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from phantom.storage.interface import (
    AttemptWriteOutcome,
    CancelOutcome,
    DeletedRowAccounting,
    DiscardOutcome,
)
from phantom.workers.saturation import (
    AdmissionGranted,
    SaturationGate,
    SlotDelta,
    row_holds_slot,
)

_STAMP = datetime(2026, 9, 8, 12, 0, 0, tzinfo=UTC)

# A gate roomy enough that admission never refuses for a reason a test did not
# ask for. Individual tests tighten a cap when the cap is the subject.
_ROOMY = {
    "max_in_flight": 1000,
    "max_in_flight_bytes": 1 << 40,
    "max_disk_bytes": 1 << 40,
    "large_body_threshold_bytes": 1 << 20,
    "max_large_in_flight": 4,
}


def _gate(**overrides: int) -> SaturationGate:
    """Build a gate with roomy caps unless a test overrides one."""
    return SaturationGate(**{**_ROOMY, **overrides})  # type: ignore[arg-type]


async def _admit(gate: SaturationGate, declared_bytes: int) -> AdmissionGranted:
    """Admit and assert the grant, returning it so a test can settle on it."""
    result = await gate.admit(declared_bytes=declared_bytes)
    assert isinstance(result, AdmissionGranted), f"expected a grant, got {result!r}"
    return result


# --------------------------------------------------------------------------
# The predicate the whole rule rests on.
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("state", "discarded_at", "expected"),
    [
        ("queued", None, True),
        ("attempting", None, True),
        ("stored", None, True),
        ("succeeded", None, False),
        ("failed", None, False),
        ("cancelled", None, False),
        ("corrupted", None, False),
        ("expired", None, False),
        ("auth_expired", None, False),
        # The stamp disqualifies ``stored`` ALONE. That asymmetry is the
        # predicate's one subtlety and it is deliberate: a stored row's slot is
        # released by the reaper's body-discard pass, so counting it again at
        # the later row removal would double-free. A queued or attempting row
        # is still being worked and its slot is released by the transition
        # that ends the work, not by the stamp.
        ("queued", _STAMP, True),
        ("attempting", _STAMP, True),
        ("stored", _STAMP, False),
    ],
)
def test_row_holds_slot_truth_table(
    state: str, discarded_at: datetime | None, expected: bool
) -> None:
    """Objective: the slot-holding predicate, pinned for every state.

    Expected: exactly the three in-flight states hold a slot, and the
    body-discard stamp subtracts only ``stored``. This is the input to every
    crossing, so a change here silently re-derives all twenty-three sites.
    """
    assert row_holds_slot(state, discarded_at) is expected


# --------------------------------------------------------------------------
# settle's four arms.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_crossing_without_a_reservation_moves_nothing() -> None:
    """Objective: a write that did not change slot-holding status owes nothing.

    Expected: the ledger is untouched. queued to attempting is the common
    case, and charging it would double-count every send attempt.
    """
    gate = _gate()
    grant = await _admit(gate, 1000)
    await gate.settle(SlotDelta(held_before=True, holds_after=True, size_bytes=1000))

    assert gate.in_flight == 1
    assert gate.in_flight_bytes == 1000
    # Keep the grant alive so the test is about settle, not about a leak.
    assert grant.reservation.declared_bytes == 1000


@pytest.mark.asyncio
async def test_no_crossing_with_a_reservation_gives_it_back() -> None:
    """Objective: a speculative reservation on a write that did not land returns.

    Expected: the reservation is unwound, so the ledger returns to empty. This
    is the arm the replay reconcile proves has to exist: the caller reserved
    before the write, and the write turned out to move nothing.
    """
    gate = _gate()
    grant = await _admit(gate, 4096)
    assert gate.in_flight == 1

    await gate.settle(
        SlotDelta(held_before=False, holds_after=False, size_bytes=0),
        consumes=grant.reservation,
    )

    assert gate.in_flight == 0
    assert gate.in_flight_bytes == 0


@pytest.mark.asyncio
async def test_charge_with_a_reservation_does_not_charge_twice() -> None:
    """Objective: a reservation IS the charge; the landing write must not re-add it.

    Expected: one row and one body's bytes, not two. Charging again here is a
    double-charge on every successful kicker wake, which walks the gate to its
    cap and 503s all ingress while nothing is really in flight.
    """
    gate = _gate()
    grant = await _admit(gate, 8192)

    await gate.settle(
        SlotDelta(held_before=False, holds_after=True, size_bytes=8192),
        consumes=grant.reservation,
    )

    assert gate.in_flight == 1
    assert gate.in_flight_bytes == 8192


@pytest.mark.asyncio
async def test_charge_without_a_reservation_charges_uncapped() -> None:
    """Objective: a row that entered the in-flight set with no reservation is charged.

    Expected: the ledger gains the row and its bytes even though nothing was
    admitted first. The row is already live and cannot be refused, which is
    why this arm bypasses the caps rather than consulting them.
    """
    gate = _gate()
    assert gate.in_flight == 0

    await gate.settle(SlotDelta(held_before=False, holds_after=True, size_bytes=2048))

    assert gate.in_flight == 1
    assert gate.in_flight_bytes == 2048


@pytest.mark.asyncio
async def test_leaving_the_in_flight_set_releases() -> None:
    """Objective: a write that dropped the row out of the set returns its charge.

    Expected: the ledger returns to empty. This is the ordinary terminal
    transition, and it is the one every leak found in review failed to reach.
    """
    gate = _gate()
    await _admit(gate, 512)

    await gate.settle(SlotDelta(held_before=True, holds_after=False, size_bytes=512))

    assert gate.in_flight == 0
    assert gate.in_flight_bytes == 0


@pytest.mark.asyncio
async def test_unwind_returns_exactly_what_was_taken() -> None:
    """Objective: a reservation coming back restores the ledger precisely.

    Expected: empty, not merely smaller. An unwind that returned a different
    quantity than the admit took is the drift that eventually refuses fresh
    work forever.
    """
    gate = _gate()
    grant = await _admit(gate, 123_456)
    assert gate.in_flight_bytes == 123_456

    await gate.unwind(grant.reservation)

    assert gate.in_flight == 0
    assert gate.in_flight_bytes == 0


# --------------------------------------------------------------------------
# The adapters. Each must derive its crossing from the store outcome, never
# from what the caller believes happened.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_attempt_that_did_not_land_is_a_no_op() -> None:
    """Objective: rowcount 0 means the guarded UPDATE matched nothing.

    Expected: no crossing, so the reservation comes back and the ledger is
    empty. A write that did not fire moved nothing, and treating it as a
    transition is how a lost update becomes a permanent accounting error.
    """
    gate = _gate()
    grant = await _admit(gate, 777)
    outcome = AttemptWriteOutcome(
        rowcount=0,
        new_state="failed",
        previous_state="attempting",
        previous_body_discarded_at=None,
        body_size_bytes=777,
    )

    delta = SlotDelta.from_attempt(outcome, size_bytes=777)
    assert delta.held_before == delta.holds_after
    await gate.settle(delta, consumes=grant.reservation)

    assert gate.in_flight == 0


@pytest.mark.asyncio
async def test_attempt_to_a_terminal_state_releases() -> None:
    """Objective: a landed attempting-to-terminal write leaves the in-flight set.

    Expected: the charge is returned. This is the sender's ordinary success
    and failure path.
    """
    gate = _gate()
    await _admit(gate, 999)
    outcome = AttemptWriteOutcome(
        rowcount=1,
        new_state="succeeded",
        previous_state="attempting",
        previous_body_discarded_at=None,
        body_size_bytes=999,
    )

    await gate.settle(SlotDelta.from_attempt(outcome, size_bytes=999))

    assert gate.in_flight == 0
    assert gate.in_flight_bytes == 0


@pytest.mark.asyncio
async def test_discard_that_did_not_flip_is_a_no_op() -> None:
    """Objective: a discard whose guarded UPDATE matched nothing owes nothing.

    Expected: the ledger is unchanged. The flip is what makes the bytes stop
    counting, so a discard that did not flip must not release.
    """
    gate = _gate()
    await _admit(gate, 4096)
    outcome = DiscardOutcome(
        flipped=False,
        body_size_bytes=4096,
        previous_state="stored",
        discarded_at=None,
    )

    await gate.settle(SlotDelta.from_discard(outcome, size_bytes=4096))

    assert gate.in_flight == 1
    assert gate.in_flight_bytes == 4096


@pytest.mark.asyncio
async def test_discard_of_a_holding_row_releases() -> None:
    """Objective: stamping a slot-holding row returns its charge.

    Expected: the ledger empties. The stamp is the moment the bytes stop
    existing, so it is the moment the gate stops counting them.
    """
    gate = _gate()
    await _admit(gate, 4096)
    outcome = DiscardOutcome(
        flipped=True,
        body_size_bytes=4096,
        previous_state="stored",
        discarded_at=_STAMP,
    )

    await gate.settle(SlotDelta.from_discard(outcome, size_bytes=4096))

    assert gate.in_flight == 0
    assert gate.in_flight_bytes == 0


@pytest.mark.asyncio
async def test_removal_of_a_holding_row_releases() -> None:
    """Objective: deleting a slot-holding row returns its charge.

    Expected: empty. A removed row holds nothing, which the adapter must read
    from the accounting rather than from the caller's intent.
    """
    gate = _gate()
    await _admit(gate, 64)
    accounting = DeletedRowAccounting(
        chain_id=uuid4(),
        state="queued",
        body_size_bytes=64,
        body_discarded_at=None,
    )

    await gate.settle(SlotDelta.from_removal(accounting, size_bytes=64))

    assert gate.in_flight == 0


@pytest.mark.asyncio
async def test_removal_of_a_stamped_stored_row_is_a_no_op() -> None:
    """Objective: deleting an already-discarded ``stored`` row releases nothing.

    Expected: unchanged. Its bytes were released by the body-discard pass, and
    releasing again at row removal is the double-return that silently steals
    another row's accounting. ``stored`` is the only state where the stamp
    changes the answer, which is exactly why the reaper's two passes over the
    same row do not both release.
    """
    gate = _gate()
    await _admit(gate, 64)
    accounting = DeletedRowAccounting(
        chain_id=uuid4(),
        state="stored",
        body_size_bytes=0,
        body_discarded_at=_STAMP,
    )

    await gate.settle(SlotDelta.from_removal(accounting, size_bytes=0))

    assert gate.in_flight == 1
    assert gate.in_flight_bytes == 64


@pytest.mark.asyncio
async def test_cancel_of_a_stored_row_releases() -> None:
    """Objective: cancel admits ``stored``, which DOES hold a slot.

    Expected: the charge returns. ``stored`` is terminal but slot-holding,
    the one combination that makes cancel's crossing non-obvious, and the
    reason its outcome has to carry the pre-image stamp.
    """
    gate = _gate()
    await _admit(gate, 2048)
    # ReplayOutcome and CancelOutcome carry a full row; the adapter reads only
    # the pre-image fields, so a minimal stub is enough and keeps this test
    # about the crossing rather than about row hydration.
    outcome = CancelOutcome(
        row=None,  # type: ignore[arg-type]
        previous_state="stored",
        previous_body_discarded_at=None,
        body_size_bytes=2048,
    )

    await gate.settle(SlotDelta.from_cancel(outcome, size_bytes=2048))

    assert gate.in_flight == 0
    assert gate.in_flight_bytes == 0


@pytest.mark.asyncio
async def test_cancel_of_a_stamped_stored_row_is_a_no_op() -> None:
    """Objective: a stored row whose body is already discarded holds nothing.

    Expected: unchanged. This is why ``CancelOutcome`` carries
    ``previous_body_discarded_at``: without it the adapter would release a
    charge the discard already returned.
    """
    gate = _gate()
    await _admit(gate, 2048)
    outcome = CancelOutcome(
        row=None,  # type: ignore[arg-type]
        previous_state="stored",
        previous_body_discarded_at=_STAMP,
        body_size_bytes=0,
    )

    await gate.settle(SlotDelta.from_cancel(outcome, size_bytes=0))

    assert gate.in_flight == 1
    assert gate.in_flight_bytes == 2048


# --------------------------------------------------------------------------
# The large class, and why a truthful release basis is load-bearing.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_large_charge_releases_against_its_charge_time_size() -> None:
    """Objective: a large body's class slot returns when released truthfully.

    Expected: the large counter returns to zero and a fresh large admission is
    granted again. The class decrement is keyed on the charge-time size, so
    the release basis must be the SAME number the admit used.
    """
    gate = _gate(max_large_in_flight=1, large_body_threshold_bytes=1024)
    await _admit(gate, 4096)
    assert gate.large_in_flight == 1

    await gate.settle(SlotDelta(held_before=True, holds_after=False, size_bytes=4096))

    assert gate.large_in_flight == 0
    assert gate.in_flight_bytes == 0
    again = await gate.admit(declared_bytes=4096)
    assert isinstance(again, AdmissionGranted)


@pytest.mark.asyncio
async def test_releasing_a_large_charge_on_a_stale_zero_basis_strands_the_class_slot() -> None:
    """Objective: pin what an UNTRUTHFUL release basis costs, so callers cannot drift.

    Expected: releasing a large charge with ``size_bytes=0`` decrements the row
    count but NOT the byte total and NOT the large-class counter, because the
    class decrement looks up the charge-time size and finds no entry for zero.
    The gate then refuses a fresh, healthy large upload for the process
    lifetime.

    This is not a defect in the primitive: it cannot decrement a charge it
    cannot identify. It is the reason every adapter takes ``size_bytes`` as a
    REQUIRED keyword, and the reason a caller must never pass a basis it read
    after another writer could have zeroed it. A review found exactly this
    reached in production through a wake path that admitted a stale size after
    a concurrent discard had zeroed the row.
    """
    gate = _gate(max_large_in_flight=1, large_body_threshold_bytes=1024)
    await _admit(gate, 1 << 20)
    assert gate.large_in_flight == 1

    await gate.settle(SlotDelta(held_before=True, holds_after=False, size_bytes=0))

    assert gate.in_flight == 0, "the row count still returns"
    assert gate.in_flight_bytes == 1 << 20, "the bytes are stranded"
    assert gate.large_in_flight == 1, "the class slot is stranded"
    refused = await gate.admit(declared_bytes=1 << 20)
    assert not isinstance(refused, AdmissionGranted), (
        "a fresh healthy large upload is refused by the stranded class slot"
    )
