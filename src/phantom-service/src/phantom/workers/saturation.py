"""SaturationGate: a counter cache consulted synchronously by ingress.

The gate is TWO layers and both are public (ADR-036):

* the PRIMITIVE layer (:meth:`SaturationGate.admit`,
  :meth:`SaturationGate.release`, :meth:`SaturationGate.reconcile_admit`,
  :meth:`SaturationGate.set_disk_usage_bytes`,
  :meth:`SaturationGate.update_caps`, the properties) owns the ledger
  arithmetic: the caps, the byte total, the R9-6 large-class pairing and
  the gauge;
* the SETTLEMENT layer (:meth:`SaturationGate.settle`,
  :meth:`SaturationGate.unwind`) owns the two DECISIONS production used to
  make by hand at twenty-three sites: whether one write crossed the
  slot-holding predicate, and what happens to a speculative reservation
  that was or was not consumed.

Also the home of the two shared row predicates, which are the same kind of
thing: one statement each about what a row's persisted fields MEAN, consulted
by every worker that walks rows. :func:`row_holds_slot` answers "does this row
currently charge the gate", and :func:`is_deliverable` answers "does this row
still have bytes to send" (the H4 carve-out). Both live here so no caller
re-derives them by hand.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Final

from phantom.config.settings import SaturationCfg
from phantom.models.upload import UploadRow, UploadState
from phantom.observability.metrics import MetricsRegistry
from phantom.storage.interface import (
    AttemptWriteOutcome,
    CancelOutcome,
    DeletedRowAccounting,
    DiscardOutcome,
    ReplayOutcome,
)

logger = logging.getLogger(__name__)

# States whose row currently holds a saturation slot (R8-4). Admission
# charges the gate; the sender SETTLES the terminal transitions it
# drives (succeeded / failed / corrupted / cancelled) and the
# auth_expired park (the Kicker re-admits through the gate on
# wake). ``stored`` deliberately keeps its slot - the buffered body
# still occupies space - until the body is discarded by retention
# policy or the row is removed. Every path that REMOVES a row (admin
# cancel / delete / bulk delete, the reaper's deletion passes) and
# every path that RE-QUEUES a released row (replay) settles against
# this set, and since ADR-036 the predicate below is applied for them
# INSIDE the gate, from the store outcome's in-transaction pre-image,
# so the ledger cannot drift in either direction and no caller
# re-derives the crossing.
SLOT_HOLDING_STATES: Final[frozenset[str]] = frozenset({"queued", "attempting", "stored"})


def row_holds_slot(state: str, body_discarded_at: datetime | None) -> bool:
    """Return True when a row in ``state`` currently holds a gate slot.

    The one shared predicate for release-on-removal and
    re-admit-on-replay decisions (R8-4 / R8-6). ``stored`` is the
    special case: its slot is released by the reaper's BODY-DISCARD
    pass (the space the slot represents is freed there), so a stored
    row whose ``body_discarded_at`` is stamped no longer holds one and
    releasing again at its later row-removal would double-free.

    Since ADR-036 a TRANSITION never applies this predicate at the call
    site: it is applied for both sides of one write inside
    :meth:`SlotDelta._crossing`, from the store outcome's
    in-transaction pre-image. The two surviving direct callers ask a
    CURRENT-STATE question about a row they are not transitioning (boot
    reconstruction's ledger seed, and replay's pre-check deciding
    whether to reserve at all), which is the one question this
    predicate stays public for.

    Args:
        state: The row's state at the decision instant.
        body_discarded_at: The row's H4 carve-out stamp at the same
            instant.

    Returns:
        ``True`` when removing the row must release the gate (and,
        equivalently, re-queueing it must NOT re-admit); ``False``
        otherwise.
    """
    if state not in SLOT_HOLDING_STATES:
        return False
    return not (state == "stored" and body_discarded_at is not None)


def is_deliverable(row: UploadRow) -> bool:
    """Return True when the row still has bytes to send.

    The H4 carve-out (R6-3) stated once. A stamped ``body_discarded_at``
    means the reaper discarded this row's body by retention policy, so
    nothing is left to deliver: re-queueing the row would land it in
    ``corrupted`` on the sender's next claim (BodyMissingError), turning
    a policy-aged record into a false storage-fault diagnostic, and
    would burn a saturation slot on a row that can never succeed. Both
    kicker wake paths mirror ``replay``'s up-front refusal and leave
    such a row parked until the metadata-retention pass reaps it.

    The five row-walk callers ask the same question for different
    reasons (a wake, a recovery judgement, an audit, a migration), so
    each keeps its own one-line reason at the call site; what is shared,
    and lives here, is what the stamp MEANS.

    Args:
        row: The row being considered for delivery-shaped work.

    Returns:
        ``True`` when the body is still present as far as the row's own
        metadata is concerned; ``False`` once the discard stamp is set.
    """
    return row.body_discarded_at is None


@dataclass(frozen=True)
class SlotReservation:
    """A charge the gate is holding for a row that may not exist yet.

    Minted by :meth:`SaturationGate.admit` and by nothing else. Holding
    one means the ledger currently carries ``declared_bytes`` on this
    holder's behalf, and that the holder owes the gate exactly one of
    two things: a CONSUMPTION (the row it was taken for became live, or
    a write's charge arm consumed it through
    :meth:`SaturationGate.settle`) or an UNWIND
    (:meth:`SaturationGate.unwind`).

    Attributes:
        declared_bytes: The quantity admitted, which is the quantity an
            unwind returns. One field, and it is the R3-8 unit-symmetry
            rule expressed as a type: the release cannot use a different
            basis from the admit because it does not carry one.
    """

    declared_bytes: int


@dataclass(frozen=True)
class SlotDelta:
    """One write's effect on the saturation ledger.

    Built by an adapter classmethod, never by a caller. That is the
    point on which this item's whole value rests: if callers build
    deltas by hand, the twenty-three derivations survive under a new
    type and nothing is discharged.

    Attributes:
        held_before: ``row_holds_slot`` on the row as the write found
            it. ``False`` when there was no row.
        holds_after: the same predicate on the row as the write LEFT it.
            ``False`` when the write removed the row. When the write did
            not land, this equals ``held_before``, because a write that
            did not fire moved nothing; the gate then sees no crossing.
        size_bytes: The release basis for THIS site, supplied by the
            caller. Required on every adapter, and deliberately not
            derived from the outcome: the four sender sites release
            ``row.body_size_bytes`` (a caller-held snapshot) while
            ``workers/_expire.py`` releases an in-transaction size, the
            two genuinely differ, and unifying them is a behaviour
            change out of scope by ruling (ADR-036). A required keyword
            on every adapter is what keeps that unification from
            happening by accident.
    """

    held_before: bool
    holds_after: bool
    size_bytes: int

    @classmethod
    def _crossing(
        cls,
        *,
        before_state: UploadState | None,
        before_discarded_at: datetime | None,
        after_state: UploadState | None,
        after_discarded_at: datetime | None,
        size_bytes: int,
    ) -> SlotDelta:
        """Apply ``row_holds_slot`` across one write. The ONE application site.

        A ``None`` state means "no row on that side of the write":
        before, that the row was absent or the CAS guard matched
        nothing; after, that the write REMOVED the row. Neither holds a
        slot, which is what makes a non-landed write a no-op without any
        adapter special-casing it.
        """
        return cls(
            held_before=(
                before_state is not None and row_holds_slot(before_state, before_discarded_at)
            ),
            holds_after=(
                after_state is not None and row_holds_slot(after_state, after_discarded_at)
            ),
            size_bytes=size_bytes,
        )

    @classmethod
    def from_attempt(cls, outcome: AttemptWriteOutcome, *, size_bytes: int) -> SlotDelta:
        """Build the delta for one ``record_attempt_result`` write.

        The after-state is the state the caller asked for when the
        guarded UPDATE landed, and the before-state when it did not: a
        write that did not fire moved nothing. This writer never touches
        ``body_discarded_at``, so the stamp is the same on both sides.

        Args:
            outcome: The write's own in-transaction pre-image.
            size_bytes: This site's release basis (ADR-036: a caller
                input, never taken off the outcome).
        """
        return cls._crossing(
            before_state=outcome.previous_state,
            before_discarded_at=outcome.previous_body_discarded_at,
            after_state=outcome.new_state if outcome.landed else outcome.previous_state,
            after_discarded_at=outcome.previous_body_discarded_at,
            size_bytes=size_bytes,
        )

    @classmethod
    def from_discard(cls, outcome: DiscardOutcome, *, size_bytes: int) -> SlotDelta:
        """Build the delta for one body-discard stamp.

        The discard writes no state; the CROSSING is the stamp, which is
        why a ``stored`` row releases here and the six other swept
        states need no arm. The before-stamp is ``None`` because the
        UPDATE's own guard requires an unstamped row, and on a non-flip
        both pre-image fields are ``None``, which makes the delta a
        no-op with no special case.

        Args:
            outcome: The discard's in-transaction pre-image.
            size_bytes: This site's release basis.
        """
        return cls._crossing(
            before_state=outcome.previous_state,
            before_discarded_at=None,
            after_state=outcome.previous_state,
            after_discarded_at=outcome.discarded_at,
            size_bytes=size_bytes,
        )

    @classmethod
    def from_removal(cls, accounting: DeletedRowAccounting, *, size_bytes: int) -> SlotDelta:
        """Build the delta for one row REMOVAL.

        There is no landed question: :class:`DeletedRowAccounting` exists
        only for rows a DELETE actually removed, captured in the
        DELETE's own transaction, so the row is gone by construction and
        the after-side is empty.

        Args:
            accounting: The removal's atomically captured accounting.
            size_bytes: This site's release basis.
        """
        return cls._crossing(
            before_state=accounting.state,
            before_discarded_at=accounting.body_discarded_at,
            after_state=None,
            after_discarded_at=None,
            size_bytes=size_bytes,
        )

    @classmethod
    def from_replay(cls, outcome: ReplayOutcome, *, size_bytes: int) -> SlotDelta:
        """Build the delta for one replay re-queue.

        The after-state is the literal ``"queued"`` the UPDATE writes,
        NOT ``outcome.row.state``: the row is read after the write
        transaction commits, so a cancel landing in between would report
        ``cancelled``, giving a no-crossing delta that unwound a
        reservation whose charge the cancel had already released, and
        two releases would answer one charge (R9-4).

        The before-stamp is ``None`` and that is PROVABLE, not assumed:
        ``replay`` raises ``ReplayBodyDiscardedError`` from inside the
        write transaction before any UPDATE and refuses a stamped row in
        EVERY state, so a row that reaches this settlement can never
        have been holding-but-stamped. Passing the route's pre-fetched
        stamp here would re-open the stale-read race the in-transaction
        outcome exists to close (C5).

        Args:
            outcome: The replay's in-transaction pre-image.
            size_bytes: This site's release basis.
        """
        return cls._crossing(
            before_state=outcome.previous_state,
            before_discarded_at=None,
            after_state="queued",
            after_discarded_at=None,
            size_bytes=size_bytes,
        )

    @classmethod
    def from_cancel(cls, outcome: CancelOutcome, *, size_bytes: int) -> SlotDelta:
        """Build the delta for one admin cancel.

        The after-state is the literal ``"cancelled"`` the UPDATE writes
        on a landed cancel, and the before-state otherwise;
        ``previous_state is None`` IS the not-landed signal, because the
        UPDATE guards ``state IN
        ('queued','attempting','auth_expired','stored')`` and a terminal
        row gives rowcount 0. Reading ``outcome.row.state`` instead
        would charge for a racing replay's re-queue on a path that does
        nothing today, a permanent over-count.

        The before-stamp comes from
        ``CancelOutcome.previous_body_discarded_at``, the write's own
        in-transaction read, and NOT from the post-commit row. Cancel is
        the one outcome whose UPDATE can legally land on an already
        stamped row (its guard admits ``stored``, and it has no
        stamped-row refusal), and ``stored`` is the single state
        ``row_holds_slot`` consults the stamp for. Hard-coding ``None``
        there would release a slot the reaper's body pass already
        released.

        Args:
            outcome: The cancel's in-transaction pre-image.
            size_bytes: This site's release basis.
        """
        return cls._crossing(
            before_state=outcome.previous_state,
            before_discarded_at=outcome.previous_body_discarded_at,
            after_state=("cancelled" if outcome.previous_state is not None else None),
            after_discarded_at=outcome.previous_body_discarded_at,
            size_bytes=size_bytes,
        )


@dataclass(frozen=True)
class AdmissionGranted:
    """The gate accepted the upload, and is holding the charge.

    Attributes:
        reservation: The charge the caller now owns. Present on the
            GRANT and on no other union member, so a refusal cannot be
            mistaken for a slot the caller must return.
    """

    reservation: SlotReservation


@dataclass(frozen=True)
class AdmissionRefusedSaturation:
    """In-flight row count or in-flight bytes cap exceeded."""


@dataclass(frozen=True)
class AdmissionRefusedDiskPressure:
    """Disk-bytes ceiling reached - observed via the periodic disk probe (§2.3)."""


AdmissionResult = AdmissionGranted | AdmissionRefusedSaturation | AdmissionRefusedDiskPressure
"""Discriminated union of admission outcomes.

Callers dispatch via ``isinstance(result, AdmissionGranted)`` rather
than parsing an enum value; the three refusal classes carry no
additional payload but exist as distinct types so the type system
keeps the cases exhaustive.
"""


class SaturationGate:
    """Tracks in-flight count and bytes; admits or rejects new uploads.

    The gate is consulted synchronously by the ingress handler. Its counters
    are in-process only; boot recovery reconstructs them from persisted rows
    before workers start.

    Disk-pressure accounting is updated by an external probe via
    :meth:`set_disk_usage_bytes`; the probe runs out of band so the gate's
    synchronous admit path never does I/O. That probe re-reads
    ``max_disk_bytes`` off this gate on EVERY tick (F13), so the disk cap
    is genuinely live: a cap pushed here by ``update_caps`` applies at the
    next admit, and the observation it is compared against refreshes
    within one probe interval. While the cap is 0 the probe skips its
    walk, which is safe because :meth:`admit` short-circuits on
    ``max_disk_bytes > 0`` before reading the observation.
    """

    def __init__(
        self,
        *,
        max_in_flight: int,
        max_in_flight_bytes: int,
        max_disk_bytes: int,
        large_body_threshold_bytes: int = 0,
        max_large_in_flight: int = 0,
        metrics_registry: MetricsRegistry | None = None,
    ) -> None:
        """Construct the gate.

        Args:
            max_in_flight: Hard cap on concurrent non-terminal rows.
            max_in_flight_bytes: Hard cap on summed in-flight body bytes.
            max_disk_bytes: Hard cap on total bytes the disk tier may use.
                Zero disables the disk-pressure check (the periodic probe
                is then a no-op).
            large_body_threshold_bytes: Bodies at or above this size are
                counted in the separate large-body class. Zero disables
                the class - no row counts as large.
            max_large_in_flight: Max concurrent in-flight bodies in the
                large class. Ignored when ``large_body_threshold_bytes=0``.
            metrics_registry: Optional :class:`MetricsRegistry` for
                emit-site wiring (plan § 4.2.2). When ``None`` a
                throwaway registry is constructed so emission is a
                no-op.
        """
        self._max_in_flight = max_in_flight
        self._max_in_flight_bytes = max_in_flight_bytes
        self._max_disk_bytes = max_disk_bytes
        self._large_body_threshold_bytes = large_body_threshold_bytes
        self._max_large_in_flight = max_large_in_flight
        self._lock = asyncio.Lock()
        self._in_flight = 0
        self._in_flight_bytes = 0
        self._large_in_flight = 0
        # Outstanding LARGE charges keyed by declared size (R9-6): the
        # charge-time classification memory the release path consults,
        # so a hot-reloaded large_body_threshold_bytes never
        # desynchronizes the class ledger. Bounded by the number of
        # large bodies in flight.
        self._large_charge_sizes: dict[int, int] = {}
        # Last-observed disk usage from the periodic probe. Zero means
        # "no observation yet" - the gate behaves as if disk is empty
        # until the first probe lands.
        self._disk_usage_bytes = 0
        # Metrics surface (plan § 4.2.2). saturation_balance is the
        # plan-canonical Gauge name for current in-flight declared
        # bytes; admin endpoints serialize it via the registry.
        self._metrics = metrics_registry if metrics_registry is not None else MetricsRegistry()
        self._saturation_balance = self._metrics.register_gauge(
            "saturation_balance",
            "Current in-flight declared bytes admitted by the gate.",
        )

    @property
    def in_flight(self) -> int:
        """Current in-flight row count."""
        return self._in_flight

    @property
    def in_flight_bytes(self) -> int:
        """Current in-flight bytes."""
        return self._in_flight_bytes

    @property
    def max_in_flight(self) -> int:
        """Configured row cap."""
        return self._max_in_flight

    @property
    def max_in_flight_bytes(self) -> int:
        """Configured bytes cap."""
        return self._max_in_flight_bytes

    @property
    def max_disk_bytes(self) -> int:
        """Configured disk-usage cap (0 = disabled)."""
        return self._max_disk_bytes

    @property
    def disk_usage_bytes(self) -> int:
        """Most recent disk-usage observation from the probe."""
        return self._disk_usage_bytes

    @property
    def large_in_flight(self) -> int:
        """Current in-flight large-body count (§1.3)."""
        return self._large_in_flight

    @property
    def max_large_in_flight(self) -> int:
        """Configured large-class cap."""
        return self._max_large_in_flight

    @property
    def large_body_threshold_bytes(self) -> int:
        """Configured threshold (0 = large-body class disabled)."""
        return self._large_body_threshold_bytes

    @property
    def saturated(self) -> bool:
        """True if any cap is currently exceeded."""
        return (
            self._in_flight >= self._max_in_flight
            or self._in_flight_bytes >= self._max_in_flight_bytes
        )

    def set_disk_usage_bytes(self, value: int) -> None:
        """Update the cached disk-usage observation (called by DiskPressureProbe).

        Synchronous on purpose: the probe runs in its own coroutine and
        reads ``FileBodyStore.total_bytes`` (which is async); calling this
        setter is the last step. The setter is not lock-protected because
        write/read of a single Python int is atomic under the GIL - the
        cost of contention is reading a slightly-stale value for one
        admit call, which is acceptable.
        """
        self._disk_usage_bytes = max(0, value)

    async def admit(self, declared_bytes: int) -> AdmissionResult:
        """Try to admit a new upload and return the typed outcome.

        Returns:
            :class:`AdmissionGranted` on success;
            :class:`AdmissionRefusedSaturation` if the row/bytes cap
            was hit; :class:`AdmissionRefusedDiskPressure` if the
            configured ``max_disk_bytes`` is set and the cached
            observation is at or above it. Callers dispatch via
            ``isinstance``.
        """
        async with self._lock:
            # Disk pressure is checked first: it's the cheaper signal
            # (read of a cached int) and the producer's operator wants to see
            # disk_pressure when it's the actual cause. The check uses
            # `>=` so a row landing exactly at the cap is still refused -
            # a 0-byte ingress at-cap would otherwise be admitted and
            # then promptly trip ENOSPC when its row arrives on disk.
            if (
                self._max_disk_bytes > 0
                and self._disk_usage_bytes + declared_bytes >= self._max_disk_bytes
            ):
                return AdmissionRefusedDiskPressure()
            if self._in_flight + 1 > self._max_in_flight:
                return AdmissionRefusedSaturation()
            # Bytes cap. A 0 cap means "refuse all" - matching the row
            # cap, where ``max_in_flight=0`` refuses every admission via
            # ``0 + 1 > 0``. Finding A-1: a naive ``+ declared > cap``
            # admitted 0-byte bodies under a 0 cap (``0 + 0 > 0`` is
            # False), an inconsistency with the row cap's zero-semantics.
            # We special-case 0 rather than switch to ``>=`` so a body
            # that *exactly* fills a positive cap is still admitted
            # (``in_flight_bytes + declared == cap`` is fine), preserving
            # the existing "fill to the brim" behavior.
            if self._max_in_flight_bytes == 0:
                return AdmissionRefusedSaturation()
            if self._in_flight_bytes + declared_bytes > self._max_in_flight_bytes:
                return AdmissionRefusedSaturation()
            # Large-body class cap (§1.3): enforce only when the threshold
            # is set (> 0) and the candidate body qualifies as large.
            if (
                self._large_body_threshold_bytes > 0
                and declared_bytes >= self._large_body_threshold_bytes
                and self._large_in_flight + 1 > self._max_large_in_flight
            ):
                return AdmissionRefusedSaturation()
            self._in_flight += 1
            self._in_flight_bytes += declared_bytes
            if self._is_large(declared_bytes):
                # R9-6: remember the classification AT CHARGE TIME. The
                # release must decrement the large class iff THIS charge
                # was large, and the threshold may have been hot-reloaded
                # in between; recomputing at release desynchronized the
                # class ledger in both directions. The multiset of
                # outstanding large charge sizes is the identityless
                # memory: releases pair against it by size (equal-size
                # mis-pairing conserves the count exactly, because
                # release amounts always equal admit amounts per
                # invariant #2).
                self._large_in_flight += 1
                self._large_charge_sizes[declared_bytes] = (
                    self._large_charge_sizes.get(declared_bytes, 0) + 1
                )
        # Emit gauge after releasing the lock - Gauge.set acquires its
        # own asyncio.Lock and we forbid await inside async with lock
        # (plan § 0.3).
        await self._saturation_balance.set(self._in_flight_bytes)
        # The ONE place a SlotReservation is minted (ADR-036): the
        # granted charge travels as a token its holder must consume or
        # unwind, so the release basis is structurally the admit basis.
        return AdmissionGranted(SlotReservation(declared_bytes))

    def _is_large(self, body_bytes: int) -> bool:
        """Classify ``body_bytes`` against the large-body threshold."""
        threshold = self._large_body_threshold_bytes
        return threshold > 0 and body_bytes >= threshold

    async def reconcile_admit(self, actual_bytes: int) -> None:
        """SEED the ledger for a row that already exists, bypassing caps.

        ONE caller: boot reconstruction
        (:func:`phantom.workers.recovery.reconcile_saturation`), which
        walks the recovered rows and charges the gate for every one the
        slot predicate says is holding. There is no write, no CAS, no
        outcome and no crossing there: the ROW's slot-holding status
        does not change, the LEDGER's knowledge of it does, which is why
        boot reconstruction is a seed rather than a transition and keeps
        its own verb (ADR-036).

        The R9-4 replay repair, which used to share this method, IS a
        real transition (the row crossed into ``queued``) that merely
        needs to bypass the caps, so it moved to :meth:`settle`'s charge
        arm. Every ordinary admission MUST go through :meth:`admit`.
        """
        await self._charge_uncapped(actual_bytes)

    async def _charge_uncapped(self, actual_bytes: int) -> None:
        """Charge the ledger without consulting the caps.

        The body of the pre-ADR-036 ``reconcile_admit``, extracted so
        the settlement layer's charge arm and boot reconstruction share
        one copy of the R9-6 large-class bookkeeping instead of two.
        """
        async with self._lock:
            self._in_flight += 1
            self._in_flight_bytes += actual_bytes
            if self._is_large(actual_bytes):
                self._large_in_flight += 1
                self._large_charge_sizes[actual_bytes] = (
                    self._large_charge_sizes.get(actual_bytes, 0) + 1
                )
        # Emit after releasing the lock (plan § 0.3 forbids await
        # inside async with lock).
        await self._saturation_balance.set(self._in_flight_bytes)

    async def settle(self, delta: SlotDelta, *, consumes: SlotReservation | None = None) -> None:
        """Apply one write's effect on the ledger, and dispose of any reservation.

        The settlement layer's whole contract (ADR-036). The caller
        supplies a :class:`SlotDelta` built by an adapter from a STORE
        OUTCOME, plus the reservation it is holding if it took one
        before the write; the gate decides what the ledger owes. A
        caller never computes a slot transition.

        Args:
            delta: The crossing, built by a :class:`SlotDelta` adapter.
            consumes: The reservation this write was speculatively
                charged against, when the caller took one. A write whose
                crossing is a CHARGE consumes it; every other arm
                unwinds it.
        """
        if delta.held_before == delta.holds_after:
            # No crossing. The row's slot-holding status is what it was,
            # either because the write moved it within one side of the
            # predicate or because the write did not land at all. A
            # reservation presented here was SPECULATIVE and turned out
            # not to be needed, so it goes back (replay's reconcile is
            # the site that proves this arm has to exist).
            if consumes is not None:
                await self.unwind(consumes)
            return
        if delta.holds_after:
            # The row ENTERED the in-flight set.
            if consumes is not None:
                # The ledger already carries this charge: the caller
                # reserved before the write and the write is what
                # consumes the reservation. Charging again is the
                # double-charge on every successful kicker wake.
                return
            # Uncapped: the row is already live and cannot be refused
            # (the R9-4 replay reconcile).
            await self._charge_uncapped(delta.size_bytes)
            return
        # The row LEFT the in-flight set.
        if consumes is not None:
            # Unreachable at every site on today's tree, and present so
            # the rule is TOTAL rather than a case analysis over current
            # callers: a holder who reserved AND whose write dropped the
            # row out of the in-flight set owes the gate both.
            await self.unwind(consumes)
        await self.release(delta.size_bytes)

    async def unwind(self, reservation: SlotReservation) -> None:
        """Return a reservation its holder did not consume.

        The one entry point for a speculative charge coming back, for
        callers that have NO write outcome to settle on: an admission
        scope exiting without a committed row, a store write that
        RAISED, a row that vanished before the write. Callers that DO
        have an outcome present the reservation to :meth:`settle`
        instead, which unwinds it on every arm except the charge.
        """
        await self.release(reservation.declared_bytes)

    async def release(self, actual_bytes: int) -> None:
        """Decrement counters when the row leaves the in-flight set.

        A PRIMITIVE (ADR-036): it performs the arithmetic and decides
        nothing. Production reaches it through :meth:`settle` (a write
        whose crossing left the in-flight set) or :meth:`unwind` (a
        reservation coming back), never directly; the gate's own unit
        tests drive it directly, which is what the primitive layer is
        for.

        The large class decrements against the charge-time
        classification remembered by :meth:`admit` (R9-6), never by
        reclassifying ``actual_bytes`` against the current threshold: a
        hot-reloaded threshold otherwise stranded the counter (raise:
        large charges never released, the class cap refusing fresh
        large work forever) or over-drained it (lower: small charges
        releasing other rows' large slots).
        """
        async with self._lock:
            self._in_flight = max(0, self._in_flight - 1)
            self._in_flight_bytes = max(0, self._in_flight_bytes - actual_bytes)
            outstanding = self._large_charge_sizes.get(actual_bytes, 0)
            if outstanding > 0:
                if outstanding == 1:
                    del self._large_charge_sizes[actual_bytes]
                else:
                    self._large_charge_sizes[actual_bytes] = outstanding - 1
                self._large_in_flight = max(0, self._large_in_flight - 1)
        # Emit after releasing the lock (plan § 0.3 forbids await
        # inside async with lock).
        await self._saturation_balance.set(self._in_flight_bytes)

    async def update_caps(self, snapshot_saturation: SaturationCfg) -> None:
        """Update the gate's caps from a fresh :class:`SaturationCfg`.

        Called by the hot-reload handler after the snapshot is swapped.
        The update is protected by the same lock as admit/release, so a
        cap change cannot interleave with a half-completed admit.

        In-flight counters (``_in_flight``, ``_in_flight_bytes``,
        ``_large_in_flight``) are NOT reset - they reflect actual current
        state. Newly-arriving admits are evaluated against the new caps
        immediately. A changed ``large_body_threshold_bytes`` applies to
        NEW classifications only: in-flight large charges release
        against their remembered charge-time classification (R9-6), so
        the class ledger stays exact across the transition.

        Args:
            snapshot_saturation: The freshly-loaded ``SaturationCfg``
                sub-block from the validated reloaded Settings. Every
                probe-fillable field is guaranteed non-None by the
                Settings validator.
        """
        # Validator (Settings._resolve_defaults) fills every probe-fillable
        # field; assert non-None so the gate's int-only state stays well-typed.
        assert snapshot_saturation.max_in_flight is not None
        assert snapshot_saturation.max_in_flight_bytes is not None
        assert snapshot_saturation.max_disk_bytes is not None
        assert snapshot_saturation.large_body_threshold_bytes is not None
        assert snapshot_saturation.max_large_in_flight is not None
        async with self._lock:
            self._max_in_flight = snapshot_saturation.max_in_flight
            self._max_in_flight_bytes = snapshot_saturation.max_in_flight_bytes
            self._max_disk_bytes = snapshot_saturation.max_disk_bytes
            self._large_body_threshold_bytes = snapshot_saturation.large_body_threshold_bytes
            self._max_large_in_flight = snapshot_saturation.max_large_in_flight
