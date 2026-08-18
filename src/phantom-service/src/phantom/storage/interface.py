"""Storage Protocols (UploadStore, BodyStore, TokenCache).

These three Protocols define the storage seams the rest of Phantom
composes against. Concrete implementations live in the sibling modules:

* :class:`SqliteUploadStore` (one persistent SQLite post-Phase-1).
* :class:`RamBodyStore` / :class:`FileBodyStore`.
* :class:`SqliteTokenCache` (disk-only per ADR-003).

Plan § 2.3.4 / § 2.3.8 changes:

* :class:`UploadStore` — dropped ``tier`` property and the legacy
  ``mark_committed`` / ``list_uncommitted`` methods. Added the new
  ``mark_persisted`` (sole writer of the body_location ram→file
  transition), ``mark_corrupted``, ``iter_rows``,
  ``list_oldest_ram_bodies``, ``list_chain_ids``, and
  ``insert_with_idempotency_claim`` (atomic admission transaction).
* :class:`BodyStore` — dropped ``tier`` property. Added
  ``list_orphans(known_chain_ids)``.
"""

from __future__ import annotations

import enum
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID

from phantom.models.admin import TokenSlot
from phantom.models.credential import (
    CredCacheRow,
    CredentialSource,
    DestinationCredential,
    HostCredKey,
)
from phantom.models.token import TokenCacheRow, TokenSource
from phantom.models.upload import CapturedValues, UploadRow, UploadState


@dataclass(frozen=True)
class DeletedRowAccounting:
    """Saturation-relevant view of one row, captured atomically at removal.

    Returned by every row-REMOVING store method (single delete, bulk
    delete, the reaper's retention deletion and count-cap eviction) and
    handed to :meth:`phantom.workers.saturation.SlotDelta.from_removal`,
    which is where the R8-4 slot-holding predicate is applied. The
    caller supplies the outcome and the release basis; the gate decides
    whether the removal crossed the predicate (ADR-036). Captured inside
    the same write transaction as the DELETE, so the values cannot race
    a concurrent transition.
    """

    chain_id: UUID
    state: UploadState
    body_size_bytes: int
    body_discarded_at: datetime | None


@dataclass(frozen=True)
class PersistCandidateState:
    """The columns the RAM-pressure watcher reads off a migration candidate.

    ``state`` and ``updated_at`` are the whole input to the loop's freshness
    window (F-P8-A), so U11 reads those two rather than decoding a full
    :class:`~phantom.models.upload.UploadRow` per candidate.
    """

    state: UploadState
    updated_at: datetime


@dataclass(frozen=True)
class ParkedCandidate:
    """The columns a kicker rescan reads off one parked row.

    A projection rather than a whole :class:`~phantom.models.upload.UploadRow`
    (CL5): the rescan runs every second against a backlog nothing bounds, and
    decoding a strict row per candidate cost more than the query that found it.
    Every field here is read by :meth:`phantom.workers.kicker.Kicker._rescan`;
    nothing else is selected.

    ``body_size_bytes`` is carried even though the wake's
    :meth:`phantom.workers.saturation.SlotDelta.from_attempt` arm that reads it
    never fires on this path, because the gate is handed the row's true size
    rather than a zero that would be a lie about the row (ADR-036).
    """

    chain_id: UUID
    endpoint: str
    uid: str
    auth_blocked_host: str | None
    received_at: datetime
    body_size_bytes: int
    attempts: int


@dataclass(frozen=True)
class AttemptWriteOutcome:
    """Result of :meth:`UploadStore.record_attempt_result`.

    Carries the in-transaction pre-image of the row the guarded UPDATE
    targeted, so a caller settling the saturation ledger never has to
    reason from a snapshot the write already invalidated. Same
    discipline as :class:`DeletedRowAccounting` (R8-4),
    :class:`ReplayOutcome` (R9-4) and :class:`CancelOutcome` (R8-4).

    Attributes:
        rowcount: Rows updated, 0 or 1. The exact value this method
            returned before the type existed, kept so the store's
            documented contract and its tests read unchanged.
        new_state: The state the caller asked for, echoed so a delta
            adapter needs no second argument.
        previous_state: The row's state read in the SAME transaction
            immediately before the UPDATE, or ``None`` when no row with
            that ``chain_id`` existed. On a landed write this always
            equals ``expected_state``, because the UPDATE is CAS-guarded
            on it; on a NON-landed write it is the state that actually
            beat the guard, which is information the bare rowcount threw
            away.
        previous_body_discarded_at: The row's H4 stamp from the same
            read. ``record_attempt_result`` never writes that column, so
            this is also the post-write value.
        body_size_bytes: The row's size from the same read; 0 when no
            row existed. This is NOT the release basis for every
            caller. The four sender sites deliberately pass
            ``row.body_size_bytes`` instead; see
            :class:`phantom.workers.saturation.SlotDelta`.
    """

    rowcount: int
    new_state: UploadState
    previous_state: UploadState | None
    previous_body_discarded_at: datetime | None
    body_size_bytes: int

    @property
    def landed(self) -> bool:
        """Whether the guarded UPDATE actually fired."""
        return self.rowcount == 1


@dataclass(frozen=True)
class DiscardOutcome:
    """Result of :meth:`UploadStore.discard_body_and_zero_accounting`.

    ``flipped`` is True iff THIS call stamped the row (the in-transaction
    guard matched ``expected_state`` and found the stamp NULL);
    ``body_size_bytes`` is the pre-zero size captured in the same
    transaction, the release basis for a ``stored`` row's saturation
    slot (R9-5). A False outcome means another owner moved or already
    stamped the row; the caller must touch nothing.

    Attributes:
        flipped: Whether THIS call stamped the row.
        body_size_bytes: The pre-zero size captured in the same
            transaction.
        previous_state: The state the in-transaction guard matched, so
            ``expected_state`` on a flip and ``None`` otherwise. The
            before-side input
            :meth:`phantom.workers.saturation.SlotDelta.from_discard`
            applies the slot predicate to; no pre-image means no
            crossing, which is what lets that adapter carry no
            special case for a non-flip.
        discarded_at: The stamp this call wrote, or ``None`` when it
            did not flip. The after-side stamp of the same crossing.
    """

    flipped: bool
    body_size_bytes: int
    previous_state: UploadState | None
    discarded_at: datetime | None


@dataclass(frozen=True)
class ReplayOutcome:
    """Result of :meth:`UploadStore.replay` with atomic accounting truth.

    ``previous_state`` is the state the in-transaction precheck saw
    immediately before the re-queue UPDATE (R9-4): the gate's settlement
    of the re-queue (re-admit a released row; never double-charge a
    holding one) is computed from THIS value, because a route-side
    pre-fetch races the kicker's wake and the sender's terminal
    transitions in both directions. The route hands it to
    :meth:`phantom.workers.saturation.SlotDelta.from_replay` and the
    gate applies the predicate (ADR-036).
    """

    row: UploadRow
    previous_state: UploadState


@dataclass(frozen=True)
class CancelOutcome:
    """Result of :meth:`UploadStore.cancel` with atomic accounting truth.

    ``previous_state`` is the cancellable state the in-transaction
    precheck saw immediately before the UPDATE, or ``None`` when the
    row was already terminal and the cancel was a no-op. The gate
    settles the cancel against it (R8-4, ADR-036); a route-side
    pre-fetched state cannot serve because the row may transition
    between the fetch and the UPDATE (an ``auth_expired`` row woken and
    re-admitted by the kicker must release; a row the sender
    terminalized, and therefore released, must not).

    Attributes:
        row: The row as it reads AFTER the transaction commits. Its
            ``body_size_bytes`` is the route's release basis; it is
            never a slot-predicate input, because it is an
            unsynchronised later read.
        previous_state: The cancellable state the in-transaction
            precheck saw, or ``None`` when the UPDATE did not land.
        previous_body_discarded_at: The row's H4 stamp from that same
            in-transaction read. Cancel is the one outcome that needs
            it: its UPDATE guard admits ``stored``, which is the single
            state :func:`phantom.workers.saturation.row_holds_slot`
            consults the stamp for, and unlike :meth:`UploadStore.replay`
            it has no stamped-row refusal at all.
    """

    row: UploadRow
    previous_state: UploadState | None
    previous_body_discarded_at: datetime | None


class InsertClaimOutcome(enum.Enum):
    """Outcome of :meth:`UploadStore.insert_with_idempotency_claim`.

    The atomic admission transaction inserts the upload row AND the
    idempotency claim. Three distinguishable terminal outcomes — the
    caller (admission) maps each to a different HTTP response:

    * :attr:`INSERTED` — both rows committed; a new chain is admitted
      (HTTP 202).
    * :attr:`IDEMPOTENCY_COLLISION` — the ``idempotency_index`` UNIQUE
      claim already exists for this ingress key. Admission resolves the
      existing row and either replays it (HTTP 200, same body) or rejects
      with ``idempotency_key_conflict`` (HTTP 422, different body —
      finding G-1).
    * :attr:`CHAIN_ID_COLLISION` — the ``uploads.chain_id`` PRIMARY KEY
      already exists (the envelope reused a live chain_id). Admission
      rejects with ``chain_id_in_use`` (HTTP 409 — finding D-1) rather
      than letting the raw ``sqlite3.IntegrityError`` escape as a naked
      500.
    """

    INSERTED = "inserted"
    IDEMPOTENCY_COLLISION = "idempotency_collision"
    CHAIN_ID_COLLISION = "chain_id_collision"


@dataclass(frozen=True)
class StateTally:
    """One state's row count and summed body bytes.

    The storage-layer value type returned per state by
    :meth:`UploadStore.counts_by_state`. Distinct from the API-facing
    ``phantom.models.admin.TierBreakdown`` (a Pydantic wire model): this
    is an internal, frozen aggregate carried between the store and the
    stats route, never serialized.

    Attributes:
        count: Number of ``uploads`` rows in the state.
        bytes: Sum of ``body_size_bytes`` across those rows.
    """

    count: int
    bytes: int


WakeHandler = Callable[[str, str], Awaitable[None]]
"""Callback registered with :class:`TokenCache.register_wake_handler`.

Args: ``(endpoint, uid)`` — the slot that was just written. The ``uid``
here is the credential-cache axis (X-Phantom-Uid value); not to be
confused with chain_id.
"""

CredentialWakeHandler = Callable[[HostCredKey], Awaitable[None]]
"""Callback registered with :class:`CredentialStore.register_wake_handler`.

COPY of :data:`WakeHandler` with the ONE forced difference: the credential
store keys on the destination host alone, so the handler takes a single
``(dest_host)`` argument rather than the token cache's ``(endpoint, uid)``.
"""


class UploadStore(Protocol):
    """Owns metadata rows; one instance per SQLite DB (post-Phase-1: one)."""

    async def start(self) -> None:
        """Open the underlying SQLite connections and apply schema.

        Implementations that split reads from writes (cycle-7 task 4.1)
        open BOTH the serialized writer and the dedicated read-only
        connection here; the composition root only calls ``start`` after
        the instance's integrity gate and mode guard have finished
        moving files, so both descriptors bind to the settled paths.
        """
        ...

    async def stop(self) -> None:
        """Close the underlying SQLite connection(s)."""
        ...

    async def insert(self, row: UploadRow) -> None:
        """Insert a new row. Caller supplies a fully populated row."""
        ...

    async def get(self, chain_id: UUID) -> UploadRow | None:
        """Return the row for ``chain_id`` or ``None`` if no row exists."""
        ...

    async def update_state(
        self,
        chain_id: UUID,
        *,
        new_state: UploadState,
        expected_state: UploadState | None = None,
    ) -> bool:
        """Atomic state transition.

        Returns ``False`` if ``expected_state`` was supplied and did not
        match the current row state (claim contention).
        """
        ...

    async def claim_due(self, now: datetime, limit: int) -> list[UploadRow]:
        """Atomic ``queued`` → ``attempting`` claim.

        Returns up to ``limit`` rows whose ``state == 'queued'`` and
        ``next_attempt_at <= now`` after flipping each to ``attempting``.
        """
        ...

    async def record_attempt_result(
        self,
        chain_id: UUID,
        *,
        new_state: UploadState,
        attempts: int,
        next_attempt_at: datetime | None,
        last_error: str | None,
        upstream_status: int | None,
        upstream_headers_json: str | None,
        captured_values: CapturedValues | None,
        current_step_index: int | None,
        last_step_completed: str | None,
        expected_state: UploadState = "attempting",
        stamp_sent_at: bool = False,
        auth_blocked_host: str | None = None,
    ) -> AttemptWriteOutcome:
        """Persist the result of one attempt against this row.

        M-W4-F7 audit closure: ``expected_state`` is a defensive
        precondition — the UPDATE only fires when the row is currently
        in that state. The default ``"attempting"`` matches the
        sender's normal flow (a row is in ``attempting`` between
        ``claim_due`` and the terminal UPDATE). If the row's state
        moved under the sender (admin cancel, admin replay, or a
        concurrent worker), the UPDATE finds no rows and returns
        ``rowcount=0``; the caller logs and continues without
        overwriting the new state.

        ``stamp_sent_at`` (cycle-7 task 2.5): when True, ``sent_at`` is
        stamped write-once (only while still NULL) with the same
        timestamp written to ``updated_at``. Passed True ONLY by the
        sender's chain-done success branch; the stamp survives operator
        replay (the NULL guard keeps the original delivery time).

        ``auth_blocked_host`` (D2/F6) records the host whose credential
        slot rejected this row; the sender's ``auth_expired`` park is
        the only caller that passes one. What THIS writer guarantees:
        the column is written UNCONDITIONALLY from a parameter
        defaulting to ``None``, so every transition through it leaves
        the column consistent with the state it just wrote. The limit,
        stated in the same breath: THREE live CAS writers (``replay``,
        ``cancel``, ``mark_corrupted``) change ``state`` without going
        through here, so a row can leave ``auth_expired`` still
        carrying its recorded host. The column is AUTHORITATIVE only
        while the row is in ``auth_expired`` (the only state either
        kicker reads it in); off-park it is inert history.

        Returns:
            An :class:`AttemptWriteOutcome` whose ``rowcount`` is the
            number of rows updated (0 or 1) and whose remaining fields
            are the row's in-transaction pre-image, read immediately
            before the UPDATE in the same transaction. The gate settles
            the saturation ledger from that pre-image (ADR-036), so no
            caller reasons about the crossing from its own snapshot.
        """
        ...

    async def list_uploads(
        self,
        *,
        state: UploadState | None = None,
        route: str | None = None,
        multifile_id: UUID | None = None,
        group_id: UUID | None = None,
        since: datetime | None = None,
        limit: int = 100,
        cursor: str | None = None,
        instance: str | None = None,
    ) -> tuple[list[UploadRow], str | None]:
        """Filter + cursor-paginate rows. Returns ``(rows, next_cursor)``.

        With the ``multifile_id`` filter the results are ordered
        ``send_order ASC``, the keyset ``cursor`` is rejected
        (``ValueError``), and ``next_cursor`` is always ``None``; every
        other filter combination (``group_id`` included) paginates in
        receipt-time order exactly as before.
        """
        ...

    async def list_by_key_value(
        self,
        key: str,
        value: str,
        *,
        instance: str | None = None,
        limit: int = 100,
    ) -> list[UploadRow]:
        """Find rows whose chain envelope's metadata key-value-store contains key=value."""
        ...

    async def list_by_group_id(
        self,
        group_id: UUID,
        *,
        instance: str | None = None,
    ) -> list[UploadRow]:
        """Every row in one query group (cycle-7 task 4.2).

        An indexed equality scan, deliberately UN-paginated: a query
        group is producer-scale bound, and the group rollup is a status
        summary, not a firehose. Ordered ``received_at ASC, chain_id
        ASC``. Callers needing paginated raw rows use
        :meth:`list_uploads` with its ``group_id`` filter.
        """
        ...

    async def find_by_captured_value(
        self,
        capture_name: str,
        subpath: str,
        value: str,
    ) -> list[UploadRow]:
        """Find rows carrying ``value`` inside their captured values (cycle-7 task 4.2).

        JSON1 extract over ``captured_values_json`` at
        ``$.steps.<capture_name>.values.<subpath>``. The binding values
        are deployment-supplied per-instance configuration
        (``InstanceCfg.admin_lookup``), keeping the service
        upstream-ignorant. A miss returns an empty list, never raises.
        """
        ...

    async def find_by_local_uuid(self, local_uuid: UUID) -> list[UploadRow]:
        """Find rows stamped with ``local_uuid`` in their metadata KVS (cycle-7 task 4.2).

        The extract path is PINNED to the ``phantom_local_uuid`` metadata
        key (the exact path the generic key-value match builds for that
        key); callers never spell a JSON path. A list because Phantom
        enforces no global uniqueness on the key.
        """
        ...

    async def list_non_terminal(self) -> list[UploadRow]:
        """Every row whose state is not in the terminal set."""
        ...

    async def list_parked_candidates(self) -> list[ParkedCandidate]:
        """Parked, still-deliverable rows, projected to the kicker's columns.

        ``WHERE state = 'auth_expired' AND body_discarded_at IS NULL``,
        ordered oldest first. Both predicates were Python filters over
        :meth:`list_non_terminal`'s result set until CL5 moved them into the
        statement; because they read fields of rows fetched by one SELECT,
        evaluating them inside that SELECT selects exactly the same rows.

        The stamp half is the H4 carve-out (R6-3) that
        :func:`phantom.workers.saturation.is_deliverable` states in full: a
        stamped ``body_discarded_at`` means the reaper discarded the body by
        retention policy, so waking the row would land it in ``corrupted`` on
        the sender's next claim and burn a saturation slot on a row that can
        never succeed. The predicate lives in SQL now; the reason lives here.

        The order is DECLARED rather than incidental. Today's scan and the
        index seek return the same sequence, because the single writer of
        ``auth_expired`` stores a NULL ``next_attempt_at`` and equal index
        keys fall back to rowid order, but nothing pins that invariant. Under
        a saturated gate the rescan skips refused rows and retries next tick,
        so WHICH rows wake in a tick is order dependent, and oldest-parked
        first is the fairness property an operator expects of a backlog.

        Returns:
            One :class:`ParkedCandidate` per parked row, oldest
            ``received_at`` first.
        """
        ...

    async def counts_by_state(self) -> dict[UploadState, StateTally]:
        """Row count and summed body bytes per state, in one read.

        A read-only ``GROUP BY state`` aggregate over the whole
        ``uploads`` table (every state, terminal and non-terminal). The
        stats route uses it to populate ``by_state.stored`` (terminal, so
        invisible to :meth:`list_non_terminal`) and the parked-backlog
        total.

        Returns:
            A mapping from each :data:`UploadState` that has at least one
            row to its :class:`StateTally`. States with zero rows are
            ABSENT from the mapping; callers default a missing state to
            ``StateTally(0, 0)``.
        """
        ...

    async def list_all_chain_ids(self) -> list[UUID]:
        """Return every chain_id in this store regardless of state.

        The per-store enumeration helper. The reaper's
        idempotency-cleanup carve-out preserve set is sourced from
        the single store.
        """
        ...

    async def list_chain_ids(self) -> list[UUID]:
        """Return every chain_id currently in ``uploads`` (alias of list_all_chain_ids).

        Provided so the body-orphan janitor (plan § 2.3.14)
        gets a clearly-named "known set" source for ``BodyStore.list_orphans``.
        """
        ...

    async def reset_attempting_to_queued(self) -> int:
        """Init-recovery sweep. Returns the count of rows reset."""
        ...

    async def mark_persisted(self, chain_id: UUID) -> int:
        """Flip body_location from 'ram' to 'file'; return the rowcount.

        SOLE writer of this transition per the single-writer manifest
        (plan § 0.5 invariant #6). Called by the PersistController
        (plan § 2.3.11) after fsync of the body file(s) and
        their parent directory completes. Implementations MUST guard
        with ``WHERE body_location = 'ram'`` (duplicate call after an
        unrelated race is a defensive no-op) AND ``body_discarded_at
        IS NULL`` (R7-2: a migration racing the reaper's body-discard
        must not resurrect policy-discarded bytes), and MUST return the
        UPDATE rowcount so the caller can undo a raced disk write.
        """
        ...

    async def mark_corrupted(self, chain_id: UUID, reason: str) -> None:
        """Quarantine the row in the ``corrupted`` terminal state.

        Used by recovery (plan § 2.3.15) when a row's body
        files vanish between persist and process restart, and by the
        sender on body-hash mismatch. Writes ``state='corrupted'`` plus
        ``last_error=reason`` and updates ``updated_at``.
        """
        ...

    def iter_rows(self, *, deliverable_only: bool = False) -> AsyncIterator[UploadRow]:
        """Stream every row, or only the still-deliverable ones.

        Used by recovery and the invariant-audit coroutine
        (Phase 3) for row-walk passes. Implementations may iterate the
        underlying connection's cursor; callers MUST consume promptly
        because the cursor is held for the duration of iteration.

        Note the non-``async def`` shape: an async generator's
        ``__call__`` synchronously returns the generator object, then
        the generator is awaited via ``async for``. Implementations
        use ``async def`` + ``yield`` (which itself is type-compatible
        with this Protocol).

        Args:
            deliverable_only: When True, the walk yields only rows outside
                :data:`TERMINAL_STATES` whose ``body_discarded_at`` is NULL.
                Both halves are carve-outs the invariant auditor used to apply
                in Python, and both are stated here because this is now where
                the decision lives (U10).

                The TERMINAL half is finding R9-PM-3: a row in a terminal
                state is FINISHED and its body is legitimately gone (a
                ``succeeded`` row's body is deleted on delivery without ever
                stamping ``body_discarded_at``), so auditing body presence on
                it fires spurious ``missing_body_*`` violations every sweep.
                ``auth_expired`` is deliberately NOT terminal and is still
                walked, because it is still deliverable and its body MUST be
                present.

                The STAMP half is the H4 carve-out (R6-3): the reaper aged
                that row's body out by policy, so there is no invariant left
                to check.

                The default is False and it is load-bearing rather than
                polite. Recovery's two walks MUST keep seeing terminal and
                discarded rows: one of them reconstructs the saturation ledger
                at boot, and filtering it would silently under-count the gate.
        """
        ...

    async def get_persist_candidate_state(self, chain_id: UUID) -> PersistCandidateState | None:
        """The two columns the RAM-pressure loop reads off a migration candidate.

        A narrow read rather than :meth:`get` (U11): the loop re-reads each of
        up to 64 candidates per pressure tick to apply its freshness window,
        and needs only ``state`` and ``updated_at``.

        The re-read's TIMING is deliberate and unchanged. Folding these two
        columns into :meth:`list_oldest_ram_bodies` would make the loop judge
        every candidate against a snapshot taken before the first enqueue,
        and the loop awaits an enqueue and a re-sample per candidate, so by
        the 64th that snapshot is measurably stale. What U11 narrows is the
        read's WIDTH, not when it happens.

        Returns:
            The candidate's state and stamp, or ``None`` when no row with
            that ``chain_id`` exists, which is the arm the caller already had.
        """
        ...

    async def list_oldest_ram_bodies(self, limit: int) -> list[UUID]:
        """Return the oldest ``body_location='ram'`` chain_ids.

        Used by the RAM-pressure watcher (plan § 2.3.12)
        to pick migration candidates when RAM pressure breaches the
        ceiling. Ordered by ``received_at`` ASC; capped at ``limit``.
        """
        ...

    async def find_by_chain_id_at_ingress(self, chain_id_at_ingress: str) -> UUID | None:
        """Return any row's chain_id whose ``chain_id_at_ingress`` matches.

        Admission-side dedup fallback used when the
        ``idempotency_index`` lookup misses (e.g., a buggy cleanup
        sweep pruned the index row while the upload row is still
        live). Returns the first match or ``None``. The match column
        captures the producer-supplied ``X-Phantom-Idempotency-Key`` at
        admission time. Distinct from ``idempotency_key`` (the envelope
        field that Phantom forwards to upstream).
        """
        ...

    async def insert_with_idempotency_claim(
        self,
        row: UploadRow,
        idempotency_key: str,
    ) -> InsertClaimOutcome:
        """Atomically INSERT the upload row AND its idempotency claim.

        Returns an :class:`InsertClaimOutcome` distinguishing a clean
        insert from an idempotency-claim collision and a
        ``uploads.chain_id`` PRIMARY KEY collision (finding D-1 — the
        latter previously escaped as a naked HTTP 500). Either both
        INSERTs commit or neither does (single SQLite transaction).
        Closes H7 structurally per plan § 2.3.17.
        """
        ...

    async def claim_idempotency(
        self,
        chain_id_at_ingress: str,
        chain_id: UUID,
    ) -> UUID:
        """INSERT-OR-IGNORE dedup. Returns existing chain_id if seen, else the new one.

        The non-atomic admission path; admission uses
        ``insert_with_idempotency_claim`` for the H7 structural
        closure.
        """
        ...

    async def cleanup_idempotency_index(
        self,
        *,
        preserve_chain_ids: Iterable[UUID] = (),
    ) -> int:
        """Drop idempotency-index rows whose linked upload has been reaped.

        ``preserve_chain_ids`` carves out chain_ids the caller knows are
        still live but absent from this store's ``uploads`` table.
        The parameter means "rows that should never be pruned
        regardless of ``uploads`` membership."
        """
        ...

    async def delete(self, chain_id: UUID) -> DeletedRowAccounting | None:
        """Hard delete one row by chain_id.

        Returns the row's :class:`DeletedRowAccounting`, captured inside
        the same write transaction as the DELETE, or ``None`` when no
        row matched. The caller releases the saturation gate for rows
        that still held a slot (R8-4).
        """
        ...

    async def replay(self, chain_id: UUID) -> ReplayOutcome | None:
        """Reset attempts/state and transition to ``queued`` for retry.

        Returns a :class:`ReplayOutcome` carrying the in-transaction
        ``previous_state`` (R9-4: the caller's gate reconciliation
        input), or ``None`` when the row does not exist.

        M-W4-F7 audit closure: the UPDATE is guarded by
        ``state IN ('succeeded','failed','corrupted','cancelled','queued',
        'auth_expired','stored')`` — every state EXCEPT ``attempting``
        and the body-released terminal ``expired`` (ADR-032). A sender is
        actively driving an ``attempting`` row, so replay must refuse
        rather than clobber the sender's in-flight work; an ``expired``
        row has no body to replay and is refused up front by the
        body-accounting guard below, so it too stays out of the IN-set.
        Round 1 defender fix (R1-1): that refusal raises the typed
        :class:`phantom.storage.errors.ReplayRefusedAttemptingError`
        and leaves the row untouched (caller responds with the
        canonical 409 ``replay_refused_attempting`` envelope).

        Returns the freshly-updated :class:`UploadRow` on success, or
        ``None`` when the row does not exist (caller responds 404).

        Body-accounting refusal (cycle-7 phase 7 pre-round defender
        fix): when the row's ``body_discarded_at`` is stamped the bytes
        are gone and a re-queue could only land the row in
        ``corrupted`` on the sender's next claim, so replay raises
        :class:`phantom.storage.errors.ReplayBodyDiscardedError`
        instead and leaves the row untouched (caller responds 409
        ``replay_body_discarded``). Checked before the state guard: a
        stamped row refuses in every state, including ``attempting``.
        """
        ...

    async def cancel(self, chain_id: UUID) -> CancelOutcome:
        """Transition to ``cancelled`` if non-terminal; no-op otherwise.

        Returns a :class:`CancelOutcome` whose ``previous_state`` is the
        cancellable state the in-transaction precheck saw immediately
        before the UPDATE, or ``None`` when the row was already terminal
        (no transition). The caller releases the saturation gate iff the
        previous state held a slot (R8-4); a route-side pre-fetch cannot
        serve because the row may transition between fetch and UPDATE.
        """
        ...

    async def bulk_delete(
        self,
        *,
        state: UploadState | None = None,
        route: str | None = None,
        since: datetime | None = None,
        instance: str | None = None,
    ) -> list[DeletedRowAccounting]:
        """Bulk delete by filter. Raises ValueError on all-None filter (ADR-004).

        Returns one :class:`DeletedRowAccounting` per deleted row,
        captured inside the same write transaction as the DELETE, so the
        caller can delete the corresponding bodies (C1) and release the
        saturation gate for rows that still held a slot (R8-4).
        """
        ...

    async def discard_body_and_zero_accounting(
        self, chain_id: UUID, *, expected_state: UploadState
    ) -> DiscardOutcome:
        """Record a body discard: stamp ``body_discarded_at`` AND zero ``body_size_bytes``.

        The ONE owner of the row-side body-discard effect (cycle-7 task
        4.7, D9/D10); the zeroing is load-bearing for saturation
        accounting. The UPDATE is guarded in-transaction (R9-5): it
        fires only while the row is still in ``expected_state`` AND
        unstamped, so a cross-owner sweep acting on a snapshot cannot
        discard a row that a replay or kicker wake revived mid-sweep,
        and a second discard attempt is a no-op. Returns a
        :class:`DiscardOutcome` whose ``flipped`` reports whether THIS
        call stamped the row and whose ``body_size_bytes`` is the
        in-transaction pre-zero size (the ``stored``-slot release
        basis). Exactly three callers: the sender's immediate discard
        (``succeeded_body_seconds == 0``), the reaper's scheduled
        discard (non-zero window elapsed), and the shared
        ``expire_row`` writer (``workers/_expire.py``, ADR-032). All
        three stamp first and delete body files only after a confirmed
        flip (R9-5 for the reaper, R10-1 for the sender, F3 for
        ``expire_row``). A crash between stamp and file delete leaves a
        stamped row whose files the metadata-retention pass and the
        orphan janitor converge.
        """
        ...

    async def vacuum(self) -> None:
        """Run a SQLite ``VACUUM`` on this store.

        Holds the store's write lock so concurrent writes wait until
        the VACUUM completes (SQLite VACUUM itself requires exclusive
        access; the lock just keeps Phantom's own writers honest).
        """
        ...

    async def list_terminal_older_than(
        self,
        state: UploadState,
        cutoff: datetime,
    ) -> list[UploadRow]:
        """Return terminal rows in ``state`` whose ``updated_at`` < ``cutoff``.

        Reaper helper (plan § 2.3.16) — called for the body-discard
        pass: every row returned has its body deleted and is then
        marked ``body_discarded_at`` via :meth:`discard_body`. Filters
        on ``body_discarded_at IS NULL`` so already-discarded rows do
        not re-enter the pass.
        """
        ...

    async def delete_terminal_older_than(
        self,
        state: UploadState,
        cutoff: datetime,
    ) -> list[DeletedRowAccounting]:
        """Hard-delete terminal rows in ``state`` whose ``updated_at`` < ``cutoff``.

        Reaper helper (plan § 2.3.16) — the metadata-retention pass.
        Returns one :class:`DeletedRowAccounting` per deleted row,
        captured in the same write transaction, so the reaper can
        release the gate for ``stored`` rows whose body was never
        separately discarded (R8-4). Raises ``ValueError`` if ``state``
        is non-terminal (single-purpose surface — the reaper should
        never call this with a queued/attempting state).
        """
        ...

    async def evict_terminal_over_limit(self, max_rows: int) -> list[DeletedRowAccounting]:
        """Count-cap backstop (V3) — evict oldest-DONE rows over ``max_rows``.

        Reaper helper. When the ``uploads`` table holds more than
        ``max_rows`` rows, delete the oldest fully-terminal rows
        (ordered by ``updated_at`` ASC) until the total count is at or
        below the cap, and return one :class:`DeletedRowAccounting` per
        deleted row (the caller deletes their bodies, trims the
        idempotency index, and releases the gate for rows that still
        held a slot — R8-4). Only :data:`TERMINAL_STATES` rows are
        evictable — in-flight and still-deliverable ``auth_expired``
        rows are never dropped, so the backstop cannot lose an
        undelivered upload. ``max_rows < 0`` is unbounded (no-op →
        ``[]``), preserving time-only retention.
        """
        ...


class BodyStore(Protocol):
    """Owns body bytes; one instance per binding (RAM dict, disk files, hybrid).

    The ``tier`` property is gone; ``list_orphans`` added per plan
    § 2.3.8. The plural-by-design ``body_refs`` shape on ``put`` is
    preserved for the multipart-atomic-unit semantics. ``put``'s
    namespace semantics are binding-ASYMMETRIC by design - see
    :meth:`put` for the contract every caller must respect.
    """

    async def start(self) -> None:
        """Open underlying resources (no-op for RAM)."""
        ...

    async def stop(self) -> None:
        """Release underlying resources."""
        ...

    async def put(self, chain_id: UUID, body_refs: dict[str, bytes]) -> int:
        """Store every named body_ref for ``chain_id``. Returns total bytes stored.

        Namespace semantics are binding-ASYMMETRIC (R11-a): the
        Protocol guarantees only that every ref in ``body_refs`` is
        readable afterwards - it does NOT guarantee the chain_id's
        namespace holds nothing else. ``RamBodyStore.put`` REPLACES
        the whole chain entry; ``FileBodyStore.put`` is ADDITIVE
        (per-ref files written into the chain directory, pre-existing
        names not in ``body_refs`` survive); ``HybridBodyStore.put``
        writes the RAM half only (replace there, the disk half
        untouched). A caller that needs a clean namespace - one whose
        readers take the namespace UNION, like the sender's
        ``get_all`` verify path - must :meth:`delete` first, which is
        exactly what admission's R11-1 chain_id namespace clear does
        before its put. The asymmetry is documented rather than
        unified: every current caller either puts whole body sets
        into a cleared/virgin namespace (admission) or re-puts the
        same row's full ref set (the PersistController migration), so
        replace-on-put would add an unconditional directory clear to
        the migration path for no behavioral gain.
        """
        ...

    async def get(self, chain_id: UUID, name: str) -> bytes:
        """Read one named body_ref."""
        ...

    async def get_all(self, chain_id: UUID) -> dict[str, bytes]:
        """Read every body_ref for ``chain_id`` as ``{name: bytes}``."""
        ...

    async def has_body_ref(self, chain_id: UUID, name: str) -> bool:
        """Return whether a body_ref named ``name`` exists for ``chain_id``.

        Used by recovery to detect persisted rows whose body files
        vanished between persist and the next process start. For the
        RAM binding, this is a dict lookup; for the file binding, a
        non-blocking filesystem check via :mod:`aiofiles`.
        """
        ...

    async def delete(self, chain_id: UUID) -> None:
        """Remove all body_refs for ``chain_id``."""
        ...

    async def total_bytes(self) -> int:
        """Saturation accounting — sum of stored body bytes.

        Describes the VALUE, not how an implementation arrives at it:
        :class:`~phantom.storage.ram_body_store.RamBodyStore` and
        :class:`~phantom.storage.file_body_store.FileBodyStore` both keep a
        running counter, and the file store's is orphan-aware because its
        boot seed is a tree walk (CL6, U11).
        """
        ...

    async def list_chain_ids(self) -> list[UUID]:
        """For orphan-sweep on startup."""
        ...

    async def list_orphans(self, known_chain_ids: set[UUID]) -> list[UUID]:
        """Return chain_ids the body store holds that are absent from ``known_chain_ids``.

        The body-orphan janitor (plan § 2.3.14) calls this
        with the union of every live ``uploads.chain_id`` and reaps the
        returned set. RAM bindings return ``[]`` (no orphans by
        construction — RAM is purged on chain drop). File bindings
        compare on-disk chain dirs against the known set.
        """
        ...


class TokenCache(Protocol):
    """ADR-002 cache keyed by ``(endpoint, uid)``. Disk-backed per ADR-003."""

    async def start(self) -> None:
        """Open the underlying connection."""
        ...

    async def stop(self) -> None:
        """Close the underlying connection."""
        ...

    async def get(self, endpoint: str, uid: str) -> TokenCacheRow | None:
        """Return the cached row for ``(endpoint, uid)`` or ``None``."""
        ...

    async def set(
        self,
        endpoint: str,
        uid: str,
        bearer: str,
        *,
        source: TokenSource,
    ) -> TokenCacheRow:
        """Write the slot, update ``observed_at``, fire registered wake handlers."""
        ...

    async def mark_bad(self, endpoint: str, uid: str) -> None:
        """ADR-003: bad tokens stay in cache; status flips to ``bad``."""
        ...

    async def mark_all_bad(self) -> int:
        """ADR-003: flip every slot to ``bad`` (preserve, don't delete). Returns count."""
        ...

    async def list_slots(
        self,
        *,
        endpoint: str | None = None,
    ) -> list[TokenSlot]:
        """Return slot metadata only — never the bearer (ADR-004)."""
        ...

    async def delete(self, endpoint: str, uid: str) -> None:
        """Hard delete one slot."""
        ...

    async def delete_all(self) -> int:
        """Hard delete every slot. Returns the count."""
        ...

    def register_wake_handler(self, handler: WakeHandler) -> None:
        """Register a callback invoked on every ``set()``."""
        ...


class CredentialStore(Protocol):
    """Host-keyed destination-credential store. Disk-backed per ADR-003.

    A COPY of the :class:`TokenCache` Protocol surface, keyed by the resolved
    destination host alone (no ``uid`` axis) and holding a structured
    :data:`~phantom.models.credential.DestinationCredential` value rather than a
    bearer string. The credential value is read internally only (the signer
    retrieves it at sign time); the admin surface exposes status only (ADR-004).
    """

    async def start(self) -> None:
        """Open the underlying connection."""
        ...

    async def stop(self) -> None:
        """Close the underlying connection."""
        ...

    async def get(self, dest_host: HostCredKey) -> CredCacheRow | None:
        """Return the cached row for ``dest_host`` or ``None``."""
        ...

    async def set(
        self,
        dest_host: HostCredKey,
        credential: DestinationCredential,
        *,
        source: CredentialSource,
    ) -> CredCacheRow:
        """Write the slot, update ``observed_at``, fire registered wake handlers."""
        ...

    async def mark_bad(self, dest_host: HostCredKey) -> None:
        """ADR-003: bad credentials stay in the store; status flips to ``bad``."""
        ...

    def register_wake_handler(self, handler: CredentialWakeHandler) -> None:
        """Register a callback invoked on every ``set()``."""
        ...


TERMINAL_STATES: frozenset[
    Literal["succeeded", "failed", "stored", "cancelled", "corrupted", "expired"]
] = frozenset({"succeeded", "failed", "stored", "cancelled", "corrupted", "expired"})
"""States after which a row never advances on its own — only the reaper
or admin actions move it. ``auth_expired`` is NOT terminal (auth_kicker
re-queues it on token refresh). ``corrupted`` is terminal — body
verification failed at send time and no retry will resolve it. ``expired``
is terminal (ADR-032): the per-route send-deadline elapsed, the body was
released, and the row is never re-admitted — the deliberate OPPOSITE of
``auth_expired``, which stays out of this set so the kickers keep sweeping
it (``list_non_terminal`` is ``WHERE state NOT IN (TERMINAL_STATES)``).
"""
