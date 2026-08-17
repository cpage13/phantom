"""Typed exceptions for storage-side corruption detection and refusals."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID


class BodyMissingError(Exception):
    """Body files declared on the row but absent from the body store.

    Phase 2 § 3.2.6 (H8 audit closure). The sender previously caught
    ``KeyError`` from ``body_store.get_all`` and returned an empty
    dict — making a row with declared bodies but absent body store
    entries look identical to a row with no body declared. The sender
    then forwarded an empty payload upstream, an effective data loss.

    Per ADR-014's runtime missing-body contract, the correct action is
    to mark the row corrupted immediately with
    ``last_error='storage_corruption:bodies_missing'``. The reaper's
    body-discard pass (and recovery's ``body_discarded_at`` carve-out)
    cover the legitimate "body deliberately deleted" cases; an
    unexpected miss in the sender's load path is a real corruption
    signal.

    TWO triggers construct this, both inside ``Sender._load_body_refs``:

    1. The whole-directory ``KeyError`` out of ``body_store.get_all``
       (a vanished chain directory, or a file lost mid-traversal).
       ``missing`` is then every declared ref.
    2. The declared-versus-returned shortfall (F2): the store returned a
       dict smaller than ``row.body_hashes``, because the chain directory
       was already partial or already empty when it was listed. ``missing``
       is the sorted set difference. ADR-014 treats a body as an atomic
       unit, so an incomplete body is a missing body.
    """

    def __init__(self, chain_id: UUID, missing: list[str]) -> None:
        super().__init__(f"chain_id={chain_id} missing body_refs={missing}")
        self.chain_id = chain_id
        self.missing = missing


class ReplayBodyDiscardedError(Exception):
    """Replay refused: the row's body was already discarded.

    Cycle-7 phase 7 pre-round defender fix. ``body_discarded_at`` is
    stamped by the ONE discard owner
    (``discard_body_and_zero_accounting``), on either trigger: the
    sender's immediate leg (``succeeded_body_seconds == 0``, the
    default) or the reaper's scheduled leg when a retention window
    elapses. Once stamped, the bytes are gone and the row's own
    accounting says so (``body_size_bytes`` zeroed by the same
    operation). Re-queuing such a row cannot possibly succeed: the
    sender's next claim finds no body and lands the row in the
    ``corrupted`` terminal state, laundering an operator action into a
    corruption signal. ``SqliteUploadStore.replay`` therefore refuses
    UP FRONT, inside the write transaction, leaving the row untouched
    (``sent_at`` and all other fields preserved).

    The admin route lets this propagate to
    ``replay_body_discarded_exception_handler`` (registered on the
    FastAPI app), which converts it into the canonical 409
    ``ErrorEnvelope`` with ``error.code='replay_body_discarded'`` so
    the SDK raises :class:`phantom_client.errors.PhantomConflictError`.

    Attributes:
        chain_id: The chain whose replay was refused.
        instance_id: The instance holding the row (read off the row
            itself; the store does not know its own instance id).
        body_discarded_at: When the body discard was recorded.
    """

    def __init__(
        self,
        *,
        chain_id: UUID,
        instance_id: str,
        body_discarded_at: datetime,
    ) -> None:
        super().__init__(
            f"replay refused for chain_id={chain_id}: the body was "
            f"discarded at {body_discarded_at.isoformat()}; a replay "
            "would have nothing to send"
        )
        self.chain_id = chain_id
        self.instance_id = instance_id
        self.body_discarded_at = body_discarded_at


class ReplayRefusedAttemptingError(Exception):
    """Replay refused: a sender is actively driving the row.

    Round 1 defender fix (finding R1-1). The M-W4-F7 audit closure
    already refused replays of rows in ``attempting`` (a re-queue
    would clobber the sender's in-flight work), but the refusal
    surfaced as a bare ``HTTPException`` with FastAPI's raw
    ``{"detail": "replay_refused_attempting"}`` body instead of the
    canonical ``ErrorEnvelope`` every other admin 4xx carries. The
    SDK's ``raise_for_error_body`` found no ``error`` object there and
    raised ``PhantomEnvelopeError`` rather than the typed conflict an
    operator catches to mean "wait for the sender, then retry".

    ``SqliteUploadStore.replay`` now raises this typed error from the
    same in-write-lock precheck that backs
    :class:`ReplayBodyDiscardedError`, so the state read is
    transactionally coupled to the re-queue UPDATE (a sender claim
    cannot flip the row between check and write). The row is left
    untouched.

    The admin route lets this propagate to
    ``replay_refused_attempting_exception_handler`` (registered on the
    FastAPI app), which converts it into the canonical 409
    ``ErrorEnvelope`` with ``error.code='replay_refused_attempting'``
    so the SDK raises
    :class:`phantom_client.errors.PhantomConflictError`.

    Attributes:
        chain_id: The chain whose replay was refused.
        instance_id: The instance holding the row (read off the row
            itself; the store does not know its own instance id).
    """

    def __init__(self, *, chain_id: UUID, instance_id: str) -> None:
        super().__init__(
            f"replay refused for chain_id={chain_id}: the row is in "
            "'attempting' (a sender is actively driving it); wait for "
            "the attempt to settle or cancel the chain first"
        )
        self.chain_id = chain_id
        self.instance_id = instance_id


class StorageCorruptionError(Exception):
    """The bytes loaded from the body store don't match storage_hash.

    Distinguishable from CodecRoundTripDriftError: this one fires
    before decode. Cause is hardware/filesystem/application-mutation;
    decode never runs.
    """

    def __init__(self, body_ref: str, expected: str, actual: str) -> None:
        super().__init__(
            f"Stored bytes for body_ref={body_ref!r} hash {actual!r}, expected {expected!r}"
        )
        self.body_ref = body_ref
        self.expected = expected
        self.actual = actual


class CodecRoundTripDriftError(Exception):
    """The codec-decoded bytes don't match body_hash.

    Storage verified clean (storage_hash matched), but
    decode(encode(x)) != x. Cause is codec library bug or wrapper
    error.
    """

    def __init__(self, body_ref: str, expected: str, actual: str) -> None:
        super().__init__(
            f"Decoded bytes for body_ref={body_ref!r} hash {actual!r}, expected {expected!r}"
        )
        self.body_ref = body_ref
        self.expected = expected
        self.actual = actual
