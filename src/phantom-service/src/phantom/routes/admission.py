"""Chain-admission orchestrator extracted from ``routes/send.py``.

The HTTP-shaped route handler stays in :mod:`phantom.routes.send`
(parse headers → parse body → dispatch → admit → respond). All
admission logic — saturation gate, auth-header → token cache write,
codec selection, body-hash computation, body-store ``put``, and the
single atomic SQLite transaction that inserts both the upload row
and the idempotency claim — lives here and is testable without
booting FastAPI.

Admission path (plan § 2.3.17).
--------------------------------------------

The earlier flow claimed the idempotency slot FIRST and did
body work LATER, leaving a window where a partially-claimed slot
could become an orphan when the body write failed. The current
flow inverts that: codec + hashes + the chain_id namespace clear +
``body_store.put`` happen BEFORE the SQLite transaction, then a
SINGLE atomic transaction inserts both ``uploads`` and
``idempotency_index`` (or rolls both back). H7 (idempotency cleanup
race) is closed structurally - no half-state can become visible to
concurrent readers. The namespace clear (R11-1) guarantees a reused
chain_id never inherits a prior occupant's body files; see
:func:`_persist_row_and_claim`.

The accepted trade-off (per plan § 2.3.17): on idempotency
collision, the caller wasted CPU on the codec + hash + body-store
write. Duplicate POSTs are rare; the structural H7 closure is
worth the wasted-collision-path work.

The size-aware persist-trigger knob narrows to one shape: in
``hybrid`` mode, when the body exceeds
``settings.storage.persist_trigger.body_size_threshold_bytes``
admission enqueues against the :class:`PersistController` so the
RAM→file migration starts immediately (skipping the retry-linger).
In ``all_ram`` mode the knob has no effect (no disk target); in
``all_disk`` mode every body is on disk already.
"""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType
from uuid import UUID

from phantom.chain.parser import envelope_from_persistence_json
from phantom.hashing import sha256_hex
from phantom.instances.context import InstanceContext
from phantom.instances.snapshot import InstanceSettingsSnapshot
from phantom.models.chain import ChainEnvelope
from phantom.models.errors import ErrorCode
from phantom.models.upload import (
    BodyHash,
    BodyHashes,
    BodyLocation,
    CapturedValues,
    StorageEncoding,
    StorageHash,
    UploadRow,
)
from phantom.routing import ResolvedRoute, host_key_for, resolve_first_step_url, resolve_route
from phantom.storage.interface import InsertClaimOutcome
from phantom.storage.sqlite_store import is_transient_lock_error
from phantom.workers.saturation import (
    AdmissionGranted,
    AdmissionRefusedDiskPressure,
    AdmissionRefusedSaturation,
    SaturationGate,
    SlotReservation,
)


@dataclass(frozen=True)
class AdmissionInputs:
    """Every input the admission flow needs.

    Constructed by the route handler from parsed headers and the
    chain envelope; passed verbatim to :func:`admit_chain`.

    The three grouping/ordering fields (cycle-7 plan section 3, task
    2.2) arrive PRE-PARSED: the route handler converts the raw
    ``X-Phantom-Group-Id`` / ``X-Phantom-Multifile-Id`` /
    ``X-Phantom-Order`` header strings to typed values (rejecting
    malformed values with the 400 ``header_invalid`` envelope), so
    admission only ever sees valid-or-absent:

    * ``group_id``: the query-grouping handle; ``None`` means absent
      and admission defaults the stored column to ``chain_id`` (every
      upload is a group of one).
    * ``multifile_id``: the multi-file association id; ``None`` means
      absent and the stored column stays NULL (standalone upload).
    * ``send_order``: the recorded position within a multi-file set
      (display only, never enforced at delivery); ``None`` means absent
      and the stored column defaults to 0.

    Plan § 2.3.17 (F-P1-B) removed
    ``persist_trigger_override``. The per-upload "persist after N
    attempts" knob is gone (subsumed by ``body_store.mode``); the
    operator-tunable size threshold lives entirely in
    ``settings.storage.persist_trigger.body_size_threshold_bytes``.
    """

    request_id: str
    uid_header: str
    instance_header: str | None
    idempotency_header: str | None
    envelope: ChainEnvelope
    body_refs: dict[str, bytes]
    authorization: str | None
    content_encoding: str | None
    group_id: UUID | None = None
    multifile_id: UUID | None = None
    send_order: int | None = None


@dataclass(frozen=True)
class AdmissionOutcome:
    """Result of a successful admission.

    ``row`` is the freshly constructed (or replayed) :class:`UploadRow`;
    ``status_code`` is ``202`` for a new admission and ``200`` for an
    idempotency-replay hit.
    """

    row: UploadRow
    status_code: int


class ChainAdmissionError(Exception):
    """Admission refused with a specific error code.

    The route handler catches and translates these into the canonical
    :class:`ErrorEnvelope` HTTP response shape. The ``code`` /
    ``message`` / ``instance_id`` map directly onto :class:`ErrorBody`;
    ``details`` and ``headers`` flow through to the HTTP response.
    """

    def __init__(
        self,
        *,
        code: ErrorCode,
        message: str,
        instance_id: str,
        details: dict[str, str | int] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.instance_id = instance_id
        self.details = details
        self.headers = headers


# RFC 7230 §3.2.6 token chars. Header names are case-insensitive but
# must consist entirely of these. Leading/trailing whitespace is never
# permitted (RFC 7230 §3.2 — field-name has no LWS allowance).
_HTTP_TOKEN_CHARS: frozenset[str] = frozenset(
    "!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)


def _validate_step_headers(envelope: ChainEnvelope, *, instance_id: str) -> None:
    """Reject envelopes carrying RFC 7230-illegal header names.

    The X-Phantom-* strip in the executor is a name-prefix check
    that case-insensitively matches ``x-phantom-`` at position zero.
    A producer that sends ``"  X-Phantom-Probe  "`` slips past the strip
    AND past httpx's eventual send (httpx raises a local error and
    the chain stalls in retry). Reject at admission so the producer sees
    the malformed name immediately as a 422.

    Args:
        envelope: The validated chain envelope (Pydantic-parsed).
        instance_id: For error-envelope attribution.

    Raises:
        ChainAdmissionError: code ``envelope_invalid`` when any step
            carries a header name with whitespace, control characters,
            or other non-token bytes.
    """
    for step_idx, step in enumerate(envelope.steps):
        for name in step.headers:
            if not name:
                raise ChainAdmissionError(
                    code="envelope_invalid",
                    message=(
                        f"step[{step_idx}].headers carries an empty "
                        f"header name; HTTP header names must be a "
                        f"non-empty RFC 7230 token."
                    ),
                    instance_id=instance_id,
                )
            for ch in name:
                if ch not in _HTTP_TOKEN_CHARS:
                    raise ChainAdmissionError(
                        code="envelope_invalid",
                        message=(
                            f"step[{step_idx}].headers carries an "
                            f"illegal header name {name!r}: "
                            f"character {ch!r} is not a valid RFC 7230 "
                            f"token character (no whitespace, no "
                            f"control chars)."
                        ),
                        instance_id=instance_id,
                    )


def _resolved_route_or_none(url: str, instance_ctx: InstanceContext) -> ResolvedRoute | None:
    """Resolve ``url``'s route, or ``None`` when no route matches.

    ``resolve_route`` raises ``ValueError`` on a miss. Admission deliberately
    tolerates a miss (a chain whose first step matches no route is still
    admitted; the executor classifies it at send time, F1), so both admission
    consumers need the non-raising form.

    Args:
        url: The absolute URL to resolve, normally the chain's first step.
        instance_ctx: The owning instance, whose ``cfg`` carries the routes.

    Returns:
        The resolved route, or ``None`` on a miss.
    """
    try:
        return resolve_route(url, instance_ctx.cfg)
    except ValueError:
        return None


def _route_name(url: str, instance_ctx: InstanceContext) -> str:
    """Resolve route name; fall back to ``unknown`` on miss.

    The miss is deliberately TOLERATED here rather than refused. Admission
    route-checks only the FIRST step's URL, so refusing on a miss would still
    admit a chain whose later step has no route; the complete check is the
    executor's, which classifies an unmatched host at send time as
    ``RouteUnresolved`` and parks the row in ``stored`` for operator repair
    (F1). The ``"unknown"`` fallback is therefore recorded, not unchecked.
    """
    resolved = _resolved_route_or_none(url, instance_ctx)
    return resolved.route_name if resolved is not None else "unknown"


def _body_hashes_diverge(
    existing: dict[str, BodyHashes],
    incoming: dict[str, BodyHashes],
) -> bool:
    """Return True if two submissions carry different body bytes.

    Compares the raw-byte ``body_hash`` (SHA-256 of the bytes the producer
    sent, codec-independent) per ``body_ref`` name. Used to decide
    replay-vs-conflict on an idempotency-key collision (finding G-1):
    the same key with the same body is a legitimate replay; the same key
    with different bytes is a conflict.

    Divergence is any of: a different set of ref names, or a differing
    ``body_hash`` for a shared name. ``storage_hash`` is deliberately
    NOT compared — it depends on the deployment's codec, and two
    submissions of identical raw bytes are "the same upload" regardless
    of how they happen to be encoded at rest.

    Args:
        existing: ``body_hashes`` of the row the idempotency claim
            already points at.
        incoming: ``body_hashes`` computed for the colliding submission.

    Returns:
        ``True`` when the two bodies differ (→ reject as conflict),
        ``False`` when they are byte-identical (→ replay).
    """
    if existing.keys() != incoming.keys():
        return True
    return any(existing[name].body_hash != incoming[name].body_hash for name in incoming)


def _envelope_destinations(envelope: ChainEnvelope) -> tuple[tuple[str, str], ...]:
    """Return the ordered ``(method, resolved-URL)`` tuples for every step.

    This is the *destination fingerprint* of a chain: where the bytes go
    and how (the HTTP method + the fully-resolved per-step URL, with
    ``default_target`` applied to the first step the same way the executor
    will resolve it). Two submissions whose destination fingerprints match
    deliver to the same place; a divergent fingerprint means the bytes
    would land somewhere else.

    Used by the idempotency divergence check (finding R3-3): an idempotency
    key identifies an OPERATION, and the operation includes its
    destination. Binding only the body bytes (finding G-1) is necessary but
    not sufficient — same key + same body + DIFFERENT destination would
    otherwise silently replay to the wrong place behind a 200.

    Deliberately scoped to ``(method, URL)`` per step, NOT the whole
    envelope: the established body-only dedup contract (E2E
    ``test_aggressor_idempotency_dedup``) replays two submissions that
    differ only in step-body fields (e.g. an embedded ``file_name`` or
    ``phantom_local_uuid``) but share key + body + destination. Folding
    those fields into the fingerprint would break that contract. The
    fingerprint captures exactly the delivery target, which is the part of
    the envelope the idempotency identity must cover.

    Args:
        envelope: The validated chain envelope.

    Returns:
        An ordered tuple of ``(method, resolved-URL)`` pairs, one per step.
    """
    destinations: list[tuple[str, str]] = []
    for step in envelope.steps:
        url = step.url
        if envelope.default_target and "://" not in url:
            url = str(envelope.default_target).rstrip("/") + (
                url if url.startswith("/") else "/" + url
            )
        destinations.append((step.method, url))
    return tuple(destinations)


def _envelopes_diverge(existing: ChainEnvelope, incoming: ChainEnvelope) -> bool:
    """Return True if two submissions deliver to different destinations.

    Compares the destination fingerprints (see
    :func:`_envelope_destinations`). Used alongside
    :func:`_body_hashes_diverge` on an idempotency-key collision: the
    operation an idempotency key names is "deliver THESE bytes to THIS
    destination", so divergence in EITHER the body or the destination is a
    conflict (finding R3-3 — the destination half).

    Args:
        existing: The envelope of the row the idempotency claim points at.
        incoming: The colliding submission's envelope.

    Returns:
        ``True`` when the destination fingerprints differ (→ conflict).
    """
    return _envelope_destinations(existing) != _envelope_destinations(incoming)


@dataclass(frozen=True)
class _EncodedBodies:
    """Typed output of the encode-and-dual-hash admission stage.

    Carries the codec products destined for the body store plus the two
    derived byte quantities the later stages consume. ``stored_size`` is
    the summed encoded size (what the body store will hold);
    ``admit_bytes`` is the quantity the saturation gate accounts and the
    sender later releases (the R3-8 unit-symmetry contract: gate basis
    equals ``UploadRow.body_size_bytes``, the STORED size).
    """

    storage_encoding: StorageEncoding
    stored_body_refs: dict[str, bytes]
    body_hashes_map: dict[str, BodyHashes]
    stored_size: int
    admit_bytes: int


async def _encode_and_hash_bodies(
    instance_ctx: InstanceContext,
    body_refs: dict[str, bytes],
) -> _EncodedBodies:
    """Codec-encode every body_ref and compute its dual hashes (stage 2).

    Always-encode (ADR-014): per body_ref this computes ``body_hash`` (raw
    SHA-256, codec-independent) and ``storage_hash`` (SHA-256 of the encoded
    bytes), and returns the encoded bytes destined for the body store.

    Runs BEFORE the saturation gate (finding R3-8): the gate must account
    the STORED byte size (the bytes actually buffered, which the sender
    later releases as ``UploadRow.body_size_bytes``), so the encoded size
    has to be known before ``admit``. No saturation slot is held while this
    runs, so a codec/OOM failure here simply propagates with nothing to
    release.

    The raw-body hash and the codec-encode both consume the raw bytes and
    are independent, so they ``asyncio.gather`` on the thread pool; the
    storage hash depends on the encoded output and runs serially after.
    For the identity codec, encoded == raw and one hash covers both.

    Args:
        instance_ctx: The instance context (codec factory access).
        body_refs: The raw submitted body bytes per ref name.

    Returns:
        An :class:`_EncodedBodies`. When ``body_refs`` is empty the
        encoding is ``"original"`` with empty maps and zero byte counts
        (an empty submission accounts zero at the gate either way).
    """
    if not body_refs:
        return _EncodedBodies(
            storage_encoding="original",
            stored_body_refs={},
            body_hashes_map={},
            stored_size=0,
            admit_bytes=0,
        )
    codec = instance_ctx.codec_factory()
    storage_encoding: StorageEncoding = codec.algorithm_name
    stored_body_refs: dict[str, bytes] = {}
    body_hashes_map: dict[str, BodyHashes] = {}
    for name, data in body_refs.items():
        if storage_encoding == "original":
            # Identity codec — encoded == raw; one hash covers both.
            body_hash_hex = await asyncio.to_thread(sha256_hex, data)
            encoded = data
            storage_hash_hex = body_hash_hex
        else:
            body_hash_hex, encoded = await asyncio.gather(
                asyncio.to_thread(sha256_hex, data),
                asyncio.to_thread(codec.encode, data),
            )
            storage_hash_hex = await asyncio.to_thread(sha256_hex, encoded)
        stored_body_refs[name] = encoded
        body_hashes_map[name] = BodyHashes(
            body_hash=BodyHash(body_hash_hex),
            storage_hash=StorageHash(storage_hash_hex),
        )
    stored_size = sum(len(b) for b in stored_body_refs.values())
    # The gate accounts (and the sender releases) the stored size when a
    # body is present; an empty submission accounts zero either way.
    return _EncodedBodies(
        storage_encoding=storage_encoding,
        stored_body_refs=stored_body_refs,
        body_hashes_map=body_hashes_map,
        stored_size=stored_size,
        admit_bytes=stored_size,
    )


async def _resolve_existing_idempotent_row(
    instance_ctx: InstanceContext,
    *,
    idempotency_key: str,
    fallback_chain_id: UUID,
) -> UploadRow | None:
    """Resolve the live row an idempotency-key collision points at.

    Tries the row-level ingress-key index first (covers the case where
    the ``idempotency_index`` entry was reaped but the upload row is
    still live), then falls back to the index directly.

    Returns ``None`` when the claim resolves to a ``chain_id`` whose
    ``uploads`` row no longer exists — an ORPHANED index entry. This window
    is real (finding R3-2): the reaper deletes the row
    (``delete_terminal_older_than``) before it cleans the index
    (``cleanup_idempotency_index``) in a later transaction, and admin /
    bulk deletes never touch the index at all. The caller treats ``None``
    as recoverable (the colliding submission becomes the new owner of the
    key). This previously asserted ``existing_row is not None`` and crashed
    admission with a bare ``AssertionError`` → naked HTTP 500 — exactly the
    ADR-017-violating anti-pattern finding D-1 set out to eliminate.

    Note: with the orphan-aware atomic claim in
    :meth:`SqliteUploadStore.insert_with_idempotency_claim`, an orphaned
    claim is replaced inside the admission transaction, so a genuine
    ``IDEMPOTENCY_COLLISION`` only fires for a LIVE claim. This helper's
    ``None`` return is defense in depth for the non-atomic
    ``claim_idempotency`` path and any future caller.

    Args:
        instance_ctx: The instance context (store access).
        idempotency_key: The colliding ingress key.
        fallback_chain_id: The just-rejected submission's chain_id,
            passed to ``claim_idempotency`` to read the surviving claim.

    Returns:
        The existing :class:`UploadRow` the claim resolves to, or ``None``
        when the claim is orphaned (its row was deleted).
    """
    existing_chain_id = await instance_ctx.store.find_by_chain_id_at_ingress(idempotency_key)
    if existing_chain_id is not None:
        existing_row = await instance_ctx.store.get(existing_chain_id)
        if existing_row is not None:
            return existing_row
    # Fallback: read the surviving chain_id straight off the index.
    existing_chain_id = await instance_ctx.store.claim_idempotency(
        idempotency_key, fallback_chain_id
    )
    return await instance_ctx.store.get(existing_chain_id)


class _AdmittedSlot:
    """Structural ownership of one admitted saturation slot (stage 3 product).

    Replaces the hand-threaded ``slot_released`` boolean that previously
    traveled across every branch of ``admit_chain`` (strategy section 4,
    D11). Exactly one of three things happens to an admitted slot, and
    each is a named operation instead of flag bookkeeping at a distance:

    * :meth:`commit` (happy path) - the reservation is CONSUMED by the
      row's creation and ownership transfers to the sender, whose
      terminal transition settles the slot through the gate. Exit then
      returns nothing.
    * :meth:`release_on_rejection` (expected rejection: the collision
      branch) - the slot is released exactly once, immediately. Exit
      then releases nothing, even when the branch goes on to raise.
    * neither (any UNEXPECTED failure: body-store I/O error, SQLite
      write failure, asyncio cancellation, even KeyboardInterrupt) -
      ``__aexit__`` releases. This is the H1 audit closure: without it
      any exception between ``saturation.admit`` and the commit point
      leaked the slot until the gate wedged. The async-with form runs
      on EVERY unwind, including ``asyncio.CancelledError``, which a
      plain ``except Exception`` would miss (the regression mode the
      H1 audit flagged).

    The released flag is set BEFORE the release call is awaited (both in
    :meth:`release_on_rejection` and in ``__aexit__``): the decrements
    inside ``SaturationGate.release`` are synchronous under its lock (no
    await between them), so a cancellation can only land before the lock
    body (slot not yet freed, at worst a single-slot leak, the safe H1
    direction) or after the decrements complete, never a torn
    half-decrement. Flag-first guarantees ZERO double-releases, the hard
    invariant (finding R3-1: a double release would steal accounting
    from a different live in-flight row, the over-release direction of
    the H1 class). The unwind returns the :class:`SlotReservation` the
    gate minted, which carries the admitted quantity itself, so finding
    R3-8's unit-symmetry is STRUCTURAL rather than conventional: there
    is no other basis this scope could return (ADR-036).
    """

    def __init__(self, saturation: SaturationGate, reservation: SlotReservation) -> None:
        """Take ownership of a slot the gate just granted.

        Args:
            saturation: The gate that granted the slot.
            reservation: The charge token the gate minted, carrying the
                byte quantity it admitted (the STORED body size). It is
                consumed by :meth:`commit` or returned by
                :meth:`unwind`.
        """
        self._saturation = saturation
        self._reservation = reservation
        self._committed = False
        self._released = False

    def commit(self) -> None:
        """Happy path: a live row committed, CONSUMING the reservation.

        The sender now owns the slot and settles it through the gate at
        the row's terminal transition.
        """
        self._committed = True

    async def release_on_rejection(self) -> None:
        """Expected-rejection release: free the slot exactly once, now.

        For the collision branch (finding R3-1): no new in-flight row
        committed, so the slot must be freed (H1 contract), and the
        rejection raised afterwards must NOT trigger a second release on
        exit. Idempotent: a second call is a no-op.
        """
        if self._released or self._committed:
            return
        self._released = True
        await self._saturation.unwind(self._reservation)

    async def __aenter__(self) -> _AdmittedSlot:
        """Enter the ownership scope; the slot was admitted by the caller."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Release the slot unless committed or already released; never swallow.

        Returns ``None`` (falsy) so exceptions always propagate; mypy
        relies on the ``None`` return type to know this context manager
        cannot swallow, keeping ``admit_chain``'s
        every-path-returns-or-raises analysis sound.
        """
        if not self._committed and not self._released:
            self._released = True
            await self._saturation.unwind(self._reservation)


async def _admit_saturation_slot(
    instance_ctx: InstanceContext,
    admit_bytes: int,
) -> _AdmittedSlot:
    """Run the saturation gate (stage 3) and return the owned slot.

    Refusals surface as :class:`ChainAdmissionError` with the canonical
    ADR-017 codes (``disk_pressure`` / ``saturation_cap``), each carrying
    ``Retry-After``. On a grant, returns an :class:`_AdmittedSlot` that
    structurally owns the release (see its docstring).

    Args:
        instance_ctx: The instance context (gate access, error attribution).
        admit_bytes: The stored body size to account (R3-8: the gate
            basis equals ``UploadRow.body_size_bytes``).

    Returns:
        The owned slot, to be used as an async context manager.

    Raises:
        ChainAdmissionError: ``disk_pressure`` or ``saturation_cap``.
    """
    result = await instance_ctx.saturation.admit(admit_bytes)
    if isinstance(result, AdmissionRefusedDiskPressure):
        raise ChainAdmissionError(
            code="disk_pressure",
            message="Disk usage at or above max_disk_bytes cap",
            instance_id=instance_ctx.cfg.id,
            headers={"Retry-After": str(_RETRY_AFTER_SECONDS)},
        )
    if isinstance(result, AdmissionRefusedSaturation):
        raise ChainAdmissionError(
            code="saturation_cap",
            message="Saturation cap hit",
            instance_id=instance_ctx.cfg.id,
            headers={"Retry-After": str(_RETRY_AFTER_SECONDS)},
        )
    assert isinstance(result, AdmissionGranted)
    return _AdmittedSlot(instance_ctx.saturation, result.reservation)


@dataclass(frozen=True)
class _PreparedRow:
    """Typed output of the row-preparation stage.

    ``row`` is the fully constructed (not yet persisted) upload row;
    ``ingress_dedup_key`` is the server-side admission dedup key, used at
    BOTH the row's ``chain_id_at_ingress`` column and the
    ``idempotency_index`` claim so the two can never diverge; ``snapshot``
    is the settings snapshot read ONCE during preparation, so the
    persist-trigger stage decides against the same configuration the row
    was built under (a concurrent hot-reload cannot split the two).
    """

    row: UploadRow
    ingress_dedup_key: str
    snapshot: InstanceSettingsSnapshot


async def _build_row(
    inputs: AdmissionInputs,
    instance_ctx: InstanceContext,
    encoded: _EncodedBodies,
) -> _PreparedRow:
    """Build the upload row and its dedup key; cache inbound auth on bearer routes.

    The one side effect is deliberate and stated: when the submission
    carries an ``Authorization`` header AND the first step resolves to a
    route whose ``auth_mode`` is ``phantom_bearer``, it is written to the
    token cache (keyed by the resolved first-step endpoint + uid) before the
    row is constructed.

    The ``auth_mode`` gate is D3 (F11). The four cases are exhaustive over
    ``AuthMode`` plus the no-route miss:

    * ``phantom_bearer``: CACHE. The only mode where Phantom injects a
      bearer at egress, so the only mode where a cached bearer is ever read.
      Raw intake on a bearer route still caches, which is the documented
      pilot behaviour.
    * ``aws_sigv4``: DO NOT CACHE. The inbound ``Authorization`` is an AWS
      SigV4 credential string bound to one request's canonical form. It is
      useless as a bearer, and caching it overwrites a real bearer for that
      slot, flips a ``bad`` slot back to ``fresh`` in the operator's own
      token view, and fires the kickers' wake handlers on every raw PUT. At
      egress this route re-signs from the host-keyed credential store and
      never reads the token cache.
    * ``none``: DO NOT CACHE. Forward-as-is injects nothing at egress, so
      the value can never be read back; writing it can only cause the same
      wake-and-churn side effects with no upside.
    * No route matches: DO NOT CACHE. Admission deliberately admits such a
      chain (F1's premise) and the executor classifies it at send time.
      Caching a bearer for a destination Phantom has no route to cannot help
      delivery, and the write would wake parked rows on that slot for
      nothing.

    The row's ``endpoint`` column is set from the first step's hostname on
    EVERY path, gate or no gate: it records where the row was headed, and it
    is not a cache-key decision.

    Args:
        inputs: The parsed admission inputs.
        instance_ctx: The owning instance (config, token cache, settings).
        encoded: The encode-stage products (hashes, sizes, encoding).

    Returns:
        The :class:`_PreparedRow` for the persist stage.
    """
    first_step_url = resolve_first_step_url(inputs.envelope)
    endpoint = host_key_for(first_step_url)
    resolved = _resolved_route_or_none(first_step_url, instance_ctx)
    if inputs.authorization and resolved is not None and resolved.auth_mode == "phantom_bearer":
        await instance_ctx.token_cache.set(
            endpoint=endpoint,
            uid=inputs.uid_header,
            bearer=inputs.authorization,
            source="inbound_request",
        )

    chain_id = inputs.envelope.chain_id

    # Server-side admission dedup key (§ 2, Item 2). Derived ONCE here and
    # used at BOTH the stored ``chain_id_at_ingress`` column and the
    # ``idempotency_index`` claim (the persist stage) so the two can never
    # diverge: the duplicate-resend fallback ``find_by_chain_id_at_ingress``
    # reads the ROW's ``chain_id_at_ingress`` while the claim is written to
    # ``idempotency_index`` from the key argument, so a header-absent resend
    # would miss its own claim if the two defaulted differently.
    #
    # A non-blank inbound ``X-Phantom-Idempotency-Key`` is kept VERBATIM;
    # ``None``, empty, or whitespace-only is treated as absent and filled
    # with ``str(chain_id)``. Treating blank as absent stops many
    # empty-header submissions from colliding on a shared ``""`` dedup key.
    # The SERVER thus always writes a dedup claim regardless of client
    # behavior (the official SDK already defaults the header to
    # ``str(chain_id)``; this protects raw-HTTP clients that omit it).
    ingress_dedup_key: str = (
        inputs.idempotency_header
        if (inputs.idempotency_header and inputs.idempotency_header.strip())
        else str(chain_id)
    )

    snapshot = instance_ctx.current_settings()
    mode = snapshot.body_store.mode

    # Mode-aware initial body_location.
    #
    # In all_disk mode the FileBodyStore.put fsyncs immediately, so
    # the row is born ``body_location='file'``. In hybrid and all_ram
    # mode the body lives in RAM at the moment of insertion; in hybrid
    # mode the PersistController is the sole writer of the ram→file
    # transition (plan § 0.5 invariant #6 — admission MUST NOT flip
    # this column).
    initial_body_location: BodyLocation = "file" if mode == "all_disk" else "ram"

    # UploadRow construction.
    # Pydantic Field-defaulted args (attempts, last_error, …) are omitted
    # below; without the Pydantic mypy plugin (incompatible with this
    # workspace's mypy 2.0 + Pydantic 2.13) mypy can't see those defaults
    # and flags every omitted field as ``call-arg``. Behavior is correct.
    now = datetime.now(tz=UTC)
    row = UploadRow(  # type: ignore[call-arg]
        chain_id=chain_id,
        instance_id=instance_ctx.cfg.id,
        # Grouping/ordering (cycle-7 task 2.3): the pre-parsed header
        # values, with the settled defaults when absent. group_id falls
        # back to chain_id (every upload is a group of one, and the
        # response echo is therefore always present); multifile_id stays
        # None for a standalone upload (SQL NULL never equals NULL, so
        # standalone rows can never correlate accidentally); send_order
        # defaults to 0 and is recorded for display only, never enforced
        # at delivery.
        group_id=inputs.group_id if inputs.group_id is not None else chain_id,
        multifile_id=inputs.multifile_id,
        send_order=inputs.send_order if inputs.send_order is not None else 0,
        route_name=_route_name(first_step_url, instance_ctx),
        state="queued",
        body_location=initial_body_location,
        next_attempt_at=now,
        received_at=now,
        updated_at=now,
        endpoint=endpoint,
        uid=inputs.uid_header,
        chain_envelope_json=inputs.envelope.model_dump_json(),
        captured_values=CapturedValues(),
        current_step_index=0,
        idempotency_key=inputs.envelope.idempotency_key,
        chain_id_at_ingress=ingress_dedup_key,
        capture_reexecution_active=snapshot.capture_reexecution,
        # The saturation basis (InvariantAuditor invariant #2): this is
        # the quantity the gate admitted (stage 3) and the sender will
        # release on a terminal transition — the STORED size. Zero when
        # the submission carried no body_refs.
        body_size_bytes=encoded.admit_bytes,
        storage_encoding=encoded.storage_encoding,
        body_hashes=encoded.body_hashes_map,
    )
    return _PreparedRow(row=row, ingress_dedup_key=ingress_dedup_key, snapshot=snapshot)


async def _persist_row_and_claim(
    instance_ctx: InstanceContext,
    *,
    row: UploadRow,
    idempotency_key: str,
    stored_body_refs: dict[str, bytes],
) -> InsertClaimOutcome:
    """Persist stage: pre-check, namespace clear + body put, atomic insert.

    The SQLite transaction is unchanged from the monolithic flow: the
    upload INSERT and the idempotency-claim INSERT land in ONE atomic
    transaction inside ``store.insert_with_idempotency_claim`` (closes H7
    structurally). Either both rows commit or neither does.

    Between the live-row pre-check and the put, the chain_id's body
    namespace is cleared (R11-1) so a reused chain_id - legal the
    instant the prior row is removed - never inherits a prior
    occupant's body files. See the inline block for the ordering and
    cost rationale.

    Runs with the saturation slot held; every raise below unwinds into
    the slot's ``__aexit__``, which releases exactly once (H1 contract).

    Args:
        instance_ctx: The owning instance (store, body store).
        row: The prepared upload row.
        idempotency_key: The ingress dedup key (never ``None``; the
            header-absent mint guarantees a value).
        stored_body_refs: Encoded bytes destined for the body store.

    Returns:
        The typed :class:`InsertClaimOutcome` distinguishing a clean
        insert, an idempotency-claim collision (replay-or-conflict, the
        caller resolves), and a chain_id PK collision.

    Raises:
        ChainAdmissionError: ``chain_id_in_use`` on the pre-check hit;
            ``storage_unavailable`` on a body-store write fault or a
            transient cross-process database lock.
    """
    chain_id = row.chain_id

    # chain_id-collision PRE-CHECK — BEFORE the body-store put
    # (finding R7-4b, HIGH data loss).
    #
    # The body store is keyed by ``chain_id``. A duplicate submit of an
    # ALREADY-LIVE chain_id (the canonical at-least-once client retry —
    # the first 202 was lost, or a freeze made it look stalled, catalog
    # D-12) must NEVER touch the original upload's body. If we let the
    # ``body_store.put`` below run for a chain_id that is already live,
    # it clobbers the original's body at the SHARED key, and the
    # CHAIN_ID_COLLISION rejection's ``body_store.delete(chain_id)`` then
    # destroys it outright → the acknowledged original goes ``corrupted``
    # (``body_missing_in_sender``) and is never delivered (invariant #1
    # violated by a normal client retry). This was a side-effect of the
    # D-1 409 cleanup, which assumed the put was for a NEW row's body.
    #
    # So we detect the collision UP FRONT with a cheap PK lookup and
    # reject WITHOUT ever writing the body — there is nothing of the
    # duplicate's to roll back, and the original's body is never touched.
    # The raise unwinds into the slot's ``__aexit__``, which releases the
    # saturation slot EXACTLY once (the H1 leak protection; finding
    # R3-1's single-release invariant is preserved — no double release,
    # because no body was put and the collision arm is not reached).
    #
    # The atomic ``insert_with_idempotency_claim`` below remains the
    # authoritative backstop for the residual tight-race window where two
    # same-chain_id submits both pass this pre-check before either
    # inserts; that path is handled body-safely by the collision resolver
    # (the CHAIN_ID collision arm does not delete the shared body).
    if await instance_ctx.store.get(chain_id) is not None:
        raise ChainAdmissionError(
            code="chain_id_in_use",
            message=(
                f"chain_id {chain_id} is already in use by a live upload; mint a fresh chain_id"
            ),
            instance_id=instance_ctx.cfg.id,
            details={"chain_id": str(chain_id)},
        )

    # R11-1: clear the chain_id namespace, THEN put. The body store is
    # keyed by chain_id, and a reused chain_id's namespace can hold a
    # PRIOR occupant's files: the R10-D1 step-aside leaves a removed
    # row's files when this re-POST lands inside a cleanup loop's
    # window (admin bulk delete, reaper eviction), and a dead chain_id
    # keeps its files with no race at all between a crash inside the
    # stamp-first discard legs (R9-5/R10-1 posture) and the metadata
    # pass, or while an orphan waits out the janitor's two-sweep
    # confirmation (R6-1). FileBodyStore.put is ADDITIVE (per-ref
    # writes into the chain directory; see the BodyStore.put contract
    # in storage/interface.py), so without the clear a stale ref the
    # prior occupant declared and this row omits survives into this
    # row's namespace; the sender's get_all reads the namespace union
    # and fails verification on any file with no body_hashes entry,
    # corrupting the ACCEPTED upload (deterministic in all_disk; in
    # hybrid the moment the body migrates off RAM). The clear closes
    # every variant by construction: a row admitted past this point
    # owns a virgin namespace.
    #
    # Ordering makes the clear legal: the chain_id_in_use pre-check
    # ABOVE raises before this line on the same path, so the clear
    # runs only when no live row held the chain_id at the pre-check
    # instant - on any path a protocol-honoring client can reach (one
    # in-flight submission per chain_id) it can only ever remove a
    # DEAD namespace's leftovers, never a live row's bytes. Two
    # CONCURRENT same-chain_id POSTs can both pass the pre-check; that
    # is the documented protocol-violation forfeit (see
    # _resolve_collision - the losing CHAIN_ID_COLLISION arm still
    # never deletes), and the clear does not enlarge it: RamBodyStore
    # .put already REPLACES the whole chain entry, so hybrid/all_ram
    # admissions had winner-takes-namespace semantics before this
    # change; the clear extends the same semantics to the disk half.
    #
    # Cost: one idempotent BodyStore.delete per admission. The
    # namespace is empty for every fresh chain_id (the overwhelmingly
    # common case): RamBodyStore.delete is a dict pop, FileBodyStore
    # .delete is one exists() check dispatched off-thread, and
    # HybridBodyStore.delete fans to both halves. The perf baseline
    # gate is the empirical check on this hot-path addition.
    #
    # The put stays RAM-first via the mode-selected binding:
    # HybridBodyStore.put writes to its RAM half; FileBodyStore.put
    # fsyncs in all_disk mode; RamBodyStore.put is a dict insert.
    #
    # finding R7-1-A/B / R7-2-A: a storage-layer ``OSError`` (fsync EIO /
    # ENOSPC) from the clear or the put must surface as a registered
    # ADR-017 retryable code, NOT escape the ``/send`` handler as a naked
    # HTTP 500 (the D-1/R3-2 naked-500 class on a storage-write trigger).
    # Map it to ``storage_unavailable`` (503, Retry-After) - a transient
    # SD-card write fault tells the producer "retry later", preserving
    # its buffering rather than tripping the upstream client's 5xx
    # fallback. Durability already holds (R7-1/R7-2 proved the failed
    # put commits no row); this is purely the error surface. The slot is
    # released by the slot's ``__aexit__`` since no row committed.
    try:
        await instance_ctx.body_store.delete(chain_id)
        if stored_body_refs:
            await instance_ctx.body_store.put(chain_id, stored_body_refs)
    except OSError as exc:
        raise ChainAdmissionError(
            code="storage_unavailable",
            message=(
                "Phantom could not durably buffer the upload body: a "
                "storage write failed (the disk may be full or "
                "returning I/O errors); retry shortly"
            ),
            instance_id=instance_ctx.cfg.id,
            details={"reason": "body_store_write_failed"},
            headers={"Retry-After": str(_RETRY_AFTER_SECONDS)},
        ) from exc

    # Upload row + idempotency claim land in ONE atomic SQLite
    # transaction (closes H7 structurally). See
    # store.insert_with_idempotency_claim, which returns a typed
    # InsertClaimOutcome distinguishing a clean insert, an
    # idempotency-claim collision (replay-or-conflict), and a
    # chain_id PK collision (finding D-1 — was a naked 500).
    #
    # § 2 (Item 2): EVERY admission now writes a dedup claim. The key is
    # the ``ingress_dedup_key`` derived once in the preparation stage
    # (verbatim client header, or ``str(chain_id)`` when the header is
    # absent/blank), so it is never ``None`` and the former header-absent
    # plain-``insert`` branch is gone. ``insert_with_idempotency_claim``
    # already handles a chain_id PK collision body-safely inside its own
    # ``IntegrityError`` arm (returning ``CHAIN_ID_COLLISION``), so
    # dropping the plain-insert branch loses no behavior.
    #
    # finding R9-V6-1: a cross-process SQLITE_BUSY that outlasts the
    # store's ``busy_timeout`` (a sibling connection holding the WAL
    # write lock — a stray ``sqlite3 uploads.db`` admin session, a backup
    # tool mid-snapshot, a second instance mis-sharing the data_dir) raises
    # ``sqlite3.OperationalError: database is locked`` from the upload
    # INSERT. That is neither the ``IntegrityError`` (chain_id collision)
    # nor the ``OSError`` (body-store fault) the arms below/above catch, so
    # pre-fix it escaped ``admit_chain`` past the ``/send`` handler's
    # ``except ChainAdmissionError`` → a NAKED HTTP 500 (the D-1/R3-2/R7-1
    # naked-500 class on the external-lock trigger), tripping the upstream
    # client's 5xx fallback instead of preserving the producer's buffered retry. A
    # transient lock is the SAME "retry shortly, nothing lost" posture as a
    # storage write fault, so map it to the SAME ADR-017
    # ``storage_unavailable`` 503 (+ Retry-After) the body-store ``OSError``
    # arm uses. The store's rollback-on-any-exception (``_write_txn`` /
    # ``insert_with_idempotency_claim``'s ``except BaseException``) has
    # already cleared the open transaction, and no row committed, so
    # durability holds (R9-V6-3 confirms the data layer never corrupts under
    # the lock); the saturation slot is freed by the slot's ``__aexit__``.
    # A NON-lock ``OperationalError`` (a genuine schema/type fault —
    # ``is_transient_lock_error`` returns False) is re-raised so it
    # surfaces as ``internal_error`` rather than being masked behind a
    # misleading retryable 503.
    try:
        return await instance_ctx.store.insert_with_idempotency_claim(row, idempotency_key)
    except sqlite3.OperationalError as exc:
        if not is_transient_lock_error(exc):
            raise
        raise ChainAdmissionError(
            code="storage_unavailable",
            message=(
                "Phantom could not durably buffer the upload: the storage "
                "database is temporarily locked by another process; retry "
                "shortly"
            ),
            instance_id=instance_ctx.cfg.id,
            details={"reason": "database_locked"},
            headers={"Retry-After": str(_RETRY_AFTER_SECONDS)},
        ) from exc


async def _resolve_collision(
    inputs: AdmissionInputs,
    instance_ctx: InstanceContext,
    *,
    outcome: InsertClaimOutcome,
    prepared: _PreparedRow,
    encoded: _EncodedBodies,
    slot: _AdmittedSlot,
) -> AdmissionOutcome:
    """Resolve a non-INSERTED persist outcome: clean up, then replay or reject.

    Collision — roll back the body-store put + the saturation grant. No
    new in-flight row committed, so the slot must be freed (H1 contract).
    This is an EXPECTED rejection, not a failure: the slot is released
    exactly once via :meth:`_AdmittedSlot.release_on_rejection`, and the
    slot's ``__aexit__`` does NOT release a second time when a rejection
    below raises (finding R3-1 — the over-release direction of the H1
    slot-accounting class).

    Args:
        inputs: The admission inputs (envelope access for divergence).
        instance_ctx: The owning instance.
        outcome: The non-INSERTED claim outcome being resolved.
        prepared: The prepared row + dedup key that collided.
        encoded: The encode-stage products (body hashes, stored refs).
        slot: The held saturation slot (released here, exactly once).

    Returns:
        ``AdmissionOutcome(existing_row, 200)`` for a genuine replay.

    Raises:
        ChainAdmissionError: ``chain_id_in_use`` (409) on a PK collision;
            ``idempotency_key_conflict`` (422) on an orphaned claim or a
            body/destination divergence.
    """
    chain_id = prepared.row.chain_id
    idempotency_key = prepared.ingress_dedup_key

    # Body-store rollback is keyed on the COLLISION KIND (finding
    # R7-4b). The body store is keyed by ``chain_id``:
    #
    # * IDEMPOTENCY_COLLISION — a DIFFERENT chain_id reused the same
    #   idempotency KEY, so this submission's body sits at its own,
    #   non-colliding ``chain_id`` key. Delete it (otherwise every
    #   duplicate POST orphans bytes — finding G-1's cleanup).
    #
    # * CHAIN_ID_COLLISION — this would only fire in the residual
    #   tight race where a concurrent submit of the SAME chain_id won
    #   the PK after both passed the persist stage's pre-check. The body
    #   at that shared key belongs to the WINNING (live) row; deleting it
    #   would destroy a live upload's body — exactly the R7-4b data
    #   loss. So we DO NOT delete on a chain_id collision. (Under the
    #   normal single-retry path the pre-check already rejected
    #   before any put, so no put of ours exists here anyway.)
    if encoded.stored_body_refs and outcome is InsertClaimOutcome.IDEMPOTENCY_COLLISION:
        await instance_ctx.body_store.delete(chain_id)
    await slot.release_on_rejection()

    if outcome is InsertClaimOutcome.CHAIN_ID_COLLISION:
        # D-1: the envelope reused a chain_id (the row PK) that is
        # already live. Deterministic 409 with the ADR-017
        # envelope rather than a naked 500.
        raise ChainAdmissionError(
            code="chain_id_in_use",
            message=(
                f"chain_id {chain_id} is already in use by a live upload; mint a fresh chain_id"
            ),
            instance_id=instance_ctx.cfg.id,
            details={"chain_id": str(chain_id)},
        )

    # IDEMPOTENCY_COLLISION — resolve the existing row, then decide
    # replay-vs-conflict (G-1 body, R3-3 destination). ``idempotency_key``
    # is the ``ingress_dedup_key`` (§ 2): always set, typed ``str`` (no
    # None-guard needed since the header-absent mint guarantees a value).
    existing_row = await _resolve_existing_idempotent_row(
        instance_ctx, idempotency_key=idempotency_key, fallback_chain_id=chain_id
    )
    if existing_row is None:
        # Finding R3-2: the claim resolved to a row that no longer
        # exists — an ORPHANED index entry. The atomic insert
        # (insert_with_idempotency_claim) replaces an orphaned claim
        # in-transaction, so a genuine IDEMPOTENCY_COLLISION almost
        # never reaches here with an orphan; this branch is defense
        # in depth for the non-atomic claim_idempotency path. Surface
        # a typed, retryable conflict — NEVER a bare AssertionError
        # → naked 500 (the D-1 anti-pattern this helper had
        # reintroduced). The producer retries; the orphaned claim is gone
        # by then (the atomic insert cleaned it), so the retry is
        # admitted fresh.
        raise ChainAdmissionError(
            code="idempotency_key_conflict",
            message=(
                "X-Phantom-Idempotency-Key claim pointed at a "
                "reaped row; the stale claim was cleared — retry to "
                "be admitted under this key"
            ),
            instance_id=instance_ctx.cfg.id,
            details={"idempotency_key": idempotency_key},
        )
    if _body_hashes_diverge(existing_row.body_hashes, encoded.body_hashes_map):
        # G-1: same idempotency key, DIFFERENT body. Replaying the
        # first chain would silently drop this body behind a
        # success-shaped 200. Reject so the producer learns its
        # key-reuse dropped data. An idempotency key MUST be a
        # function of the body (see ADR-017 + operator-playbook).
        raise ChainAdmissionError(
            code="idempotency_key_conflict",
            message=(
                "X-Phantom-Idempotency-Key reused with a different "
                "body than the original submission; an idempotency "
                "key must be a function of the body"
            ),
            instance_id=instance_ctx.cfg.id,
            details={"idempotency_key": idempotency_key},
        )
    if _envelopes_diverge(
        envelope_from_persistence_json(existing_row.chain_envelope_json),
        inputs.envelope,
    ):
        # R3-3: same idempotency key, same body, but a DIFFERENT
        # destination (the resolved step (method, URL) tuples
        # diverge). Replaying the first chain would deliver these
        # bytes to the WRONG place behind a success-shaped 200. The
        # operation an idempotency key names is "deliver THESE bytes
        # to THIS destination"; a divergent destination is a
        # conflict, parallel to a divergent body. An idempotency key
        # must be a function of the body AND the destination (see
        # ADR-017 + operator-playbook).
        raise ChainAdmissionError(
            code="idempotency_key_conflict",
            message=(
                "X-Phantom-Idempotency-Key reused with the same body "
                "but a different destination than the original "
                "submission; an idempotency key must be a function of "
                "the body and the destination"
            ),
            instance_id=instance_ctx.cfg.id,
            details={"idempotency_key": idempotency_key},
        )
    # Same key + same body + same destination — a genuine replay.
    # Return the existing row with status 200 (ADR-017
    # idempotency_replay posture).
    return AdmissionOutcome(row=existing_row, status_code=200)


async def _maybe_enqueue_immediate_persist(
    instance_ctx: InstanceContext,
    *,
    chain_id: UUID,
    stored_size: int,
    snapshot: InstanceSettingsSnapshot,
) -> None:
    """Hybrid-mode size-threshold immediate persist (plan § 2.3.17).

    In hybrid mode only: large bodies skip retry-linger and enqueue
    immediately so the PersistController migrates them to disk. The
    threshold knob has no effect in all_ram (no disk target) or
    all_disk (already on disk).

    Args:
        instance_ctx: The owning instance (controller access).
        chain_id: The freshly inserted row's id.
        stored_size: The encoded body size that was buffered.
        snapshot: The settings snapshot captured at row preparation
            (the same configuration the row was admitted under).
    """
    threshold = snapshot.persist_trigger.body_size_threshold_bytes
    if (
        snapshot.body_store.mode == "hybrid"
        and threshold is not None
        and threshold > 0
        and stored_size >= threshold
        and instance_ctx.persist_controller is not None
    ):
        # Fire-and-forget enqueue; the controller migrates the body
        # asynchronously and flips body_location='file' on completion.
        await instance_ctx.persist_controller.enqueue(chain_id)


async def admit_chain(
    inputs: AdmissionInputs,
    instance_ctx: InstanceContext,
) -> AdmissionOutcome:
    """Run the staged validate-through-atomic-insert admission pipeline.

    The stages, in execution order (each a typed module function; the
    encode stage runs BEFORE the gate because of finding R3-8, see the
    stage docstrings for the per-stage rationale):

    1. Parse/validate: header-name well-formedness (RFC 7230 §3.2),
       :func:`_validate_step_headers`.
    2. Encode + dual-hash (always-encode, ADR-014):
       :func:`_encode_and_hash_bodies`. Runs before the gate so the gate
       can account the STORED size (finding R3-8); no slot is held here.
    3. Saturation admit: :func:`_admit_saturation_slot`, refusing with
       ``saturation_cap`` / ``disk_pressure``, granting an
       :class:`_AdmittedSlot` that structurally owns the release.
    4. Row preparation: :func:`_build_row` (auth header to token cache,
       ingress dedup key, mode-aware body_location, row construction).
    5. Persist: :func:`_persist_row_and_claim` (chain_id pre-check,
       chain_id namespace clear (R11-1) + body-store put, then the
       upload INSERT and idempotency-claim INSERT in ONE atomic SQLite
       transaction, unchanged; closes H7).
       A non-INSERTED outcome routes through :func:`_resolve_collision`
       (replay with status 200, or a typed rejection).
    6. Respond: hybrid-mode size-threshold immediate-persist enqueue
       (:func:`_maybe_enqueue_immediate_persist`), then commit the slot
       (the sender owns the release once the row is live) and return
       ``AdmissionOutcome(row, status_code=202)``.

    Slot ownership (H1 audit closure, finding R3-1): the
    :class:`_AdmittedSlot` context manager releases the slot on ANY
    unwind (including cancellation) unless the happy path committed it
    or the collision resolver already released it. See its docstring for
    the single-release invariant.

    Raises:
        ChainAdmissionError: On any admission refusal or rejection
            (validation, saturation, storage fault, collision conflict).
    """
    # Stage 1: header-name well-formedness (RFC 7230).
    #
    # Reject envelopes with malformed step-header names at admission
    # time. RFC 7230 §3.2 defines header names as ``token`` — visible
    # ASCII excluding separator characters; leading/trailing whitespace
    # is forbidden. A producer that sends ``"  X-Phantom-Probe  "`` slips
    # past the X-Phantom-* prefix strip (which checks
    # ``name.lower().startswith("x-phantom-")``), and httpx then
    # refuses to send the request. With the default ``max_attempts=-1``
    # this stalls the chain in retry forever. Surfacing the error at
    # admission gives the producer a clean 422 immediately.
    _validate_step_headers(inputs.envelope, instance_id=instance_ctx.cfg.id)

    # Stage 2: codec + body hashes (always-encode), BEFORE the saturation
    # gate.
    #
    # Finding R3-8: the saturation gate must account the STORED (buffered)
    # byte size, not the raw declared size. The sender releases
    # ``UploadRow.body_size_bytes`` (the stored size) on every terminal
    # transition (sender.py), and the auth-kicker re-admits at
    # ``body_size_bytes`` too — so admission MUST admit the same quantity
    # or the byte counter (and the large-body class counter) drift on every
    # compressed upload until the cap wedges. InvariantAuditor invariant #2
    # ("saturation-bytes basis equals body_size_bytes") names this contract.
    # We therefore encode first to learn the stored size, then admit
    # that. No saturation slot is held during encoding, so a codec/OOM
    # failure here simply propagates with nothing to release.
    encoded = await _encode_and_hash_bodies(instance_ctx, inputs.body_refs)

    # Stage 3: saturation gate. From here to the commit point the slot is
    # structurally owned by the context manager (H1 / R3-1; see
    # _AdmittedSlot).
    slot = await _admit_saturation_slot(instance_ctx, encoded.admit_bytes)
    async with slot:
        # Stage 4: row preparation (auth cache write inside).
        prepared = await _build_row(inputs, instance_ctx, encoded)

        # Stage 5: persist (pre-check, body put, atomic insert + claim).
        outcome = await _persist_row_and_claim(
            instance_ctx,
            row=prepared.row,
            idempotency_key=prepared.ingress_dedup_key,
            stored_body_refs=encoded.stored_body_refs,
        )
        if outcome is not InsertClaimOutcome.INSERTED:
            return await _resolve_collision(
                inputs,
                instance_ctx,
                outcome=outcome,
                prepared=prepared,
                encoded=encoded,
                slot=slot,
            )

        # Stage 6: respond. The persist-trigger enqueue runs before the
        # slot commit, exactly as the monolithic flow ordered it: an
        # enqueue failure unwinds into the slot's release (no committed
        # slot is left behind by a failed respond stage).
        await _maybe_enqueue_immediate_persist(
            instance_ctx,
            chain_id=prepared.row.chain_id,
            stored_size=encoded.stored_size,
            snapshot=prepared.snapshot,
        )

        # Happy path: a new in-flight row landed. The saturation slot
        # stays held; the sender releases it when the row reaches a
        # terminal state.
        slot.commit()
        return AdmissionOutcome(row=prepared.row, status_code=202)


# Retry-After value (seconds) attached to every 503 response per
# RFC 7231 §7.1.3. Defined here so admission failures can attach
# the header without importing the route module.
_RETRY_AFTER_SECONDS = 60
