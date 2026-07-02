"""Sender — the load-bearing worker loop (plan §4.26).

Per worker:

1. Poll the persistent SQLite store via ``claim_due``.
2. For each claimed row: load body_refs via the mode-selected
   :class:`BodyStore` (HybridBodyStore in hybrid mode; RamBodyStore in
   all_ram; FileBodyStore in all_disk — the body store itself routes
   the read), decode storage encoding, call
   ``executor.execute_one_step``, classify result.
3. Persist the attempt result via ``record_attempt_result``.
4. On retryable failure with a remaining retry budget, call
   ``retry_strategy.schedule_next_attempt`` and re-queue. When the row
   has been in RAM longer than ``body_store.linger_seconds`` and the
   PersistController is wired, enqueue the chain for RAM→disk migration
   (plan § 2.3.11 retry-linger trigger; § 2.3.18 sender wiring).

Plan § 2.3.18 collapsed the dual-store
round-robin to a single-store loop; body reads route through
:attr:`InstanceContext.body_store`; the retry-linger trigger calls
``persist_controller.enqueue(chain_id)`` rather than the deleted
``persist_now`` function.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import UTC, datetime

from phantom.chain.executor import (
    CaptureExpiredRewind,
    CaptureExpiredStored,
    CaptureIncomplete,
    Failed4xx,
    Failed5xx,
    FailedAuth,
    FailedNetwork,
    SendDeadlineExpired,
    Succeeded,
    TemplateUnresolved,
)
from phantom.compression import build_codec_for_algorithm
from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadRow
from phantom.observability.metrics import MetricsRegistry
from phantom.storage.errors import (
    BodyMissingError,
    CodecRoundTripDriftError,
    StorageCorruptionError,
)
from phantom.storage.interface import UploadStore
from phantom.workers._expire import expire_row

logger = logging.getLogger(__name__)

# Rows each worker claims per poll. One at a time on purpose: claim_due
# atomically flips claimed rows to ``attempting``, and a worker drives one
# row at a time, so claiming more would park the extras in ``attempting``
# while they wait behind the first. Concurrency comes from the worker
# pool (``worker_count``), not from the claim batch.
CLAIM_BATCH_SIZE: int = 1


class Sender:
    """Pool of worker coroutines that drive chains forward."""

    def __init__(
        self,
        *,
        instance: InstanceContext,
        worker_count: int,
        poll_interval_ms: int,
        metrics_registry: MetricsRegistry | None = None,
    ) -> None:
        """Construct the sender.

        Args:
            instance: The instance whose stores/executor/strategies to drive.
            worker_count: Number of worker coroutines.
            poll_interval_ms: Sleep duration between empty polls.
            metrics_registry: Optional :class:`MetricsRegistry` for
                emit-site wiring (plan § 4.2.2). When ``None`` a
                throwaway registry is constructed so emission is a
                no-op.
        """
        self._instance = instance
        self._worker_count = worker_count
        self._poll_seconds = poll_interval_ms / 1000.0
        # Metrics surface (plan § 4.2.2):
        # * record_attempt_result_no_op_total — bumped when a state
        #   transition UPDATE finds rowcount=0 (the M-W4-F7 race-aware
        #   path landed in Phase 2).
        # * body_missing_total — bumped when the body store has no
        #   bytes for a row that declared body_hashes (H8 corrupted
        #   route landed in Phase 2).
        self._metrics = metrics_registry if metrics_registry is not None else MetricsRegistry()
        self._no_op_total = self._metrics.register_counter(
            "record_attempt_result_no_op_total",
            "record_attempt_result UPDATEs that affected zero rows (M-W4-F7).",
        )
        self._body_missing_total = self._metrics.register_counter(
            "body_missing_total",
            "BodyMissingError events caught in the sender (H8).",
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        """Spawn the worker coroutines and wait for ``stop_event``."""
        async with asyncio.TaskGroup() as tg:
            for i in range(self._worker_count):
                tg.create_task(self._worker_loop(i, stop_event))

    async def _worker_loop(self, idx: int, stop_event: asyncio.Event) -> None:
        """One worker's polling loop.

        Single-store poll (plan § 2.3.18): one :class:`UploadStore`
        reference holds every row regardless of ``body_location``.
        """
        store: UploadStore = self._instance.store
        while not stop_event.is_set():
            now = datetime.now(tz=UTC)
            try:
                claimed = await store.claim_due(now, limit=CLAIM_BATCH_SIZE)
            except Exception:
                logger.exception("claim_due failed in worker %d", idx)
                await asyncio.sleep(self._poll_seconds)
                continue
            if not claimed:
                await asyncio.sleep(self._poll_seconds)
                continue
            row = claimed[0]
            try:
                await self._drive_one(store, row)
            except Exception:
                logger.exception("Worker %d failed to drive row %s", idx, row.chain_id)

    async def _load_body_refs(self, row: UploadRow) -> dict[str, bytes]:
        """Load and verify body_refs for ``row``.

        Per body_ref:

        1. Read stored bytes from the body store.
        2. Compute SHA-256; compare to row.body_hashes[name].storage_hash.
           Mismatch raises :class:`StorageCorruptionError` (no retry;
           corrupted state).
        3. Decode via the codec named in row.storage_encoding.
        4. Compute SHA-256 of decoded bytes; compare to
           row.body_hashes[name].body_hash. Mismatch raises
           :class:`CodecRoundTripDriftError` (no retry; corrupted state).
        5. Return decoded bytes.

        The body store (plan § 2.3.18) is the
        mode-selected :class:`BodyStore` binding on
        :class:`InstanceContext`. In hybrid mode the
        :class:`HybridBodyStore.get_all` routes RAM-first with disk
        fallback (so a body migrated mid-attempt still resolves). In
        all_ram / all_disk only one half exists.
        """
        # A row with no declared body_hashes (e.g., a metadata-only POST
        # with no body_refs at admission) skips the body store entirely
        # — return an empty dict immediately. The KeyError-to-corrupted
        # path only applies when the row claims body files exist but
        # the body store has none.
        if not row.body_hashes:
            return {}
        try:
            raw_refs = await self._instance.body_store.get_all(row.chain_id)
        except KeyError as exc:
            # H8 audit closure — sender no longer silently routes a
            # missing body as an empty payload. Per ADR-014's runtime
            # missing-body contract, an absent body store entry for a
            # row with declared body_hashes is storage corruption, not
            # a happy-path "empty body" case. Routing the row to
            # ``corrupted`` matches the storage_hash mismatch path
            # (StorageCorruptionError) — same _on_corrupted handler,
            # same terminal transition, same last_error formatting.
            #
            # Pre-Phase-2 the empty-dict return looked identical to a
            # legitimate bodyless chain (e.g., a metadata-only POST),
            # so the sender forwarded zero bytes upstream. That was an
            # effective data loss disguised as a successful upload.
            missing = list(row.body_hashes.keys())
            raise BodyMissingError(row.chain_id, missing) from exc
        codec = build_codec_for_algorithm(row.storage_encoding)
        decoded: dict[str, bytes] = {}
        for name, stored_bytes in raw_refs.items():
            hashes = row.body_hashes.get(name)
            if hashes is None:
                # Row without hashes — cannot verify. Treat as corruption
                # so the row terminates rather than silently propagating
                # un-verified bytes upstream.
                raise StorageCorruptionError(name, "<expected-hash-missing>", "<no-row-hash>")
            actual_storage = hashlib.sha256(stored_bytes).hexdigest()
            if actual_storage != hashes.storage_hash:
                raise StorageCorruptionError(name, hashes.storage_hash, actual_storage)
            decoded_bytes = await asyncio.to_thread(codec.decode, stored_bytes)
            actual_body = hashlib.sha256(decoded_bytes).hexdigest()
            if actual_body != hashes.body_hash:
                raise CodecRoundTripDriftError(name, hashes.body_hash, actual_body)
            decoded[name] = decoded_bytes
        return decoded

    async def _drive_one(self, store: UploadStore, row: UploadRow) -> None:
        """Drive one claimed row through one step."""
        try:
            body_refs = await self._load_body_refs(row)
        except StorageCorruptionError as exc:
            await self._on_corrupted(store, row, error_code="storage_corruption", detail=str(exc))
            return
        except CodecRoundTripDriftError as exc:
            await self._on_corrupted(
                store, row, error_code="codec_round_trip_drift", detail=str(exc)
            )
            return
        except BodyMissingError as exc:
            # H8 audit closure (Phase 2 § 3.2.6). Per ADR-014: missing
            # bodies route through the same _on_corrupted path as
            # storage_hash mismatches. The `body_missing_in_sender`
            # detail token lets operator logs distinguish this from a
            # hash mismatch when inspecting `last_error`.
            await self._body_missing_total.inc()
            await self._on_corrupted(
                store,
                row,
                error_code="storage_corruption",
                detail=f"body_missing_in_sender:{exc.missing}",
            )
            return
        result = await self._instance.executor.execute_one_step(row, body_refs)

        if isinstance(result, Succeeded):
            await self._on_succeeded(store, row, result)
            return
        if isinstance(result, CaptureExpiredRewind):
            await self._on_rewind(store, row, result)
            return
        if isinstance(result, CaptureExpiredStored):
            await self._on_stored(store, row, last_error=f"capture_expired:{result.producing_step}")
            return
        if isinstance(result, TemplateUnresolved):
            await self._on_terminal_failure(
                store, row, last_error=f"template_unresolved:{result.placeholder}"
            )
            return
        if isinstance(result, FailedAuth):
            await self._on_auth_failure(store, row, result)
            return
        if isinstance(result, Failed4xx):
            await self._on_terminal_failure(store, row, last_error=f"4xx_status_{result.status}")
            return
        if isinstance(result, SendDeadlineExpired):
            await self._on_send_deadline_expired(store, row, result)
            return
        if isinstance(result, (Failed5xx, FailedNetwork, CaptureIncomplete)):
            # CaptureIncomplete (finding R7-5-B): a 2xx whose body was missing a
            # required downstream capture is RETRYABLE — the same step re-runs
            # (does NOT advance), so a complete body on a later attempt produces
            # the capture. Shares the retry/exhaust→stored path with 5xx/network.
            await self._on_retryable_failure(store, row, result)
            return
        # Exhaustiveness tail (ADR-032): this dispatch is a fall-through
        # ``isinstance`` chain with NO static ``assert_never`` (the executor's
        # ``ExecuteStepResult`` union is not statically checked at THIS site —
        # only its ``auth_mode`` dispatch is). A result member added to the union
        # without an arm above would otherwise fall through and return ``None``
        # silently, leaving the row wedged in ``attempting`` still holding a
        # saturation slot (executor.py "invariant #1 bent"). Crash loudly instead
        # so a forgotten handler is a visible failure, not a stuck row.
        raise AssertionError(f"unhandled ExecuteStepResult: {type(result).__name__}")

    async def _on_succeeded(self, store: UploadStore, row: UploadRow, result: Succeeded) -> None:
        """Persist a successful-step result.

        Each ``record_attempt_result`` call passes the next-state literal
        directly (``"succeeded"`` or ``"queued"``) so the documentation
        test in ``tests/unit/test_transition_table.py`` can scan the
        sender's AST for state-write call sites without chasing local
        variable indirection.
        """
        attempts = row.attempts + 1
        if result.chain_done:
            rowcount = await store.record_attempt_result(
                row.chain_id,
                new_state="succeeded",
                attempts=attempts,
                next_attempt_at=None,
                last_error=None,
                upstream_status=result.upstream_status,
                upstream_headers_json=json.dumps(result.upstream_headers),
                captured_values=result.captured,
                current_step_index=result.next_step_index,
                last_step_completed=result.step_name,
                # The ONLY stamp_sent_at=True call site (cycle-7 task
                # 2.5): sent_at permanently records the moment of first
                # confirmed upstream delivery. The store's IS NULL guard
                # makes it write-once, surviving operator replay.
                stamp_sent_at=True,
            )
            if rowcount == 0:
                # M-W4-F7: admin cancel/replay took the row between
                # claim_due and now. Do not release saturation or
                # delete the body — those side-effects are tied to a
                # successful state transition.
                await self._no_op_total.inc()
                logger.info(
                    "record_attempt_result no-op: chain_id=%s — "
                    "admin cancel/replay took the row from attempting",
                    row.chain_id,
                )
                return
            await self._instance.saturation.release(row.body_size_bytes)
            # H4 audit closure — body-retention contract reconciliation.
            #
            # Sender deletes the body immediately ONLY when
            # ``succeeded_body_seconds == 0`` (the default — bodies are
            # ephemeral past success). When the operator configures a
            # non-zero ``succeeded_body_seconds`` retention window,
            # admin GET /chains/{id}/body must still surface the body
            # for that window — so the reaper owns deletion at the
            # configured time, not the sender.
            #
            # Pre-Phase-2 behavior: sender always deleted on success.
            # That contradicted non-zero retention and broke five E2E
            # tests that configure retention windows for admin
            # inspection (F-Slice1F-A). The reaper already has the
            # right machinery via ``list_terminal_older_than`` +
            # ``body_store.delete`` (workers/reaper.py:127-141); the
            # sender now just steps out of its way.
            retention = self._instance.current_settings().retention
            if retention.succeeded_body_seconds == 0:
                # The IMMEDIATE leg of the one body-discard operation
                # (cycle-7 task 4.7, D9/D10): drop the bytes right away
                # per the default contract (req §5b). This branch fires
                # ONLY when succeeded_body_seconds == 0; any non-zero
                # window leaves body and row untouched for the reaper's
                # SCHEDULED leg.
                #
                # R10-1 confirm-then-act: stamp FIRST via the guarded
                # discard. The uploads write lock was released after
                # the succeeded commit above, so an admin replay can
                # legally re-queue the row before this point; the guard
                # then mismatches (flipped is False) and this leg
                # touches NOTHING - the live replayed row keeps its
                # bodies, and whoever revived the row owns it. Body
                # files are deleted only after a confirmed flip, so a
                # crash in between leaves a stamped row whose files the
                # metadata-retention pass and the orphan janitor
                # converge (bounded, self-healing) - the same crash
                # posture as the reaper's R9-5 leg. Pre-R10-1 this leg
                # ran files-first on an ownership argument that
                # excluded a crash but not a replay.
                #
                # NO saturation release on the flip: "succeeded" is not
                # in SLOT_HOLDING_STATES - the slot was already released
                # at the terminal transition above, unlike the reaper's
                # "stored" leg where the discard itself is the release
                # point. Releasing here would double-free.
                outcome = await store.discard_body_and_zero_accounting(
                    row.chain_id, expected_state="succeeded"
                )
                if outcome.flipped:
                    # RAM freed promptly so actual memory tracks the
                    # gate's already-released accounting (a fresh
                    # ingress burst admitted against freed slots must
                    # not land on RAM still occupied by delivered
                    # bodies). :class:`BodyStore.delete` is idempotent
                    # on both halves of :class:`HybridBodyStore`, so
                    # the call does not branch on body_location
                    # (disk-resident bodies are deleted alongside their
                    # RAM half; no-op when absent).
                    await self._instance.body_store.delete(row.chain_id)
                else:
                    logger.info(
                        "immediate-discard no-op: chain_id=%s - row left "
                        "'succeeded' before the guarded stamp (admin "
                        "replay or removal); bodies preserved for the "
                        "new owner",
                        row.chain_id,
                    )
        else:
            rowcount = await store.record_attempt_result(
                row.chain_id,
                new_state="queued",
                attempts=0,
                next_attempt_at=datetime.now(tz=UTC),  # immediately re-queue
                last_error=None,
                upstream_status=result.upstream_status,
                upstream_headers_json=json.dumps(result.upstream_headers),
                captured_values=result.captured,
                current_step_index=result.next_step_index,
                last_step_completed=result.step_name,
            )
            if rowcount == 0:
                await self._no_op_total.inc()
                logger.info(
                    "record_attempt_result no-op: chain_id=%s — "
                    "admin cancel/replay took the row from attempting (mid-chain step)",
                    row.chain_id,
                )

    async def _on_corrupted(
        self,
        store: UploadStore,
        row: UploadRow,
        *,
        error_code: str,
        detail: str,
    ) -> None:
        """Transition row to ``corrupted`` (terminal; no retry).

        Body verification failed — either the stored bytes don't match
        the recorded ``storage_hash`` (hardware / filesystem mutation)
        or the codec round-trip drifted from the recorded ``body_hash``
        (codec library bug). The row never advances; saturation is
        released so the slot is reclaimed.
        """
        rowcount = await store.record_attempt_result(
            row.chain_id,
            new_state="corrupted",
            attempts=row.attempts,
            next_attempt_at=None,
            last_error=f"{error_code}:{detail}",
            upstream_status=None,
            upstream_headers_json=None,
            captured_values=None,
            current_step_index=None,
            last_step_completed=None,
        )
        if rowcount == 0:
            await self._no_op_total.inc()
            logger.info(
                "_on_corrupted no-op: chain_id=%s — admin cancel/replay "
                "took the row from attempting",
                row.chain_id,
            )
            return
        await self._instance.saturation.release(row.body_size_bytes)

    async def _on_rewind(
        self, store: UploadStore, row: UploadRow, result: CaptureExpiredRewind
    ) -> None:
        """ADR-011 reexecute=True — rewind ``current_step_index`` and re-queue."""
        rowcount = await store.record_attempt_result(
            row.chain_id,
            new_state="queued",
            attempts=0,
            next_attempt_at=datetime.now(tz=UTC),
            last_error=f"rewind:{result.producing_step}",
            upstream_status=None,
            upstream_headers_json=None,
            captured_values=None,
            current_step_index=result.rewind_to_step_index,
            last_step_completed=None,
        )
        if rowcount == 0:
            await self._no_op_total.inc()
            logger.info(
                "_on_rewind no-op: chain_id=%s — admin cancel/replay took the row from attempting",
                row.chain_id,
            )

    async def _on_stored(self, store: UploadStore, row: UploadRow, *, last_error: str) -> None:
        """Transition row to ``stored`` (recoverable via export.tar).

        Attempts are NOT incremented on this path: a capture-expired
        chain never reached the upstream, so the attempt did not burn
        retry budget.
        """
        await self._record_stored(
            store,
            row,
            attempts=row.attempts,
            last_error=last_error,
            upstream_status=None,
            no_op_context="_on_stored",
        )

    async def _record_stored(
        self,
        store: UploadStore,
        row: UploadRow,
        *,
        attempts: int,
        last_error: str,
        upstream_status: int | None,
        no_op_context: str,
    ) -> None:
        """Perform the stored transition (cycle-7 task 2.6, finding D7).

        The SINGLE writer of ``new_state="stored"``: both paths that
        park a row in ``stored`` (capture expired via ``_on_stored``,
        retry budget exhausted via ``_on_retryable_failure``) run
        through this helper, so the literal has exactly one call site
        (one-writer-per-effect; pinned by
        ``tests/unit/test_stored_single_writer.py``).

        Saturation is deliberately NOT released for stored: the body
        still occupies space until export or replay resolves the row.

        Args:
            store: The instance's upload store.
            row: The claimed row to park.
            attempts: The attempts value to persist (the two callers
                differ: capture-expired keeps ``row.attempts``, budget
                exhaustion has already counted the failed attempt).
            last_error: Typed error token for the operator.
            upstream_status: Last upstream status code, when one exists.
            no_op_context: Caller tag for the rowcount=0 log line.
        """
        rowcount = await store.record_attempt_result(
            row.chain_id,
            new_state="stored",
            attempts=attempts,
            next_attempt_at=None,
            last_error=last_error,
            upstream_status=upstream_status,
            upstream_headers_json=None,
            captured_values=None,
            current_step_index=None,
            last_step_completed=None,
        )
        if rowcount == 0:
            await self._no_op_total.inc()
            logger.info(
                "%s no-op: chain_id=%s - admin cancel/replay took the row from attempting",
                no_op_context,
                row.chain_id,
            )

    async def _on_terminal_failure(
        self, store: UploadStore, row: UploadRow, *, last_error: str
    ) -> None:
        """Transition row to ``failed`` (terminal)."""
        rowcount = await store.record_attempt_result(
            row.chain_id,
            new_state="failed",
            attempts=row.attempts + 1,
            next_attempt_at=None,
            last_error=last_error,
            upstream_status=None,
            upstream_headers_json=None,
            captured_values=None,
            current_step_index=None,
            last_step_completed=None,
        )
        if rowcount == 0:
            await self._no_op_total.inc()
            logger.info(
                "_on_terminal_failure no-op: chain_id=%s — admin cancel/replay "
                "took the row from attempting",
                row.chain_id,
            )
            return
        await self._instance.saturation.release(row.body_size_bytes)

    async def _on_auth_failure(
        self, store: UploadStore, row: UploadRow, result: FailedAuth
    ) -> None:
        """Park the row in ``auth_expired`` and notify the AD minter if any."""
        rowcount = await store.record_attempt_result(
            row.chain_id,
            new_state="auth_expired",
            attempts=row.attempts + 1,
            next_attempt_at=None,
            last_error=f"auth_{result.status}",
            upstream_status=result.status,
            upstream_headers_json=None,
            captured_values=None,
            current_step_index=None,
            last_step_completed=None,
        )
        if rowcount == 0:
            await self._no_op_total.inc()
            logger.info(
                "_on_auth_failure no-op: chain_id=%s — admin cancel/replay "
                "took the row from attempting",
                row.chain_id,
            )
            return
        # Release saturation accounting on park (§3.1). Without this, every
        # row that hits 401 permanently charges the gate's in_flight / bytes
        # counters; an AD outage with N flapping rows accumulates N forever
        # and eventually 503-rejects fresh ingress when actual in-flight is
        # zero. The auth-kicker re-admits when the row wakes back to queued.
        await self._instance.saturation.release(row.body_size_bytes)
        minter = self._instance.minter
        if minter is None:
            return
        try:
            await minter.on_401(row.endpoint, row.uid, result.observed_at)
        except Exception:
            logger.warning(
                "minter.on_401 raised for endpoint=%s uid=%s; ignored",
                row.endpoint,
                row.uid,
                exc_info=True,
            )

    async def _on_send_deadline_expired(
        self, store: UploadStore, row: UploadRow, result: SendDeadlineExpired
    ) -> None:
        """Give up on a claimed row past its route's send-deadline (path A → ``expired``).

        The executor's send-deadline gate (a') classified this ``attempting`` row
        as over-deadline. Delegate to the shared ``expire_row`` writer (the single
        ``new_state="expired"`` call site, ADR-032 / ADR-015) which flips the row
        terminal-``expired``, discards the body, and releases the saturation slot
        — passing the ``"attempting"`` CAS pre-state for this claimed-row path.
        ``release_saturation=True``: the ``attempting`` row STILL HOLDS the slot it
        was admitted with, so expiring it must release that slot (path A).
        """
        await expire_row(
            store,
            self._instance.saturation,
            row,
            expected_state="attempting",
            last_error=f"send_deadline:{result.deadline_seconds}s",
            upstream_status=None,
            release_saturation=True,
        )

    async def _on_retryable_failure(
        self,
        store: UploadStore,
        row: UploadRow,
        result: Failed5xx | FailedNetwork | CaptureIncomplete,
    ) -> None:
        """Schedule the next attempt or transition to ``stored`` if exhausted."""
        attempts = row.attempts + 1
        since_received = datetime.now(tz=UTC) - row.received_at
        if isinstance(result, Failed5xx):
            last_error = f"5xx_status_{result.status}"
            upstream_status: int | None = result.status
        elif isinstance(result, CaptureIncomplete):
            # finding R7-5-B: a 2xx whose body was missing a required capture.
            # The status WAS a success, but the body was incomplete; surface a
            # typed, retryable last_error naming the missing capture(s) so the
            # operator sees the chain isn't wedged silently — it's retrying a
            # step whose response body arrived incomplete.
            last_error = (
                f"capture_incomplete:{result.upstream_status}:{list(result.missing_captures)}"
            )
            upstream_status = result.upstream_status
        else:
            last_error = f"network:{result.error}"
            upstream_status = None
        delay = self._instance.retry_strategy.schedule_next_attempt(
            attempts=attempts,
            since_received=since_received,
            last_error=last_error,
            route_name=row.route_name,
        )
        if delay is None:
            # Retry budget exhausted: park the row via the single
            # stored writer (task 2.6, D7).
            await self._record_stored(
                store,
                row,
                attempts=attempts,
                last_error=last_error,
                upstream_status=upstream_status,
                no_op_context="_on_retryable_failure(stored)",
            )
            return
        next_attempt_at = datetime.now(tz=UTC) + delay
        rowcount = await store.record_attempt_result(
            row.chain_id,
            new_state="queued",
            attempts=attempts,
            next_attempt_at=next_attempt_at,
            last_error=last_error,
            upstream_status=upstream_status,
            upstream_headers_json=None,
            captured_values=None,
            current_step_index=None,
            last_step_completed=None,
        )
        if rowcount == 0:
            await self._no_op_total.inc()
            logger.info(
                "_on_retryable_failure(queued) no-op: chain_id=%s — "
                "admin cancel/replay took the row from attempting",
                row.chain_id,
            )
            return
        # Retry-linger trigger (plan § 2.3.11 / § 2.3.18).
        # When the row has been in RAM longer than ``linger_seconds``
        # AND the controller is wired (hybrid mode only), enqueue the
        # chain for RAM→disk migration. Fire-and-forget — the
        # controller serializes via its own queue and the next attempt
        # will read from disk via :class:`HybridBodyStore`'s fall-through.
        controller = self._instance.persist_controller
        if controller is None or row.body_location != "ram":
            return
        linger_seconds = self._instance.current_settings().body_store.linger_seconds
        if since_received.total_seconds() <= linger_seconds:
            return
        try:
            await controller.enqueue(row.chain_id)
        except Exception:
            logger.warning(
                "PersistController.enqueue raised for chain_id=%s; "
                "row stays at body_location='ram'",
                row.chain_id,
                exc_info=True,
            )


__all__ = ["Sender"]
