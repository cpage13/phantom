"""PersistController - SOLE mover of bodies from RAM to disk.

Per plan § 0.5 single-writer manifest invariant #6 and plan § 2.3.11.

The controller is the SOLE writer of ``UploadRow.body_location='file'``
(via :meth:`SqliteUploadStore.mark_persisted`). Workers other than this
class never call :meth:`FileBodyStore.put` for the persistence-handoff
path AND never set ``body_location='file'`` on the SQLite row.

Triggers (callers that ``enqueue`` against this controller):

* **Retry-linger** - sender's failure handler enqueues when a row's
  retry count + linger window indicate the body should move off RAM
  so the next attempt reads from a durable store.
* **RAM-pressure** - :class:`RamPressureWatcher` enqueues oldest-
  resident chain_ids when the ``RamBodyStore`` byte total exceeds
  ``body_store.ram_ceiling_bytes``.
* **Size-threshold** - admission enqueues immediately when a body
  exceeds ``persist_trigger.body_size_threshold_bytes``.

Commit-last-column ordering (plan § 0.5 + § 2.3.11):

  1. Read body bytes from RAM (``RamBodyStore.get_all``).
  2. Write bytes to disk (``FileBodyStore.put`` - fsyncs each body
     file + the parent directory before returning).
  3. Flip ``body_location='ram'`` → ``'file'`` on the SQLite row
     (``SqliteUploadStore.mark_persisted`` - THE commit point).
  4. Drop the RAM body bytes.

A crash between (2) and (3) leaves a durable file on disk PLUS a row
still at ``body_location='ram'``. The startup recovery sweep
(plan § 2.3.15) handles this; the body-orphan janitor
(plan § 2.3.14) sweeps the leftover file.
"""

from __future__ import annotations

import asyncio
import logging
from uuid import UUID

from phantom.observability.metrics import MetricsRegistry
from phantom.storage.file_body_store import FileBodyStore
from phantom.storage.ram_body_store import RamBodyStore
from phantom.storage.sqlite_store import SqliteUploadStore
from phantom.workers.saturation import is_deliverable

logger = logging.getLogger(__name__)

# Metric outcomes (plan § 4.2.2). Counter labels are stable so admin
# dashboards can rely on them across deployments.
_PERSIST_OUTCOME_SUCCESS: str = "success"
_PERSIST_OUTCOME_FAILURE: str = "failure"

# Cadence at which the run loop checks ``stop_event`` between queue
# polls. A short interval keeps shutdown latency low; a 0.25 s tick
# is well below typical lifespan-shutdown timeout budgets and adds
# negligible overhead under steady-state load.
_STOP_POLL_INTERVAL_SECONDS: float = 0.25


class PersistController:
    """Public contract: :meth:`enqueue` returns an awaitable handle.

    Idempotent: a duplicate :meth:`enqueue` for an in-flight chain_id
    returns the existing :class:`asyncio.Future`, not a new one. Every
    caller awaiting the same chain_id sees the same completion signal
    regardless of how many ``enqueue`` calls fire.

    Workers other than this class never call
    :meth:`FileBodyStore.put` for the persistence handoff AND never
    call :meth:`SqliteUploadStore.mark_persisted`. Single-writer-per-
    purpose discipline (plan § 0.5 invariant #6).
    """

    def __init__(
        self,
        *,
        store: SqliteUploadStore,
        ram_body_store: RamBodyStore,
        file_body_store: FileBodyStore,
        metrics_registry: MetricsRegistry | None = None,
    ) -> None:
        """Construct the controller.

        The constructor deliberately takes NO config values (ADR-031
        decision 1): the controller's behavior is fully determined by
        its enqueue inputs, and any future tunable would arrive via the
        live snapshot, not construction. (A round-10 dead-code sweep
        removed a never-read ``settings`` parameter that had been held
        "for future tunable knobs".)

        Args:
            store: The persistent upload store. The controller calls
                ``mark_persisted`` here after the disk write fsyncs
                (commit point).
            ram_body_store: Source for body bytes prior to migration.
            file_body_store: Destination for body bytes after migration.
                Its ``put`` method fsyncs each body file + the parent
                directory before returning - the fsync-before-flip
                ordering invariant (plan § 0.5) is enforced by that
                contract, not by an explicit fsync call here.
            metrics_registry: Optional :class:`MetricsRegistry` for
                emit-site wiring (plan § 4.2.2). When ``None`` (test
                contexts that do not exercise observability) a
                throwaway registry is constructed so emission is a
                no-op. The composition root passes the runtime's real
                registry.
        """
        self._store = store
        self._ram = ram_body_store
        self._file = file_body_store
        self._queue: asyncio.Queue[UUID] = asyncio.Queue()
        self._in_flight: dict[UUID, asyncio.Future[None]] = {}
        self._handles_lock = asyncio.Lock()
        # Metrics surface (plan § 4.2.2). Register both counters
        # eagerly so the admin endpoint surfaces a zero-valued bucket
        # for each outcome before the first migration runs.
        self._metrics = metrics_registry if metrics_registry is not None else MetricsRegistry()
        self._persist_total = self._metrics.register_counter(
            "persist_total",
            "PersistController migration outcomes (labels: success, failure).",
        )
        self._queue_depth = self._metrics.register_gauge(
            "persist_controller_queue_depth",
            "Current enqueued RAM→disk migrations.",
        )

    async def enqueue(self, chain_id: UUID) -> asyncio.Future[None]:
        """Idempotent enqueue. Returns the per-chain completion future.

        First call for a chain_id allocates a fresh :class:`asyncio.Future`,
        records it in ``_in_flight``, and puts the chain_id on the
        internal queue. Subsequent calls for the same chain_id return
        the existing future (collapsing every concurrent caller to the
        same completion handle).

        Args:
            chain_id: The upload whose body should migrate RAM → disk.

        Returns:
            An :class:`asyncio.Future[None]` that resolves to ``None``
            on successful migration or carries an exception if the
            migration fails. Callers may ``await`` the future to block
            on completion or fire-and-forget (the controller's
            :meth:`run` loop drains the queue regardless).
        """
        async with self._handles_lock:
            existing = self._in_flight.get(chain_id)
            if existing is not None:
                return existing
            handle: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._in_flight[chain_id] = handle
            await self._queue.put(chain_id)
            await self._queue_depth.set(self._queue.qsize())
            return handle

    async def run(self, stop_event: asyncio.Event) -> None:
        """Main loop - drain the queue, migrate each chain_id, signal handle.

        Supervised by the composition root - :func:`phantom.app.create_app`'s
        lifespan :class:`asyncio.TaskGroup` (plan § 2.3.10). The loop never
        re-raises an exception - unrelated chain_ids must continue migrating
        even when one fails. Failures on a single chain are logged with full
        context AND the failing chain's handle gets the exception set, so any
        awaiting caller sees it.

        On failure the row's ``body_location`` stays at ``'ram'``. The
        leftover disk file (if (2) succeeded but (3) did not) is the
        body-orphan janitor's responsibility (plan § 2.3.14).

        The loop polls the queue with a short timeout and exits the moment
        ``stop_event`` fires, draining cleanly at shutdown so the lifespan
        TaskGroup is not left waiting on a never-returning task - the same
        ``stop_event``-drain idiom every other lifespan worker uses (sender /
        kicker / vacuum / reaper / auditor / …).

        Args:
            stop_event: Set by the lifespan on shutdown; the loop checks it
                between bounded queue polls and exits cleanly when it fires.
        """
        while not stop_event.is_set():
            try:
                chain_id = await asyncio.wait_for(
                    self._queue.get(),
                    timeout=_STOP_POLL_INTERVAL_SECONDS,
                )
            except TimeoutError:
                continue
            await self._handle_one(chain_id)

    async def _handle_one(self, chain_id: UUID) -> None:
        """Migrate one chain_id, resolve its handle, never re-raise."""
        try:
            await self._migrate_one(chain_id)
            self._resolve_handle(chain_id, exception=None)
            await self._persist_total.inc(label_value=_PERSIST_OUTCOME_SUCCESS)
        except Exception as exc:
            logger.exception(
                "PersistController migration failed: chain_id=%s",
                chain_id,
            )
            self._resolve_handle(chain_id, exception=exc)
            await self._persist_total.inc(label_value=_PERSIST_OUTCOME_FAILURE)
            # Do NOT re-raise - TaskGroup would cancel the entire
            # runtime. Errors on one chain don't kill the service.
        finally:
            # Whether success or failure, the queue depth dropped.
            await self._queue_depth.set(self._queue.qsize())

    async def _migrate_one(self, chain_id: UUID) -> None:
        """Run the RAM → disk migration for one chain_id.

        Order matters (plan § 0.5 commit-last-column):

        0. live-row pre-check - skip rows already body-discarded or
           gone (R7-2; narrows the race window before any work).
        1. ``ram.get_all`` - pull body bytes.
        2. ``file.put`` - write + fsync (fsync inside FileBodyStore).
        3. ``store.mark_persisted`` - flip ``body_location`` (the
           commit point). Rowcount 0 means the reaper's body-discard
           (or a row deletion) landed between steps 1 and 3: the
           just-written disk bytes are policy-discarded, so they are
           deleted again here and never flipped live (R7-2).
        4. ``ram.delete`` - release RAM bytes.
        """
        row = await self._store.get(chain_id)
        if row is None or not is_deliverable(row):
            # H4 carve-out (R7-2): migrating a discarded body would
            # resurrect bytes the operator's window dropped. The ``row is
            # None`` disjunct is a DIFFERENT question (this caller does its
            # own fresh ``get``, so the row can have vanished) and stays
            # visibly separate from the predicate.
            logger.info(
                "PersistController skipping chain_id=%s: row %s",
                chain_id,
                "body-discarded" if row is not None else "gone",
            )
            return
        body_refs = await self._ram.get_all(chain_id)
        # FileBodyStore.put fsyncs every body file + the parent dir
        # before returning. The fsync-before-flip ordering invariant
        # (plan § 0.5) is enforced by that contract - see
        # FileBodyStore._put_one + parent-dir fsync after the loop.
        await self._file.put(chain_id, body_refs)
        # The commit point: SOLE writer of body_location='file'. The
        # store's WHERE guards (body_location='ram' AND
        # body_discarded_at IS NULL) refuse the flip when the reaper's
        # discard raced the disk write above; rowcount 0 reports it.
        flipped = await self._store.mark_persisted(chain_id)
        if flipped == 0:
            # The discard (or a row deletion) landed mid-migration. The
            # disk write above resurrected policy-discarded bytes; undo
            # it so the bytes stay dropped (R7-2). The undo removes ONLY
            # this migration's own artifact, the disk write: a
            # same-chain_id upload can be legally re-admitted at ANY
            # instant after a mid-migration row deletion, so no check
            # can make a RAM delete here safe (R8-3; wiping it would
            # destroy the accepted new upload). RAM needs no cleanup
            # from us anyway: both discard owners delete the body store
            # before stamping, so the original bytes are already gone.
            # The chain's migrations are serialized by the in-flight
            # dedupe, so the disk entry is exclusively ours to remove.
            await self._file.delete(chain_id)
            logger.info(
                "PersistController undid migration for chain_id=%s: the "
                "row was body-discarded (or deleted) mid-migration; the "
                "disk write was removed, RAM untouched",
                chain_id,
            )
            return
        # Cleanup (not load-bearing for durability).
        await self._ram.delete(chain_id)
        logger.info(
            "PersistController migrated chain_id=%s (%d body_refs) to disk",
            chain_id,
            len(body_refs),
        )

    def _resolve_handle(self, chain_id: UUID, *, exception: BaseException | None) -> None:
        """Pop ``chain_id`` from ``_in_flight`` and signal its future.

        Called from :meth:`run`'s success and failure paths. The
        ``_handles_lock`` is NOT held - popping from the dict is fast
        and not racing with a concurrent ``enqueue`` (queue ordering
        guarantees enqueue happens-before run-dequeue for the same
        chain_id within one event loop).
        """
        handle = self._in_flight.pop(chain_id, None)
        if handle is None:
            return
        if handle.done():
            return
        if exception is None:
            handle.set_result(None)
        else:
            handle.set_exception(exception)
