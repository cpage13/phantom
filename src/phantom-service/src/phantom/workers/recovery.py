"""Startup recovery sweep (plan § 2.3.15).

The single persistent ``uploads`` table replaces
the dual-store layout, and the ``committed`` column is gone (replaced
by ``body_location``). The sweep does two things only:

1. Reset every ``attempting`` row to ``queued`` (invariant #7).
   Sender pools were mid-attempt at process exit; their writes never
   landed, so the next process must re-attempt.
2. Integrity guard — multipart-aware. For every non-discarded row,
   verify every declared body_ref exists in the body store. A missing
   body_ref anywhere in the body list quarantines the row in the
   ``corrupted`` terminal state. This preserves the multipart-as-
   atomic-unit commitment (strategy §1 / plan § 0.4 glossary): a
   single missing file in the body list means the chain cannot be
   sent intact, so no retry will fix it.

Body-location discriminates the integrity check's expectations:

* ``body_location='ram'`` rows whose RAM bytes vanished (RAM lost on
  restart — by design for the RAM tier) quarantine as
  ``ram_body_lost_on_restart`` UNLESS the body has been
  discarded (``body_discarded_at IS NOT NULL`` — the H4 carve-out
  added in Phase 2).
* ``body_location='file'`` rows whose file body_refs cannot be located
  on disk quarantine as ``file_body_missing_on_recovery`` (same H4
  carve-out applies).

The orphan-file side of the picture is the body-orphan janitor's
responsibility (plan § 2.3.14); recovery only marks rows as
``corrupted`` — it never deletes body files itself.

Recovery is invoked from ``app.py``'s lifespan (the composition root)
BEFORE workers are scheduled, so the workers see
the corrected row population.

Cursor-drain discipline (V1/V2 crash-recovery fix). The integrity
guard walks ``store.iter_rows()`` — an open ``SELECT`` cursor on the
store's single connection — and must NOT write while that cursor is
open. Over a SIGKILL-hot WAL past SQLite's ``wal_autocheckpoint``
threshold, a mid-walk ``mark_corrupted`` write triggers an
autocheckpoint that collides with the open read cursor →
``SQLITE_LOCKED`` ("database is locked") → recovery crashes → the
service never starts → the buffered backlog is stranded (defeating
the core durability promise on the single most likely real disaster:
power loss / OOM-kill on a Pi). So the guard COLLECTS the
``(chain_id, reason)`` quarantine targets during the walk and issues
the ``mark_corrupted`` writes only AFTER the cursor is drained. (The
store also checkpoints + truncates a hot WAL at ``start()`` as
defense in depth, so the WAL is cold before this sweep runs.)
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING
from uuid import UUID

from phantom.runtime.lock_retry import (
    LOCK_RETRY_BUDGET_SECONDS,
    retry_on_transient_lock,
)
from phantom.storage.interface import TERMINAL_STATES

if TYPE_CHECKING:
    from phantom.storage.interface import BodyStore, UploadStore

logger = logging.getLogger(__name__)


class RecoveryLockError(RuntimeError):
    """Boot recovery could not acquire the DB write lock within the budget.

    Raised when an external holder kept the ``uploads.db`` write lock for the
    entire :data:`phantom.runtime.lock_retry.LOCK_RETRY_BUDGET_SECONDS` budget —
    past the point a transient contender would have released. This is the loud,
    operator-facing surface for a PERSISTENT lock (a wedged sibling process, a
    stuck backup): it propagates as a clean startup failure the supervisor
    restarts into (and the next boot rides out the lock if it has since
    cleared), rather than a raw ``sqlite3.OperationalError`` traceback. A
    transient holder never reaches this — it is ridden out by the bounded
    retry (finding R9-V6-2).
    """


async def _retry_recovery_write_on_lock[T](
    op: Callable[[], Awaitable[T]],
    *,
    description: str,
) -> T:
    """Run a recovery write, riding out a TRANSIENT external DB lock.

    Thin recovery-specific wrapper over the shared
    :func:`phantom.runtime.lock_retry.retry_on_transient_lock` primitive
    (§ 4D.1 factored the bounded retry-with-backoff loop + its budget there so
    recovery's WRITE retry and the boot-OPEN retry cannot drift). Rides out a
    transient SQLite lock with bounded exponential backoff until the lock clears
    or the budget is exhausted, at which point it raises :class:`RecoveryLockError`
    (a clean operator-facing failure, not a raw traceback). A NON-lock exception
    (a genuine fault) propagates immediately.

    Recovery writes are idempotent (``reset_attempting_to_queued`` is an
    unconditional state reset; ``mark_corrupted`` a guarded terminal write), so
    re-running a write whose predecessor was rejected by the lock is safe — no
    double-effect, nothing lost. This is what keeps a transient boot-time lock
    (operator ``sqlite3`` session, backup tool, lingering prior process) from
    crashing startup and stranding the buffered backlog (finding R9-V6-2).

    Args:
        op: A zero-arg factory returning the awaitable write to run. A factory
            (not a bare awaitable) so each retry issues a FRESH statement on the
            store's connection.
        description: Short label for the write, used in the wait/failure logs so
            an operator can see which recovery step is blocked.

    Returns:
        The value ``op`` returns once it succeeds.

    Raises:
        RecoveryLockError: The lock was held for the entire recovery budget.
        Exception: Any non-transient-lock error from ``op`` (propagated as-is).
    """

    def _budget_exhausted(last_lock_error: BaseException) -> RecoveryLockError:
        """Build the recovery-specific budget-exhausted error for the shared loop."""
        return RecoveryLockError(
            f"recovery write {description!r} could not acquire the "
            f"uploads.db write lock within "
            f"{LOCK_RETRY_BUDGET_SECONDS:.0f}s — an external holder "
            f"(an open 'sqlite3 uploads.db' session, a backup/snapshot "
            f"tool, or a stale prior process) is holding the lock past "
            f"the boot budget. Clear the holder and restart; the "
            f"buffered backlog is intact on disk and will be recovered "
            f"on the next clean boot."
        )

    return await retry_on_transient_lock(
        op,
        description=f"recovery write {description!r}",
        on_budget_exhausted=_budget_exhausted,
    )


async def run_recovery(
    store: UploadStore,
    body_store: BodyStore,
) -> None:
    """Run the boot-time recovery sweep against a single store.

    Two passes over the integrity guard so no write is ever issued
    while the ``iter_rows`` read cursor is open (V1/V2 fix — see the
    module docstring): pass 1 walks every row and COLLECTS the
    quarantine targets; pass 2 issues the ``mark_corrupted`` writes
    after the cursor is drained.

    Args:
        store: The persistent :class:`UploadStore`. Recovery reads
            ``iter_rows`` to walk every row and writes via
            :meth:`UploadStore.reset_attempting_to_queued` +
            :meth:`UploadStore.mark_corrupted` (the latter only after
            the walk's cursor is closed).
        body_store: The mode-selected :class:`BodyStore`. Recovery
            consults :meth:`BodyStore.has_body_ref` per body_ref to
            verify integrity. In hybrid mode this is
            :class:`HybridBodyStore` (RAM-presence-authoritative); in
            ``all_disk`` mode it is :class:`FileBodyStore`; in
            ``all_ram`` mode it is :class:`RamBodyStore` (and every
            previously-persisted ``body_location='ram'`` row is
            quarantined because RAM is empty post-restart).
    """
    # Step 1 — attempting → queued. Invariant #7.
    #
    # finding R9-V6-2: this is recovery's FIRST write and the one that crashed
    # startup when an external process held the uploads.db write lock past
    # busy_timeout. Ride out a transient holder with a bounded
    # retry-with-backoff instead of letting the lock-class OperationalError
    # propagate out of the lifespan and wedge the service.
    reset = await _retry_recovery_write_on_lock(
        store.reset_attempting_to_queued,
        description="reset_attempting_to_queued",
    )
    if reset:
        logger.info("Recovery reset %d attempting rows to queued", reset)

    # Step 2 — integrity guard. Multipart-aware (strategy §1 / plan
    # § 0.4 glossary): the body is an atomic unit; a single missing
    # ref anywhere in body_hashes invalidates the chain.
    #
    # CURSOR-DRAIN DISCIPLINE (V1/V2 fix): the ``iter_rows`` walk holds
    # an open ``SELECT`` cursor on the store's single aiosqlite
    # connection for the duration of the ``async for``. Issuing a WRITE
    # (``mark_corrupted``) on that same connection mid-walk is unsafe:
    # over a SIGKILL-hot WAL that has crossed SQLite's
    # ``wal_autocheckpoint`` threshold, the write triggers an automatic
    # checkpoint that conflicts with the still-open read cursor →
    # ``SQLITE_LOCKED``/"database is locked" → recovery crashes →
    # service never starts → the buffered backlog is stranded. RAM-lost
    # rows (and, in all_disk, mid-write-truncated body rows) ALWAYS
    # exist after a real crash, so this write path is guaranteed, not
    # incidental. The ``busy_timeout`` does not cover it (same-connection
    # cursor-vs-checkpoint, not a cross-process BUSY). So we COLLECT the
    # quarantine targets during the walk and issue every ``mark_corrupted``
    # write only AFTER the cursor is fully drained (the ``async for`` has
    # exited and the cursor context-manager in ``iter_rows`` is closed).
    to_quarantine: list[tuple[UUID, str]] = []
    async for row in store.iter_rows():
        if row.state in TERMINAL_STATES:
            # Terminal carve-out (finding R9-PM-3). A row that already
            # reached a terminal state is FINISHED — its missing body is
            # EXPECTED, not a corruption signal: a ``succeeded`` row's body
            # is deleted on delivery (``succeeded_body_seconds=0`` default,
            # sender.py — and that delete does NOT stamp
            # ``body_discarded_at``, so the H4 carve-out below would miss
            # it), and ``failed``/``cancelled``/``stored``/``corrupted``
            # rows are likewise done. Recovery's job is to make DELIVERABLE
            # rows consistent, never to re-judge a finished one. Without
            # this skip, every routine restart re-quarantines the success
            # records of all uploads delivered-then-reaped since the last
            # boot, destroying the durable ``succeeded`` record and firing
            # spurious InvariantAuditor alerts. ``auth_expired`` is NOT
            # terminal (still deliverable — its body MUST be present), so it
            # falls through to the body-existence check below, as intended.
            continue
        if row.body_discarded_at is not None:
            # H4 carve-out (auth_expired bodies were intentionally
            # discarded by the reaper; missing body files there are
            # not a corruption signal).
            continue
        if not row.body_hashes:
            continue
        missing: list[str] = []
        for name in row.body_hashes:
            if not await body_store.has_body_ref(row.chain_id, name):
                missing.append(name)
        if not missing:
            continue
        if row.body_location == "ram":
            reason = f"ram_body_lost_on_restart: {missing}"
        else:
            reason = f"file_body_missing_on_recovery: {missing}"
        logger.warning(
            "Recovery quarantined row chain_id=%s body_location=%s missing=%s",
            row.chain_id,
            row.body_location,
            missing,
        )
        to_quarantine.append((row.chain_id, reason))

    # The read cursor is now closed (the walk above has exited). Issue
    # the quarantine WRITES — safe even over a hot WAL, since no read
    # cursor is open on the connection to conflict with an autocheckpoint.
    # Each is also wrapped in the transient-lock retry (finding R9-V6-2): an
    # external holder that appears between the reset above and these writes
    # must be ridden out too, not crash the boot. ``mark_corrupted`` is a
    # guarded terminal write, so a retry after a lock-rejected attempt is a
    # safe no-op-or-set, never a double-effect.
    for chain_id, reason in to_quarantine:
        await _retry_recovery_write_on_lock(
            functools.partial(store.mark_corrupted, chain_id, reason),
            description=f"mark_corrupted(chain_id={chain_id})",
        )


__all__ = ["RecoveryLockError", "run_recovery"]
