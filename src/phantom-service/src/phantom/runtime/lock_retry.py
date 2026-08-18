"""Shared bounded retry-with-backoff for a transient SQLite lock at boot.

A single definition of "ride out a transient ``SQLITE_BUSY`` / ``SQLITE_LOCKED``
holder with a bounded exponential backoff, then surface a clean operator-facing
failure if the holder outlasts the budget." Two boot-path call sites consume it
so they cannot drift on the backoff discipline (the explicit goal of § 4D.1's
"do NOT duplicate the backoff logic"):

* :func:`phantom.workers.recovery.run_recovery` - recovery's idempotent WRITES
  (``reset_attempting_to_queued`` / ``mark_corrupted``) on the ``uploads.db``
  write lock (finding R9-V6-2); raises :class:`phantom.workers.recovery.RecoveryLockError`
  past budget.
* :func:`phantom.app._build_instance_context` - the boot-time DATABASE OPEN
  (``store.start()`` / ``token_cache.start()``, § 4D.1); raises
  :class:`BootOpenLockError` past budget so the open is then treated as an
  unrecoverable access failure and the DB is isolated.

The lock CLASS is judged by the one shared classifier
:func:`phantom.storage.sqlite_store.is_transient_lock_error` (ADR-023), so the
admission 503 path, recovery, and the boot-open guard all agree on what
"transient" means. The budget and backoff live HERE as the single source of
truth (recovery imports them, rather than re-declaring its own).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from phantom.storage.sqlite_store import is_transient_lock_error

logger = logging.getLogger(__name__)

# Total wall-clock the boot path will wait on a transient lock before giving up
# (finding R9-V6-2). Generous (a slow boot is acceptable per the durability
# contract - a wedged service is not) yet bounded so a TRULY stuck holder
# surfaces a clean, loud failure rather than hanging the boot forever. Shared by
# recovery's write retry and § 4D.1's boot-open retry so both wait the same.
LOCK_RETRY_BUDGET_SECONDS: float = 120.0
# Backoff between contended attempts. Each attempt already blocks up to the
# store's ``busy_timeout`` before raising, so these sleeps are the ADDED pause
# between attempts; exponential from a small base, capped so we keep probing
# roughly every few seconds once backed off (the holder could release at any
# instant, so we do not want an overlong sleep after release).
LOCK_RETRY_BACKOFF_BASE_SECONDS: float = 0.5
LOCK_RETRY_BACKOFF_MAX_SECONDS: float = 5.0


class BootOpenLockError(RuntimeError):
    """A boot-time database OPEN could not acquire the lock within the budget.

    Raised by the § 4D.1 boot-open retry when an external holder kept the DB
    locked for the entire :data:`LOCK_RETRY_BUDGET_SECONDS` budget - past the
    point a transient contender would have released. The boot-open guard catches
    it (it is NOT a transient-lock error and so falls out of the retry) and
    treats the open as an UNRECOVERABLE access failure: the database is isolated
    and the instance boots fresh, exactly as for a permission / I/O open fault.
    A transient holder that releases within the budget never reaches this - the
    open simply succeeds on a later attempt.
    """


async def retry_on_transient_lock[T](
    op: Callable[[], Awaitable[T]],
    *,
    description: str,
    on_budget_exhausted: Callable[[BaseException], BaseException],
    budget_seconds: float = LOCK_RETRY_BUDGET_SECONDS,
    backoff_base_seconds: float = LOCK_RETRY_BACKOFF_BASE_SECONDS,
    backoff_max_seconds: float = LOCK_RETRY_BACKOFF_MAX_SECONDS,
) -> T:
    """Run ``op``, riding out a TRANSIENT SQLite lock with bounded backoff.

    Calls ``op`` and returns its result. If ``op`` raises a transient SQLite
    lock error (:func:`is_transient_lock_error` - a cross-process ``SQLITE_BUSY``
    or a ``SQLITE_LOCKED`` that outlasted the connection's ``busy_timeout``),
    retries with bounded exponential backoff until the lock clears or
    ``budget_seconds`` is exhausted. On exhaustion it raises whatever
    ``on_budget_exhausted(last_lock_error)`` returns (the caller's own
    operator-facing error, chained from the last lock exception). A NON-lock
    exception (a genuine fault) propagates immediately - only the transient-lock
    class is retried.

    ``op`` MUST be safe to re-run: each retry calls it again. Recovery's writes
    are idempotent (an unconditional reset / a guarded terminal write); the
    boot-open path issues a FRESH connection per attempt (a new ``start()`` on a
    new store / cache), which is the natural unit of work for an open.

    Args:
        op: A zero-arg factory returning the awaitable to run. A factory (not a
            bare awaitable) so each retry issues a fresh statement / open.
        description: Short label for the operation, used in the wait/failure
            logs so an operator can see which step is blocked.
        on_budget_exhausted: Builds the exception to raise when the lock is held
            for the entire budget. Receives the last transient-lock exception so
            the raised error can chain from it (``raise ... from last``). Lets
            each call site surface its OWN clean failure (recovery's
            ``RecoveryLockError`` vs the boot-open ``BootOpenLockError``) over
            the shared loop.
        budget_seconds: Total wall-clock to wait on the lock before giving up.
            Defaults to :data:`LOCK_RETRY_BUDGET_SECONDS`.
        backoff_base_seconds: Exponential-backoff base. Defaults to
            :data:`LOCK_RETRY_BACKOFF_BASE_SECONDS`.
        backoff_max_seconds: Per-sleep backoff cap. Defaults to
            :data:`LOCK_RETRY_BACKOFF_MAX_SECONDS`.

    Returns:
        The value ``op`` returns once it succeeds.

    Raises:
        BaseException: Whatever ``on_budget_exhausted`` returns once the budget
            is exhausted; or any non-transient-lock error from ``op`` (as-is).
    """
    deadline = asyncio.get_running_loop().time() + budget_seconds
    attempt = 0
    while True:
        try:
            return await op()
        except Exception as exc:
            if not is_transient_lock_error(exc):
                raise
            now = asyncio.get_running_loop().time()
            remaining = deadline - now
            if remaining <= 0:
                raise on_budget_exhausted(exc) from exc
            backoff = min(backoff_base_seconds * (2**attempt), backoff_max_seconds)
            # Never sleep past the deadline - keep the total wait bounded.
            backoff = min(backoff, remaining)
            logger.warning(
                "%s is blocked on a transient SQLite lock (an external process "
                "holds it past busy_timeout); riding it out - retrying in %.1fs "
                "(%.0fs of the %.0fs budget remaining)",
                description,
                backoff,
                remaining,
                budget_seconds,
            )
            await asyncio.sleep(backoff)
            attempt += 1


__all__ = [
    "LOCK_RETRY_BACKOFF_BASE_SECONDS",
    "LOCK_RETRY_BACKOFF_MAX_SECONDS",
    "LOCK_RETRY_BUDGET_SECONDS",
    "BootOpenLockError",
    "retry_on_transient_lock",
]
