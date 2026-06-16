"""Time-based polling and pacing helpers for the E2E suite.

The helpers here implement four named primitives used throughout the
E2E suite. They are the ONLY places in ``tests/e2e/`` that wrap
``asyncio.sleep``; every test file goes through one of these names so
the synchronization mechanism is explicit at the call site:

- :func:`await_until` — poll a condition until truthy or timeout (the
  canonical "wait until state X is observed" primitive).
- :func:`pace` — wait a small interval between paced calls (RPS-style
  pacing, e.g. matching the upstream client's throttler cadence).
- :func:`settle_for` — wait a fixed interval for a system event to
  propagate (e.g. wait for a reload to land in the SQLite snapshot
  before reading it). The caller passes a ``reason`` string that is
  attached to the call so the wait is self-documenting.
- :func:`stagger` — front-load a coroutine in a burst so concurrent
  submissions don't all hit the wire at t=0.

The conftest re-exports :func:`await_until` under the alternate
name ``wait_until``.

Lower-level polling for Phantom's admin chain state already lives in
:func:`phantom_client.poll_until`; this module covers the gaps
(arbitrary condition callables, emulator-side log polling) without
duplicating that logic.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

# Default poll-loop interval. Tight enough that tests don't wait
# noticeably past their condition; large enough that the loop doesn't
# spin against a slow emulator.
DEFAULT_POLL_INTERVAL_SECONDS: float = 0.1

# Default timeout — most E2E assertions need a few seconds to let the
# sender pool pick up a queued row, attempt it, and transition to
# `succeeded`. Tests override per-call when they need a tighter or
# wider window.
DEFAULT_TIMEOUT_SECONDS: float = 10.0


class TimeoutWaitingError(AssertionError):
    """Raised by :func:`await_until` when the condition is never true.

    Subclassing :class:`AssertionError` so pytest reports the failure
    in the natural assertion-error idiom.
    """


async def await_until(
    condition: Callable[[], Awaitable[bool]],
    *,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
    message: str | None = None,
) -> None:
    """Poll ``condition`` until it returns True or the timeout fires.

    The condition is awaited every ``poll_interval_seconds`` until it
    returns truthy. The total wait time is bounded by
    ``timeout_seconds``; on expiry, :class:`TimeoutWaitingError` is raised.

    Args:
        condition: Async callable returning a boolean. Called
            repeatedly until it returns True.
        timeout_seconds: Maximum total wait time.
        poll_interval_seconds: Sleep between condition probes.
        message: Optional human-readable description used in the
            raised :class:`TimeoutWaitingError` exception. Default is the
            condition's repr.

    Raises:
        TimeoutWaitingError: When ``condition`` never returns truthy
            within the timeout.
    """
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout_seconds
    while True:
        if await condition():
            return
        if loop.time() >= deadline:
            descr = message or f"condition={condition!r}"
            raise TimeoutWaitingError(f"condition not met within {timeout_seconds}s: {descr}")
        await asyncio.sleep(poll_interval_seconds)  # pre-commit-allow: sleep


async def pace(seconds: float) -> None:
    """Wait ``seconds`` between paced operations.

    Use for RPS pacing, client stagger, or any "intentional throttling"
    where the wait IS the production-faithful behavior (matching the
    upstream client's throttler 10 RPS cap, for example).

    Args:
        seconds: How long to wait. Must be non-negative.
    """
    if seconds <= 0:
        return
    await asyncio.sleep(seconds)  # pre-commit-allow: sleep


async def settle_for(seconds: float, reason: str) -> None:
    """Wait ``seconds`` for a non-observable system event to propagate.

    The ``reason`` argument is required so the wait carries its own
    documentation at the call site. Use this only when there is no
    observable predicate to wait on (e.g., "give the reaper a tick to
    sweep the row, then assert it is absent"); prefer :func:`await_until`
    when an observable predicate exists.

    Args:
        seconds: How long to wait. Must be non-negative.
        reason: One-line human-readable description of WHY this wait is
            needed. Not used in logging or error messages; exists purely
            for code-reading clarity.
    """
    del reason  # The argument is consumed at the call site for documentation.
    if seconds <= 0:
        return
    await asyncio.sleep(seconds)  # pre-commit-allow: sleep


async def sleep_until_monotonic(deadline: float) -> None:
    """Sleep until ``time.monotonic() >= deadline``.

    Used by tests that exercise time-based behavior (e.g. reaper
    retention expiry); the deadline is computed from the system's
    monotonic clock at the moment of an event. Returns immediately if
    the deadline is already past.

    Args:
        deadline: Target ``time.monotonic()`` value.
    """
    import time

    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return
    await asyncio.sleep(remaining)  # pre-commit-allow: sleep
