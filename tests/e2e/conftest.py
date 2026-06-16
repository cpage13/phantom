"""Shared pytest fixtures for the cross-package E2E suite.

The fixtures here boot the four-package stack once per session (or
per test where needed) so individual tests stay short. The session-
scoped `stack` fixture is the load-bearing one — every test depends
on it, directly or transitively.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any

import pytest
import pytest_asyncio
from phantom_client import PhantomClient

from ._driver import PhantomDriver
from ._harness.subprocess_harness import DAEMON_REGISTRY
from .helpers.payloads import build_upload_payload
from .helpers.stack import E2EStack, EmulatorControl, boot_stack
from .helpers.timing import await_until

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def _phantom_daemon_reaper() -> Iterator[None]:
    """Session finalizer: reap any ``python -m phantom`` daemon a test leaked.

    Every spawn site registers its daemon ``Popen`` with
    :data:`tests.e2e._harness.subprocess_harness.DAEMON_REGISTRY` at spawn
    time. After the last test of the session this finalizer terminates (then
    kills) every tracked process still alive and logs exactly what it
    reaped, so a test that failed before its own teardown can no longer
    leak an orphaned daemon past session end.
    """
    yield
    reaped = DAEMON_REGISTRY.reap_all()
    if reaped:
        logger.warning(
            "session-end daemon reaper killed %d leaked phantom daemon(s): %s",
            len(reaped),
            ", ".join(f"pid={r.pid} ({r.label})" for r in reaped),
        )
    else:
        logger.info("session-end daemon reaper: no leaked phantom daemons")


@pytest_asyncio.fixture
async def stack() -> AsyncIterator[E2EStack]:
    """Boot the full cross-package stack for one test.

    The fixture is function-scoped intentionally — pytest-asyncio's
    loop handling in this workspace creates a per-test event loop
    even when a session-scoped fixture is declared with
    ``loop_scope="session"``, and the resulting cross-loop traffic
    causes multipart POSTs to deadlock (uvicorn's receive starves
    against httpx's send when they're on different loops).

    The cost is ~0.5-1 s of boot overhead per test. For the < 60 s
    suite target with 35 tests that's still comfortably under
    budget, but a future agent who tracks down the loop interaction
    can promote this back to session scope for further speedup.
    """
    s = await boot_stack()
    try:
        yield s
    finally:
        await s.tear_down()


@pytest.fixture
def phantom(stack: E2EStack) -> str:
    """Phantom's base URL for the running stack."""
    return stack.phantom_url


@pytest.fixture
def emulator(stack: E2EStack) -> EmulatorControl:
    """Typed control wrapper around the emulator."""
    return stack.emulator


@pytest.fixture
def phantom_client(stack: E2EStack) -> PhantomClient:
    """Open :class:`PhantomClient` pointed at Phantom's URL.

    The client is owned by the stack fixture (closed on tear_down);
    tests just borrow it.
    """
    return stack.phantom_client


@pytest.fixture
def driver(stack: E2EStack) -> PhantomDriver:
    """Public test-owned driver bound to the running stack.

    The driver submits two-step upload chains through Phantom via the
    stack's open :class:`PhantomClient` using only public types (see
    :mod:`tests.e2e._driver`). The fake security token is signed with
    the emulator's HS256 secret so JWT verify on the upstream side
    succeeds; ``files_api`` is the emulator's URL - used as the
    metadata-POST base (Phantom routes by the first step's URL).

    The driver does not own the client lifecycle: the ``stack`` fixture
    opens the :class:`PhantomClient` and closes it on tear_down.
    """
    return PhantomDriver(
        stack.phantom_client,
        files_api=stack.emulator_url,
        get_security_token=stack.fake_security_token,
    )


@pytest.fixture
def upload_payload_factory() -> Any:
    """Callable that builds an :class:`UploadPayload` (request + body)."""
    return build_upload_payload


class CrashAt:
    """Crash-position registry used by ``tests/e2e/crash_recovery/`` (§ 6.2.2).

    The WS-4 crash-window matrix names a small set of positions
    inside the persist controller's run loop + the admission
    transaction. Each labelled position has one canonical end state
    after recovery; ``CrashAt`` makes the position string
    grep-checkable and prevents typo drift between the matrix doc
    and the test names.

    The crash-recovery tests do not need a runtime-injected hook —
    they construct the storage layer directly and halt at the
    labelled position by not calling the next step. This class is the
    typed inventory of valid positions; tests reference its members
    in docstrings + assertions so the matrix coverage is auditable.
    """

    BETWEEN_GET_ALL_AND_FILE_PUT = "between_get_all_and_file_put"
    BETWEEN_FILE_PUT_AND_MARK_PERSISTED = "between_file_put_and_mark_persisted"
    BETWEEN_MARK_PERSISTED_AND_RAM_DELETE = "between_mark_persisted_and_ram_delete"
    DURING_ADMISSION_ATOMIC_TRANSACTION = "during_admission_atomic_transaction"
    DURING_RECOVERY_SWEEP = "during_recovery_sweep"


@pytest.fixture
def crash_at() -> type[CrashAt]:
    """Expose :class:`CrashAt` to crash-recovery tests as a fixture.

    Per plan § 6.2.2 the fixture is the documented entry point for
    naming crash positions. Returning the class itself rather than
    an instance keeps tests using the ``CrashAt.NAME`` constants
    directly while still pulling in the fixture explicitly.
    """
    return CrashAt


async def wait_until(
    predicate: Callable[[], bool | Awaitable[bool]],
    *,
    timeout: float = 5.0,
    poll: float = 0.05,
    message: str | None = None,
) -> None:
    """Plan-mandated ``wait_until`` polling helper (per § 6.2.1).

    Thin wrapper over :func:`tests.e2e.helpers.timing.await_until` that
    accepts both sync and async predicates and uses the plan's
    parameter names (``timeout`` / ``poll``). Tests that previously
    used a naked sleep to wait for a state change should
    use ``await wait_until(lambda: row.state == "succeeded")``.

    Args:
        predicate: Callable returning bool or Awaitable[bool]. Called
            repeatedly until truthy.
        timeout: Maximum total wait time in seconds.
        poll: Sleep between predicate probes in seconds.
        message: Optional description for the timeout error.

    Raises:
        TimeoutWaitingError: When ``predicate`` never returns truthy
            within ``timeout``.
    """

    async def _adapter() -> bool:
        result = predicate()
        if isinstance(result, Awaitable):
            return bool(await result)
        return bool(result)

    await await_until(
        _adapter,
        timeout_seconds=timeout,
        poll_interval_seconds=poll,
        message=message,
    )


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Auto-mark every test under tests/e2e/ as e2e + session-scope loop.

    Tests can also declare ``@pytest.mark.e2e`` explicitly; this
    hook makes the marker non-optional so the suite is always
    runnable via ``pytest -m e2e``.

    The ``asyncio(loop_scope="session")`` marker is also added so the
    session-scoped :func:`stack` fixture and its tests share one
    event loop — without this, the fixture's uvicorn Server tasks
    run on a separate loop from the test's httpx client and
    multipart POSTs deadlock (the receiving loop is starved by the
    sending loop yielding to its own).
    """
    for item in items:
        item.add_marker(pytest.mark.e2e)


def pytest_configure(config: pytest.Config) -> None:
    """Register the `e2e` marker so `-m e2e` works without warning.

    The marker is also declared in the workspace pyproject so
    ``pytest --strict-markers`` accepts it; this hook is a
    belt-and-braces guarantee for local runs that don't pass
    ``--strict-markers``.
    """
    config.addinivalue_line(
        "markers", "e2e: end-to-end test exercising the full four-package stack"
    )
    config.addinivalue_line(
        "markers",
        "perf: performance-budget assertions (SYNTHETIC_RETURN_BUDGET_SECONDS); "
        "run on a quiet/dedicated lane via `-m perf`; excluded from the default suite",
    )
