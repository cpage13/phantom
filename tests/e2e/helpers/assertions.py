"""High-level assertion helpers for the E2E suite.

These wrap the three observable surfaces the suite asserts against:

- Phantom's admin API (chain state, captured values).
- The emulator's ``/control/received`` log (upstream-side body shape
  and headers).
- Source-side return values (handled inline by each test; no helper
  here because the call shape is already a one-liner).

Each helper has a polling shape: the upstream side is asynchronous,
so tests must wait for a state change rather than read it once.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol
from uuid import UUID

import aiofiles  # type: ignore[import-untyped]  # types-aiofiles not in workspace dev deps
import aiofiles.os  # type: ignore[import-untyped]  # types-aiofiles not in workspace dev deps
from phantom_client import ChainAdminDetail, PhantomClient
from phantom_client.models.chain import ChainState
from phantom_emulator.control_models import ReceivedEntry

from .stack import E2EStack
from .timing import (
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_TIMEOUT_SECONDS,
    TimeoutWaitingError,
    await_until,
)

logger = logging.getLogger(__name__)


# How long to wait for the emulator to record a received body. The
# upper bound here is the sum of phantom's poll interval, two
# round-trips to the emulator (step 1 + step 2), and the small async
# settle delay — well under 10 seconds for any healthy stack.
DEFAULT_EMULATOR_RECEIVE_TIMEOUT_SECONDS: float = DEFAULT_TIMEOUT_SECONDS

# How long to wait for a chain to reach a terminal state. Driven by
# the retry strategy's first interval (0s) + poll_interval_ms +
# emulator round-trips. The plan's smoke test asserts succeeded
# within 5 s; this default leaves a small cushion for slower runners.
DEFAULT_TERMINAL_STATE_TIMEOUT_SECONDS: float = 10.0


async def assert_chain_reaches_state(
    pc: PhantomClient,
    chain_id: UUID,
    *,
    state: str = "succeeded",
    timeout_seconds: float = DEFAULT_TERMINAL_STATE_TIMEOUT_SECONDS,
) -> ChainAdminDetail:
    """Poll Phantom's admin API until the chain reaches ``state``.

    Delegates to :meth:`PhantomClient.poll_until` with ``state`` as
    the only terminal-state member. On timeout, raises
    :class:`phantom_client.PollDeadlineExceeded` (the SDK's natural
    exception type) rather than wrapping it — tests want the
    original :class:`ChainAdminDetail` snapshot for diagnostics.

    Args:
        pc: An open :class:`PhantomClient`.
        chain_id: The chain to poll.
        state: The terminal state to wait for. Default ``succeeded``.
        timeout_seconds: Maximum total wait time.

    Returns:
        The :class:`ChainAdminDetail` snapshot at the moment ``state``
        was reached.
    """
    deadline = datetime.now(UTC) + timedelta(seconds=timeout_seconds)
    return await pc.poll_until(
        chain_id,
        terminal_states=frozenset({state}),
        deadline=deadline,
    )


class ReceivedLog(Protocol):
    """The single emulator surface the receipt assertions read.

    Narrower than :class:`EmulatorControl` on purpose. Stating the one method
    this helper actually needs keeps its contract honest and lets the helper's
    own self-test drive it with a log in a shape the real emulator only reaches
    under a genuine delivery fault.
    """

    def received(self) -> list[ReceivedEntry]:
        """Return the accepted-body entries, oldest first."""
        ...


def _describe(entries: list[ReceivedEntry]) -> list[dict[str, object]]:
    """Compact per-entry view for failure messages and diagnostic logs."""
    return [
        {
            "phantom_local_uuid": e.metadata_kvs.get("phantom_local_uuid"),
            "upload_token": e.upload_token,
            "body_size": e.body_size,
        }
        for e in entries
    ]


async def assert_emulator_received(
    emulator: ReceivedLog,
    *,
    phantom_local_uuid: str,
    body_size: int | None = None,
    timeout_seconds: float = DEFAULT_EMULATOR_RECEIVE_TIMEOUT_SECONDS,
) -> ReceivedEntry:
    """Wait for the emulator to record the upload, then assert on what it got.

    Waiting and asserting are deliberately separate. The poll waits on IDENTITY
    alone, meaning an entry whose ``metadata_kvs['phantom_local_uuid']``
    matches. Everything else is checked once, after the wait, as an assertion.

    That split is the point of this helper. ``body_size`` used to sit inside the
    poll predicate, so an entry of the wrong size was skipped and the poll kept
    looking. A body delivered with the wrong number of bytes was then reported
    as a body that never arrived, collapsing two different failures into the
    less alarming one and sending the reader after timing rather than
    correctness.

    A second entry for one ``phantom_local_uuid`` fails here rather than being
    ignored. The emulator keys accepted bodies by upload token and projects the
    latest per token, so a retry against the same token overwrites its own
    entry. Two entries therefore mean two distinct upstream objects exist for
    one source upload. Under the old predicate a wrong-size copy sitting beside
    a correct copy was filtered away and the assertion passed in silence.

    The duplicate check sees only what has arrived by the time the first match
    lands, so a copy delivered later still escapes it. Callers that care assert
    the chain has reached a terminal state first, which in practice settles
    delivery before this runs.

    Args:
        emulator: Any object exposing the emulator's received log, in practice
            the :class:`EmulatorControl` on the stack.
        phantom_local_uuid: The ``phantom_local_uuid`` minted by the driver.
            Should equal ``str(returned_file_info.id)``.
        body_size: Optional expected size of the PUT body in bytes. When given
            it is asserted, not used to select among entries.
        timeout_seconds: Maximum total wait time for the entry to appear.

    Returns:
        The single matching :class:`ReceivedEntry`.

    Raises:
        TimeoutWaitingError: When no entry carrying ``phantom_local_uuid``
            appears within the timeout.
        AssertionError: When the body arrived more than once, or arrived with a
            size other than ``body_size``.
    """

    def _matches() -> list[ReceivedEntry]:
        return [
            entry
            for entry in emulator.received()
            if entry.metadata_kvs.get("phantom_local_uuid") == phantom_local_uuid
        ]

    async def _arrived() -> bool:
        return bool(_matches())

    try:
        await await_until(
            _arrived,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=DEFAULT_POLL_INTERVAL_SECONDS,
            message=(
                f"emulator recorded no accepted body at all for "
                f"phantom_local_uuid={phantom_local_uuid!r}"
            ),
        )
    except TimeoutWaitingError:
        # Surface what was actually received for diagnostic context.
        logger.error(
            "assert_emulator_received timed out; emulator received entries: %s",
            _describe(emulator.received()),
        )
        raise

    matched = _matches()

    assert len(matched) == 1, (
        f"emulator accepted {len(matched)} separate bodies for "
        f"phantom_local_uuid={phantom_local_uuid!r}, expected exactly 1. "
        f"Entries are keyed by upload token, so a retry against one token "
        f"overwrites its entry; more than one entry means more than one "
        f"upstream object exists for a single source upload. "
        f"Entries: {_describe(matched)}"
    )

    entry = matched[0]
    if body_size is not None:
        assert entry.body_size == body_size, (
            f"emulator accepted the body for "
            f"phantom_local_uuid={phantom_local_uuid!r} at {entry.body_size} "
            f"bytes, expected {body_size}. The body WAS delivered; its size is "
            f"wrong, which is a corruption or truncation rather than a "
            f"delivery failure."
        )
    return entry


async def assert_row_body_location(
    stack: E2EStack,
    chain_id: UUID,
    body_location: Literal["ram", "file"],
    *,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> ChainAdminDetail:
    """Poll until the row identified by ``chain_id`` has ``body_location``.

    Queries the admin endpoint ``GET /v1/admin/chains/{chain_id}`` and waits
    for ``detail.body_location == body_location``. Used to assert that a
    persist transition (RAM → file) has actually landed before checking
    the on-disk body file.

    Phase 1 Slice 1.E renamed this helper from ``assert_row_tier`` (the
    old ``tier`` column + ``Literal['memory','persisted']`` value pair
    collapsed into ``body_location`` per plan § 2.3.2 / § 2.3.19).

    Args:
        stack: The booted :class:`E2EStack`.
        chain_id: The chain to inspect.
        body_location: Expected location — ``"ram"`` or ``"file"``.
        timeout_seconds: Maximum total wait time.

    Returns:
        The :class:`ChainAdminDetail` at the moment the body_location
        matched.

    Raises:
        TimeoutWaitingError: When the row never reaches ``body_location``.
    """
    snapshot: list[ChainAdminDetail] = []

    async def _condition() -> bool:
        detail = await stack.phantom_client.get_upload(chain_id)
        snapshot.clear()
        snapshot.append(detail)
        return detail.body_location == body_location

    await await_until(
        _condition,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=DEFAULT_POLL_INTERVAL_SECONDS,
        message=(
            f"chain_id={chain_id} did not reach body_location={body_location!r}; "
            f"last snapshot body_location="
            f"{snapshot[-1].body_location if snapshot else None!r}"
        ),
    )
    return snapshot[-1]


async def assert_body_file_exists(
    stack: E2EStack,
    chain_id: UUID,
    body_ref_name: str,
    *,
    instance_id: str = "primary",
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> bytes:
    """Poll until the disk body file exists; return its contents.

    Computes the path from the stack's ``data_dir`` + per-instance
    ``data_dir`` + ``shard_prefix_chars`` of the ``chain_id`` +
    ``body_ref_name``. Reads via :mod:`aiofiles` so the helper plays
    nicely with the test loop.

    Args:
        stack: The booted :class:`E2EStack`.
        chain_id: The chain whose body file to read.
        body_ref_name: The named body_ref (e.g., ``"body"``).
        instance_id: Which instance owns the row. Defaults to
            ``"primary"`` (the suite's default instance id).
        timeout_seconds: Maximum total wait time.

    Returns:
        The raw bytes from the body file.

    Raises:
        TimeoutWaitingError: When the file never appears.
    """
    path = stack.body_path(chain_id, body_ref_name, instance_id=instance_id)

    async def _condition() -> bool:
        exists: bool = await aiofiles.os.path.isfile(path)
        return exists

    await await_until(
        _condition,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=DEFAULT_POLL_INTERVAL_SECONDS,
        message=f"body file did not appear at {path}",
    )
    async with aiofiles.open(path, "rb") as fh:
        data: bytes = await fh.read()
    return data


async def assert_body_file_absent(
    stack: E2EStack,
    chain_id: UUID,
    body_ref_name: str,
    *,
    instance_id: str = "primary",
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> None:
    """Poll until the disk body file no longer exists.

    Useful for asserting that recovery / reaper / persist-handoff
    cleanup actually removed a body file. Returns immediately when the
    file is already absent; raises on timeout if it remains.

    Args:
        stack: The booted :class:`E2EStack`.
        chain_id: The chain whose body file to wait on.
        body_ref_name: The named body_ref (e.g., ``"body"``).
        instance_id: Which instance owns the row. Defaults to
            ``"primary"`` (the suite's default instance id).
        timeout_seconds: Maximum total wait time.

    Raises:
        TimeoutWaitingError: When the file never disappears.
    """
    path = stack.body_path(chain_id, body_ref_name, instance_id=instance_id)

    async def _condition() -> bool:
        exists: bool = await aiofiles.os.path.isfile(path)
        return not exists

    await await_until(
        _condition,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=DEFAULT_POLL_INTERVAL_SECONDS,
        message=f"body file did not disappear from {path}",
    )


async def assert_row_state(
    stack: E2EStack,
    chain_id: UUID,
    state: ChainState,
    *,
    timeout_seconds: float = DEFAULT_TERMINAL_STATE_TIMEOUT_SECONDS,
) -> ChainAdminDetail:
    """Poll until the row reaches ``state`` (including ``"corrupted"``).

    Wraps :meth:`PhantomClient.poll_until` with a single-state
    terminal set. Unlike :func:`assert_chain_reaches_state`, this
    helper accepts ``ChainState`` directly (typed alias from the SDK)
    rather than a bare ``str``.

    Args:
        stack: The booted :class:`E2EStack`.
        chain_id: The chain to poll.
        state: The expected :class:`ChainState`.
        timeout_seconds: Maximum total wait time.

    Returns:
        The :class:`ChainAdminDetail` snapshot at the moment ``state``
        was reached.

    Raises:
        PollDeadlineExceeded: When the row never reaches ``state``.
    """
    deadline = datetime.now(UTC) + timedelta(seconds=timeout_seconds)
    return await stack.phantom_client.poll_until(
        chain_id,
        terminal_states=frozenset({state}),
        deadline=deadline,
    )
