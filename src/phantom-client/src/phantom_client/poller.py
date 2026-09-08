"""Adaptive polling helpers for chain submissions and query groups.

Phantom emits ``X-Phantom-Suggested-Poll-After`` (integer seconds) on
every response; the pollers honor that hint as the next-sleep duration.
On the first iteration there's no previous response, so the configured
``initial_delay_seconds`` is used. Responses without the hint (the
admin reads do not emit it today) fall back to the same configured
delay, so both pollers share one backoff shape.

Every poll iteration is one call to
:meth:`~phantom_client.transport.Transport.get_json_with_poll_hint`, the
transport's PUBLIC read-plus-hint method. That matters: the pollers used to
issue the GET on the transport's raw ``httpx`` client through two private
members, which silently cost every iteration the retry policy AND the
httpx-to-Phantom exception translation, so one ``ConnectError`` from a Phantom
restarting mid-poll aborted the whole poll with an untranslated exception.

Two pollers share that shape:

- :func:`poll_until`: one chain, stop when ``state`` enters the
  stop-set. The default stop-set is :data:`TERMINAL_STATES`, which
  covers every terminal ``ChainState``: it includes ``auth_expired``
  (the SDK's stance is that auth_expired means "no further attempt
  without external intervention"; the caller pushes a fresh token or
  accepts the auth-failed result) and ``corrupted`` (R6-5: Phantom
  never retries a body-verification failure, so polling past it could
  only run to the deadline). Callers who explicitly want to poll
  *through* ``auth_expired`` pass a custom set, typically
  ``frozenset({"succeeded", "failed"})``.
- :func:`poll_group_until_finished`: one query group, stop when the
  rollup reports ``all_finished`` (no member queued or attempting;
  ``auth_expired`` and ``corrupted`` count as finished). A token push
  that revives an ``auth_expired`` member can honestly flip the flag
  back to false while it re-attempts, so the loop only ever exits on
  an ``all_finished=True`` observation.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from uuid import UUID

from phantom_client.errors import PollDeadlineExceeded
from phantom_client.models.admin import ChainAdminDetail, GroupStatusResponse
from phantom_client.models.status import TERMINAL_STATES
from phantom_client.transport import PATH_CHAIN, PATH_GROUP_STATUS, Transport

_LOG = logging.getLogger(__name__)

# Delay before the first poll, and the fallback sleep whenever a
# response carries no X-Phantom-Suggested-Poll-After hint. Half a
# second keeps the happy path snappy (most test uploads finish within
# one or two polls) without hammering the admin API.
DEFAULT_INITIAL_POLL_DELAY_SECONDS = 0.5


async def poll_until(
    transport: Transport,
    chain_id: UUID,
    *,
    terminal_states: frozenset[str] = TERMINAL_STATES,
    deadline: datetime | None = None,
    initial_delay_seconds: float = DEFAULT_INITIAL_POLL_DELAY_SECONDS,
) -> ChainAdminDetail:
    """Poll Phantom's admin API until the chain reaches a terminal state.

    Args:
        transport: Started :class:`Transport` to drive HTTP calls.
        chain_id: The chain's id (= envelope.chain_id).
        terminal_states: Set of states that end the loop. Defaults to
            :data:`TERMINAL_STATES`; pass a smaller set to poll through
            ``auth_expired``.
        deadline: When set, an absolute UTC timestamp after which
            :class:`PollDeadlineExceeded` is raised. ``None`` means no
            timeout. A NAIVE datetime is read as UTC, so the natural
            ``datetime.utcnow() + timedelta(...)`` spelling of this
            parameter's own documentation works; see :func:`_as_utc`.
        initial_delay_seconds: Delay before the first poll. Subsequent
            sleeps come from the response's
            ``X-Phantom-Suggested-Poll-After`` header (falling back to
            this value when absent).

    Returns:
        The final :class:`ChainAdminDetail`, where ``state`` is in
        ``terminal_states``.

    Raises:
        PollDeadlineExceeded: When the deadline elapses before a
            terminal state is reached.
        PhantomHttpError: When Phantom returns a non-2xx (e.g., 404 if
            the row was reaped).
    """
    path = PATH_CHAIN.format(chain_id=chain_id)
    utc_deadline = _as_utc(deadline)
    delay = initial_delay_seconds
    while True:
        await _sleep_with_deadline(delay, deadline=utc_deadline)
        polled = await transport.get_json_with_poll_hint(path, model=ChainAdminDetail)
        if polled.body.state in terminal_states:
            _LOG.debug("poll terminal: chain_id=%s state=%s", chain_id, polled.body.state)
            return polled.body
        delay = (
            polled.suggested_poll_after_seconds
            if polled.suggested_poll_after_seconds is not None
            else initial_delay_seconds
        )


async def poll_group_until_finished(
    transport: Transport,
    group_id: UUID,
    *,
    deadline: datetime | None = None,
    initial_delay_seconds: float = DEFAULT_INITIAL_POLL_DELAY_SECONDS,
) -> GroupStatusResponse:
    """Poll a query group's rollup until it reports ``all_finished``.

    The group twin of :func:`poll_until`: loops
    ``GET /v1/admin/groups/{group_id}`` with the same
    sleep / fetch / check / suggested-delay backoff shape, stopping on
    the structural finished rule (``all_finished`` is true iff no
    member is queued or attempting; ``auth_expired`` and ``corrupted``
    members count as finished).

    Args:
        transport: Started :class:`Transport` to drive HTTP calls.
        group_id: The query group's id (the value submitted as
            ``SubmitOptions.group_id``, or a ``chain_id`` for the
            default singleton group).
        deadline: When set, an absolute UTC timestamp after which
            :class:`PollDeadlineExceeded` is raised. ``None`` means no
            timeout. A NAIVE datetime is read as UTC, so the natural
            ``datetime.utcnow() + timedelta(...)`` spelling of this
            parameter's own documentation works; see :func:`_as_utc`.
        initial_delay_seconds: Delay before the first poll. Subsequent
            sleeps come from the response's
            ``X-Phantom-Suggested-Poll-After`` header (falling back to
            this value when absent).

    Returns:
        The final :class:`GroupStatusResponse`, where ``all_finished``
        is ``True``.

    Raises:
        PollDeadlineExceeded: When the deadline elapses before the
            group finishes.
        PhantomNotFoundError: When no upload anywhere carries
            ``group_id`` (the rollup is the one lookup that 404s).
        PhantomHttpError: On any other non-2xx admin response.
    """
    path = PATH_GROUP_STATUS.format(group_id=group_id)
    utc_deadline = _as_utc(deadline)
    delay = initial_delay_seconds
    while True:
        await _sleep_with_deadline(delay, deadline=utc_deadline)
        polled = await transport.get_json_with_poll_hint(path, model=GroupStatusResponse)
        if polled.body.all_finished:
            _LOG.debug("group poll finished: group_id=%s total=%d", group_id, polled.body.total)
            return polled.body
        delay = (
            polled.suggested_poll_after_seconds
            if polled.suggested_poll_after_seconds is not None
            else initial_delay_seconds
        )


def _as_utc(deadline: datetime | None) -> datetime | None:
    """Return ``deadline`` as an aware UTC timestamp, accepting a naive one.

    Both pollers compare the caller's deadline against
    ``datetime.now(tz=UTC)``, and Python refuses to order a naive datetime
    against an aware one - it raises ``TypeError``. The deadline parameter is
    documented as "an absolute UTC timestamp", and the most natural spelling
    of exactly that, ``datetime.utcnow() + timedelta(seconds=60)``, returns a
    NAIVE value. Left unhandled it aborted the poll with a bare ``TypeError``
    before the first HTTP call, an exception no caller could have predicted
    from the documented ``Raises`` list.

    Reading a naive value as UTC is chosen over rejecting it with a typed
    error: under the parameter's own documentation a naive datetime is not
    ambiguous, it is UTC, so honouring it is the answer that matches what the
    caller asked for. Rejecting would add a failure mode to a call that has a
    correct interpretation available, and it would make the documented
    spelling of the parameter an error.

    An aware deadline in any zone is returned unchanged: comparing two aware
    datetimes across zones is well-defined, so no conversion is needed.

    Args:
        deadline: The caller-supplied deadline, or ``None`` for no timeout.

    Returns:
        ``None`` when ``deadline`` is ``None``; otherwise an aware datetime.
    """
    if deadline is None or deadline.tzinfo is not None:
        return deadline
    return deadline.replace(tzinfo=UTC)


async def _sleep_with_deadline(seconds: float, *, deadline: datetime | None) -> None:
    """Sleep ``seconds`` unless ``deadline`` is sooner; otherwise raise.

    ``deadline`` must already be timezone-aware; the pollers normalize it
    once through :func:`_as_utc` before entering their loops.
    """
    if deadline is None:
        await asyncio.sleep(seconds)
        return
    now = datetime.now(tz=UTC)
    if now >= deadline:
        raise PollDeadlineExceeded(f"deadline {deadline.isoformat()} elapsed")
    remaining = (deadline - now).total_seconds()
    await asyncio.sleep(min(seconds, max(0.0, remaining)))
    if datetime.now(tz=UTC) >= deadline:
        raise PollDeadlineExceeded(f"deadline {deadline.isoformat()} elapsed")


__all__ = [
    "DEFAULT_INITIAL_POLL_DELAY_SECONDS",
    "poll_group_until_finished",
    "poll_until",
]
