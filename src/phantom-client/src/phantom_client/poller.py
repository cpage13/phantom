"""Adaptive polling helpers for chain submissions and query groups.

Phantom emits ``X-Phantom-Suggested-Poll-After`` (integer seconds) on
every response; the pollers honor that hint as the next-sleep duration.
On the first iteration there's no previous response, so the configured
``initial_delay_seconds`` is used. Responses without the hint (the
admin reads do not emit it today) fall back to the same configured
delay, so both pollers share one backoff shape.

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

from pydantic import BaseModel, ValidationError

from phantom_client.errors import PhantomEnvelopeError, PollDeadlineExceeded
from phantom_client.headers import X_PHANTOM_SUGGESTED_POLL_AFTER
from phantom_client.models.admin import ChainAdminDetail, GroupStatusResponse
from phantom_client.models.status import TERMINAL_STATES
from phantom_client.transport import Transport

_LOG = logging.getLogger(__name__)

# Path template for fetching one upload row by chain_id.
_PATH_UPLOADS = "/v1/admin/chains/{chain_id}"

# Path template for fetching one query-group rollup by group_id.
_PATH_GROUP = "/v1/admin/groups/{group_id}"

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
            timeout.
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
    path = _PATH_UPLOADS.format(chain_id=chain_id)
    delay = initial_delay_seconds
    while True:
        await _sleep_with_deadline(delay, deadline=deadline)
        response, suggested = await _get_with_suggested(transport, path, model=ChainAdminDetail)
        if response.state in terminal_states:
            _LOG.debug("poll terminal: chain_id=%s state=%s", chain_id, response.state)
            return response
        delay = suggested if suggested is not None else initial_delay_seconds


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
            timeout.
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
    path = _PATH_GROUP.format(group_id=group_id)
    delay = initial_delay_seconds
    while True:
        await _sleep_with_deadline(delay, deadline=deadline)
        response, suggested = await _get_with_suggested(transport, path, model=GroupStatusResponse)
        if response.all_finished:
            _LOG.debug("group poll finished: group_id=%s total=%d", group_id, response.total)
            return response
        delay = suggested if suggested is not None else initial_delay_seconds


async def _sleep_with_deadline(seconds: float, *, deadline: datetime | None) -> None:
    """Sleep ``seconds`` unless ``deadline`` is sooner; otherwise raise."""
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


async def _get_with_suggested[T: BaseModel](
    transport: Transport, path: str, *, model: type[T]
) -> tuple[T, float | None]:
    """Fetch one poll target and the suggested-poll-after seconds.

    Internal helper shared by both pollers; it reuses the transport's
    client directly so a poll iteration doesn't double-fetch. The
    response body is parsed as ``model``; the header is parsed as
    float seconds.

    Returns:
        ``(parsed_response, suggested_seconds)``. ``suggested_seconds``
        is ``None`` if the header is absent.

    Raises:
        PhantomEnvelopeError: When the body fails to parse or the
            header is non-integer.
        PhantomHttpError: When the HTTP status is non-2xx.
    """
    client = transport._require_client()
    response = await client.get(path)
    if response.status_code >= 400:
        transport._raise_for_status(response)
    try:
        parsed = model.model_validate_json(response.content)
    except ValidationError as exc:
        raise PhantomEnvelopeError(f"could not parse {model.__name__} from {path}: {exc}") from exc
    raw = response.headers.get(X_PHANTOM_SUGGESTED_POLL_AFTER)
    suggested: float | None
    if raw is None:
        suggested = None
    else:
        try:
            suggested = float(int(raw))
        except (TypeError, ValueError) as exc:
            raise PhantomEnvelopeError(
                f"non-integer {X_PHANTOM_SUGGESTED_POLL_AFTER!r}: {raw!r}"
            ) from exc
    return parsed, suggested


__all__ = [
    "DEFAULT_INITIAL_POLL_DELAY_SECONDS",
    "poll_group_until_finished",
    "poll_until",
]
