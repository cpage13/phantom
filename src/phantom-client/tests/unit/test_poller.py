"""Unit tests for ``phantom_client.poller`` (chain and group pollers)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import httpx
import pytest
from phantom_client.config import ClientConfig, RetryPolicy
from phantom_client.errors import PhantomNotFoundError, PollDeadlineExceeded
from phantom_client.poller import poll_group_until_finished, poll_until
from phantom_client.transport import Transport


def _row_response(state: str, *, chain_id: str, poll_after: str | None = "1") -> httpx.Response:
    """Build a fake ``GET /v1/admin/chains/{id}`` response.

    The admin endpoint returns :class:`ChainAdminDetail` (not
    :class:`ChainResponse`); this fixture mirrors that shape.
    """
    headers = {"Content-Type": "application/json"}
    if poll_after is not None:
        headers["X-Phantom-Suggested-Poll-After"] = poll_after
    now = "2026-06-10T00:00:00+00:00"
    return httpx.Response(
        200,
        content=json.dumps(
            {
                "chain_id": chain_id,
                "state": state,
                # Cycle-7 task 4.5 row-sourced fields (required on the model).
                "received_at": now,
                "updated_at": now,
                "next_attempt_at": None,
                "sent_at": None,
                "group_id": chain_id,
                "multifile_id": None,
                "send_order": 0,
                "body_location": "ram",
                "last_step_completed": None,
                "captured": [],
                "attempts": 0,
                "last_error": None,
            }
        ),
        headers=headers,
    )


def _make_transport(handler: Any) -> Transport:
    cfg = ClientConfig(
        phantom_url="http://test",
        retry_policy=RetryPolicy(max_attempts=1, backoff_jitter=False),
    )
    return Transport(cfg, transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_polls_until_succeeded() -> None:
    """Polls until state moves into TERMINAL_STATES."""
    chain_id = uuid4()
    sequence = iter(["queued", "attempting", "succeeded"])

    def handler(request: httpx.Request) -> httpx.Response:
        state = next(sequence)
        return _row_response(state, chain_id=str(chain_id), poll_after="0")

    transport = _make_transport(handler)
    await transport.start()
    try:
        response = await poll_until(transport, chain_id, initial_delay_seconds=0.0)
    finally:
        await transport.aclose()
    assert response.state == "succeeded"


@pytest.mark.asyncio
async def test_auth_expired_is_terminal_by_default() -> None:
    """The default stop-set includes auth_expired."""
    chain_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        return _row_response("auth_expired", chain_id=str(chain_id), poll_after="0")

    transport = _make_transport(handler)
    await transport.start()
    try:
        response = await poll_until(transport, chain_id, initial_delay_seconds=0.0)
    finally:
        await transport.aclose()
    assert response.state == "auth_expired"


@pytest.mark.asyncio
async def test_custom_terminal_set_polls_through_auth_expired() -> None:
    """Override with a smaller stop-set to poll through auth_expired."""
    chain_id = uuid4()
    sequence = iter(["auth_expired", "queued", "succeeded"])

    def handler(request: httpx.Request) -> httpx.Response:
        return _row_response(next(sequence), chain_id=str(chain_id), poll_after="0")

    transport = _make_transport(handler)
    await transport.start()
    try:
        response = await poll_until(
            transport,
            chain_id,
            initial_delay_seconds=0.0,
            terminal_states=frozenset({"succeeded", "failed"}),
        )
    finally:
        await transport.aclose()
    assert response.state == "succeeded"


@pytest.mark.asyncio
async def test_honors_suggested_poll_after() -> None:
    """The suggested-poll-after header value is used for the next sleep."""
    chain_id = uuid4()
    poll_afters = iter(["2", "0"])  # first response says wait 2s, second says 0
    states = iter(["queued", "succeeded"])

    def handler(request: httpx.Request) -> httpx.Response:
        return _row_response(next(states), chain_id=str(chain_id), poll_after=next(poll_afters))

    # Use a fake clock substitute by setting initial_delay_seconds to 0 and
    # checking that the request hits the suggested-after path by being patient.
    transport = _make_transport(handler)
    await transport.start()
    started = datetime.now(tz=UTC)
    try:
        # The 2s wait will dominate; we patch asyncio.sleep behavior by
        # asserting elapsed >= 2.0 (real-time test is short enough).
        # Instead use a tighter assertion: deadline at 5s should pass.
        response = await poll_until(
            transport,
            chain_id,
            initial_delay_seconds=0.0,
            deadline=started + timedelta(seconds=5),
        )
    finally:
        await transport.aclose()
    elapsed = (datetime.now(tz=UTC) - started).total_seconds()
    assert response.state == "succeeded"
    # The 2s suggested header should produce at least 2s of waiting.
    assert elapsed >= 1.9


@pytest.mark.asyncio
async def test_deadline_raises() -> None:
    """Deadline elapsed before terminal state raises PollDeadlineExceeded."""
    chain_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        return _row_response("queued", chain_id=str(chain_id), poll_after="60")

    transport = _make_transport(handler)
    await transport.start()
    try:
        with pytest.raises(PollDeadlineExceeded):
            await poll_until(
                transport,
                chain_id,
                initial_delay_seconds=0.0,
                deadline=datetime.now(tz=UTC) + timedelta(milliseconds=200),
            )
    finally:
        await transport.aclose()


@pytest.mark.asyncio
async def test_404_propagates() -> None:
    """404 from /v1/admin/chains/{id} raises PhantomNotFoundError."""
    chain_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404,
            json={
                "error": {
                    "code": "instance_unknown",
                    "message": "no row",
                    "request_id": "r",
                    "instance_id": "unrouted",
                    "details": {},
                }
            },
        )

    from phantom_client.errors import PhantomBadRequestError

    transport = _make_transport(handler)
    await transport.start()
    try:
        with pytest.raises(PhantomBadRequestError):
            await poll_until(transport, chain_id, initial_delay_seconds=0.0)
    finally:
        await transport.aclose()


# ---------------------------------------------------------------------------
# poll_group_until_finished: the group twin (cycle-7 task 5.1).
# ---------------------------------------------------------------------------


def _group_response(
    group_id: str,
    *,
    member_states: list[str],
    poll_after: str | None = "0",
) -> httpx.Response:
    """Build a fake ``GET /v1/admin/groups/{group_id}`` rollup response.

    ``all_finished`` is derived with the structural rule the service
    uses (no member queued or attempting), so a test only declares the
    member states.
    """
    headers = {"Content-Type": "application/json"}
    if poll_after is not None:
        headers["X-Phantom-Suggested-Poll-After"] = poll_after
    now = "2026-06-10T00:00:00+00:00"
    counts: dict[str, int] = {}
    for state in member_states:
        counts[state] = counts.get(state, 0) + 1
    members = [
        {
            "chain_id": str(uuid4()),
            "state": state,
            "received_at": now,
            "sent_at": now if state == "succeeded" else None,
            "attempts": 1,
            "last_error": None,
            "send_order": 0,
            "multifile_id": None,
        }
        for state in member_states
    ]
    all_finished = not any(state in {"queued", "attempting"} for state in member_states)
    return httpx.Response(
        200,
        content=json.dumps(
            {
                "group_id": group_id,
                "total": len(member_states),
                "counts_by_state": counts,
                "all_finished": all_finished,
                "first_received_at": now,
                "last_sent_at": now if "succeeded" in member_states else None,
                "members": members,
            }
        ),
        headers=headers,
    )


@pytest.mark.asyncio
async def test_group_poll_flips_all_finished_mid_poll() -> None:
    """The loop keeps polling while members move, exits on the flip.

    Three observations: two members still moving, one member still
    attempting, then everything finished: the loop must consume all
    three and return the final rollup.
    """
    group_id = uuid4()
    observations = iter(
        [
            ["queued", "queued"],
            ["succeeded", "attempting"],
            ["succeeded", "succeeded"],
        ]
    )
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.url.path == f"/v1/admin/groups/{group_id}"
        return _group_response(str(group_id), member_states=next(observations))

    transport = _make_transport(handler)
    await transport.start()
    try:
        response = await poll_group_until_finished(transport, group_id, initial_delay_seconds=0.0)
    finally:
        await transport.aclose()
    assert calls == 3
    assert response.all_finished is True
    assert response.total == 2
    assert all(m.state == "succeeded" for m in response.members)


@pytest.mark.asyncio
async def test_group_poll_auth_expired_counts_as_finished() -> None:
    """A group of succeeded + auth_expired members is finished.

    Mirrors the service's structural rule: auth_expired does not
    progress without intervention, so the poll must not spin on it.
    """
    group_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        return _group_response(str(group_id), member_states=["succeeded", "auth_expired"])

    transport = _make_transport(handler)
    await transport.start()
    try:
        response = await poll_group_until_finished(transport, group_id, initial_delay_seconds=0.0)
    finally:
        await transport.aclose()
    assert response.all_finished is True
    assert response.counts_by_state["auth_expired"] == 1


@pytest.mark.asyncio
async def test_group_poll_deadline_raises() -> None:
    """Deadline elapsed before all_finished raises PollDeadlineExceeded."""
    group_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        return _group_response(str(group_id), member_states=["queued"], poll_after="60")

    transport = _make_transport(handler)
    await transport.start()
    try:
        with pytest.raises(PollDeadlineExceeded):
            await poll_group_until_finished(
                transport,
                group_id,
                initial_delay_seconds=0.0,
                deadline=datetime.now(tz=UTC) + timedelta(milliseconds=200),
            )
    finally:
        await transport.aclose()


@pytest.mark.asyncio
async def test_group_poll_honors_suggested_poll_after() -> None:
    """The suggested-poll-after header drives the inter-poll sleep."""
    group_id = uuid4()
    observations = iter([["queued"], ["succeeded"]])
    poll_afters = iter(["2", "0"])

    def handler(request: httpx.Request) -> httpx.Response:
        return _group_response(
            str(group_id),
            member_states=next(observations),
            poll_after=next(poll_afters),
        )

    transport = _make_transport(handler)
    await transport.start()
    started = datetime.now(tz=UTC)
    try:
        response = await poll_group_until_finished(
            transport,
            group_id,
            initial_delay_seconds=0.0,
            deadline=started + timedelta(seconds=5),
        )
    finally:
        await transport.aclose()
    elapsed = (datetime.now(tz=UTC) - started).total_seconds()
    assert response.all_finished is True
    # The 2s suggested header should produce at least 2s of waiting.
    assert elapsed >= 1.9


@pytest.mark.asyncio
async def test_group_poll_404_propagates() -> None:
    """An unknown group 404s (the rollup is the one lookup that 404s)."""
    group_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404,
            json={
                "error": {
                    "code": "not_found",
                    "message": f"group {group_id} not found",
                    "request_id": "r",
                    "instance_id": "unrouted",
                    "details": {},
                }
            },
        )

    transport = _make_transport(handler)
    await transport.start()
    try:
        with pytest.raises(PhantomNotFoundError):
            await poll_group_until_finished(transport, group_id, initial_delay_seconds=0.0)
    finally:
        await transport.aclose()
