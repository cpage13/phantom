"""Unit tests for phantom.transport.httpx_client."""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest
from phantom.transport import HttpxUpstreamClient, UpstreamRequest

# How long the stalling transport below would hang for if nothing bounded it.
# An hour, so a test that passes cannot be passing by accident of duration.
_STALL_SECONDS = 3600.0

# The whole-operation deadline the stall tests configure. Short enough that a
# working bound costs the suite nothing, long enough not to fire on a loaded
# machine before the transport is even entered.
_SHORT_DEADLINE_SECONDS = 0.25

# The test's own guard. A client that does not bound the attempt hangs, and a
# hanging test is a wedged suite rather than a failure, so the guard turns the
# hang into a visible TimeoutError. Well clear of _SHORT_DEADLINE_SECONDS so
# only a MISSING bound can trip it.
_GUARD_SECONDS = 5.0


class _StallingTransport(httpx.AsyncBaseTransport):
    """A transport that accepts the request and then never answers.

    Stands in for the upstream S9-4 describes: one whose socket stays alive
    and whose response never completes. httpx's ``timeout=`` is enforced by
    the NETWORK transport, so a transport that ignores it is exactly the
    shape where only a whole-operation bound in ``send`` can end the attempt.
    """

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Sleep past any plausible deadline instead of responding."""
        await asyncio.sleep(_STALL_SECONDS)
        raise AssertionError("the stalling transport must never answer")


@pytest.mark.asyncio
async def test_send_roundtrip() -> None:
    """A simple GET round-trips through httpx.MockTransport."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    client = HttpxUpstreamClient(timeout_seconds=5.0, transport=transport)
    await client.start()
    try:
        response = await client.send(UpstreamRequest(method="GET", url="https://example.com/"))
        assert response.status == 200
        assert b'"ok": true' in response.body or b'"ok":true' in response.body
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_send_post_with_body() -> None:
    """POST with a body forwards the bytes verbatim."""
    received: dict[str, bytes] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        received["body"] = request.read()
        return httpx.Response(202)

    transport = httpx.MockTransport(handler)
    client = HttpxUpstreamClient(timeout_seconds=5.0, transport=transport)
    await client.start()
    try:
        await client.send(UpstreamRequest(method="POST", url="https://e/upload", body=b"abc"))
        assert received["body"] == b"abc"
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_send_requires_start() -> None:
    """``send`` before ``start`` raises RuntimeError."""
    client = HttpxUpstreamClient(timeout_seconds=1.0)
    with pytest.raises(RuntimeError):
        await client.send(UpstreamRequest(method="GET", url="https://x/"))


@pytest.mark.asyncio
async def test_per_request_timeout_propagates_to_httpx() -> None:
    """``UpstreamRequest.timeout_seconds`` overrides the client default (§5.2).

    httpx exposes its effective timeout via ``request.extensions['timeout']``;
    we capture that in the MockTransport handler and assert the per-request
    override won when set.
    """
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["timeout"] = request.extensions.get("timeout")
        return httpx.Response(200)

    transport = httpx.MockTransport(handler)
    client = HttpxUpstreamClient(timeout_seconds=5.0, transport=transport)
    await client.start()
    try:
        # Per-request override of 600 s.
        await client.send(
            UpstreamRequest(method="GET", url="https://example.com/x", timeout_seconds=600.0)
        )
    finally:
        await client.stop()
    # httpx stores the timeout as a dict with read/write/connect/pool keys.
    timeout_ext = captured["timeout"]
    assert timeout_ext is not None
    assert isinstance(timeout_ext, dict)
    # All four keys should reflect the override (httpx applies the scalar
    # uniformly when given an int/float).
    for k in ("read", "write", "connect", "pool"):
        assert timeout_ext[k] == 600.0


@pytest.mark.asyncio
async def test_default_timeout_used_when_no_override() -> None:
    """No per-request override -> client uses its constructor default."""
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["timeout"] = request.extensions.get("timeout")
        return httpx.Response(200)

    transport = httpx.MockTransport(handler)
    client = HttpxUpstreamClient(timeout_seconds=5.0, transport=transport)
    await client.start()
    try:
        await client.send(UpstreamRequest(method="GET", url="https://example.com/x"))
    finally:
        await client.stop()
    timeout_ext = captured["timeout"]
    assert timeout_ext is not None
    assert isinstance(timeout_ext, dict)
    for k in ("read", "write", "connect", "pool"):
        assert timeout_ext[k] == 5.0


@pytest.mark.asyncio
async def test_send_bounds_the_whole_attempt_not_each_socket_operation() -> None:
    """S9-4: an attempt that never completes ends at the configured deadline.

    Objective: prove ``send`` carries a WHOLE-OPERATION bound. The float handed
    to ``httpx.AsyncClient`` expands to per-socket-operation timeouts, so an
    upstream that keeps dribbling bytes never trips one and the attempt runs
    forever, holding a sender worker, its saturation slot and its declared
    bytes until the process restarts.

    Expected outcome: the send raises ``httpx.TimeoutException`` (the class the
    executor already classifies as a retryable ``FailedNetwork``) at roughly
    the configured deadline, well inside the test's own guard. Without the
    bound the guard fires instead and the test fails with a bare TimeoutError.
    """
    client = HttpxUpstreamClient(
        timeout_seconds=_SHORT_DEADLINE_SECONDS, transport=_StallingTransport()
    )
    await client.start()
    started = time.monotonic()
    try:
        async with asyncio.timeout(_GUARD_SECONDS):
            with pytest.raises(httpx.TimeoutException):
                await client.send(UpstreamRequest(method="PUT", url="https://example.com/obj"))
    finally:
        await client.stop()
    assert time.monotonic() - started < _GUARD_SECONDS


@pytest.mark.asyncio
async def test_per_request_timeout_also_bounds_the_whole_attempt() -> None:
    """S9-4: the per-route override sets the whole-operation deadline too.

    Objective: the ``ResolvedRoute`` override (§5.2) must not be a per-socket
    knob while the constructor default is a deadline; a route with its own
    timeout is the S3-PUT case the finding is about.

    Expected outcome: with a long constructor default and a short per-request
    override, the stalled attempt ends on the OVERRIDE, raising
    ``httpx.TimeoutException`` inside the guard.
    """
    client = HttpxUpstreamClient(timeout_seconds=_STALL_SECONDS, transport=_StallingTransport())
    await client.start()
    try:
        async with asyncio.timeout(_GUARD_SECONDS):
            with pytest.raises(httpx.TimeoutException):
                await client.send(
                    UpstreamRequest(
                        method="PUT",
                        url="https://example.com/obj",
                        timeout_seconds=_SHORT_DEADLINE_SECONDS,
                    )
                )
    finally:
        await client.stop()
