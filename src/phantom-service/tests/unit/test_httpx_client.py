"""Unit tests for phantom.transport.httpx_client."""

from __future__ import annotations

import httpx
import pytest
from phantom.transport import HttpxUpstreamClient, UpstreamRequest


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
