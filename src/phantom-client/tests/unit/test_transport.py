"""Unit tests for ``phantom_client.transport.Transport``.

Every test uses :class:`httpx.MockTransport` so no real network I/O
happens. The mock captures request bytes for body-shape assertions.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
from phantom_client.config import ClientConfig, RetryPolicy
from phantom_client.errors import (
    PhantomConnectError,
    PhantomEnvelopeError,
    PhantomServerError,
    PhantomTimeoutError,
    PhantomUnavailableError,
    PhantomValidationError,
)
from phantom_client.models.chain import (
    ChainBodyJson,
    ChainBodyRef,
    ChainCapture,
    ChainEnvelope,
    ChainStep,
)
from phantom_client.models.status import HealthResponse
from phantom_client.transport import Transport, _compute_backoff, _uds_socket_path


def _make_envelope(*, with_body_ref: bool = False) -> ChainEnvelope:
    chain_id = uuid4()
    steps: list[ChainStep] = [
        ChainStep(
            name="create_file",
            method="POST",
            url="https://files.example.com/v2/files",
            body=ChainBodyJson(value={"metadata": {"key_value_store": {}}}),
            capture=[
                ChainCapture(name="upload_url", from_path="$.uploadUrl", ttl_seconds=604_800),
            ],
            idempotency_header="Idempotency-Key",
        ),
    ]
    if with_body_ref:
        steps.append(
            ChainStep(
                name="put_s3",
                method="PUT",
                url="{{create_file.upload_url}}",
                body=ChainBodyRef(name="body"),
            )
        )
    return ChainEnvelope(
        chain_id=chain_id,
        idempotency_key=str(chain_id),
        steps=steps,
    )


def _ok_response_for(envelope: ChainEnvelope) -> httpx.Response:
    body = json.dumps(
        {
            "chain_id": str(envelope.chain_id),
            "state": "queued",
            "last_step_completed": None,
            "captured": [],
        }
    )
    return httpx.Response(202, content=body, headers={"Content-Type": "application/json"})


@pytest.fixture
def transport_factory() -> Callable[..., Transport]:
    """Factory returning a started Transport with an injected MockTransport."""

    def _factory(
        handler: Callable[[httpx.Request], httpx.Response],
        *,
        retry_policy: RetryPolicy | None = None,
    ) -> Transport:
        cfg = ClientConfig(
            phantom_url="http://test",
            retry_policy=retry_policy or RetryPolicy(),
        )
        return Transport(cfg, transport=httpx.MockTransport(handler))

    return _factory


@pytest.mark.asyncio
async def test_submit_chain_json_path(
    transport_factory: Callable[..., Transport],
) -> None:
    """When no body_refs are supplied, the SDK sends JSON content."""
    envelope = _make_envelope(with_body_ref=False)
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["content_type"] = request.headers.get("Content-Type")
        captured["content"] = request.content
        captured["headers"] = dict(request.headers)
        return _ok_response_for(envelope)

    transport = transport_factory(handler)
    await transport.start()
    try:
        response = await transport.submit_chain(
            envelope,
            body_refs=None,
            uid="u",
            auth_token="Bearer t",
            options=None,
        )
    finally:
        await transport.aclose()
    assert response.state == "queued"
    assert captured["url"].endswith("/v1/send")
    assert captured["content_type"] == "application/json"
    payload = json.loads(captured["content"].decode())
    assert payload["chain_id"] == str(envelope.chain_id)
    assert payload["steps"][0]["name"] == "create_file"
    # ADR-010 wire form uses ``from`` not ``from_path``.
    assert payload["steps"][0]["capture"][0]["from"] == "$.uploadUrl"
    # Headers exposed in test.
    assert captured["headers"]["x-phantom-uid"] == "u"
    assert captured["headers"]["authorization"] == "Bearer t"
    assert captured["headers"]["x-phantom-idempotency-key"] == str(envelope.chain_id)


@pytest.mark.asyncio
async def test_submit_chain_multipart_path(
    transport_factory: Callable[..., Transport],
) -> None:
    """When body_refs are supplied, the SDK sends multipart/form-data."""
    envelope = _make_envelope(with_body_ref=True)
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["content_type"] = request.headers.get("Content-Type", "")
        captured["content"] = request.content
        return _ok_response_for(envelope)

    transport = transport_factory(handler)
    await transport.start()
    try:
        await transport.submit_chain(
            envelope,
            body_refs={"body": b"hello world"},
            uid="u",
            auth_token=None,
            options=None,
        )
    finally:
        await transport.aclose()
    assert captured["content_type"].startswith("multipart/form-data; boundary=")
    body = captured["content"]
    # The envelope part header.
    assert b'name="envelope"' in body
    assert b'name="body_refs[body]"' in body
    # The body bytes are embedded verbatim.
    assert b"hello world" in body


@pytest.mark.asyncio
async def test_error_envelope_raises_typed(
    transport_factory: Callable[..., Transport],
) -> None:
    """A non-2xx with a valid error envelope raises the mapped typed exception."""
    envelope = _make_envelope()
    err_body = {
        "error": {
            "code": "saturation_cap",
            "message": "busy",
            "request_id": "r",
            "instance_id": "primary",
            "details": {},
        }
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json=err_body)

    transport = transport_factory(handler)
    await transport.start()
    try:
        with pytest.raises(PhantomUnavailableError) as exc:
            await transport.submit_chain(
                envelope,
                body_refs=None,
                uid="u",
                auth_token=None,
                options=None,
            )
        assert exc.value.error_code == "saturation_cap"
        assert exc.value.status_code == 503
    finally:
        await transport.aclose()


@pytest.mark.asyncio
async def test_validation_error_envelope_raises(
    transport_factory: Callable[..., Transport],
) -> None:
    """envelope_invalid 422 raises PhantomValidationError."""
    envelope = _make_envelope()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            422,
            json={
                "error": {
                    "code": "envelope_invalid",
                    "message": "bad envelope",
                    "request_id": "r",
                    "instance_id": "unrouted",
                    "details": {},
                }
            },
        )

    transport = transport_factory(handler)
    await transport.start()
    try:
        with pytest.raises(PhantomValidationError):
            await transport.submit_chain(
                envelope,
                body_refs=None,
                uid=None,
                auth_token=None,
                options=None,
            )
    finally:
        await transport.aclose()


@pytest.mark.asyncio
async def test_non_2xx_non_json_body_raises_envelope_error(
    transport_factory: Callable[..., Transport],
) -> None:
    """Non-2xx with garbage body raises PhantomEnvelopeError."""
    envelope = _make_envelope()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"<html>broken</html>")

    transport = transport_factory(handler)
    await transport.start()
    try:
        with pytest.raises(PhantomEnvelopeError):
            await transport.submit_chain(
                envelope,
                body_refs=None,
                uid=None,
                auth_token=None,
                options=None,
            )
    finally:
        await transport.aclose()


@pytest.mark.asyncio
async def test_retry_on_connect_error(
    transport_factory: Callable[..., Transport],
) -> None:
    """ConnectError triggers a retry; eventual success returns normally."""
    envelope = _make_envelope()
    state = {"calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        state["calls"] += 1
        if state["calls"] < 2:
            raise httpx.ConnectError("refused")
        return _ok_response_for(envelope)

    transport = transport_factory(
        handler,
        retry_policy=RetryPolicy(
            max_attempts=3,
            backoff_initial_seconds=0.001,
            backoff_max_seconds=0.001,
            backoff_jitter=False,
        ),
    )
    await transport.start()
    try:
        response = await transport.submit_chain(
            envelope,
            body_refs=None,
            uid=None,
            auth_token=None,
            options=None,
        )
    finally:
        await transport.aclose()
    assert response.state == "queued"
    assert state["calls"] == 2


@pytest.mark.asyncio
async def test_retry_exhausted_raises_connect_error(
    transport_factory: Callable[..., Transport],
) -> None:
    """When max_attempts is exhausted, PhantomConnectError surfaces."""
    envelope = _make_envelope()

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    transport = transport_factory(
        handler,
        retry_policy=RetryPolicy(
            max_attempts=2,
            backoff_initial_seconds=0.001,
            backoff_max_seconds=0.001,
            backoff_jitter=False,
        ),
    )
    await transport.start()
    try:
        with pytest.raises(PhantomConnectError):
            await transport.submit_chain(
                envelope,
                body_refs=None,
                uid=None,
                auth_token=None,
                options=None,
            )
    finally:
        await transport.aclose()


@pytest.mark.asyncio
async def test_no_retry_on_5xx(
    transport_factory: Callable[..., Transport],
) -> None:
    """5xx never triggers a retry — Phantom IS the retry engine."""
    envelope = _make_envelope()
    state = {"calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        state["calls"] += 1
        return httpx.Response(
            500,
            json={
                "error": {
                    "code": "internal_error",
                    "message": "boom",
                    "request_id": "r",
                    "instance_id": "primary",
                    "details": {},
                }
            },
        )

    transport = transport_factory(
        handler,
        retry_policy=RetryPolicy(
            max_attempts=5,
            backoff_initial_seconds=0.001,
            backoff_max_seconds=0.001,
            backoff_jitter=False,
        ),
    )
    await transport.start()
    try:
        with pytest.raises(PhantomServerError):
            await transport.submit_chain(
                envelope,
                body_refs=None,
                uid=None,
                auth_token=None,
                options=None,
            )
    finally:
        await transport.aclose()
    assert state["calls"] == 1  # NO retries on HTTP status.


@pytest.mark.asyncio
async def test_timeout_classifies_as_timeout_error(
    transport_factory: Callable[..., Transport],
) -> None:
    """ReadTimeout is classified as PhantomTimeoutError."""
    envelope = _make_envelope()

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow")

    transport = transport_factory(
        handler,
        retry_policy=RetryPolicy(
            max_attempts=1,
            backoff_initial_seconds=0.001,
            backoff_max_seconds=0.001,
            backoff_jitter=False,
        ),
    )
    await transport.start()
    try:
        with pytest.raises(PhantomTimeoutError):
            await transport.submit_chain(
                envelope,
                body_refs=None,
                uid=None,
                auth_token=None,
                options=None,
            )
    finally:
        await transport.aclose()


@pytest.mark.asyncio
async def test_get_json_admin_helper(
    transport_factory: Callable[..., Transport],
) -> None:
    """get_json validates the response against the supplied model."""
    from phantom_client.models.status import HealthResponse

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url).endswith("/v1/healthz")
        return httpx.Response(200, json={"status": "ok", "version": "0.1.0"})

    transport = transport_factory(handler)
    await transport.start()
    try:
        h = await transport.get_json("/v1/healthz", model=HealthResponse)
    finally:
        await transport.aclose()
    assert h.status == "ok"
    assert h.version == "0.1.0"


@pytest.mark.asyncio
async def test_delete_json_passes_filter_body(
    transport_factory: Callable[..., Transport],
) -> None:
    """delete_json sends the filter body and parses BulkDeleteResponse."""
    from phantom_client.models.admin import BulkDeleteResponse, DeleteFilter

    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["content"] = request.content
        return httpx.Response(200, json={"deleted": 7})

    transport = transport_factory(handler)
    await transport.start()
    try:
        result = await transport.delete_json(
            "/v1/admin/chains",
            body=DeleteFilter(state="failed"),
            model=BulkDeleteResponse,
        )
    finally:
        await transport.aclose()
    assert result.deleted == 7
    assert captured["method"] == "DELETE"
    body = json.loads(captured["content"].decode())
    assert body["state"] == "failed"


@pytest.mark.asyncio
async def test_stream_bytes_yields_chunks(
    transport_factory: Callable[..., Transport],
) -> None:
    """stream_bytes yields response chunks until exhausted."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"abcdef")

    transport = transport_factory(handler)
    await transport.start()
    try:
        out = b""
        async for chunk in transport.stream_bytes("/v1/admin/export.tar"):
            out += chunk
    finally:
        await transport.aclose()
    assert out == b"abcdef"


@pytest.mark.asyncio
async def test_stream_bytes_4xx_raises(
    transport_factory: Callable[..., Transport],
) -> None:
    """stream_bytes drains the response and raises on 4xx."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404,
            json={
                "error": {
                    "code": "instance_unknown",
                    "message": "no",
                    "request_id": "r",
                    "instance_id": "unrouted",
                    "details": {},
                }
            },
        )

    transport = transport_factory(handler)
    await transport.start()
    try:
        with pytest.raises(Exception) as exc:
            async for _chunk in transport.stream_bytes("/v1/admin/chains/abc/body"):
                pass
        from phantom_client.errors import PhantomBadRequestError

        assert isinstance(exc.value, PhantomBadRequestError)
    finally:
        await transport.aclose()


@pytest.mark.asyncio
async def test_authorization_redacted_in_logs(
    transport_factory: Callable[..., Transport],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The 'Bearer <token>' value is masked in any DEBUG log record."""
    envelope = _make_envelope()

    def handler(request: httpx.Request) -> httpx.Response:
        return _ok_response_for(envelope)

    transport = transport_factory(handler)
    await transport.start()
    caplog.set_level(logging.DEBUG, logger="phantom_client.transport")
    try:
        await transport.submit_chain(
            envelope,
            body_refs=None,
            uid="u",
            auth_token="Bearer SECRETtokenVALUE",
            options=None,
        )
    finally:
        await transport.aclose()
    # The bearer literal must not appear in any record's formatted text.
    for record in caplog.records:
        rendered = record.getMessage()
        assert "SECRETtokenVALUE" not in rendered, rendered


def test_compute_backoff_no_jitter_doubles() -> None:
    """Backoff doubles on each attempt and is capped."""
    policy = RetryPolicy(
        max_attempts=5,
        backoff_initial_seconds=0.5,
        backoff_max_seconds=2.0,
        backoff_jitter=False,
    )
    assert _compute_backoff(policy, 1) == 0.5
    assert _compute_backoff(policy, 2) == 1.0
    assert _compute_backoff(policy, 3) == 2.0
    assert _compute_backoff(policy, 4) == 2.0  # capped


@pytest.mark.asyncio
async def test_start_idempotent() -> None:
    """Multiple calls to start() reuse the same client."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok", "version": "0.1.0"})

    transport = Transport(
        ClientConfig(phantom_url="http://test"),
        transport=httpx.MockTransport(handler),
    )
    await transport.start()
    await transport.start()  # must not raise
    await transport.aclose()


@pytest.mark.asyncio
async def test_aclose_safe_when_never_started() -> None:
    """aclose() on a never-started transport is a no-op."""
    transport = Transport(ClientConfig(phantom_url="http://test"))
    await transport.aclose()


@pytest.mark.asyncio
async def test_require_client_before_start() -> None:
    """Using the client before start() raises a typed RuntimeError."""
    from phantom_client.models.status import HealthResponse

    transport = Transport(ClientConfig(phantom_url="http://test"))
    with pytest.raises(RuntimeError):
        await transport.get_json("/v1/healthz", model=HealthResponse)


@pytest.mark.asyncio
async def test_submit_chain_emits_grouping_headers(
    transport_factory: Callable[..., Transport],
) -> None:
    """SubmitOptions grouping tags ride the wire as the renamed headers.

    Cycle-7 task 5.2/5.3: group_id emits X-Phantom-Group-Id,
    multifile_id emits X-Phantom-Multifile-Id, order emits
    X-Phantom-Order, and no batch-, target-, or metadata-named header
    is ever emitted.
    """
    from phantom_client.config import SubmitOptions

    envelope = _make_envelope()
    gid = uuid4()
    mid = uuid4()
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["headers"] = dict(request.headers)
        return _ok_response_for(envelope)

    transport = transport_factory(handler)
    await transport.start()
    try:
        await transport.submit_chain(
            envelope,
            body_refs=None,
            uid="u",
            auth_token=None,
            options=SubmitOptions(group_id=gid, multifile_id=mid, order=1),
        )
    finally:
        await transport.aclose()
    headers = captured["headers"]
    assert headers["x-phantom-group-id"] == str(gid)
    assert headers["x-phantom-multifile-id"] == str(mid)
    assert headers["x-phantom-order"] == "1"
    # The dead-header sweep: nothing batch-, target-, or metadata-named.
    stale = [name for name in headers if name.startswith("x-phantom-")]
    assert set(stale) <= {
        "x-phantom-uid",
        "x-phantom-group-id",
        "x-phantom-multifile-id",
        "x-phantom-order",
        "x-phantom-idempotency-key",
    }


# ---------------------------------------------------------------------------
# UDS phantom_url form (the documented unix: contract).
# ---------------------------------------------------------------------------


def test_uds_socket_path_accepts_documented_forms_and_rejects_tcp() -> None:
    """Objective: pin the unix: URL parse.

    Expected: the documented ``unix:/abs/path`` form and the tolerated
    ``unix:///abs/path`` alias both yield the socket path; TCP URLs yield
    ``None`` (unchanged TCP routing).
    """
    assert _uds_socket_path("unix:/var/run/phantom.sock") == "/var/run/phantom.sock"
    assert _uds_socket_path("unix:///var/run/phantom.sock") == "/var/run/phantom.sock"
    assert _uds_socket_path("http://127.0.0.1:8080") is None
    assert _uds_socket_path("https://phantom.example:8443") is None


async def test_uds_missing_socket_maps_to_connect_error(tmp_path: Path) -> None:
    """Objective: a unix: URL with an absent socket is a typed CONNECT failure.

    Expected: ``PhantomConnectError`` — the bare-string form routes through a
    real UDS transport, so a missing socket fails at connect exactly like a
    refused TCP port. Before the unix: routing existed the same call fell
    through to httpx as an unsupported URL scheme and surfaced as the generic
    ``PhantomNetworkError``, which is the defect this pins against.
    """
    config = ClientConfig(
        phantom_url=f"unix:{tmp_path / 'absent-phantom.sock'}",
        retry_policy=RetryPolicy(enabled=False),
    )
    transport = Transport(config)
    await transport.start()
    try:
        with pytest.raises(PhantomConnectError):
            await transport.get_json("/v1/healthz", model=HealthResponse)
    finally:
        await transport.aclose()
