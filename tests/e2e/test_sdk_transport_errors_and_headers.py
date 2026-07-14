"""SDK transport-error mapping and ``ResponseHeaders`` over real sockets (audit T9 / G6).

Two evidence layers:

1. A tiny test-owned adversarial loopback HTTP server produces responses the
   real Phantom stack cannot legitimately emit (held reads, malformed success
   bodies, non-JSON errors, synthetic typed error envelopes). It binds from an
   already-open socket and retains ownership until shutdown, so there is no
   allocate-then-close port race. The connect-refused case uses a
   freshly-closed ephemeral port, the portable RST shape on macOS and Linux.
2. The real in-process Phantom stack supplies authentic ``/v1/send`` response
   headers. The test POSTs a public serialized ``ChainEnvelope`` directly with
   a test-owned httpx client, parses the same raw body as the exported
   ``ChainResponse``, and feeds ``response.headers`` to the exported
   ``parse_response_headers``. Removing or corrupting each required header in
   a copied header map must raise ``PhantomEnvelopeError``, proving the parser
   is what produced the fields rather than JSON reconstruction.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
from datetime import datetime
from uuid import uuid4

import httpx
import pytest
from phantom_client import (
    ClientConfig,
    PhantomClient,
    PhantomConnectError,
    PhantomEnvelopeError,
    PhantomTimeoutError,
    PhantomValidationError,
    RetryPolicy,
    Timeouts,
)
from phantom_client.models import parse_response_headers
from phantom_client.models.chain import ChainEnvelope, ChainResponse

from tests.e2e._driver import build_in_memory_upload_envelope
from tests.e2e.helpers.payloads import build_create_file_request
from tests.e2e.helpers.stack import E2EStack

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]

# Fast-failure client knobs: short read deadline and near-zero backoff keep
# every adversarial case bounded well under the suite budget.
_READ_TIMEOUT_SECONDS = 0.4
_BACKOFF_INITIAL_SECONDS = 0.01
_BACKOFF_MAX_SECONDS = 0.02
_TIMEOUT_ATTEMPT_BUDGET = 2
# The service's documented admission polling hint
# (phantom.routes.send.SUGGESTED_POLL_AFTER_SECONDS).
_DOCUMENTED_POLL_AFTER_SECONDS = 5
_REQUIRED_HEADERS = (
    "X-Phantom-Upload-Id",
    "X-Phantom-Group-Id",
    "X-Phantom-Status",
    "X-Phantom-Attempts",
    "X-Phantom-Suggested-Poll-After",
)
_CORRUPT_VALUE_FOR_HEADER = {
    "X-Phantom-Upload-Id": "not-a-uuid",
    "X-Phantom-Group-Id": "also-not-a-uuid",
    "X-Phantom-Status": "bogus_state",
    "X-Phantom-Attempts": "-1",
    "X-Phantom-Suggested-Poll-After": "nan",
}


def _raw_http_response(status_line: str, headers: dict[str, str], body: bytes) -> bytes:
    """Serialize one fixed HTTP/1.1 response with correct framing."""
    lines = [status_line, *[f"{name}: {value}" for name, value in headers.items()]]
    lines.append(f"Content-Length: {len(body)}")
    lines.append("Connection: close")
    return ("\r\n".join(lines) + "\r\n\r\n").encode("ascii") + body


class _AdversarialHttpServer:
    """One-response loopback HTTP server; ``raw_response=None`` holds forever."""

    def __init__(self, raw_response: bytes | None) -> None:
        self._raw_response = raw_response
        self._server: asyncio.AbstractServer | None = None
        self.port: int = 0
        self.request_count = 0

    async def __aenter__(self) -> _AdversarialHttpServer:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        sock.listen(8)
        sock.setblocking(False)
        self.port = int(sock.getsockname()[1])
        # asyncio takes ownership of the still-open socket: bind-to-serve
        # with no window where the port could be reassigned.
        self._server = await asyncio.start_server(self._handle, sock=sock)
        return self

    async def __aexit__(self, *_exc: object) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Serve one connection: consume the full request, then respond or hold."""
        self.request_count += 1
        header_block = b""
        with contextlib.suppress(Exception):
            header_block = await reader.readuntil(b"\r\n\r\n")
        if self._raw_response is None:
            # Hold past the client's read deadline: drain until the client
            # gives up and disconnects, never writing a byte.
            with contextlib.suppress(Exception):
                await reader.read()
        else:
            # Consume the declared body before responding, so the client is
            # never mid-write when the response and close arrive.
            content_length = 0
            for line in header_block.split(b"\r\n"):
                name, _, value = line.partition(b":")
                if name.strip().lower() == b"content-length":
                    content_length = int(value.strip())
            with contextlib.suppress(Exception):
                await reader.readexactly(content_length)
            writer.write(self._raw_response)
            with contextlib.suppress(Exception):
                await writer.drain()
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


def _fast_client(port: int, *, max_attempts: int = 1) -> PhantomClient:
    """Public SDK client aimed at a loopback port with bounded budgets."""
    config = ClientConfig(
        phantom_url=f"http://127.0.0.1:{port}",
        timeouts=Timeouts(
            connect=2.0,
            read=_READ_TIMEOUT_SECONDS,
            write=2.0,
            pool=2.0,
        ),
        retry_policy=RetryPolicy(
            enabled=True,
            max_attempts=max_attempts,
            backoff_initial_seconds=_BACKOFF_INITIAL_SECONDS,
            backoff_max_seconds=_BACKOFF_MAX_SECONDS,
            backoff_jitter=False,
        ),
    )
    return PhantomClient(config)


def _throwaway_envelope() -> ChainEnvelope:
    """Valid public envelope for requests that never reach a real Phantom."""
    chain_id = uuid4()
    request = build_create_file_request(file_name=f"t9-{chain_id.hex[:12]}")
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base="http://127.0.0.1:9",
        local_uuid=chain_id,
    )
    return envelope


async def _submit(client: PhantomClient) -> None:
    # The builder declares one body_ref named "body"; the caller supplies
    # its bytes, mirroring the production submit path.
    async with client:
        await client.submit_chain(
            _throwaway_envelope(),
            body_refs={"body": b"t9-adversarial-body"},
            uid="t9-uid",
            auth_token="Bearer t9-synthetic",
        )


async def test_connect_refused_maps_to_connect_error() -> None:
    """A refused connection surfaces as PhantomConnectError."""
    # A freshly-closed ephemeral port refuses with RST on both macOS and
    # Linux (httpx.ConnectError). A bound-but-unlistened socket is NOT a
    # portable refusal: macOS silently drops the SYN, which is a connect
    # timeout, not a refusal.
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    port = int(probe.getsockname()[1])
    probe.close()
    with pytest.raises(PhantomConnectError):
        await _submit(_fast_client(port))


async def test_held_response_times_out_after_attempt_budget() -> None:
    """A never-responding server raises PhantomTimeoutError after the budget."""
    async with _AdversarialHttpServer(raw_response=None) as server:
        with pytest.raises(PhantomTimeoutError):
            await _submit(_fast_client(server.port, max_attempts=_TIMEOUT_ATTEMPT_BUDGET))
        assert server.request_count == _TIMEOUT_ATTEMPT_BUDGET


async def test_malformed_success_body_maps_to_envelope_error() -> None:
    """A 200 whose body is not a ChainResponse raises PhantomEnvelopeError."""
    body = json.dumps({"not": "a chain response"}).encode()
    raw = _raw_http_response("HTTP/1.1 200 OK", {"Content-Type": "application/json"}, body)
    async with _AdversarialHttpServer(raw) as server:
        with pytest.raises(PhantomEnvelopeError):
            await _submit(_fast_client(server.port))
        assert server.request_count == 1


async def test_non_json_error_body_maps_to_envelope_error() -> None:
    """A 500 with a text body raises PhantomEnvelopeError and never retries."""
    raw = _raw_http_response(
        "HTTP/1.1 500 Internal Server Error",
        {"Content-Type": "text/plain"},
        b"boom",
    )
    async with _AdversarialHttpServer(raw) as server:
        with pytest.raises(PhantomEnvelopeError):
            await _submit(_fast_client(server.port))
        # 5xx is not transport-class: Phantom is the retry engine.
        assert server.request_count == 1


async def test_structured_error_maps_to_typed_http_error_with_headers() -> None:
    """An ADR-010 error envelope maps to its typed error, headers preserved."""
    error_body = json.dumps(
        {
            "error": {
                "code": "envelope_invalid",
                "message": "synthetic adversarial envelope error",
                "request_id": "t9-request",
                "instance_id": "unrouted",
                "details": {},
            }
        }
    ).encode()
    raw = _raw_http_response(
        "HTTP/1.1 422 Unprocessable Entity",
        {"Content-Type": "application/json", "Retry-After": "7"},
        error_body,
    )
    async with _AdversarialHttpServer(raw) as server:
        with pytest.raises(PhantomValidationError) as excinfo:
            await _submit(_fast_client(server.port))
    assert excinfo.value.status_code == 422
    assert excinfo.value.error_code == "envelope_invalid"
    assert excinfo.value.response_headers["retry-after"] == "7"


async def test_live_send_headers_parse_and_match_chain_response(stack: E2EStack) -> None:
    """Real /v1/send headers parse via the exported parser and match the body."""
    bearer = stack.fake_security_token()
    chain_id = uuid4()
    group_id = uuid4()
    envelope, _ = build_in_memory_upload_envelope(
        request=build_create_file_request(file_name=f"t9-live-{chain_id.hex[:12]}"),
        files_api_base=stack.emulator_url,
        local_uuid=chain_id,
    )
    body_refs = {"body": b"t9-live-body"}
    envelope_json = envelope.model_dump_json(by_alias=True)
    files = [
        ("envelope", ("envelope.json", envelope_json.encode("utf-8"), "application/json")),
        *[
            (f"body_refs[{name}]", (name, blob, "application/octet-stream"))
            for name, blob in body_refs.items()
        ],
    ]
    async with httpx.AsyncClient(timeout=10.0) as http:
        response = await http.post(
            f"{stack.phantom_url}/v1/send",
            files=files,
            headers={
                "Authorization": f"Bearer {bearer}",
                "X-Phantom-Uid": "t9-live-uid",
                "X-Phantom-Group-Id": str(group_id),
            },
        )
    assert response.status_code == 202

    chain = ChainResponse.model_validate_json(response.content)
    parsed = parse_response_headers(response.headers)

    # Synchronous-domain oracle: header fields versus the same raw response.
    assert parsed.upload_id == chain.chain_id == chain_id
    assert parsed.status == chain.state
    assert parsed.group_id == group_id
    assert parsed.attempts == 0
    assert parsed.suggested_poll_after_seconds == _DOCUMENTED_POLL_AFTER_SECONDS
    raw_next_attempt = response.headers.get("X-Phantom-Next-Attempt-At")
    if raw_next_attempt:
        assert parsed.next_attempt_at == datetime.fromisoformat(raw_next_attempt)
    else:
        assert parsed.next_attempt_at is None

    # Control: an unmodified copy parses, so matrix failures below are causal.
    intact = httpx.Headers(response.headers)
    assert parse_response_headers(intact) == parsed

    for name in _REQUIRED_HEADERS:
        removed = httpx.Headers(response.headers)
        del removed[name]
        with pytest.raises(PhantomEnvelopeError):
            parse_response_headers(removed)
        corrupted = httpx.Headers(response.headers)
        corrupted[name] = _CORRUPT_VALUE_FOR_HEADER[name]
        with pytest.raises(PhantomEnvelopeError):
            parse_response_headers(corrupted)


async def test_live_send_default_group_is_chain_id(stack: E2EStack) -> None:
    """Without X-Phantom-Group-Id the parsed group equals the chain id."""
    bearer = stack.fake_security_token()
    chain_id = uuid4()
    envelope, _ = build_in_memory_upload_envelope(
        request=build_create_file_request(file_name=f"t9-default-{chain_id.hex[:12]}"),
        files_api_base=stack.emulator_url,
        local_uuid=chain_id,
    )
    body_refs = {"body": b"t9-default-body"}
    envelope_json = envelope.model_dump_json(by_alias=True)
    files = [
        ("envelope", ("envelope.json", envelope_json.encode("utf-8"), "application/json")),
        *[
            (f"body_refs[{name}]", (name, blob, "application/octet-stream"))
            for name, blob in body_refs.items()
        ],
    ]
    async with httpx.AsyncClient(timeout=10.0) as http:
        response = await http.post(
            f"{stack.phantom_url}/v1/send",
            files=files,
            headers={
                "Authorization": f"Bearer {bearer}",
                "X-Phantom-Uid": "t9-default-uid",
            },
        )
    assert response.status_code == 202
    parsed = parse_response_headers(response.headers)
    assert parsed.upload_id == chain_id
    assert parsed.group_id == chain_id
