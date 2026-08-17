"""F9: hop-by-hop and framing headers never reach the upstream.

uvicorn de-frames a chunked request body before the handler sees it, but it
still exposes the raw ``Transfer-Encoding`` header, and the catch-all's strip
set held only ``host`` and ``content-length``. So ``Transfer-Encoding``,
``Connection`` and ``Expect`` were persisted into the synthesized step, and at
egress httpx only ``setdefault``s its own computed framing headers, so h11
gave the persisted ``chunked`` value precedence and emitted BOTH framing
headers over a fixed-length body. The upstream rejected the malformed request,
and because the header was baked into the persisted envelope, every retry
reproduced it until the row died.

The ENVELOPE path had the same hole and no guard at all: a producer-supplied
chain can put ``Transfer-Encoding: chunked`` straight into ``step.headers``,
and admission validates header NAME well-formedness only.

These tests drive the executor, which is where the guarantee lives: the strip
runs for every step regardless of how the step was created, so it covers the
envelope path too. The steps below are producer-authored and never pass
through the catch-all, which is what makes this file the evidence for that
claim. The catch-all's own strip keeps the persisted envelope honest and is
tested in ``test_catch_all_route.py``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from phantom.chain.executor import ChainExecutor, Succeeded
from phantom.chain.parser import parse_json_request
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.models.upload import CapturedValues, UploadRow
from phantom.routing import resolve_route
from phantom.transport import UpstreamRequest, UpstreamResponse

pytestmark = pytest.mark.asyncio

# The forward-as-is host every step below targets.
_HOST = "up.example"
_URL = f"https://{_HOST}/bucket/key"


class _CapturingUpstream:
    """Stub :class:`UpstreamClient` recording the request it was handed."""

    def __init__(self) -> None:
        self.requests: list[UpstreamRequest] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, req: UpstreamRequest) -> UpstreamResponse:
        self.requests.append(req)
        return UpstreamResponse(status=200, headers={}, body=b"")


class _UnusedTokenCache:
    """A TokenCache that must never be touched on a forward-as-is route."""

    async def get(self, endpoint: str, uid: str) -> None:
        raise AssertionError("token_cache.get must not be called on an auth_mode=none route")

    async def set(self, endpoint: str, uid: str, bearer: str, *, source: object) -> None:
        raise AssertionError("token_cache.set must not be called on an auth_mode=none route")

    async def mark_bad(self, endpoint: str, uid: str) -> None:
        raise AssertionError("token_cache.mark_bad must not be called on an auth_mode=none route")


def _executor(client: _CapturingUpstream) -> ChainExecutor:
    """Build an executor over one forward-as-is route matching ``_HOST``."""
    cfg = InstanceCfg(
        id="primary",
        host_prefixes=[_HOST],
        data_dir="primary",
        routes=[RouteCfg(name="up", hosts=[_HOST], auth_mode="none")],
    )
    return ChainExecutor(
        token_cache=_UnusedTokenCache(),
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=cfg,
    )


async def _row(chain_id: UUID, headers: dict[str, str]) -> UploadRow:
    """Build an ``attempting`` row for a one-step PRODUCER-authored chain.

    The envelope goes through ``parse_json_request``, the real admission
    parser, so these headers are exactly what a producer can submit today:
    admission validates header NAME well-formedness and nothing else.

    Args:
        chain_id: The chain's identity and the row's primary key.
        headers: The step headers to persist.

    Returns:
        The persisted-shape :class:`UploadRow`.
    """
    header_json = ",".join(f'"{name}":"{value}"' for name, value in headers.items())
    envelope_json = (
        b'{"chain_id":"'
        + str(chain_id).encode()
        + b'","idempotency_key":"k","steps":['
        + b'{"name":"upload","method":"PUT","url":"'
        + _URL.encode()
        + b'","headers":{'
        + header_json.encode()
        + b'},"body":{"kind":"text","value":"the-body"}}'
        + b"]}"
    )
    envelope, _ = await parse_json_request(
        envelope_json, instance_id="primary", request_id="r", max_buffered_bytes=10_000
    )
    now = datetime.now(tz=UTC)
    return UploadRow(
        chain_id=chain_id,
        instance_id="primary",
        group_id=chain_id,
        multifile_id=chain_id,
        send_order=0,
        route_name="up",
        state="attempting",
        body_location="ram",
        received_at=now,
        updated_at=now,
        endpoint=_HOST,
        uid="u",
        chain_envelope_json=envelope.model_dump_json(),
        captured_values=CapturedValues(),
        current_step_index=0,
        idempotency_key="k",
        capture_reexecution_active=False,
    )


async def _forward(headers: dict[str, str]) -> UpstreamRequest:
    """Drive one producer-authored step and return the captured upstream request."""
    client = _CapturingUpstream()
    result = await _executor(client).execute_one_step(await _row(uuid4(), headers), body_refs={})
    assert isinstance(result, Succeeded), f"the step must send; got {type(result).__name__}"
    return client.requests[0]


def _lowered(request: UpstreamRequest) -> set[str]:
    """The forwarded header names, lower-cased, for case-insensitive assertions."""
    return {name.lower() for name in request.headers}


async def test_executor_strips_every_hop_by_hop_header() -> None:
    """No hop-by-hop or framing header reaches the upstream from an authored chain.

    Objective: the egress guarantee, at the only place that can guarantee it.
    These step headers are producer-authored and never passed through the
    catch-all, so this test is the evidence that the executor is the only
    guard the envelope path has. Success: the ordinary header survives and
    none of the hop-by-hop names does, case-insensitively.
    """
    sent = await _forward(
        {
            "Transfer-Encoding": "chunked",
            "Connection": "keep-alive",
            "Expect": "100-continue",
            "TE": "trailers",
            "X-Custom": "keep",
        }
    )

    assert sent.headers.get("X-Custom") == "keep"
    for dropped in ("transfer-encoding", "connection", "expect", "te"):
        assert dropped not in _lowered(sent), f"{dropped} must not reach the upstream"


async def test_connection_listed_tokens_are_also_stripped() -> None:
    """RFC 7230: the headers ``Connection`` names are hop-by-hop for that connection.

    Objective: ``Connection: keep-alive, X-Hop-Token`` makes ``X-Hop-Token``
    connection-scoped, so forwarding it is exactly the class of error F9 is
    about. Success: neither the ``Connection`` header nor the header it names
    reaches the upstream.
    """
    sent = await _forward(
        {
            "Connection": "keep-alive, X-Hop-Token",
            "X-Hop-Token": "v",
            "X-Custom": "keep",
        }
    )

    assert sent.headers.get("X-Custom") == "keep"
    assert "connection" not in _lowered(sent)
    assert "x-hop-token" not in _lowered(sent)


async def test_aws_chunked_companions_are_forwarded() -> None:
    """The aws-chunked pair describes the BODY and must survive the strip.

    Objective: this is the test that stops a future tidy-up from corrupting
    aws-chunked bodies. ``Content-Encoding: aws-chunked`` declares that the
    forwarded BYTES are in S3's chunked-payload encoding, which uvicorn does
    not decode, and ``x-amz-decoded-content-length`` states that body's
    decoded length. Dropping either makes the upstream read chunk-size lines
    and per-chunk signatures as object content, or leaves it unable to size
    the object.

    Scope: this pins FORWARDING for the ``auth_mode: none`` case. It does NOT
    prove aws-chunked works over an ``aws_sigv4`` route, where the per-chunk
    signatures chain from the CLIENT's seed signature that Phantom replaces.
    That combination is pre-existing and unsupported; F9 neither creates nor
    fixes it.

    Success: both aws-chunked headers reach the upstream and the framing
    header alongside them does not.
    """
    sent = await _forward(
        {
            "Content-Encoding": "aws-chunked",
            "x-amz-decoded-content-length": "1234",
            "Transfer-Encoding": "chunked",
        }
    )

    assert sent.headers.get("Content-Encoding") == "aws-chunked"
    assert sent.headers.get("x-amz-decoded-content-length") == "1234"
    assert "transfer-encoding" not in _lowered(sent)


async def test_exactly_one_framing_mechanism_reaches_the_transport() -> None:
    """The egress guarantee, stated as an assertion rather than as prose.

    Objective: after F9 exactly one framing mechanism reaches the wire, the
    ``Content-Length`` the transport computes over the bytes actually
    forwarded. No ``Transfer-Encoding`` can be present, because the executor
    strips it from every step however the step was created, and no stale
    ``Content-Length`` can be present, because it is in the same set.
    Success: neither framing name is in the outbound header map.
    """
    sent = await _forward(
        {
            "Transfer-Encoding": "chunked",
            "Content-Length": "999999",
            "Host": "phantom.internal",
        }
    )

    names = _lowered(sent)
    assert "transfer-encoding" not in names
    assert "content-length" not in names
    assert "host" not in names, "Host names the connection Phantom terminated, not the message"
