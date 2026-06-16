"""Transport retries must re-send the SAME idempotency key (hardening, round 9).

The transport's documented dedupe contract is the line that makes its
connect-error retries safe at all: "Every attempt carries the same
``X-Phantom-Idempotency-Key`` so Phantom dedupes a re-arrival"
(``transport.py`` module docstring). The service side honors exactly
that: admission's atomic claim makes a same-key resubmission a 200
replay, and a same-key submission with DIFFERENT content a 409
conflict. So if a retry ever re-derived or mutated the key (or dropped
the header), a flaky wire would turn one producer upload into two
admitted rows - double delivery through the transparent proxy; and if
it mutated the body, the same key would 409. The existing retry pins
(``test_transport.py``) count attempts and assert the surfaced error
type; NONE of them asserts the key's stability across attempts. This
module pins it on both encoding paths.

Also pinned: ``httpx.ConnectTimeout`` is retried. The retry clause
enumerates ``ConnectError`` and the read/write/pool timeouts
explicitly; ``ConnectTimeout`` rides the ``httpx.HTTPError`` catch-all
today, which is behavior-equivalent (retried, surfaced as a transport
error after exhaustion) - but it is the single most common
"Phantom unreachable" shape on a real producer network (dropped SYN), so
its retry behavior must not regress if the exception clauses are ever
reshuffled.

These are PASSING attacks: the transport already behaves; the pins make
the surface regression-proof (loop convention: a passing attack on an
unpinned surface banks as permanent hardening).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from uuid import uuid4

import httpx
import pytest
from phantom_client.config import ClientConfig, RetryPolicy
from phantom_client.errors import PhantomTransportError
from phantom_client.models.chain import (
    ChainBodyJson,
    ChainBodyRef,
    ChainEnvelope,
    ChainStep,
)
from phantom_client.transport import Transport

# Attempts before the wire heals: the first two attempts fail, the
# third succeeds, so the key must survive TWO re-sends.
_FAILURES_BEFORE_SUCCESS: int = 2

# Retry budget comfortably above the failure count.
_MAX_ATTEMPTS: int = 4

# Sub-millisecond backoff keeps the retry loop instant in unit tests.
_TEST_BACKOFF_SECONDS: float = 0.001

# One body_ref so the multipart path is exercised end to end.
_BODY_REF_NAME: str = "body"
_BODY_REF_BYTES: bytes = b"retry-stable-payload"


def _make_envelope(*, with_body_ref: bool) -> ChainEnvelope:
    """A minimal envelope; optionally carrying one body_ref step."""
    chain_id = uuid4()
    steps: list[ChainStep] = [
        ChainStep(
            name="create_file",
            method="POST",
            url="https://files.example.com/v1/files/create",
            body=ChainBodyJson(value={"metadata": {}}),
        ),
    ]
    if with_body_ref:
        steps.append(
            ChainStep(
                name="put_s3",
                method="PUT",
                url="https://files.example.com/v1/files/upload",
                body=ChainBodyRef(name=_BODY_REF_NAME),
            )
        )
    return ChainEnvelope(
        chain_id=chain_id,
        idempotency_key=str(chain_id),
        steps=steps,
    )


def _ok_response_for(envelope: ChainEnvelope) -> httpx.Response:
    """A 202 ChainResponse body the transport parses successfully."""
    body = json.dumps(
        {
            "chain_id": str(envelope.chain_id),
            "state": "queued",
            "last_step_completed": None,
            "captured": [],
        }
    )
    return httpx.Response(202, content=body, headers={"Content-Type": "application/json"})


def _transport_for(
    handler: Callable[[httpx.Request], httpx.Response],
) -> Transport:
    """A transport over a MockTransport wire with a fast retry policy."""
    config = ClientConfig(
        phantom_url="http://phantom.test",
        retry_policy=RetryPolicy(
            max_attempts=_MAX_ATTEMPTS,
            backoff_initial_seconds=_TEST_BACKOFF_SECONDS,
            backoff_max_seconds=_TEST_BACKOFF_SECONDS,
            backoff_jitter=False,
        ),
    )
    return Transport(config, transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
@pytest.mark.parametrize("with_body_refs", [False, True], ids=["json", "multipart"])
async def test_every_retry_attempt_carries_the_same_idempotency_key(
    with_body_refs: bool,
) -> None:
    """The dedupe key must be byte-identical on every attempt, both encodings.

    Attack: fail the first two attempts at the transport layer (connect
    refused), capture the ``X-Phantom-Idempotency-Key`` header of every
    attempt, and require all three observed values to be the one
    documented key (``str(envelope.chain_id)``). A re-derived or
    mutated key would defeat the service-side atomic claim and admit
    the upload twice on a flaky wire.
    """
    envelope = _make_envelope(with_body_ref=with_body_refs)
    seen_keys: list[str | None] = []
    state = {"calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        state["calls"] += 1
        seen_keys.append(request.headers.get("X-Phantom-Idempotency-Key"))
        if state["calls"] <= _FAILURES_BEFORE_SUCCESS:
            raise httpx.ConnectError("refused")
        return _ok_response_for(envelope)

    transport = _transport_for(handler)
    await transport.start()
    try:
        response = await transport.submit_chain(
            envelope,
            body_refs={_BODY_REF_NAME: _BODY_REF_BYTES} if with_body_refs else None,
            uid=None,
            auth_token=None,
            options=None,
        )
    finally:
        await transport.aclose()

    assert response.state == "queued"
    expected_attempts = _FAILURES_BEFORE_SUCCESS + 1
    assert len(seen_keys) == expected_attempts, (
        f"expected {expected_attempts} attempts on the wire, saw {len(seen_keys)}"
    )
    expected_key = str(envelope.chain_id)
    assert seen_keys == [expected_key] * expected_attempts, (
        "the idempotency key drifted across retry attempts "
        f"({seen_keys!r}): the service-side atomic claim dedupes ONLY "
        "same-key re-arrivals, so a drifting key admits the same producer "
        "upload twice on a flaky wire"
    )


@pytest.mark.asyncio
async def test_connect_timeout_is_retried_like_every_transport_failure() -> None:
    """``httpx.ConnectTimeout`` (dropped SYN) must ride the retry loop.

    Attack: raise ``ConnectTimeout`` on the first attempts and require
    the transport to keep trying and succeed - the same posture as
    ``ConnectError``. Today the exception rides the ``httpx.HTTPError``
    catch-all; this pin keeps the behavior if the clauses are ever
    reshuffled, because the unreachable-Phantom producer story depends on it.
    """
    envelope = _make_envelope(with_body_ref=False)
    state = {"calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        state["calls"] += 1
        if state["calls"] <= _FAILURES_BEFORE_SUCCESS:
            raise httpx.ConnectTimeout("SYN dropped")
        return _ok_response_for(envelope)

    transport = _transport_for(handler)
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
    assert state["calls"] == _FAILURES_BEFORE_SUCCESS + 1


@pytest.mark.asyncio
async def test_connect_timeout_exhaustion_surfaces_a_transport_error() -> None:
    """Exhausted ``ConnectTimeout`` retries surface as a transport-class error.

    A downstream adapter's non-4xx fallback predicate is "any non-4xx
    falls back to the direct path", keyed on the ``PhantomTransportError``
    hierarchy, so the exhausted error must stay inside it regardless of
    which except clause classifies the timeout.
    """
    envelope = _make_envelope(with_body_ref=False)

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("SYN dropped")

    transport = _transport_for(handler)
    await transport.start()
    try:
        with pytest.raises(PhantomTransportError):
            await transport.submit_chain(
                envelope,
                body_refs=None,
                uid=None,
                auth_token=None,
                options=None,
            )
    finally:
        await transport.aclose()
