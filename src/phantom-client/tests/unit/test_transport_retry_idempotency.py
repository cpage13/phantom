"""Which transport failures may be re-sent, and with what key (round 9 + F12).

Two contracts share this file, because they are two halves of one question:
when the wire fails, is it safe to send the request again?

**The key.** ``submit_chain`` sends the same ``X-Phantom-Idempotency-Key`` on
every attempt, which is what makes ITS re-sends safe: admission's atomic claim
turns a same-key resubmission into a 200 replay, and a same-key submission
with DIFFERENT content into a 409 conflict. A retry that re-derived or mutated
the key (or dropped the header) would turn one producer upload into two
admitted rows on a flaky wire. The existing retry pins in ``test_transport.py``
count attempts and assert the surfaced error type; none of them pins the key's
stability, so this module does, on both encoding paths.

**The class split (F12).** That promise was once written as a property of the
whole transport, and it never was one: ``submit_chain`` is the only caller that
sets the header, so every admin call retried blind. A read timeout does not
prove Phantom never saw the request, so re-sending a replay POST can re-queue a
row that has since succeeded and deliver the upload twice. Failures are now
partitioned into never-landed (``ConnectError``, ``ConnectTimeout``,
``PoolTimeout``, and the two unbuildable-request errors), which retry always,
and may-have-landed (read/write timeouts plus the ``HTTPError`` catch-all's
members), which retry only for callers that opt in.

The retry decision and the error TYPE are independent axes: ``ConnectTimeout``
and ``PoolTimeout`` are never-landed AND timeouts, so they keep their
``PhantomTimeoutError`` mapping, and clause ORDER in the loop is what preserves
it, since both subclass ``TimeoutException``.

The key tests and the never-landed tests are PASSING attacks that pin existing
behaviour; the three may-have-landed tests failed before F12 and are its
regression witnesses.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from uuid import uuid4

import httpx
import pytest
from phantom_client.config import ClientConfig, RetryPolicy
from phantom_client.errors import (
    PhantomConnectError,
    PhantomNetworkError,
    PhantomTimeoutError,
    PhantomTransportError,
)
from phantom_client.models.chain import (
    ChainBodyJson,
    ChainBodyRef,
    ChainEnvelope,
    ChainStep,
)
from phantom_client.transport import Transport
from pydantic import BaseModel, ConfigDict

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

    Attack: raise ``ConnectTimeout`` on the first attempts and require the
    transport to keep trying and succeed, the same posture as ``ConnectError``.
    Since F12 the exception rides the FIRST clause, the never-landed pair, and
    that placement is load-bearing: ``ConnectTimeout`` subclasses
    ``TimeoutException``, so a clause split written in the wrong order would
    put it behind the gated may-have-landed clause and silently stop retrying
    the single most common "Phantom unreachable" shape.
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
    """Exhausted ``ConnectTimeout`` retries surface as ``PhantomTimeoutError``.

    A downstream adapter's non-4xx fallback predicate is "any non-4xx
    falls back to the direct path", keyed on the ``PhantomTransportError``
    hierarchy, so the exhausted error must stay inside it; and since a
    dropped SYN is a timeout condition, it must carry the timeout type,
    not the generic network catch-all.
    """
    envelope = _make_envelope(with_body_ref=False)

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("SYN dropped")

    transport = _transport_for(handler)
    await transport.start()
    try:
        with pytest.raises(PhantomTimeoutError) as excinfo:
            await transport.submit_chain(
                envelope,
                body_refs=None,
                uid=None,
                auth_token=None,
                options=None,
            )
        assert isinstance(excinfo.value, PhantomTransportError)
    finally:
        await transport.aclose()


# ---------------------------------------------------------------------------
# F12: only the never-landed failure class retries without an opt-in.
# ---------------------------------------------------------------------------

# A replay path, which is the mutating admin POST the F12 interleaving is
# about: its CAS re-queues a row from seven states including ``succeeded``.
_REPLAY_PATH: str = "/v1/admin/chains/00000000-0000-0000-0000-000000000001/replay"

# A token-push path, which is what ``put_json`` actually carries: a pure
# overwrite of one slot, so a second write stores the same value.
_TOKEN_PATH: str = "/v1/admin/tokens/files.example.com/user-1"

# A bulk-delete path, the second non-idempotent mutating route: its filter is
# re-evaluated against the live table on every call.
_BULK_DELETE_PATH: str = "/v1/admin/chains"

# The contract for a may-have-landed failure on a mutating call: exactly one
# request reaches the wire, and the caller decides what to do about it.
_ONE_ATTEMPT: int = 1


class _EmptyModel(BaseModel):
    """A permissive response model for the generic-helper tests."""

    model_config = ConfigDict(extra="ignore")


def _empty_json_response() -> httpx.Response:
    """A 200 with an empty JSON object, parsable by :class:`_EmptyModel`."""
    return httpx.Response(200, content="{}", headers={"Content-Type": "application/json"})


def _counting_raiser(
    exc: Exception,
    state: dict[str, int],
) -> Callable[[httpx.Request], httpx.Response]:
    """A handler that counts attempts and always raises ``exc``.

    Args:
        exc: The transport-layer exception every attempt raises.
        state: A mutable counter dict with a ``calls`` key.

    Returns:
        The mock-transport handler.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        state["calls"] += 1
        raise exc

    return handler


@pytest.mark.asyncio
async def test_a_read_timeout_on_a_mutating_admin_post_is_not_retried() -> None:
    """A read timeout on a replay POST is surfaced, not re-sent (F12).

    Objective: the defect itself. A read timeout does NOT prove Phantom never
    saw the request: the replay may have landed, re-queued the row and
    delivered it, with only the response lost. Re-sending then re-queues a row
    that has since succeeded and delivers the upload a SECOND time.

    Success: exactly ONE request is attempted and ``PhantomTimeoutError`` is
    raised.

    Pre-fix failure mode: three attempts, because ``_send_with_retry`` retried
    every ``TimeoutException`` for every verb.
    """
    state = {"calls": 0}
    transport = _transport_for(_counting_raiser(httpx.ReadTimeout("response lost"), state))
    await transport.start()
    try:
        with pytest.raises(PhantomTimeoutError):
            await transport.post_json(_REPLAY_PATH, body=None, model=_EmptyModel)
    finally:
        await transport.aclose()

    assert state["calls"] == _ONE_ATTEMPT, (
        f"a mutating admin POST that MAY have landed must be attempted once; "
        f"the wire saw {state['calls']} attempts"
    )


@pytest.mark.asyncio
async def test_a_remote_protocol_error_on_a_mutating_admin_post_is_not_retried() -> None:
    """A server disconnect mid-response is not retried either (F12).

    Objective: the third clause. A server that executes the replay and then
    drops the connection raises ``httpx.RemoteProtocolError``, not a timeout,
    so a split that repairs only the timeout clause leaves F12's own harm
    channel open through the catch-all.

    Success: exactly ONE attempt, and ``PhantomNetworkError`` is raised.

    Pre-fix failure mode: three attempts, via the untouched catch-all.
    """
    state = {"calls": 0}
    transport = _transport_for(
        _counting_raiser(httpx.RemoteProtocolError("server disconnected"), state)
    )
    await transport.start()
    try:
        with pytest.raises(PhantomNetworkError):
            await transport.post_json(_REPLAY_PATH, body=None, model=_EmptyModel)
    finally:
        await transport.aclose()

    assert state["calls"] == _ONE_ATTEMPT, (
        f"a server disconnect after the request executed must not be re-sent; "
        f"the wire saw {state['calls']} attempts"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("with_body_refs", [False, True], ids=["json", "multipart"])
async def test_a_read_timeout_on_submit_chain_is_still_retried(with_body_refs: bool) -> None:
    """``submit_chain`` keeps its read-timeout retry, on BOTH encodings.

    Objective: the carve-out. ``submit_chain`` is the one call that really does
    send ``X-Phantom-Idempotency-Key`` on every attempt, and admission's atomic
    claim turns a re-arrival into a 200 replay, so a may-have-landed retry is
    safe there and nowhere else.

    Parametrised over both encodings deliberately: ``submit_chain`` calls the
    retry helper once per arm, so an executor that edits one arm would leave
    every upload of the other encoding silently un-retried.

    Success: the full retry budget is used and every attempt carries the same
    key.
    """
    envelope = _make_envelope(with_body_ref=with_body_refs)
    seen_keys: list[str | None] = []
    state = {"calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        state["calls"] += 1
        seen_keys.append(request.headers.get("X-Phantom-Idempotency-Key"))
        if state["calls"] <= _FAILURES_BEFORE_SUCCESS:
            raise httpx.ReadTimeout("response lost")
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
    arm = "multipart" if with_body_refs else "JSON"
    assert state["calls"] == expected_attempts, (
        f"submit_chain must keep retrying a read timeout on the {arm} arm; "
        f"the wire saw {state['calls']} attempts"
    )
    assert seen_keys == [str(envelope.chain_id)] * expected_attempts


@pytest.mark.asyncio
async def test_a_connect_timeout_is_still_retried_on_a_mutating_post() -> None:
    """The never-landed class is not caught by the new gate.

    Objective: the ordering trap. ``ConnectTimeout`` subclasses
    ``TimeoutException``, so a clause split written in the wrong order lets the
    broad may-have-landed clause swallow the single most common "Phantom
    unreachable" shape and stop retrying it.

    Success: the full retry budget is used for a ``ConnectTimeout`` on
    ``post_json``, AND the exhausted error is ``PhantomTimeoutError`` rather
    than ``PhantomConnectError``: the retry decision and the error type are
    independent axes, and the committed pin at
    ``test_connect_timeout_exhaustion_surfaces_a_transport_error`` depends on
    the mapping this assertion repeats.
    """
    state = {"calls": 0}
    transport = _transport_for(_counting_raiser(httpx.ConnectTimeout("SYN dropped"), state))
    await transport.start()
    try:
        with pytest.raises(PhantomTimeoutError) as excinfo:
            await transport.post_json(_REPLAY_PATH, body=None, model=_EmptyModel)
        assert not isinstance(excinfo.value, PhantomConnectError), (
            "a connect timeout must keep its timeout typing; re-typing it as a "
            "connect error turns the committed exhaustion pin red"
        )
    finally:
        await transport.aclose()

    assert state["calls"] == _MAX_ATTEMPTS, (
        f"a request that was never delivered must still be retried on a mutating "
        f"POST; the wire saw {state['calls']} attempts"
    )


@pytest.mark.asyncio
async def test_a_pool_timeout_is_still_retried_on_a_mutating_post() -> None:
    """The second never-landed timeout subclass keeps its retry too.

    Objective: ``PoolTimeout`` means the client never got a connection out of
    its own pool, so the request was never delivered. It is easy to forget
    because it is a ``TimeoutException`` that has nothing to do with the wire.

    Success: the full retry budget, and ``PhantomTimeoutError`` for the same
    typing reason as the connect-timeout case.
    """
    state = {"calls": 0}
    transport = _transport_for(_counting_raiser(httpx.PoolTimeout("no connection"), state))
    await transport.start()
    try:
        with pytest.raises(PhantomTimeoutError):
            await transport.post_json(_REPLAY_PATH, body=None, model=_EmptyModel)
    finally:
        await transport.aclose()

    assert state["calls"] == _MAX_ATTEMPTS, (
        f"a pool timeout never delivered the request and must still be retried; "
        f"the wire saw {state['calls']} attempts"
    )


@pytest.mark.asyncio
async def test_a_read_timeout_on_a_put_is_retried() -> None:
    """``put_json`` opts back in, because its callers are pure overwrites.

    Objective: the one place the split re-enables a may-have-landed retry on a
    mutating call. ``put_json``'s only callers are the token and credential
    pushes, which write one slot's value; a second write stores the same value,
    so a lost response costs nothing and an automatic retry is worth keeping.

    Success: the full retry budget is used for a ``ReadTimeout`` through
    ``put_json``. Driven at the transport level because this file builds a bare
    :class:`Transport`, while ``push_token`` is a ``PhantomClient`` method.
    """
    state = {"calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        state["calls"] += 1
        if state["calls"] <= _FAILURES_BEFORE_SUCCESS:
            raise httpx.ReadTimeout("response lost")
        return _empty_json_response()

    transport = _transport_for(handler)
    await transport.start()
    try:
        await transport.put_json(_TOKEN_PATH, body={"token": "Bearer x"})
    finally:
        await transport.aclose()

    assert state["calls"] == _FAILURES_BEFORE_SUCCESS + 1, (
        f"a token push is a pure overwrite and keeps its retry; the wire saw "
        f"{state['calls']} attempts"
    )


@pytest.mark.asyncio
async def test_delete_paths_do_not_retry_a_read_timeout() -> None:
    """Both DELETE helpers stay conservative on a may-have-landed failure.

    Objective: pin the deliberately conservative answer. ``delete_json``'s
    caller is ``bulk_delete``, which is genuinely unsafe: its
    ``{state, route, since, instance}`` filter is re-evaluated against the live
    table on every call, so a retry sweeps up rows the first request never saw.
    ``delete_no_body``'s callers are all convergent, and it is kept off anyway
    for consistency with the other DELETE helper, at a cost of one manual
    operator retry.

    Success: exactly one attempt through each helper.
    """
    state = {"calls": 0}
    transport = _transport_for(_counting_raiser(httpx.ReadTimeout("response lost"), state))
    await transport.start()
    try:
        with pytest.raises(PhantomTimeoutError):
            await transport.delete_json(
                _BULK_DELETE_PATH, body={"state": "succeeded"}, model=_EmptyModel
            )
        assert state["calls"] == _ONE_ATTEMPT, (
            f"bulk_delete's filter re-evaluates per call, so a retry must not be "
            f"automatic; the wire saw {state['calls']} attempts"
        )

        state["calls"] = 0
        with pytest.raises(PhantomTimeoutError):
            await transport.delete_no_body(_TOKEN_PATH)
        assert state["calls"] == _ONE_ATTEMPT, (
            f"the body-less DELETE helper is kept off for consistency; the wire saw "
            f"{state['calls']} attempts"
        )
    finally:
        await transport.aclose()
