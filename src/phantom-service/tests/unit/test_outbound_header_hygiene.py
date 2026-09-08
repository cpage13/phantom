"""What the executor is allowed to put on the wire, header by header.

Four defects with one shape: a header name is not a case-sensitive string, and
every place this package treated it as one produced a request that either
disclosed a credential, contradicted its own framing, or could not be signed.

1. **The bearer arm forwarded the producer's own credential.**
   ``headers["Authorization"] = slot.bearer`` wrote a canonical-cased key into a
   case-sensitive dict that already held the producer's lower-cased
   ``authorization`` (ASGI lower-cases inbound names and ``routes/catch_all.py``
   deliberately preserves Authorization), so both reached the upstream on the
   one route whose whole purpose is to substitute Phantom's own.
2. **A foreign STS token survived re-signing.** botocore deletes
   ``X-Amz-Security-Token`` only when its own credential carries one, so an
   inbound client token was left in place AND signed, pairing Phantom's access
   key with somebody else's session and earning a 403 that marks the whole
   HOST's credential slot bad for every producer.
3. **The idempotency header was injected after the hop-by-hop strip and after
   signing**, so it was outside ``SignedHeaders`` on an ``aws_sigv4`` route and
   a producer could name it ``Content-Length`` or ``Host`` and bypass the strip.
4. **A duplicate Content-Type was stamped** on every raw-intake upload that
   declared one, because the presence test was ``"Content-Type" not in headers``
   against a lower-cased key.

Every assertion that matters is made on the BYTES httpx would emit, not on the
dict, because the dict is exactly where the pinned httpx faithfully carries two
spellings of one name through to the wire.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

# botocore ships no py.typed marker; the inline ignore matches the signer module.
from botocore.auth import S3SigV4Auth  # type: ignore[import-untyped]
from botocore.awsrequest import AWSRequest  # type: ignore[import-untyped]
from botocore.credentials import Credentials  # type: ignore[import-untyped]
from phantom.chain.executor import ChainExecutor, Succeeded
from phantom.chain.parser import envelope_from_persistence_json
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.models.credential import HostCredKey, SigningService, SigV4StaticCreds
from phantom.models.upload import CapturedValues, UploadRow
from phantom.routes.catch_all import _synthesize_envelope
from phantom.routing import resolve_route
from phantom.storage import SqliteTokenCache
from phantom.storage.credential_store import SqliteCredentialStore
from phantom.transport import UpstreamRequest, UpstreamResponse

_BEARER_HOST = "files.example.com"
_S3_HOST = "bucket.s3.us-east-1.amazonaws.com"
_REGION = "us-east-1"

# The producer's own credential. It must never appear on the wire from a
# ``phantom_bearer`` route, and the test greps the raw bytes for this literal.
_PRODUCER_BEARER = "Bearer PRODUCER-SECRET-DO-NOT-FORWARD"
_PHANTOM_BEARER = "Bearer PHANTOM-CACHED-TOKEN"

_UID = "user-1"


class FakeUpstreamClient:
    """Stub upstream recording each :class:`UpstreamRequest` and 200-ing it."""

    def __init__(self) -> None:
        self.requests: list[UpstreamRequest] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, req: UpstreamRequest) -> UpstreamResponse:
        self.requests.append(req)
        return UpstreamResponse(status=200, headers={}, body=b"{}")


@pytest.fixture
async def token_cache(tmp_path: Path) -> AsyncIterator[SqliteTokenCache]:
    """A started token cache on a tmp SQLite file."""
    cache = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await cache.start()
    yield cache
    await cache.stop()


@pytest.fixture
async def cred_store(tmp_path: Path) -> AsyncIterator[SqliteCredentialStore]:
    """A started destination-credential store on a tmp SQLite file."""
    store = SqliteCredentialStore(str(tmp_path / "credential_store.db"))
    await store.start()
    yield store
    await store.stop()


def _instance(*, host: str, auth_mode: str) -> InstanceCfg:
    """One route over ``host`` in the requested auth mode."""
    return InstanceCfg(
        id="primary",
        host_prefixes=["*"],
        data_dir="primary",
        routes=[RouteCfg(name="dest", hosts=[host], auth_mode=auth_mode)],  # type: ignore[arg-type]  # the caller passes a literal member of AuthMode
    )


def _row_from_envelope_json(envelope_json: str, *, endpoint: str) -> UploadRow:
    """Wrap a persisted envelope in the ``attempting`` row the executor takes."""
    envelope = envelope_from_persistence_json(envelope_json)
    return UploadRow(
        chain_id=envelope.chain_id,
        instance_id="primary",
        group_id=envelope.chain_id,
        multifile_id=envelope.chain_id,
        send_order=0,
        route_name="dest",
        state="attempting",
        body_location="ram",
        received_at=datetime.now(tz=UTC),
        updated_at=datetime.now(tz=UTC),
        endpoint=endpoint,
        uid=_UID,
        chain_envelope_json=envelope_json,
        captured_values=CapturedValues(),
        current_step_index=0,
        idempotency_key=envelope.idempotency_key,
        capture_reexecution_active=False,
    )


def _authored_envelope_json(
    *,
    url: str,
    headers: dict[str, str],
    idempotency_header: str | None = None,
    body_ref: bool = True,
) -> str:
    """Serialize a PRODUCER-AUTHORED one-step envelope (``templated`` default).

    The other half of the ingress surface: ``POST /v1/send`` carries whatever
    header names the producer chose, and admission's ``_validate_step_headers``
    only checks RFC 7230 token characters, so a producer may spell
    ``authorization`` in lower case exactly as the ASGI path does.
    """
    chain_id = uuid4()
    step: dict[str, object] = {
        "name": "upload",
        "method": "PUT",
        "url": url,
        "headers": headers,
    }
    if body_ref:
        step["body"] = {"kind": "body_ref", "name": "payload", "content_type": "image/jpeg"}
    if idempotency_header is not None:
        step["idempotency_header"] = idempotency_header
    return json.dumps({"chain_id": str(chain_id), "idempotency_key": "idem-key-1", "steps": [step]})


def _raw_intake_envelope_json(*, url: str, headers: dict[str, str]) -> str:
    """Serialize the envelope ``routes/catch_all.py`` actually synthesizes.

    Built through the production synthesizer rather than hand-rolled, so the
    test cannot drift from the shape raw intake really persists: ``templated``
    off, one ``ChainBodyRef`` with NO ``content_type`` (which is what makes the
    model default ``application/octet-stream`` the stamped value), and the
    forwarded headers in whatever casing ASGI handed over.
    """
    envelope = _synthesize_envelope(
        resolved_url=url,
        method="PUT",
        headers=headers,
        has_body=True,
    )
    return envelope.model_dump_json()


def _wire_header_names(request: UpstreamRequest) -> list[bytes]:
    """Return the raw header names httpx would put on the wire, in order.

    The dict is not the wire. Verified against the pinned httpx: a request
    built from a mapping holding both ``authorization`` and ``Authorization``
    emits BOTH raw names, so a duplicate is only visible once the request has
    been built.
    """
    built = httpx.Request(
        request.method,
        request.url,
        headers=request.headers,
        content=request.body or None,
    )
    return [name for name, _ in built.headers.raw]


def _wire_bytes(request: UpstreamRequest) -> bytes:
    """Return the header block httpx would emit, for literal-substring greps."""
    built = httpx.Request(
        request.method,
        request.url,
        headers=request.headers,
        content=request.body or None,
    )
    return b"\r\n".join(name + b": " + value for name, value in built.headers.raw)


def _duplicated_names(request: UpstreamRequest) -> list[str]:
    """Return every header name emitted more than once, case-insensitively."""
    lowered = [name.lower() for name in _wire_header_names(request)]
    return sorted({name.decode() for name in lowered if lowered.count(name) > 1})


# ---------------------------------------------------------------------------
# 1. The bearer arm must not forward the producer's credential.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("shape", "spelling"),
    [("raw_intake", "authorization"), ("authored", "authorization")],
)
@pytest.mark.asyncio
async def test_a_phantom_bearer_route_never_forwards_the_producers_authorization(
    token_cache: SqliteTokenCache,
    shape: str,
    spelling: str,
) -> None:
    """Phantom's bearer REPLACES the producer's; it never rides beside it.

    Objective: on a ``phantom_bearer`` route the producer's own Authorization is
    superseded material, and a second Authorization line is also the input to a
    permanent livelock (rotate the slot, the two values disagree, the upstream
    401s, the executor marks the slot bad, every row for that ``(endpoint, uid)``
    parks, the kicker wakes them and they re-send the same persisted stale header
    beside the fresh bearer). Both ingress shapes are covered because both
    persist a LOWER-CASED name: raw intake because ASGI lower-cases inbound
    header names and the catch-all deliberately preserves Authorization, and a
    producer-authored envelope because admission only checks that a header name
    is made of RFC 7230 token characters.

    Success: exactly ONE authorization line reaches the wire, it carries
    Phantom's cached bearer, and the producer's secret appears nowhere in the
    emitted header bytes.
    """
    url = f"https://{_BEARER_HOST}/v1/files/obj"
    headers = {spelling: _PRODUCER_BEARER}
    envelope_json = (
        _raw_intake_envelope_json(url=url, headers=headers)
        if shape == "raw_intake"
        else _authored_envelope_json(url=url, headers=headers)
    )
    await token_cache.set(_BEARER_HOST, _UID, _PHANTOM_BEARER, source="inbound_request")
    client = FakeUpstreamClient()
    executor = ChainExecutor(
        token_cache=token_cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=_instance(host=_BEARER_HOST, auth_mode="phantom_bearer"),
    )
    row = _row_from_envelope_json(envelope_json, endpoint=_BEARER_HOST)

    result = await executor.execute_one_step(row, body_refs={"payload": b"bytes"})

    assert isinstance(result, Succeeded), result
    sent = client.requests[0]
    assert _duplicated_names(sent) == [], (
        f"no header name may appear twice case-insensitively on the wire; got "
        f"{_wire_header_names(sent)!r}"
    )
    auth_values = [value for name, value in sent.headers.items() if name.lower() == "authorization"]
    assert auth_values == [_PHANTOM_BEARER], (
        f"the bearer arm must leave exactly Phantom's own credential; got {auth_values!r}"
    )
    assert b"PRODUCER-SECRET-DO-NOT-FORWARD" not in _wire_bytes(sent), (
        "the producer's credential must not be disclosed to the upstream on a route "
        "whose whole purpose is to substitute Phantom's own"
    )


# ---------------------------------------------------------------------------
# 2. A foreign STS token must not survive re-signing.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_an_inbound_security_token_is_stripped_when_phantom_has_none(
    cred_store: SqliteCredentialStore,
    token_cache: SqliteTokenCache,
) -> None:
    """A client's ``x-amz-security-token`` is superseded material, like a presigned query.

    Objective: botocore's ``_modify_request_before_signing`` deletes and re-adds
    Authorization, X-Amz-Date and x-amz-content-sha256 unconditionally, but
    deletes X-Amz-Security-Token ONLY when its own credential carries a token.
    With a ``SigV4StaticCreds`` whose ``session_token`` is None the inbound
    header was therefore left in place AND signed, so AWS received Phantom's
    access key paired with an unrelated STS token and returned 403. The executor
    reads that as a bad credential and the slot is keyed on the destination host
    ALONE with no uid (ADR-033), so ONE producer's stale token parked every row
    for that host from EVERY producer.

    Success: the forwarded request carries no security-token header at all, the
    signature's ``SignedHeaders`` list does not name one, and the resulting
    Authorization is byte-identical to an independent botocore signing of the
    same request without the token.
    """
    await cred_store.set(
        HostCredKey(_S3_HOST),
        SigV4StaticCreds(
            access_key_id="AKIAPHANTOM",
            secret_access_key="wJalrXUtnFEMI/K7MDENG/EXAMPLEKEY",
            region=_REGION,
            service=SigningService.S3,
            session_token=None,
        ),
        source="admin_push",
    )
    url = f"https://{_S3_HOST}/key"
    envelope_json = _raw_intake_envelope_json(
        url=url,
        headers={"x-amz-security-token": "FOREIGN-CLIENT-STS-TOKEN", "x-amz-meta-foo": "bar"},
    )
    client = FakeUpstreamClient()
    executor = ChainExecutor(
        token_cache=token_cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=_instance(host=_S3_HOST, auth_mode="aws_sigv4"),
        signer_creds=cred_store,
    )
    row = _row_from_envelope_json(envelope_json, endpoint=_S3_HOST)

    result = await executor.execute_one_step(row, body_refs={"payload": b"bytes"})

    assert isinstance(result, Succeeded), result
    sent = client.requests[0]
    assert not [name for name in sent.headers if name.lower() == "x-amz-security-token"], (
        f"a foreign session token must not reach AWS beside Phantom's access key; got "
        f"{sorted(sent.headers)!r}"
    )
    authorization = sent.headers["Authorization"]
    assert "x-amz-security-token" not in authorization, (
        f"the token must not appear in SignedHeaders either; got {authorization!r}"
    )
    assert b"FOREIGN-CLIENT-STS-TOKEN" not in _wire_bytes(sent)

    # Independent oracle: re-sign EXACTLY the header set that is being
    # forwarded, minus the Authorization line itself, at the same timestamp. A
    # match proves the signature covers the request actually sent, so a
    # surviving token could not have been signed into it.
    expected = AWSRequest(
        method="PUT",
        url=url,
        data=b"bytes",
        headers={
            name: value for name, value in sent.headers.items() if name.lower() != "authorization"
        },
    )
    S3SigV4Auth(
        Credentials("AKIAPHANTOM", "wJalrXUtnFEMI/K7MDENG/EXAMPLEKEY", None),
        "s3",
        _REGION,
    ).add_auth(expected)
    assert authorization == expected.headers["Authorization"], (
        "the executor's signature must be the one botocore produces for the "
        "token-free request, not merely a present header"
    )


# ---------------------------------------------------------------------------
# 3. The idempotency header, ordered correctly and name-checked.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_idempotency_header_is_inside_the_sigv4_signature(
    cred_store: SqliteCredentialStore,
    token_cache: SqliteTokenCache,
) -> None:
    """A declared ``idempotency_header`` must be signed, not appended after signing.

    Objective: injection used to run AFTER ``provider.prepare``, which on an
    ``aws_sigv4`` route has already rebuilt the header map from botocore's
    signed view. A step declaring ``idempotency_header: "x-amz-meta-idempotency"``
    therefore shipped an x-amz-* header absent from ``SignedHeaders``, S3
    answered 403 SignatureDoesNotMatch, and the executor's 403 arm marked the
    HOST's credential slot bad, parking every row for that destination.

    Success: the header is present with the envelope's idempotency key AND its
    name appears in the signature's ``SignedHeaders`` list.
    """
    await cred_store.set(
        HostCredKey(_S3_HOST),
        SigV4StaticCreds(
            access_key_id="AKIAPHANTOM",
            secret_access_key="wJalrXUtnFEMI/K7MDENG/EXAMPLEKEY",
            region=_REGION,
            service=SigningService.S3,
            session_token=None,
        ),
        source="admin_push",
    )
    envelope_json = _authored_envelope_json(
        url=f"https://{_S3_HOST}/key",
        headers={},
        idempotency_header="x-amz-meta-idempotency",
    )
    client = FakeUpstreamClient()
    executor = ChainExecutor(
        token_cache=token_cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=_instance(host=_S3_HOST, auth_mode="aws_sigv4"),
        signer_creds=cred_store,
    )
    row = _row_from_envelope_json(envelope_json, endpoint=_S3_HOST)

    result = await executor.execute_one_step(row, body_refs={"payload": b"bytes"})

    assert isinstance(result, Succeeded), result
    sent = client.requests[0]
    values = [
        value for name, value in sent.headers.items() if name.lower() == "x-amz-meta-idempotency"
    ]
    assert values == ["idem-key-1"], f"the idempotency header must be sent; got {values!r}"
    authorization = sent.headers["Authorization"]
    signed_headers = authorization.split("SignedHeaders=")[1].split(",")[0]
    assert "x-amz-meta-idempotency" in signed_headers.split(";"), (
        f"the idempotency header must be inside the signature; SignedHeaders={signed_headers!r}"
    )


@pytest.mark.parametrize(
    "colliding_name", ["Content-Length", "Host", "Transfer-Encoding", "X-Phantom-Probe"]
)
@pytest.mark.asyncio
async def test_an_idempotency_header_naming_a_framing_header_is_refused(
    token_cache: SqliteTokenCache,
    colliding_name: str,
) -> None:
    """``idempotency_header`` cannot be used to smuggle a header past the strip.

    Objective: the name is producer-supplied and admission checks only RFC 7230
    token characters (``_validate_step_headers`` does not look at this field at
    all), so ``Content-Length``, ``Host``, ``Transfer-Encoding`` and the reserved
    ``X-Phantom-*`` namespace were all admissible. Injected after the strip, each
    reached the wire: verified emitting ``Content-Length: 3`` over an 11-byte
    body and a rewritten ``Host``, defeating both guarantees the hop-by-hop set
    exists to hold, on EVERY attempt because the header is persisted.

    Success: the framing httpx computes is the one that describes the body
    actually forwarded, the Host is the destination's, no ``X-Phantom-*`` header
    reaches the upstream, and no name is emitted twice.
    """
    body = b"ELEVEN-BYTE"
    envelope_json = _authored_envelope_json(
        url=f"https://{_BEARER_HOST}/v1/files/obj",
        headers={},
        idempotency_header=colliding_name,
    )
    await token_cache.set(_BEARER_HOST, _UID, _PHANTOM_BEARER, source="inbound_request")
    client = FakeUpstreamClient()
    executor = ChainExecutor(
        token_cache=token_cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=_instance(host=_BEARER_HOST, auth_mode="phantom_bearer"),
    )
    row = _row_from_envelope_json(envelope_json, endpoint=_BEARER_HOST)

    result = await executor.execute_one_step(row, body_refs={"payload": body})

    assert isinstance(result, Succeeded), result
    sent = client.requests[0]
    assert sent.body == body
    emitted = {
        name.decode().lower(): value
        for name, value in httpx.Request(
            sent.method, sent.url, headers=sent.headers, content=sent.body
        ).headers.raw
    }
    assert emitted["content-length"] == str(len(body)).encode(), (
        f"the only framing on the wire must be the one computed over the forwarded "
        f"bytes; got {emitted['content-length']!r} for a {len(body)}-byte body"
    )
    assert emitted["host"] == _BEARER_HOST.encode(), (
        f"the request must be addressed to its own destination; got {emitted['host']!r}"
    )
    assert not [name for name in emitted if name.startswith("x-phantom-")], (
        f"Phantom's reserved namespace must never be forwarded; got {sorted(emitted)!r}"
    )
    # The universal statement, and the one that covers Transfer-Encoding too:
    # the idempotency key must not appear as the VALUE of any header, because
    # the only names it could have landed under here are ones Phantom already
    # decided must not reach the upstream.
    assert b"idem-key-1" not in emitted.values(), (
        f"the idempotency key was written under a refused name; emitted {emitted!r}"
    )
    assert _duplicated_names(sent) == []


# ---------------------------------------------------------------------------
# 4. One Content-Type, whatever the producer's casing.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_raw_intake_upload_declaring_a_content_type_sends_exactly_one(
    token_cache: SqliteTokenCache,
) -> None:
    """The Content-Type presence test must be case-insensitive.

    Objective: ``"Content-Type" not in substituted_headers`` tested a capitalised
    key against a dict whose keys carry the producer's casing, which on every
    ASGI path is lower case. ``_synthesize_envelope`` builds its ``ChainBodyRef``
    with no ``content_type``, so the field took the model default and the guard
    missed the existing ``content-type: image/jpeg`` and stamped a second key.
    S3 recombines the pair to ``image/jpeg,application/octet-stream`` and answers
    403 SignatureDoesNotMatch, which the executor classifies ``Failed4xx`` and
    the sender terminates as ``failed``: silent data loss on the raw-intake path.
    Even with no signature involved, the stored object's type is corrupted.

    Success: exactly one Content-Type reaches the wire and it is the producer's.
    """
    envelope_json = _raw_intake_envelope_json(
        url=f"https://{_BEARER_HOST}/bucket/photo.jpg",
        headers={"content-type": "image/jpeg"},
    )
    await token_cache.set(_BEARER_HOST, _UID, _PHANTOM_BEARER, source="inbound_request")
    client = FakeUpstreamClient()
    executor = ChainExecutor(
        token_cache=token_cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=_instance(host=_BEARER_HOST, auth_mode="phantom_bearer"),
    )
    row = _row_from_envelope_json(envelope_json, endpoint=_BEARER_HOST)

    result = await executor.execute_one_step(row, body_refs={"payload": b"\xff\xd8\xff"})

    assert isinstance(result, Succeeded), result
    sent = client.requests[0]
    content_types = [
        value for name, value in sent.headers.items() if name.lower() == "content-type"
    ]
    assert content_types == ["image/jpeg"], (
        f"the producer's declared type must survive alone; got {content_types!r}"
    )
    assert _duplicated_names(sent) == []
