"""Unit tests for F4's presigned-credential precedence rule.

F4 preserves the inbound query string, which creates a combination that used
to work by accident: a client that presigned its request carries an
``X-Amz-Signature`` credential set in the QUERY, and on an ``aws_sigv4`` route
Phantom adds its own ``Authorization: AWS4-HMAC-SHA256`` header. S3 rejects a
request presenting two authentication mechanisms, so the upload would fail
permanently with no signal that Phantom caused it.

ADR-033 settles the precedence: on an ``aws_sigv4`` route Phantom REPLACES the
producer's signature, so the presigned query set is superseded material and is
stripped before signing. The other two auth modes keep it, because there the
conflict is cross-mechanism rather than signature-versus-signature.

The harness mirrors ``tests/unit/test_sigv4_executor.py``: a capturing fake
upstream, a real :class:`SqliteCredentialStore` on a tmp SQLite file, and a
single-step S3 PUT row driven through ``execute_one_step``.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

import pytest
from phantom.chain.executor import ChainExecutor, Succeeded
from phantom.chain.parser import parse_json_request
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.models.credential import HostCredKey, SigningService, SigV4StaticCreds
from phantom.models.upload import CapturedValues, UploadRow
from phantom.routing import resolve_route
from phantom.storage import SqliteTokenCache
from phantom.storage.credential_store import SqliteCredentialStore
from phantom.transport import UpstreamRequest, UpstreamResponse

_HOST = "bucket.s3.us-east-1.amazonaws.com"
_REGION = "us-east-1"

# A full presigned credential set plus one legitimate operation parameter and
# one unrelated ``x-amz-`` parameter that is NOT part of the presigned set.
_PRESIGNED_QUERY = (
    "X-Amz-Algorithm=AWS4-HMAC-SHA256"
    "&X-Amz-Credential=AKIACLIENT%2F20260817%2Fus-east-1%2Fs3%2Faws4_request"
    "&X-Amz-Date=20260817T000000Z"
    "&X-Amz-Expires=900"
    "&X-Amz-Security-Token=CLIENTSESSION"
    "&X-Amz-SignedHeaders=host"
    "&X-Amz-Signature=DEADBEEFCAFE"
    "&x-amz-meta-colour=blue"
    "&partNumber=3"
)
_PRESIGNED_URL = f"https://{_HOST}/key?{_PRESIGNED_QUERY}"

_PRESIGNED_NAMES = (
    "X-Amz-Algorithm",
    "X-Amz-Credential",
    "X-Amz-Date",
    "X-Amz-Expires",
    "X-Amz-Security-Token",
    "X-Amz-SignedHeaders",
    "X-Amz-Signature",
)


class FakeUpstreamClient:
    """Stub :class:`UpstreamClient` recording requests, returning canned responses."""

    def __init__(self) -> None:
        self.requests: list[UpstreamRequest] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, req: UpstreamRequest) -> UpstreamResponse:
        self.requests.append(req)
        return UpstreamResponse(status=200, headers={}, body=b"")


def _static_creds() -> SigV4StaticCreds:
    """A resolved static SigV4 key-pair fixture value (Phantom's own credential)."""
    return SigV4StaticCreds(
        access_key_id="AKIAPHANTOM",
        secret_access_key="wJalrXUtnFEMI/K7MDENG/EXAMPLEKEY",
        region=_REGION,
        service=SigningService.S3,
        session_token=None,
    )


def _instance(auth_mode: str) -> InstanceCfg:
    """One route over the S3 host in the requested auth mode."""
    return InstanceCfg(
        id="primary",
        host_prefixes=["*"],
        data_dir="primary",
        routes=[
            RouteCfg(name="dest", hosts=[_HOST], auth_mode=auth_mode)  # type: ignore[arg-type]
        ],
    )


async def _presigned_row(url: str = _PRESIGNED_URL) -> UploadRow:
    """A single-step PUT row whose step URL carries the presigned query."""
    chain_id = uuid4()
    envelope_json = (
        b'{"chain_id":"'
        + str(chain_id).encode()
        + b'","idempotency_key":"k","steps":['
        + b'{"name":"upload","method":"PUT","url":"'
        + url.encode()
        + b'","body":{"kind":"body_ref","name":"body"}}'
        + b"]}"
    )
    envelope, _ = await parse_json_request(
        envelope_json, instance_id="primary", request_id="r", max_buffered_bytes=10_000
    )
    return UploadRow(
        chain_id=chain_id,
        instance_id="primary",
        group_id=chain_id,
        multifile_id=chain_id,
        send_order=0,
        route_name="dest",
        state="attempting",
        body_location="ram",
        received_at=datetime.now(tz=UTC),
        updated_at=datetime.now(tz=UTC),
        endpoint=_HOST,
        uid="",
        chain_envelope_json=envelope.model_dump_json(),
        captured_values=CapturedValues(),
        current_step_index=0,
        idempotency_key="k",
        capture_reexecution_active=False,
    )


@pytest.fixture
async def cred_store(tmp_path: Path) -> AsyncIterator[SqliteCredentialStore]:
    """Started credential store on a tmp SQLite file."""
    store = SqliteCredentialStore(str(tmp_path / "credential_store.db"))
    await store.start()
    yield store
    await store.stop()


@pytest.fixture
async def token_cache(tmp_path: Path) -> AsyncIterator[SqliteTokenCache]:
    """Started token cache on a tmp SQLite file (the bearer arm needs a slot)."""
    cache = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await cache.start()
    yield cache
    await cache.stop()


def _query_of(request: UpstreamRequest) -> dict[str, list[str]]:
    """Parse the forwarded URL's query for name-level assertions."""
    return parse_qs(urlparse(request.url).query, keep_blank_values=True)


@pytest.mark.asyncio
async def test_presigned_query_is_stripped_before_signing_on_a_sigv4_route(
    cred_store: SqliteCredentialStore,
    token_cache: SqliteTokenCache,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The double-authentication failure F4 would otherwise create is prevented.

    Objective: on an ``aws_sigv4`` route Phantom's signature supersedes the
    client's, so the client's presigned parameter set must be gone from the
    URL that is BOTH signed and forwarded, while every other parameter
    survives. Success is a forwarded request that keeps ``partNumber=3``,
    carries none of the seven presigned parameters, carries exactly one
    ``Authorization`` header, and is accompanied by one INFO record naming the
    chain id and the destination host and no parameter value.
    """
    await cred_store.set(HostCredKey(_HOST), _static_creds(), source="admin_push")
    client = FakeUpstreamClient()
    executor = ChainExecutor(
        token_cache=token_cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=_instance("aws_sigv4"),
        signer_creds=cred_store,
    )
    row = await _presigned_row()

    with caplog.at_level(logging.INFO, logger="phantom.chain.executor"):
        result = await executor.execute_one_step(row, body_refs={"body": b"bytes"})

    assert isinstance(result, Succeeded)
    sent = client.requests[0]
    query = _query_of(sent)
    assert query["partNumber"] == ["3"]
    for name in _PRESIGNED_NAMES:
        assert name not in sent.url
        assert name.lower() not in sent.url.lower()
    # Exactly one Authorization, and it is Phantom's own fresh signature.
    auth_names = [name for name in sent.headers if name.lower() == "authorization"]
    assert auth_names == ["Authorization"]
    assert sent.headers["Authorization"].startswith("AWS4-HMAC-SHA256 ")
    assert "AKIAPHANTOM" in sent.headers["Authorization"]

    records = [r for r in caplog.records if "presigned" in r.getMessage()]
    assert len(records) == 1
    message = records[0].getMessage()
    assert str(row.chain_id) in message
    assert _HOST in message
    assert "DEADBEEFCAFE" not in message
    assert "AKIACLIENT" not in message


@pytest.mark.asyncio
async def test_presigned_query_survives_untouched_on_none_and_bearer_routes(
    cred_store: SqliteCredentialStore,
    token_cache: SqliteTokenCache,
) -> None:
    """The presigned set is forwarded verbatim on ``none`` and ``phantom_bearer``.

    Objective: keep F4's whole point intact. ``none`` IS the forward-as-is
    presigned case, and ``phantom_bearer`` is the deliberate exemption: a
    bearer header beside a presigned query is a CROSS-MECHANISM pairing, not
    the signature-versus-signature conflict ADR-033's replacement rule
    resolves, so Phantom forwards what the client sent.

    What this pins is Phantom's FORWARDING behaviour, NOT upstream acceptance.
    Against an S3-shaped upstream a bearer header plus a presigned query is
    still likely to be rejected for presenting two mechanisms; that is the
    accepted exposure the phase records, not a claim that the combination
    works.
    """
    # The bearer arm returns FailedAuth without a fresh slot, so seed one.
    await token_cache.set(_HOST, "", "Bearer client-token", source="inbound_request")

    for auth_mode in ("none", "phantom_bearer"):
        client = FakeUpstreamClient()
        executor = ChainExecutor(
            token_cache=token_cache,
            upstream_client=client,
            resolve_route=resolve_route,
            clock=lambda: datetime.now(tz=UTC),
            instance=_instance(auth_mode),
            signer_creds=cred_store,
        )
        result = await executor.execute_one_step(
            await _presigned_row(), body_refs={"body": b"bytes"}
        )
        assert isinstance(result, Succeeded), auth_mode
        sent = client.requests[0]
        assert urlparse(sent.url).query == _PRESIGNED_QUERY, auth_mode


@pytest.mark.asyncio
async def test_only_the_closed_presigned_set_is_removed(
    cred_store: SqliteCredentialStore,
    token_cache: SqliteTokenCache,
) -> None:
    """The strip is a closed set, not an ``x-amz-`` prefix match.

    Objective: a client header-signed request also carries
    ``x-amz-content-sha256``, and object metadata rides on ``x-amz-meta-*``
    parameters. Removing anything beyond the seven presigned names would drop
    request data. Success is ``x-amz-meta-colour=blue`` reaching the upstream
    on the ``aws_sigv4`` route that strips the credential set.
    """
    await cred_store.set(HostCredKey(_HOST), _static_creds(), source="admin_push")
    client = FakeUpstreamClient()
    executor = ChainExecutor(
        token_cache=token_cache,
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=_instance("aws_sigv4"),
        signer_creds=cred_store,
    )

    result = await executor.execute_one_step(await _presigned_row(), body_refs={"body": b"b"})

    assert isinstance(result, Succeeded)
    query = _query_of(client.requests[0])
    assert query["x-amz-meta-colour"] == ["blue"]
    assert query["partNumber"] == ["3"]
