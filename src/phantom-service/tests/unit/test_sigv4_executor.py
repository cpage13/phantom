"""Unit tests for the executor's ``aws_sigv4`` signer arm (Phase 2 Task 2.1).

Proves the two load-bearing properties of the arm:

1. **A valid SigV4 signature is produced.** The arm re-signs the outbound request
   with botocore ``SigV4Auth``; the test independently re-signs the SAME request
   with botocore and asserts the executor's ``Authorization`` header is
   byte-identical — i.e. a real, correct AWS4-HMAC-SHA256 signature, not merely a
   present header. The forwarded body stays byte-identical (transparent-proxy
   invariant).

2. **A SigV4 ``FailedAuth`` PARKS in ``auth_expired`` (NOT terminal).** A missing
   credential store, a missing slot, a bad slot, and an upstream 403 each return
   the SAME typed ``FailedAuth`` the ``phantom_bearer`` path returns. ``FailedAuth``
   is the executor result the sender's ``_on_auth_failure`` routes to the
   ``auth_expired`` state, and ``auth_expired`` is deliberately EXCLUDED from
   :data:`TERMINAL_STATES` — so the row parks and waits for a credential re-push
   rather than dying. The 403 path additionally flips the cred slot to ``bad`` so
   it stays parked until a re-push freshens it.

Construction mirrors ``tests/unit/test_executor.py`` (FakeUpstreamClient,
``_instance``, the envelope/row builders) and ``tests/unit/test_credential_store.py``
(the real :class:`SqliteCredentialStore` on a tmp SQLite file).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

# botocore ships no py.typed marker; the inline ignores match the signer module
# and the emulator's s3 router.
from botocore.auth import SigV4Auth  # type: ignore[import-untyped]
from botocore.awsrequest import AWSRequest  # type: ignore[import-untyped]
from botocore.credentials import Credentials  # type: ignore[import-untyped]
from phantom.chain.executor import ChainExecutor, FailedAuth, Succeeded
from phantom.chain.parser import parse_json_request
from phantom.chain.sigv4_signer import SigV4SigningError, sign_sigv4
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.models.credential import (
    HostCredKey,
    ProfileRefCred,
    SigningService,
    SigV4StaticCreds,
)
from phantom.models.upload import CapturedValues, UploadRow
from phantom.routing import resolve_route
from phantom.storage.credential_store import SqliteCredentialStore
from phantom.storage.interface import TERMINAL_STATES
from phantom.transport import UpstreamRequest, UpstreamResponse

_S3_HOST = "bucket.s3.us-east-1.amazonaws.com"
_S3_URL = f"https://{_S3_HOST}/key"
_REGION = "us-east-1"
_SERVICE = "s3"

# -------- fakes (mirror test_executor.py) ------------------------------------


class FakeUpstreamClient:
    """Stub :class:`UpstreamClient` recording requests, returning canned responses."""

    def __init__(self) -> None:
        self.requests: list[UpstreamRequest] = []
        self._responses: list[UpstreamResponse] = []

    def push(self, status: int, body: bytes = b"", headers: dict[str, str] | None = None) -> None:
        """Queue one canned response."""
        self._responses.append(UpstreamResponse(status=status, headers=headers or {}, body=body))

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, req: UpstreamRequest) -> UpstreamResponse:
        self.requests.append(req)
        if not self._responses:
            raise AssertionError("No response queued for upstream call")
        return self._responses.pop(0)


# -------- helpers ------------------------------------------------------------


def _static_creds(*, session_token: str | None = None) -> SigV4StaticCreds:
    """A resolved static SigV4 key-pair fixture value."""
    return SigV4StaticCreds(
        access_key_id="AKIAEXAMPLE",
        secret_access_key="wJalrXUtnFEMI/K7MDENG/EXAMPLEKEY",
        region=_REGION,
        service=SigningService.S3,
        session_token=session_token,
    )


def _sigv4_instance() -> InstanceCfg:
    """Instance with one ``aws_sigv4`` route matching the S3 host."""
    return InstanceCfg(
        id="primary",
        host_prefixes=["*"],
        data_dir="primary",
        routes=[RouteCfg(name="s3", hosts=[_S3_HOST], auth_mode="aws_sigv4")],
    )


async def _s3_put_row(body: bytes) -> UploadRow:
    """A single-step S3 PUT chain row whose step body is a body_ref ``body``."""
    chain_id = uuid4()
    envelope_json = (
        b'{"chain_id":"'
        + str(chain_id).encode()
        + b'","idempotency_key":"k","steps":['
        + b'{"name":"put_s3","method":"PUT","url":"'
        + _S3_URL.encode()
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
        route_name="s3",
        state="attempting",
        body_location="ram",
        received_at=datetime.now(tz=UTC),
        updated_at=datetime.now(tz=UTC),
        endpoint=_S3_HOST,
        uid="unused-under-sigv4",
        chain_envelope_json=envelope.model_dump_json(),
        captured_values=CapturedValues(),
        current_step_index=0,
        idempotency_key="k",
        capture_reexecution_active=False,
    )


def _make_executor(
    client: FakeUpstreamClient,
    *,
    signer_creds: SqliteCredentialStore | None,
) -> ChainExecutor:
    """Build a ChainExecutor over the single ``aws_sigv4`` route."""
    return ChainExecutor(
        token_cache=_UnusedTokenCache(),
        upstream_client=client,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=_sigv4_instance(),
        signer_creds=signer_creds,
    )


class _UnusedTokenCache:
    """A TokenCache that must never be touched on the SigV4 path (uid is inert)."""

    async def get(self, endpoint: str, uid: str) -> None:
        raise AssertionError("token_cache.get must not be called on the aws_sigv4 path")

    async def set(self, endpoint: str, uid: str, bearer: str, *, source: object) -> None:
        raise AssertionError("token_cache.set must not be called on the aws_sigv4 path")

    async def mark_bad(self, endpoint: str, uid: str) -> None:
        raise AssertionError("token_cache.mark_bad must not be called on the aws_sigv4 path")


@pytest.fixture
async def cred_store(tmp_path: Path) -> AsyncIterator[SqliteCredentialStore]:
    """Started credential store on a tmp SQLite file."""
    store = SqliteCredentialStore(str(tmp_path / "credential_store.db"))
    await store.start()
    yield store
    await store.stop()


# -------- (1) the arm produces a VALID SigV4 signature -----------------------


@pytest.mark.asyncio
async def test_sigv4_arm_produces_valid_signature(cred_store: SqliteCredentialStore) -> None:
    """The signed request matches an independent botocore re-sign byte-for-byte."""
    body = b"the-object-bytes"
    creds = _static_creds()
    await cred_store.set(HostCredKey(_S3_HOST), creds, source="admin_push")

    client = FakeUpstreamClient()
    client.push(200, body=b"")
    executor = _make_executor(client, signer_creds=cred_store)

    result = await executor.execute_one_step(await _s3_put_row(body), body_refs={"body": body})

    assert isinstance(result, Succeeded)
    assert len(client.requests) == 1
    sent = client.requests[0]

    # The body is forwarded BYTE-IDENTICAL (transparent-proxy invariant).
    assert sent.body == body

    # A real AWS4-HMAC-SHA256 Authorization + the fresh timestamp are present.
    auth = sent.headers.get("Authorization")
    assert auth is not None
    assert auth.startswith("AWS4-HMAC-SHA256 ")
    assert f"/{_REGION}/{_SERVICE}/aws4_request" in auth
    amz_date = sent.headers.get("X-Amz-Date")
    assert amz_date is not None

    # Independently re-sign the EXACT same request with botocore, pinning the
    # same timestamp, and assert the executor's signature is byte-identical —
    # proving the arm computed a correct SigV4 signature, not a stub header.
    # Drop the signed outputs the executor already added so botocore re-derives
    # them (HTTPHeaders has no pop, so clean the plain dict first), then pin the
    # timestamp so the canonical string matches.
    unsigned_headers = {
        name: value
        for name, value in sent.headers.items()
        if name not in {"Authorization", "X-Amz-Date"}
    }
    expected = AWSRequest(method="PUT", url=_S3_URL, data=body, headers=unsigned_headers)
    expected.context["timestamp"] = amz_date
    SigV4Auth(
        Credentials(creds.access_key_id, creds.secret_access_key),
        _SERVICE,
        _REGION,
    ).add_auth(expected)
    assert expected.headers["Authorization"] == auth


@pytest.mark.asyncio
async def test_sigv4_session_token_is_signed(cred_store: SqliteCredentialStore) -> None:
    """STS/temporary creds add the X-Amz-Security-Token signed header."""
    await cred_store.set(
        HostCredKey(_S3_HOST),
        _static_creds(session_token="FQoGZXIvtoken=="),
        source="admin_push",
    )
    client = FakeUpstreamClient()
    client.push(200, body=b"")
    executor = _make_executor(client, signer_creds=cred_store)

    result = await executor.execute_one_step(await _s3_put_row(b"x"), body_refs={"body": b"x"})

    assert isinstance(result, Succeeded)
    assert client.requests[0].headers.get("X-Amz-Security-Token") == "FQoGZXIvtoken=="


# -------- (2) a SigV4 FailedAuth PARKS (auth_expired, NOT terminal) ----------


def test_auth_expired_is_not_terminal() -> None:
    """The parking invariant: ``auth_expired`` is excluded from TERMINAL_STATES.

    This is what makes a SigV4 ``FailedAuth`` PARK (recoverable on re-push)
    rather than die: the sender writes ``FailedAuth`` rows to ``auth_expired``,
    and ``auth_expired`` is not terminal, so the row is re-admittable once a
    fresh credential is pushed.
    """
    assert "auth_expired" not in TERMINAL_STATES


@pytest.mark.asyncio
async def test_sigv4_missing_store_parks() -> None:
    """``signer_creds is None`` → FailedAuth (parks), not a crash or a no-auth send."""
    client = FakeUpstreamClient()
    executor = _make_executor(client, signer_creds=None)

    result = await executor.execute_one_step(await _s3_put_row(b"x"), body_refs={"body": b"x"})

    assert isinstance(result, FailedAuth)
    # No request was sent — the arm parked BEFORE the transport.
    assert client.requests == []


@pytest.mark.asyncio
async def test_sigv4_missing_slot_parks(cred_store: SqliteCredentialStore) -> None:
    """No credential for the host → FailedAuth (parks)."""
    client = FakeUpstreamClient()
    executor = _make_executor(client, signer_creds=cred_store)

    result = await executor.execute_one_step(await _s3_put_row(b"x"), body_refs={"body": b"x"})

    assert isinstance(result, FailedAuth)
    assert client.requests == []


@pytest.mark.asyncio
async def test_sigv4_bad_slot_parks(cred_store: SqliteCredentialStore) -> None:
    """A bad (mark_bad'd) credential slot → FailedAuth (parks), no signing."""
    await cred_store.set(HostCredKey(_S3_HOST), _static_creds(), source="admin_push")
    await cred_store.mark_bad(HostCredKey(_S3_HOST))

    client = FakeUpstreamClient()
    executor = _make_executor(client, signer_creds=cred_store)

    result = await executor.execute_one_step(await _s3_put_row(b"x"), body_refs={"body": b"x"})

    assert isinstance(result, FailedAuth)
    assert client.requests == []


@pytest.mark.asyncio
async def test_sigv4_upstream_403_marks_bad_and_parks(cred_store: SqliteCredentialStore) -> None:
    """An upstream 403 on a signed request → FailedAuth (parks) AND the slot goes bad.

    Marking the slot bad keeps the parked row from re-waking until a fresh
    credential re-push freshens it (the kicker wakes only on ``status='fresh'``).
    """
    await cred_store.set(HostCredKey(_S3_HOST), _static_creds(), source="admin_push")
    client = FakeUpstreamClient()
    client.push(403, body=b"AccessDenied")
    executor = _make_executor(client, signer_creds=cred_store)

    result = await executor.execute_one_step(await _s3_put_row(b"x"), body_refs={"body": b"x"})

    assert isinstance(result, FailedAuth)
    assert result.status == 403
    # The request WAS sent (it signed, then upstream rejected).
    assert len(client.requests) == 1
    # The slot is now bad → it stays parked until a fresh credential re-push.
    row = await cred_store.get(HostCredKey(_S3_HOST))
    assert row is not None
    assert row.status == "bad"


@pytest.mark.asyncio
async def test_sign_sigv4_emits_and_signs_content_sha256() -> None:
    """The S3-dispatched signer EMITS ``x-amz-content-sha256`` AND signs it.

    The in-architecture bug fix: real S3 requires the signed
    ``x-amz-content-sha256`` header, which only ``S3SigV4Auth`` (the dispatched
    signer for :attr:`SigningService.S3`) emits. The header must appear on the
    mutated headers dict AND be listed in the ``Authorization`` ``SignedHeaders``
    (so it is part of the canonical request, not injectable post-sign).
    """
    body = b"the-object-bytes"
    headers: dict[str, str] = {"host": _S3_HOST}
    await sign_sigv4(
        method="PUT", url=_S3_URL, headers=headers, body=body, credential=_static_creds()
    )

    # Emitted on the mutated headers dict (botocore writes the real body hash).
    assert "x-amz-content-sha256" in {k.lower() for k in headers}
    # And SIGNED — listed in the Authorization SignedHeaders segment.
    auth = headers["Authorization"]
    signed_segment = auth.split("SignedHeaders=", 1)[1].split(",", 1)[0]
    assert "x-amz-content-sha256" in signed_segment.split(";")


@pytest.mark.asyncio
async def test_sign_sigv4_unmapped_service_raises_signing_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A service with no ``_SERVICE_SIGNERS`` entry raises ``SigV4SigningError``.

    The belt fail-loud: the enum guarantees the boundary can't pass an unknown
    string, so this proves the map-vs-enum lockstep — a missing dispatch entry
    is a parkable ``SigV4SigningError`` (which the executor's ``except`` catches),
    NEVER a bare ``KeyError`` (which would escape the executor and crash the loop).
    Exercised by clearing the S3 entry so the lookup misses (patched on the
    signer MODULE global, which ``sign_sigv4`` reads — not the test's imported
    binding).
    """
    monkeypatch.setattr("phantom.chain.sigv4_signer._SERVICE_SIGNERS", {})
    with pytest.raises(SigV4SigningError):
        await sign_sigv4(
            method="PUT",
            url=_S3_URL,
            headers={"host": _S3_HOST},
            body=b"x",
            credential=_static_creds(),
        )


@pytest.mark.asyncio
async def test_store_round_trip_then_sign_emits_content_sha256(
    cred_store: SqliteCredentialStore,
) -> None:
    """A credential ``set`` then ``get`` (reloaded ``service``) still signs cleanly.

    Guards B1 end to end: the store write serializes ``service`` to ``"s3"`` and
    the read re-coerces it to :class:`SigningService`, so signing over the
    reloaded credential dispatches and emits ``x-amz-content-sha256`` — no
    ``.value`` crash on a store-reloaded credential.
    """
    await cred_store.set(HostCredKey(_S3_HOST), _static_creds(), source="admin_push")
    row = await cred_store.get(HostCredKey(_S3_HOST))
    assert row is not None
    headers: dict[str, str] = {"host": _S3_HOST}
    await sign_sigv4(
        method="PUT", url=_S3_URL, headers=headers, body=b"x", credential=row.credential
    )
    assert "x-amz-content-sha256" in {k.lower() for k in headers}


# -------- profile_ref signing arm (Phase 2 coverage gap) ---------------------
#
# ``_resolve_profile_ref`` delegates to botocore's ``Session`` (profile / default
# chain) and is the ONE signing branch with zero coverage — every other
# ``sign_sigv4`` test passes a ``SigV4StaticCreds``. These tests fake the
# botocore ``Session`` at the signer-module seam (``phantom.chain.sigv4_signer.Session``
# — the module global ``_resolve_profile_ref`` reads, NOT the test's imported
# binding), so NO real AWS/SSO/STS/config I/O happens. The fake satisfies the
# ``asyncio.to_thread`` arm with a plain in-memory triple.

# A fake frozen-credentials triple the example pair documents. Region is NOT
# part of the frozen creds — it comes from the credential / session-config /
# default fallback chain, which the region-fallback legs below exercise.
_PROFILE_ACCESS_KEY = "AKIAPROFILEEXAMPLE"
_PROFILE_SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/PROFILEEXAMPLEKEY"


class _FakeFrozenCredentials:
    """A botocore ``frozen_credentials`` stand-in (``access_key``/``secret_key``/``token``)."""

    def __init__(self, token: str | None = None) -> None:
        self.access_key = _PROFILE_ACCESS_KEY
        self.secret_key = _PROFILE_SECRET_KEY
        self.token = token


class _FakeCredentials:
    """A botocore ``Credentials`` stand-in returned by ``Session.get_credentials()``."""

    def __init__(self, token: str | None = None) -> None:
        self._frozen = _FakeFrozenCredentials(token=token)

    def get_frozen_credentials(self) -> _FakeFrozenCredentials:
        return self._frozen


class _FakeSession:
    """A botocore ``Session`` stand-in patched over the signer module's ``Session``.

    Construction records the ``profile=`` kwarg the resolver passes.
    ``get_credentials`` yields a fake credential (or ``None`` to exercise the
    empty-chain ``SigV4SigningError``); ``get_config_variable("region")`` returns
    a controllable value (or ``None``) so the three-tier region fallback is
    observable.
    """

    def __init__(
        self,
        *,
        creds: _FakeCredentials | None,
        config_region: str | None,
    ) -> None:
        self._creds = creds
        self._config_region = config_region

    def get_credentials(self) -> _FakeCredentials | None:
        return self._creds

    def get_config_variable(self, name: str) -> str | None:
        assert name == "region", f"unexpected config variable requested: {name!r}"
        return self._config_region


def _patch_session(
    monkeypatch: pytest.MonkeyPatch,
    *,
    creds: _FakeCredentials | None,
    config_region: str | None,
) -> None:
    """Patch the signer module's ``Session`` global with a configured ``_FakeSession``.

    Patches ``phantom.chain.sigv4_signer.Session`` (the global
    ``_resolve_profile_ref`` actually calls), mirroring the
    ``_SERVICE_SIGNERS`` monkeypatch precedent — NOT the test's imported binding.
    """

    def _factory(*, profile: str | None) -> _FakeSession:
        # ``profile`` is accepted (and ignored) — the resolver passes it through;
        # the fake does not branch on it.
        return _FakeSession(creds=creds, config_region=config_region)

    monkeypatch.setattr("phantom.chain.sigv4_signer.Session", _factory)


def _auth_scope_region(authorization: str) -> str:
    """Extract the region segment of the SigV4 ``Credential=`` scope.

    ``Credential=<akid>/<YYYYMMDD>/<region>/<service>/aws4_request`` — the region
    is the third slash-segment, observable proof of which region the signature
    was computed under.
    """
    cred_segment = authorization.split("Credential=", 1)[1].split(",", 1)[0]
    parts = cred_segment.split("/")
    assert parts[-1] == "aws4_request", f"unexpected credential scope shape: {cred_segment!r}"
    assert parts[-2] == _SERVICE, f"unexpected service in scope: {cred_segment!r}"
    return parts[2]


@pytest.mark.asyncio
async def test_sign_sigv4_profile_ref_resolves_and_signs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``ProfileRefCred`` resolves via the (faked) botocore chain and SIGNS.

    Proves the frozen-cred → ``Credentials`` → ``S3SigV4Auth.add_auth`` path of
    ``_resolve_profile_ref``: the resolved profile credentials produce a real
    signature, so the mutated headers carry both ``Authorization`` and the signed
    ``x-amz-content-sha256`` (the S3-dispatched signer emits it). No real AWS.
    """
    _patch_session(monkeypatch, creds=_FakeCredentials(), config_region="us-east-1")
    headers: dict[str, str] = {"host": _S3_HOST}

    await sign_sigv4(
        method="PUT",
        url=_S3_URL,
        headers=headers,
        body=b"profile-signed-body",
        credential=ProfileRefCred(service=SigningService.S3, profile="prod-account"),
    )

    lowered = {k.lower() for k in headers}
    assert "authorization" in lowered
    assert "x-amz-content-sha256" in lowered


@pytest.mark.asyncio
async def test_sign_sigv4_profile_ref_region_fallback_to_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no credential region AND no session-config region, the default wins.

    The third fallback tier: ``credential.region or session.get_config_variable
    ('region') or _DEFAULT_REGION``. With ``region=None`` on the credential and
    the fake session-config region ``None``, the signature is computed under
    ``us-east-1`` (``_DEFAULT_REGION``), observable in the ``Authorization``
    credential scope.
    """
    _patch_session(monkeypatch, creds=_FakeCredentials(), config_region=None)
    headers: dict[str, str] = {"host": _S3_HOST}

    await sign_sigv4(
        method="PUT",
        url=_S3_URL,
        headers=headers,
        body=b"x",
        credential=ProfileRefCred(service=SigningService.S3, profile=None, region=None),
    )

    assert _auth_scope_region(headers["Authorization"]) == "us-east-1"


@pytest.mark.asyncio
async def test_sign_sigv4_profile_ref_session_config_region_wins_over_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The session-config region wins over ``_DEFAULT_REGION`` (the second tier).

    With ``credential.region=None`` but the fake session-config region
    ``eu-west-1``, the signature is computed under ``eu-west-1`` — the
    middle fallback tier beats the ``us-east-1`` default.
    """
    _patch_session(monkeypatch, creds=_FakeCredentials(), config_region="eu-west-1")
    headers: dict[str, str] = {"host": _S3_HOST}

    await sign_sigv4(
        method="PUT",
        url=_S3_URL,
        headers=headers,
        body=b"x",
        credential=ProfileRefCred(service=SigningService.S3, profile=None, region=None),
    )

    assert _auth_scope_region(headers["Authorization"]) == "eu-west-1"


@pytest.mark.asyncio
async def test_sign_sigv4_profile_ref_empty_chain_raises_signing_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty botocore chain (``get_credentials() is None``) raises ``SigV4SigningError``.

    The parkable failure: when the profile / default chain yields no credentials,
    ``_resolve_profile_ref`` raises ``SigV4SigningError`` (which the executor's
    ``except`` parks in ``auth_expired``), NOT an opaque botocore error. Mirrors
    the unmapped-service ``pytest.raises`` structure.
    """
    _patch_session(monkeypatch, creds=None, config_region="us-east-1")
    with pytest.raises(SigV4SigningError):
        await sign_sigv4(
            method="PUT",
            url=_S3_URL,
            headers={"host": _S3_HOST},
            body=b"x",
            credential=ProfileRefCred(service=SigningService.S3, profile="prod-account"),
        )
