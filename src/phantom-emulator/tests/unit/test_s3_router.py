"""Unit tests for :mod:`phantom_emulator.routers.s3` — the SigV4 sink.

The tier that lands TODAY with no dependency on Phantom's (unbuilt)
``aws_sigv4`` signer: it signs requests CLIENT-side with botocore's
``SigV4Auth.add_auth`` (the correct client path), using the same known
AWS-example key-pair the emulator's :class:`S3Cfg` defaults to, and
asserts the SERVER recompute (``_verify_sigv4``, exercised through the PUT
/ GET handlers) agrees.

This is the gating proof for the recompute-from-declared mechanism: a
correct signature MUST 200 (BOTH the default body-hash path AND the
``UNSIGNED-PAYLOAD`` literal), a corrupted signature MUST 403, the
PUT->GET round-trip MUST be byte-identical, a missing key MUST 404 — all
against real botocore.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
from botocore.auth import (  # type: ignore[import-untyped]  # botocore ships no py.typed
    S3SigV4Auth,
    SigV4Auth,
)
from botocore.awsrequest import AWSRequest  # type: ignore[import-untyped]
from botocore.credentials import Credentials  # type: ignore[import-untyped]

# The phantom-service package is importable from the emulator's workspace venv;
# the drift-guard test below asserts the sink's upload-verb set against the live
# catch-all route (the strongest guard against the two diverging).
from phantom.routes import catch_all
from phantom_emulator.app import create_app
from phantom_emulator.config import AppConfig, S3Cfg
from phantom_emulator.routers._deps import UPLOAD_METHODS
from phantom_emulator.state import EmulatorState

# The AWS-doc example pair the emulator's S3Cfg defaults to. The client
# signs with the SAME pair so the server recompute matches.
_ACCESS_KEY_ID = "AKIAIOSFODNN7EXAMPLE"
_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
_REGION = "us-east-1"
_SERVICE = "s3"

# Stable base URL so str(request.url)'s host is deterministic for the
# server-side recompute (the host is part of the canonical request).
_BASE_URL = "http://emulator"

# Common pre-signing header sets (kept short so signing calls fit the line cap).
_TEXT: dict[str, str] = {"content-type": "text/plain"}
_OCTET: dict[str, str] = {"content-type": "application/octet-stream"}


def _sign(
    method: str,
    path: str,
    body: bytes,
    *,
    secret: str = _SECRET_ACCESS_KEY,
    extra_headers: dict[str, str] | None = None,
    signer: type[SigV4Auth] = S3SigV4Auth,
) -> dict[str, str]:
    """Client-side SigV4-sign a request and return the wire headers.

    Signs via ``signer.add_auth`` (the correct client path — ``add_auth``
    belongs on the client side). The default ``signer`` is ``S3SigV4Auth``,
    which EMITS + SIGNS ``x-amz-content-sha256`` (the signed header the
    emulator validator now ENFORCES, matching Phantom's own S3 signer). A
    caller exercising the pinned ``UNSIGNED-PAYLOAD`` literal must pass
    ``signer=SigV4Auth`` (base ``SigV4Auth`` does NOT overwrite a pre-set
    ``x-amz-content-sha256``, whereas ``S3SigV4Auth`` would ``del`` + recompute
    it and clobber the pinned literal) together with the header via
    ``extra_headers``.

    Args:
        method: HTTP method (``PUT`` / ``GET``).
        path: Request path, e.g. ``/mybucket/my/key.txt``.
        body: Request body (``b""`` for a GET).
        secret: Signing secret. Default is the matching known secret; a
            wrong value produces a signature the server rejects.
        extra_headers: Additional headers present BEFORE signing (so they
            are included in ``SignedHeaders``).
        signer: The botocore signer class to sign with. Default ``S3SigV4Auth``
            (emits the content-sha256 header); pass base ``SigV4Auth`` to keep a
            pre-set ``UNSIGNED-PAYLOAD`` literal intact.

    Returns:
        The full header dict to send on the wire (``Authorization`` +
        ``X-Amz-Date`` + whatever was signed).
    """
    headers: dict[str, str] = {"host": "emulator"}
    if extra_headers:
        headers.update(extra_headers)
    aws_req = AWSRequest(method=method, url=f"{_BASE_URL}{path}", data=body, headers=headers)
    creds = Credentials(_ACCESS_KEY_ID, secret)
    signer(creds, _SERVICE, _REGION).add_auth(aws_req)
    return dict(aws_req.headers.items())


@pytest.fixture
async def client_and_state(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[httpx.AsyncClient, EmulatorState]]:
    # Convention: deterministic JWT secret (app.py falls back to
    # _FALLBACK_HS256_SECRET otherwise). Inert for the S3 routes, which are
    # not behind the emulator auth plane — kept for house-style consistency.
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    app = create_app(AppConfig())
    state: EmulatorState = app.state.emulator_state
    # FastAPI satisfies the ASGI app protocol at runtime; httpx's stub types
    # the arg more narrowly. Same pattern as the rest of the emulator suite.
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url=_BASE_URL) as client:
        yield client, state


async def test_valid_put_default_hash_stores_body(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """A correctly-signed PUT (default body-hash path) -> 200 + byte-identical store.

    This is the case the deleted ``add_auth``-primary would have 403'd
    (clock-second skew + content-sha256 handling); the lower-level
    recompute now passes.
    """
    client, state = client_and_state
    body = b"the-quick-brown-fox-payload"
    headers = _sign("PUT", "/mybucket/mykey", body, extra_headers=_TEXT)

    r = await client.put("/mybucket/mykey", content=body, headers=headers)

    assert r.status_code == 200
    stored = state.s3_objects[("mybucket", "mykey")]
    assert stored.body == body
    assert stored.bucket == "mybucket"
    assert stored.key == "mykey"
    assert stored.content_type == "text/plain"


async def test_valid_put_unsigned_payload_stores_body(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """A PUT signed with ``x-amz-content-sha256: UNSIGNED-PAYLOAD`` -> 200.

    Proves the pinned-content-sha256 path: the server honors the inbound
    ``UNSIGNED-PAYLOAD`` literal verbatim and does NOT re-hash the body.

    Signed via BASE ``SigV4Auth`` (NOT the default ``S3SigV4Auth``): base does
    not overwrite a pre-set ``x-amz-content-sha256``, so the pinned
    ``UNSIGNED-PAYLOAD`` literal survives into the canonical request /
    ``SignedHeaders`` and the test genuinely exercises the literal branch.
    ``S3SigV4Auth`` would ``del`` + recompute it (clobbering the literal with the
    real body hash), turning this into a silent tautology. The validator still
    ACCEPTS it: the new enforcement only requires the header to be PRESENT, then
    keys the recompute on the inbound literal.
    """
    client, state = client_and_state
    body = b"unsigned-payload-body"
    headers = _sign(
        "PUT",
        "/mybucket/unsigned",
        body,
        extra_headers={"x-amz-content-sha256": "UNSIGNED-PAYLOAD"},
        signer=SigV4Auth,
    )

    r = await client.put("/mybucket/unsigned", content=body, headers=headers)

    assert r.status_code == 200
    assert state.s3_objects[("mybucket", "unsigned")].body == body


async def test_tampered_signature_rejected(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """A one-byte-corrupted ``Signature=`` hex -> 403 SignatureDoesNotMatch."""
    client, state = client_and_state
    body = b"will-be-tampered"
    headers = _sign("PUT", "/mybucket/tampered", body, extra_headers=_TEXT)
    auth = headers["Authorization"]
    # Flip the final hex nibble of the signature.
    flipped = "0" if auth[-1] != "0" else "1"
    headers["Authorization"] = auth[:-1] + flipped

    r = await client.put("/mybucket/tampered", content=body, headers=headers)

    assert r.status_code == 403
    assert r.json()["detail"] == "SignatureDoesNotMatch"
    assert ("mybucket", "tampered") not in state.s3_objects


async def test_wrong_secret_rejected(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """A signature computed with the WRONG secret -> 403 (the known-bad half)."""
    client, _ = client_and_state
    body = b"wrong-secret-body"
    headers = _sign(
        "PUT",
        "/mybucket/wrongsecret",
        body,
        secret="wrong-secret-value-not-the-aws-example",
        extra_headers=_TEXT,
    )

    r = await client.put("/mybucket/wrongsecret", content=body, headers=headers)

    assert r.status_code == 403
    assert r.json()["detail"] == "SignatureDoesNotMatch"


async def test_missing_authorization_rejected(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """A PUT with no ``Authorization`` header -> 403, not a 500."""
    client, _ = client_and_state
    r = await client.put("/mybucket/noauth", content=b"x")
    assert r.status_code == 403
    assert r.json()["detail"] == "SignatureDoesNotMatch"


async def test_missing_content_sha256_rejected_400(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """A validly-signed PUT with NO ``x-amz-content-sha256`` -> 400, like real S3.

    Proves the enforcement: signing with base ``SigV4Auth`` (which does NOT emit
    ``x-amz-content-sha256``) and stripping any such header yields an otherwise
    valid request that the validator rejects ``400 "Missing required header for
    this request: x-amz-content-sha256"`` — the exact status + detail real S3
    emits, kept DISTINCT from the 403 ``SignatureDoesNotMatch``.
    """
    client, _ = client_and_state
    body = b"no-content-sha256-header"
    # Base SigV4Auth signs without emitting the content-sha256 header.
    headers = _sign("PUT", "/mybucket/nosha", body, extra_headers=_TEXT, signer=SigV4Auth)
    # Belt-and-suspenders: ensure the header is genuinely absent from the wire.
    headers = {k: v for k, v in headers.items() if k.lower() != "x-amz-content-sha256"}
    assert "x-amz-content-sha256" not in {k.lower() for k in headers}

    r = await client.put("/mybucket/nosha", content=body, headers=headers)

    assert r.status_code == 400
    assert r.json()["detail"] == "Missing required header for this request: x-amz-content-sha256"


async def test_put_then_get_round_trip(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """Signed PUT then signed GET returns byte-identical bytes."""
    client, _ = client_and_state
    body = b"round-trip-bytes-\x00\x01\x02-binary-safe"
    put_headers = _sign("PUT", "/mybucket/roundtrip", body, extra_headers=_OCTET)
    put_r = await client.put("/mybucket/roundtrip", content=body, headers=put_headers)
    assert put_r.status_code == 200

    get_headers = _sign("GET", "/mybucket/roundtrip", b"")
    get_r = await client.get("/mybucket/roundtrip", headers=get_headers)

    assert get_r.status_code == 200
    assert get_r.content == body


async def test_get_missing_key_returns_404(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """A signed GET of an absent key -> 404 NoSuchKey."""
    client, _ = client_and_state
    headers = _sign("GET", "/mybucket/absent", b"")
    r = await client.get("/mybucket/absent", headers=headers)
    assert r.status_code == 404
    assert r.json()["detail"] == "NoSuchKey"


async def test_get_bad_signature_rejected_before_lookup(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """A bad-signature GET -> 403 BEFORE the store lookup (no existence leak).

    The object exists, but a corrupted GET signature still 403s rather than
    304/404/200, so a bad-sig GET cannot probe whether a key is present.
    """
    client, _ = client_and_state
    body = b"present-but-unreadable-without-a-valid-sig"
    put_headers = _sign("PUT", "/mybucket/secret", body, extra_headers=_TEXT)
    put_r = await client.put("/mybucket/secret", content=body, headers=put_headers)
    assert put_r.status_code == 200

    get_headers = _sign("GET", "/mybucket/secret", b"")
    get_headers["Authorization"] = get_headers["Authorization"][:-1] + (
        "0" if get_headers["Authorization"][-1] != "0" else "1"
    )
    r = await client.get("/mybucket/secret", headers=get_headers)
    assert r.status_code == 403
    assert r.json()["detail"] == "SignatureDoesNotMatch"


async def test_declared_signed_headers_divergence_rejected(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """The internal faithfulness guard: a declared header absent inbound -> 403.

    Tamper the signed request so its ``SignedHeaders`` list names a header
    (``x-amz-meta-foo``) that is NOT actually present on the wire. The
    server's recompute must reject this cleanly (403), not 500 on a missing
    header or silently pass.
    """
    client, _ = client_and_state
    body = b"guard-body"
    headers = _sign("PUT", "/mybucket/guard", body, extra_headers=_TEXT)
    auth = headers["Authorization"]
    # Inject an extra name into SignedHeaders that we do NOT send as a header.
    tampered = auth.replace(
        "SignedHeaders=content-type;host",
        "SignedHeaders=content-type;host;x-amz-meta-foo",
    )
    assert tampered != auth, "expected SignedHeaders substring to be present"
    headers["Authorization"] = tampered

    r = await client.put("/mybucket/guard", content=body, headers=headers)

    assert r.status_code == 403
    assert r.json()["detail"] == "SignatureDoesNotMatch"


async def test_reserved_bucket_does_not_shadow_upstream(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """A signed PUT/GET on a reserved first-segment -> 404, never stored.

    Belt-and-suspenders for the route-shadow guard: even a perfectly valid
    signature on ``/v1/...`` is refused by the S3 handler so it cannot
    swallow an upstream/control path.
    """
    client, state = client_and_state
    body = b"reserved"
    headers = _sign("PUT", "/v1/files/create", body, extra_headers=_TEXT)
    r = await client.put("/v1/files/create", content=body, headers=headers)
    assert r.status_code == 404
    assert ("v1", "files/create") not in state.s3_objects


async def test_s3_router_does_not_shadow_create_file(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """The literal ``POST /v1/files/create`` still resolves with s3 registered.

    Proves registration-order first-match: the s3 catch-all (registered
    last) does not swallow the literal upstream route.
    """
    client, _ = client_and_state
    r = await client.post(
        "/v1/files/create",
        json={
            "domain": "TheDomain",
            "fileName": "f.parquet",
            "metadata": {"keyValueStore": {}},
        },
    )
    # 200 (served) or 401 (auth required) — both prove the request reached
    # the upstream create handler, NOT the s3 catch-all (which would 404 via
    # the reserved-bucket guard or mis-store it).
    assert r.status_code in (200, 401)


# The upload verbs the SigV4 sink now accepts BEYOND the long-standing PUT.
# The PUT legs above already prove PUT; these parametrized legs prove POST and
# PATCH validate + store + record their verb via the SAME method-agnostic
# recompute (the catch-all forwards all three).
_EXTRA_UPLOAD_VERBS = ("POST", "PATCH")


@pytest.mark.parametrize("method", _EXTRA_UPLOAD_VERBS)
async def test_valid_post_patch_stores_body_and_method(
    method: str,
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """A correctly-signed POST/PATCH -> 200 + byte-identical store + ``method``.

    Mirrors :func:`test_valid_put_default_hash_stores_body` for the verbs the
    sink now accepts: the validator recomputes over ``request.method`` so a
    POST/PATCH validates exactly as a PUT, and the stored object records the
    inbound verb in ``S3Object.method``.
    """
    client, state = client_and_state
    body = b"the-quick-brown-fox-payload"
    headers = _sign(method, "/mybucket/verbkey", body, extra_headers=_TEXT)

    r = await client.request(method, "/mybucket/verbkey", content=body, headers=headers)

    assert r.status_code == 200
    stored = state.s3_objects[("mybucket", "verbkey")]
    assert stored.body == body
    assert stored.method == method
    assert stored.content_type == "text/plain"


@pytest.mark.parametrize("method", _EXTRA_UPLOAD_VERBS)
async def test_post_patch_tampered_signature_rejected(
    method: str,
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """A one-byte-corrupted ``Signature=`` on a POST/PATCH -> 403, never stored.

    Mirrors :func:`test_tampered_signature_rejected` for the new verbs.
    """
    client, state = client_and_state
    body = b"will-be-tampered"
    headers = _sign(method, "/mybucket/verbtampered", body, extra_headers=_TEXT)
    auth = headers["Authorization"]
    flipped = "0" if auth[-1] != "0" else "1"
    headers["Authorization"] = auth[:-1] + flipped

    r = await client.request(method, "/mybucket/verbtampered", content=body, headers=headers)

    assert r.status_code == 403
    assert r.json()["detail"] == "SignatureDoesNotMatch"
    assert ("mybucket", "verbtampered") not in state.s3_objects


@pytest.mark.parametrize("method", _EXTRA_UPLOAD_VERBS)
async def test_post_patch_missing_content_sha256_rejected_400(
    method: str,
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """A validly-signed POST/PATCH with NO ``x-amz-content-sha256`` -> 400.

    Mirrors :func:`test_missing_content_sha256_rejected_400` for the new verbs:
    the header enforcement is method-agnostic.
    """
    client, _ = client_and_state
    body = b"no-content-sha256-header"
    headers = _sign(method, "/mybucket/verbnosha", body, extra_headers=_TEXT, signer=SigV4Auth)
    headers = {k: v for k, v in headers.items() if k.lower() != "x-amz-content-sha256"}
    assert "x-amz-content-sha256" not in {k.lower() for k in headers}

    r = await client.request(method, "/mybucket/verbnosha", content=body, headers=headers)

    assert r.status_code == 400
    assert r.json()["detail"] == "Missing required header for this request: x-amz-content-sha256"


@pytest.mark.parametrize("method", _EXTRA_UPLOAD_VERBS)
async def test_post_patch_over_cap_rejected_413(
    method: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A POST/PATCH body over ``s3.body_max_bytes`` -> 413, before the recompute.

    Proves the cap fires before the SigV4 recompute on POST/PATCH too (the cap
    check precedes ``_verify_sigv4``), so an oversized body is rejected without
    hashing and is never stored. Uses a tiny-cap app so the over-cap body is a
    handful of bytes (the default cap is 2 GiB).
    """
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    cap = 8
    app = create_app(AppConfig(s3=S3Cfg(body_max_bytes=cap)))
    state: EmulatorState = app.state.emulator_state
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    body = b"x" * (cap + 1)
    headers = _sign(method, "/mybucket/verbtoobig", body, extra_headers=_OCTET)
    async with httpx.AsyncClient(transport=transport, base_url=_BASE_URL) as client:
        r = await client.request(method, "/mybucket/verbtoobig", content=body, headers=headers)

    assert r.status_code == 413
    assert ("mybucket", "verbtoobig") not in state.s3_objects


def test_upload_methods_match_catch_all_forwarded_set() -> None:
    """Drift-guard: the sink's ``UPLOAD_METHODS`` == the catch-all's forwarded set.

    The Phantom catch-all forwards ``["PUT", "POST", "PATCH"]`` (the
    ``raw_intake`` ``@router.api_route`` at
    ``src/phantom-service/src/phantom/routes/catch_all.py``). A forwarded verb
    the emulator sinks do NOT register would 405 unvalidated/unsunk. The
    phantom-service package is importable from the emulator's workspace venv, so
    this asserts against the LIVE route methods (the strongest drift guard) — if
    a 4th forwarded verb is added to the catch-all, this test fails until the
    sinks catch up.
    """
    forwarded: set[str] | None = None
    for route in catch_all.router.routes:
        if getattr(route, "endpoint", None) is catch_all.raw_intake:
            forwarded = set(route.methods)  # type: ignore[attr-defined]  # APIRoute carries .methods
            break
    assert forwarded is not None, "catch_all.raw_intake route not found on the router"
    assert set(UPLOAD_METHODS) == forwarded
    # And the value the sinks register is exactly the documented upload set.
    assert set(UPLOAD_METHODS) == {"PUT", "POST", "PATCH"}
