"""Unit tests for :mod:`phantom_emulator.routers.raw_sink` — the /raw sink.

The auth-free, token-free Phase-1 forward-as-is oracle: an unsigned,
tokenless ``PUT /raw/{path:path}`` MUST 200 and store the body byte-identically
(the case the token-gated ``put_upload`` would 403), ``GET`` MUST round-trip
the bytes or 404 when absent, and the literal-prefixed ``/raw/...`` router MUST
shadow nothing in either direction — in particular ``PUT /raw/mybucket/mykey``
MUST reach this sink (200), NOT the s3 SigV4 validator (403). That last leg is
the regression teeth pinning ``raw_sink`` registered BEFORE the s3 catch-all.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
from phantom_emulator.app import create_app
from phantom_emulator.config import AppConfig, UpstreamCfg
from phantom_emulator.state import EmulatorState

# Stable base URL so the ASGI transport has a deterministic host.
_BASE_URL = "http://emulator"


@pytest.fixture
async def client_and_state(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[httpx.AsyncClient, EmulatorState]]:
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    app = create_app(AppConfig())
    state: EmulatorState = app.state.emulator_state
    # FastAPI satisfies the ASGI app protocol at runtime; httpx's stub types
    # the arg more narrowly. Same pattern as the rest of the emulator suite.
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url=_BASE_URL) as client:
        yield client, state


async def test_auth_free_put_stores_body(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """An unsigned, tokenless PUT -> 200 + byte-identical store, no auth.

    This is the case the token-gated ``put_upload`` (``routers/upstream.py``)
    would 403: no ``Authorization``, no token in ``pending_uploads``. The naked
    sink stores it with no mint.
    """
    client, state = client_and_state
    body = b"hello"

    r = await client.put("/raw/bucket/key", content=body)

    assert r.status_code == 200
    stored = state.raw_bodies["bucket/key"]
    assert stored.body == body
    assert stored.path == "bucket/key"
    # No auth/token entry was created as a side effect.
    assert state.pending_uploads == {}


# The upload verbs the raw sink now accepts beyond the long-standing PUT. The
# raw sink has NO auth, so there are no 403/400 legs — only the happy store
# (with verb capture) and the over-cap 413.
_EXTRA_UPLOAD_VERBS = ("POST", "PATCH")


@pytest.mark.parametrize("method", _EXTRA_UPLOAD_VERBS)
async def test_auth_free_post_patch_stores_body_and_method(
    method: str,
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """An unsigned POST/PATCH -> 200 + byte-identical store + ``method`` recorded.

    Mirrors :func:`test_auth_free_put_stores_body` for the verbs the sink now
    accepts (the catch-all forwards PUT/POST/PATCH); the stored object records
    the inbound verb in ``RawBody.method``.
    """
    client, state = client_and_state
    body = b"hello"

    r = await client.request(method, "/raw/bucket/verbkey", content=body)

    assert r.status_code == 200
    stored = state.raw_bodies["bucket/verbkey"]
    assert stored.body == body
    assert stored.method == method
    assert stored.path == "bucket/verbkey"
    assert state.pending_uploads == {}


@pytest.mark.parametrize("method", _EXTRA_UPLOAD_VERBS)
async def test_post_patch_over_cap_rejected_413(
    method: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A POST/PATCH body over ``upstream.body_max_bytes`` -> 413, never stored.

    The cap check precedes the store, so an oversized POST/PATCH is rejected
    without retaining it (the same boundary the PUT leg enforces). Uses a
    tiny-cap app so the over-cap body is a handful of bytes (the default cap is
    2 GiB).
    """
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    cap = 8
    app = create_app(AppConfig(upstream=UpstreamCfg(body_max_bytes=cap)))
    state: EmulatorState = app.state.emulator_state
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    body = b"x" * (cap + 1)
    async with httpx.AsyncClient(transport=transport, base_url=_BASE_URL) as client:
        r = await client.request(method, "/raw/bucket/verbtoobig", content=body)

    assert r.status_code == 413
    assert "bucket/verbtoobig" not in state.raw_bodies


async def test_get_round_trips_bytes(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """GET returns the uploaded bytes byte-identically, no auth."""
    client, _ = client_and_state
    body = b"\x00\x01round-trip-bytes\xff"

    put = await client.put("/raw/bucket/key", content=body)
    assert put.status_code == 200

    got = await client.get("/raw/bucket/key")
    assert got.status_code == 200
    assert got.content == body


async def test_get_missing_returns_404(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """GET on a never-written path -> 404 NoSuchKey."""
    client, _ = client_and_state

    r = await client.get("/raw/never/written")

    assert r.status_code == 404
    assert r.json()["detail"] == "NoSuchKey"


async def test_slash_bearing_key_captured_whole(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """A deep slash-bearing key is captured whole by the :path convertor."""
    client, state = client_and_state
    body = b"deep-nested-payload"

    put = await client.put("/raw/b/deep/nested/key", content=body)
    assert put.status_code == 200
    assert state.raw_bodies["b/deep/nested/key"].body == body

    got = await client.get("/raw/b/deep/nested/key")
    assert got.status_code == 200
    assert got.content == body


async def test_no_shadow_both_directions(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """The two :path catch-alls do not shadow each other in EITHER direction.

    Three legs, per plan TASK 0.5 Step C:

    1. A non-``raw`` unsigned ``PUT /mybucket/mykey`` still reaches the s3 SigV4
       validator -> 403 (the sink did not over-claim).
    2. A literal route (``POST /v1/files/create``) still resolves -> NOT 404
       (swallowed by neither catch-all). It 401s without a bearer; the point is
       it is not a catch-all 404.
    3. THE REGRESSION TEETH: an unsigned, tokenless ``PUT /raw/mybucket/mykey``
       -> 200 (reaches ``put_raw``), explicitly NOT 403 (what the s3 validator
       returns for an unsigned PUT). Legs (1)+(2) pass under BOTH registration
       orders; only leg (3) fails under raw-after-s3, pinning raw-before-s3.
    """
    client, state = client_and_state

    # Leg 1 — non-raw unsigned PUT still hits the s3 validator (403).
    s3_put = await client.put("/mybucket/mykey", content=b"unsigned")
    assert s3_put.status_code == 403
    assert s3_put.json()["detail"] == "SignatureDoesNotMatch"
    assert ("mybucket", "mykey") not in state.s3_objects

    # Leg 2 — a fixed literal route is unshadowed (not a catch-all 404).
    v1 = await client.post("/v1/files/create", json={})
    assert v1.status_code != 404

    # Leg 3 (regression teeth) — /raw/mybucket/mykey reaches the auth-free sink.
    raw_put = await client.put("/raw/mybucket/mykey", content=b"forward-as-is")
    assert raw_put.status_code == 200
    assert raw_put.status_code != 403  # explicitly NOT the s3 validator's 403
    assert state.raw_bodies["mybucket/mykey"].body == b"forward-as-is"
    # And it did NOT leak into the s3 store under {bucket: "raw", ...}.
    assert ("raw", "mybucket/mykey") not in state.s3_objects
