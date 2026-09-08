"""Unit tests for :mod:`phantom_emulator.routers.upstream`."""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import uuid4

import httpx
import pytest
from phantom_emulator.app import create_app
from phantom_emulator.auth.modes import AuthMode
from phantom_emulator.config import AppConfig, AuthCfg
from phantom_emulator.state import BodyPutEvent, EmulatorState, MetadataCreateEvent

# The create-file body these tests post when the payload is not the subject.
_CREATE_PAYLOAD: dict[str, object] = {
    "domain": "D",
    "fileName": "f",
    "metadata": {"keyValueStore": {}},
}


def _relative(upload_url: str) -> str:
    """Re-point an absolute emulator upload URL at the in-process test client."""
    return upload_url.replace("http://emulator", "")


async def _mint_token(client: httpx.AsyncClient) -> str:
    r = await client.post(
        "/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "test-client",
            "client_secret": "test-secret",
        },
    )
    return r.json()["access_token"]


@pytest.fixture
async def client(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[httpx.AsyncClient]:
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    app = create_app(AppConfig())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://emulator") as c:
        yield c


@pytest.fixture
async def no_auth_client(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[httpx.AsyncClient]:
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    cfg = AppConfig(auth=AuthCfg(default_mode=AuthMode.NONE))
    app = create_app(cfg)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://emulator") as c:
        yield c


@pytest.fixture
async def no_auth_client_and_state(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[httpx.AsyncClient, EmulatorState]]:
    """An auth-free client plus its state, for tests that assert what was stored."""
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    app = create_app(AppConfig(auth=AuthCfg(default_mode=AuthMode.NONE)))
    state: EmulatorState = app.state.emulator_state
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://emulator") as c:
        yield c, state


async def _mint_upload_url(client: httpx.AsyncClient) -> str:
    """Create a file on an auth-free client and return the relative upload URL."""
    created = await client.post("/v1/files/create", json=_CREATE_PAYLOAD)
    return _relative(created.json()["uploadUrl"])


async def test_create_returns_upload_url(client: httpx.AsyncClient) -> None:
    token = await _mint_token(client)
    r = await client.post(
        "/v1/files/create",
        json={
            "domain": "TheDomain",
            "laneBaseName": "lane-1",
            "fileName": "f.parquet",
            "metadata": {"keyValueStore": {}},
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    body = r.json()
    assert "fileInformation" in body
    assert "uploadUrl" in body
    assert body["uploadUrl"].startswith("http://emulator/v1/files/upload/")
    assert body["fileInformation"]["domain"] == "TheDomain"
    assert body["fileInformation"]["name"] == "f.parquet"


async def test_create_preserves_phantom_local_uuid(client: httpx.AsyncClient) -> None:
    token = await _mint_token(client)
    local_uuid = str(uuid4())
    r = await client.post(
        "/v1/files/create",
        json={
            "domain": "D",
            "fileName": "f",
            "metadata": {
                "keyValueStore": {
                    "phantom_local_uuid": local_uuid,
                    "uploader_id": "alice",
                }
            },
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    kvs = r.json()["fileInformation"]["metadata"]["keyValueStore"]
    assert kvs["phantom_local_uuid"] == local_uuid
    assert kvs["uploader_id"] == "alice"


async def test_create_idempotency_dedup(client: httpx.AsyncClient) -> None:
    token = await _mint_token(client)
    headers = {
        "Authorization": f"Bearer {token}",
        "Idempotency-Key": "abc-123",
    }
    payload = {"domain": "D", "fileName": "f.parquet", "metadata": {"keyValueStore": {}}}
    r1 = await client.post("/v1/files/create", json=payload, headers=headers)
    r2 = await client.post("/v1/files/create", json=payload, headers=headers)
    assert r1.status_code == 200
    assert r2.status_code == 200
    # Same idempotency key returns the identical cached response.
    assert r1.json() == r2.json()


async def test_idempotency_cache_hit_is_a_distinct_create_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cached replay preserves response identity but appends another create event."""
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    app = create_app(AppConfig(auth=AuthCfg(default_mode=AuthMode.NONE)))
    state = app.state.emulator_state
    transport = httpx.ASGITransport(app=app)
    local_uuid = uuid4()
    payload = {
        "domain": "D",
        "fileName": "f",
        "metadata": {"keyValueStore": {"phantom_local_uuid": str(local_uuid)}},
    }
    headers = {"Idempotency-Key": "cached-event-key"}
    async with httpx.AsyncClient(transport=transport, base_url="http://emulator") as client:
        first = await client.post("/v1/files/create", json=payload, headers=headers)
        second = await client.post("/v1/files/create", json=payload, headers=headers)

    assert first.json() == second.json()
    events = [event for event in state.upstream_events if isinstance(event, MetadataCreateEvent)]
    assert len(events) == 2
    assert [event.cache_hit for event in events] == [False, True]
    assert events[0].chain_id == events[1].chain_id == local_uuid
    assert events[0].idempotency_key == events[1].idempotency_key == "cached-event-key"
    assert events[0].file_id == events[1].file_id
    assert events[0].upload_token == events[1].upload_token
    assert events[0].upload_url == events[1].upload_url


async def test_create_different_idempotency_keys_distinct(
    client: httpx.AsyncClient,
) -> None:
    token = await _mint_token(client)
    base_headers = {"Authorization": f"Bearer {token}"}
    payload = {"domain": "D", "fileName": "f", "metadata": {"keyValueStore": {}}}
    r1 = await client.post(
        "/v1/files/create",
        json=payload,
        headers={**base_headers, "Idempotency-Key": "k1"},
    )
    r2 = await client.post(
        "/v1/files/create",
        json=payload,
        headers={**base_headers, "Idempotency-Key": "k2"},
    )
    assert r1.json()["fileInformation"]["id"] != r2.json()["fileInformation"]["id"]


async def test_create_requires_auth(client: httpx.AsyncClient) -> None:
    r = await client.post("/v1/files/create", json={})
    assert r.status_code == 401


async def test_put_accepts_body_records_metadata(client: httpx.AsyncClient) -> None:
    token = await _mint_token(client)
    create_r = await client.post(
        "/v1/files/create",
        json={"domain": "D", "fileName": "f", "metadata": {"keyValueStore": {}}},
        headers={"Authorization": f"Bearer {token}"},
    )
    upload_url = create_r.json()["uploadUrl"]
    # Re-route the absolute URL to the test client.
    relative = upload_url.replace("http://emulator", "")
    r = await client.put(
        relative,
        content=b"the body bytes",
        headers={
            "x-amz-meta-uploader-id": "alice",
            "x-amz-meta-label": "alpha",
            "content-type": "application/octet-stream",
        },
    )
    assert r.status_code == 200


async def test_repeated_puts_are_append_only_events_despite_latest_value_map(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two PUTs to one token remain two events while the latest map has one key."""
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    app = create_app(AppConfig(auth=AuthCfg(default_mode=AuthMode.NONE)))
    state = app.state.emulator_state
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://emulator") as client:
        create_response = await client.post(
            "/v1/files/create",
            json={
                "domain": "D",
                "fileName": "f",
                "metadata": {"keyValueStore": {"phantom_local_uuid": str(uuid4())}},
            },
            headers={"Idempotency-Key": "append-only-key"},
        )
        relative_url = create_response.json()["uploadUrl"].replace("http://emulator", "")
        first = await client.put(relative_url, content=b"first")
        second = await client.put(relative_url, content=b"second")

    assert first.status_code == 200
    assert second.status_code == 200
    assert len(state.accepted_bodies) == 1
    create_events = [
        event for event in state.upstream_events if isinstance(event, MetadataCreateEvent)
    ]
    put_events = [event for event in state.upstream_events if isinstance(event, BodyPutEvent)]
    assert len(create_events) == 1
    assert len(put_events) == 2
    assert put_events[0].upload_token == put_events[1].upload_token
    assert put_events[0].body_hash != put_events[1].body_hash

    async with httpx.AsyncClient(transport=transport, base_url="http://emulator") as client:
        cleared = await client.post("/control/clear-received")
    assert cleared.status_code == 204
    assert state.accepted_bodies == {}
    assert state.upstream_events == []


async def test_put_expired_token_403(client: httpx.AsyncClient) -> None:
    token = await _mint_token(client)
    # Shorten presigned TTL to zero via /control/presigned-ttl.
    await client.post("/control/presigned-ttl", json={"seconds": 0})
    create_r = await client.post(
        "/v1/files/create",
        json={"domain": "D", "fileName": "f", "metadata": {"keyValueStore": {}}},
        headers={"Authorization": f"Bearer {token}"},
    )
    upload_url = create_r.json()["uploadUrl"]
    relative = upload_url.replace("http://emulator", "")
    r = await client.put(relative, content=b"hi")
    assert r.status_code == 403


async def test_put_unknown_token_403(client: httpx.AsyncClient) -> None:
    r = await client.put("/v1/files/upload/no-such-token", content=b"")
    assert r.status_code == 403


async def test_put_with_the_minted_presigned_query_is_accepted(
    no_auth_client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """The URL exactly as minted -> 200 and the bytes stored.

    Objective: the positive control for the three rejection tests below. The
    signature check must accept the URL the emulator itself issued, or the
    checks would only be proving that PUTs fail.

    Expected outcome: 200, and the body readable back off the state.
    """
    client, state = no_auth_client_and_state
    upload_url = await _mint_upload_url(client)

    r = await client.put(upload_url, content=b"the body bytes")

    assert r.status_code == 200
    assert [body.body for body in state.accepted_bodies.values()] == [b"the body bytes"]


async def test_put_with_stripped_presigned_query_is_rejected(
    no_auth_client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """The right path with no query at all -> 403, nothing stored.

    Objective: the emulator mints an S3-shaped presigned URL specifically so a
    forwarder cannot drop the query and still be served. The handler only ever
    looked up the path token, so a PUT with ``?expires=…&sig=…`` stripped
    stored the body and returned 200, and every forwarding-fidelity assertion
    built on that URL was vacuous.

    Expected outcome: 403 SignatureDoesNotMatch and an empty accepted-body
    store, so the rejection is a real refusal rather than a late error after
    the side effect.
    """
    client, state = no_auth_client_and_state
    upload_url = await _mint_upload_url(client)
    path_only = upload_url.split("?", 1)[0]

    r = await client.put(path_only, content=b"body")

    assert r.status_code == 403
    assert r.json()["detail"] == "SignatureDoesNotMatch"
    assert state.accepted_bodies == {}


async def test_put_with_wrong_signature_is_rejected(
    no_auth_client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """A ``sig`` that is not the minted one -> 403, nothing stored.

    Objective: a query that is present but wrong is the case a stripped-query
    check alone would miss. The signature is the emulator's stand-in for the
    S3 HMAC, so a rewritten value must be refused exactly like a missing one.

    Expected outcome: 403 SignatureDoesNotMatch and an empty accepted-body store.
    """
    client, state = no_auth_client_and_state
    upload_url = await _mint_upload_url(client)
    pending = next(iter(state.pending_uploads.values()))
    tampered = upload_url.replace(f"sig={pending.signature}", "sig=not-the-minted-signature")
    assert tampered != upload_url

    r = await client.put(tampered, content=b"body")

    assert r.status_code == 403
    assert r.json()["detail"] == "SignatureDoesNotMatch"
    assert state.accepted_bodies == {}


async def test_put_with_expires_disagreeing_with_the_record_is_rejected(
    no_auth_client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """An ``expires`` the emulator did not mint -> 403, nothing stored.

    Objective: the expiry advertised in the URL is part of what was signed, so
    a caller must not be able to extend (or otherwise edit) it and be served.
    The record carries the epoch that was minted, and the handler compares
    against that rather than trusting the inbound value.

    Expected outcome: 403 SignatureDoesNotMatch and an empty accepted-body
    store, even though the token and the signature are both genuine and the
    edited deadline is in the future.
    """
    client, state = no_auth_client_and_state
    upload_url = await _mint_upload_url(client)
    pending = next(iter(state.pending_uploads.values()))
    extended = upload_url.replace(
        f"expires={pending.expires_epoch}",
        f"expires={pending.expires_epoch + 1}",
    )
    assert extended != upload_url

    r = await client.put(extended, content=b"body")

    assert r.status_code == 403
    assert r.json()["detail"] == "SignatureDoesNotMatch"
    assert state.accepted_bodies == {}


async def test_clear_received_drops_the_idempotency_cache(
    no_auth_client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """One Idempotency-Key across a reset -> a fresh file id, not the cached one.

    Objective: ``clear_received`` reset the accepted bodies and the event log
    but left ``idempotency_cache``, whose entries live for the dedup window
    (an hour by default) and are pruned only lazily on a same-key lookup. Two
    scenarios in one emulator process that happened to use the same literal
    key therefore shared a file id and an upload token, with the second one
    silently served the first one's create response.

    Expected outcome: after ``POST /control/clear-received`` the same key
    mints a new file id and upload token, and the cache is empty.
    """
    client, state = no_auth_client_and_state
    headers = {"Idempotency-Key": "shared-literal-key"}
    first = await client.post("/v1/files/create", json=_CREATE_PAYLOAD, headers=headers)

    cleared = await client.post("/control/clear-received")
    assert cleared.status_code == 204
    assert state.idempotency_cache == {}

    second = await client.post("/v1/files/create", json=_CREATE_PAYLOAD, headers=headers)

    assert first.status_code == second.status_code == 200
    assert first.json()["fileInformation"]["id"] != second.json()["fileInformation"]["id"]
    assert first.json()["uploadUrl"] != second.json()["uploadUrl"]


async def test_get_returns_minted_file_information(client: httpx.AsyncClient) -> None:
    token = await _mint_token(client)
    create_r = await client.post(
        "/v1/files/create",
        json={"domain": "D", "fileName": "f.parquet", "metadata": {"keyValueStore": {}}},
        headers={"Authorization": f"Bearer {token}"},
    )
    file_id = create_r.json()["fileInformation"]["id"]
    r = await client.get(
        f"/v1/files/{file_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    assert r.json()["id"] == file_id


async def test_search_stub_returns_empty(client: httpx.AsyncClient) -> None:
    token = await _mint_token(client)
    r = await client.post(
        "/v1/files/search",
        json={"any": "query"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    assert r.json() == {"results": []}


async def test_received_log_complete(client: httpx.AsyncClient) -> None:
    token = await _mint_token(client)
    create_r = await client.post(
        "/v1/files/create",
        json={
            "domain": "D",
            "fileName": "f",
            "metadata": {"keyValueStore": {"phantom_local_uuid": "u-1"}},
        },
        headers={
            "Authorization": f"Bearer {token}",
            "Idempotency-Key": "the-key",
        },
    )
    upload_url = create_r.json()["uploadUrl"]
    relative = upload_url.replace("http://emulator", "")
    await client.put(
        relative,
        content=b"abc",
        headers={"x-amz-meta-uploader-id": "alice"},
    )

    received_r = await client.get("/control/received")
    body = received_r.json()
    assert len(body["received"]) == 1
    entry = body["received"][0]
    assert entry["body_size"] == 3
    assert entry["metadata_kvs"]["phantom_local_uuid"] == "u-1"
    assert entry["x_amz_meta_headers"]["x-amz-meta-uploader-id"] == "alice"
    assert entry["idempotency_key"] == "the-key"


async def test_no_auth_mode_skips_auth(no_auth_client: httpx.AsyncClient) -> None:
    r = await no_auth_client.post(
        "/v1/files/create",
        json={"domain": "D", "fileName": "f", "metadata": {"keyValueStore": {}}},
    )
    assert r.status_code == 200


async def test_received_log_records_all_headers(client: httpx.AsyncClient) -> None:
    """ReceivedEntry.headers captures every inbound header on the PUT.

    The capture is the test-infrastructure ground-truth for the
    transparent-proxy invariant. Tests use it to assert that
    ``X-Phantom-*`` headers were stripped on the client-Phantom boundary,
    that ``Authorization`` carries the cached bearer byte-equal, and
    that custom client headers (``User-Agent``, ``X-Custom-*``) round-trip
    intact.
    """
    token = await _mint_token(client)
    create_r = await client.post(
        "/v1/files/create",
        json={
            "domain": "D",
            "fileName": "f",
            "metadata": {"keyValueStore": {"phantom_local_uuid": "u-1"}},
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    upload_url = create_r.json()["uploadUrl"]
    relative = upload_url.replace("http://emulator", "")
    await client.put(
        relative,
        content=b"payload-bytes",
        headers={
            "Authorization": f"Bearer {token}",
            "User-Agent": "client-agent/1.2.3",
            "X-Custom-Trace-Id": "trace-abc-123",
            "x-amz-meta-ref-id": "hist-001",
        },
    )

    received_r = await client.get("/control/received")
    body = received_r.json()
    assert len(body["received"]) == 1
    entry = body["received"][0]

    # Full-header capture: every inbound header, lowercased keys.
    headers = entry["headers"]
    assert headers["authorization"] == f"Bearer {token}"
    assert headers["user-agent"] == "client-agent/1.2.3"
    assert headers["x-custom-trace-id"] == "trace-abc-123"
    assert headers["x-amz-meta-ref-id"] == "hist-001"
    # Audit invariant: NO X-Phantom-* headers should have been added by
    # the client-Phantom boundary path. (For this direct-PUT test no such
    # path exists, so the absence is trivially true; the assertion is
    # the canonical contract a downstream E2E test will rely on.)
    assert not any(k.startswith("x-phantom-") for k in headers)
    # x-amz-meta capture still works (back-compat with the narrower field).
    assert entry["x_amz_meta_headers"]["x-amz-meta-ref-id"] == "hist-001"
