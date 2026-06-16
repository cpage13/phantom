"""Unit tests for :mod:`phantom_emulator.routers.control`."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from phantom_emulator.app import create_app
from phantom_emulator.config import AppConfig
from phantom_emulator.failure.injection import FailurePolicy, FailureScope


@pytest.fixture
async def client(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[httpx.AsyncClient]:
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    app = create_app(AppConfig())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://emulator") as c:
        yield c


async def test_status_shape(client: httpx.AsyncClient) -> None:
    r = await client.get("/control/status")
    assert r.status_code == 200
    body = r.json()
    assert body["global_paused"] is False
    assert body["accepted_bodies_count"] == 0
    assert body["pending_uploads_count"] == 0
    assert body["issued_tokens_count"] == 0
    assert body["auth_mode_default"] == "oauth_client_credentials"
    assert body["policies"] == []


async def test_inject_and_clear_failure(client: httpx.AsyncClient) -> None:
    policy = FailurePolicy(scope=FailureScope.UPSTREAM_FILES_CREATE, error_rate_5xx=0.5)
    r = await client.post("/control/inject-failure", json=policy.model_dump(mode="json"))
    assert r.status_code == 204

    status_r = await client.get("/control/status")
    assert len(status_r.json()["policies"]) == 1

    clear_r = await client.post("/control/clear-failures")
    assert clear_r.status_code == 204
    after = await client.get("/control/status")
    assert after.json()["policies"] == []


async def test_pause_and_resume(client: httpx.AsyncClient) -> None:
    pause_r = await client.post("/control/pause")
    assert pause_r.status_code == 204
    status = await client.get("/control/status")
    assert status.json()["global_paused"] is True

    resume_r = await client.post("/control/resume")
    assert resume_r.status_code == 204
    status_after = await client.get("/control/status")
    assert status_after.json()["global_paused"] is False


async def test_expire_all_now(client: httpx.AsyncClient) -> None:
    # Mint a token, then expire all.
    await client.post(
        "/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "test-client",
            "client_secret": "test-secret",
        },
    )
    r = await client.post("/control/expire-all-now")
    assert r.status_code == 204
    # No external way to verify exp without decoding; check status reports
    # the same count (we don't delete on expire).
    status = await client.get("/control/status")
    assert status.json()["issued_tokens_count"] >= 1


async def test_revoke_tokens(client: httpx.AsyncClient) -> None:
    await client.post(
        "/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "test-client",
            "client_secret": "test-secret",
        },
    )
    r = await client.post("/control/revoke-tokens")
    assert r.status_code == 204
    status = await client.get("/control/status")
    assert status.json()["issued_tokens_count"] == 0


async def test_set_extra_claims(client: httpx.AsyncClient) -> None:
    r = await client.post(
        "/control/auth/extra-claims",
        json={"claims": {"sub": "12345", "department": "QA"}},
    )
    assert r.status_code == 204

    # Subsequent mint picks up the claims.
    mint_r = await client.post(
        "/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "test-client",
            "client_secret": "test-secret",
        },
    )
    import jwt as pyjwt

    token = mint_r.json()["access_token"]
    payload = pyjwt.decode(token, options={"verify_signature": False})
    assert payload["sub"] == "12345"
    assert payload["department"] == "QA"


async def test_set_auth_mode_global(client: httpx.AsyncClient) -> None:
    r = await client.post("/control/auth/mode", json={"mode": "none", "scope": "*"})
    assert r.status_code == 204
    status = await client.get("/control/status")
    assert status.json()["auth_mode_default"] == "none"


async def test_set_presigned_ttl(client: httpx.AsyncClient) -> None:
    r = await client.post("/control/presigned-ttl", json={"seconds": 1})
    assert r.status_code == 204


async def test_set_seed(client: httpx.AsyncClient) -> None:
    r = await client.post("/control/seed", json={"seed": 42})
    assert r.status_code == 204


async def test_clear_received_resets_log(client: httpx.AsyncClient) -> None:
    # Set no-auth mode and load one entry.
    await client.post("/control/auth/mode", json={"mode": "none", "scope": "*"})
    create_r = await client.post(
        "/v1/files/create",
        json={"domain": "D", "fileName": "f", "metadata": {"keyValueStore": {}}},
    )
    upload_url = create_r.json()["uploadUrl"]
    relative = upload_url.replace("http://emulator", "")
    await client.put(relative, content=b"x")
    assert len((await client.get("/control/received")).json()["received"]) == 1

    r = await client.post("/control/clear-received")
    assert r.status_code == 204
    assert (await client.get("/control/received")).json()["received"] == []


async def test_pause_blocks_upstream_only(client: httpx.AsyncClient) -> None:
    await client.post("/control/auth/mode", json={"mode": "none", "scope": "*"})
    await client.post("/control/pause")
    r = await client.post(
        "/v1/files/create",
        json={"domain": "D", "fileName": "f", "metadata": {"keyValueStore": {}}},
    )
    assert r.status_code == 503
    # Control plane is unaffected.
    assert (await client.get("/control/status")).status_code == 200


async def test_unavailable_until_policy_returns_503(client: httpx.AsyncClient) -> None:
    until = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    policy = {
        "scope": "upstream.files.create",
        "unavailable_until": until,
    }
    r = await client.post("/control/inject-failure", json=policy)
    assert r.status_code == 204
    await client.post("/control/auth/mode", json={"mode": "none", "scope": "*"})
    upstream_r = await client.post(
        "/v1/files/create",
        json={"domain": "D", "fileName": "f", "metadata": {"keyValueStore": {}}},
    )
    assert upstream_r.status_code == 503
