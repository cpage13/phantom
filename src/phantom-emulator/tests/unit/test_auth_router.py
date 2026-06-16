"""Unit tests for :mod:`phantom_emulator.routers.auth`."""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import jwt as pyjwt
import pytest
from phantom_emulator.app import create_app
from phantom_emulator.auth.modes import AuthMode
from phantom_emulator.config import AppConfig, AuthCfg, AuthClient, AuthSigningCfg


def _cfg(**overrides: object) -> AppConfig:
    return AppConfig(**overrides)  # type: ignore[arg-type]


@pytest.fixture
async def client(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[httpx.AsyncClient]:
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    app = create_app(_cfg())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://emulator") as c:
        yield c


@pytest.fixture
async def rs256_client(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[httpx.AsyncClient]:
    cfg = AppConfig(auth=AuthCfg(signing=AuthSigningCfg(mode="RS256")))
    app = create_app(cfg)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://emulator") as c:
        yield c


async def test_token_mint(client: httpx.AsyncClient) -> None:
    r = await client.post(
        "/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "test-client",
            "client_secret": "test-secret",
            "scope": "api://target/.default",
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["token_type"] == "Bearer"
    assert body["expires_in"] > 0
    assert isinstance(body["access_token"], str)


async def test_token_rejects_bad_client(client: httpx.AsyncClient) -> None:
    r = await client.post(
        "/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "wrong",
            "client_secret": "test-secret",
        },
    )
    assert r.status_code == 401


async def test_token_rejects_bad_grant_type(client: httpx.AsyncClient) -> None:
    r = await client.post(
        "/oauth/token",
        data={
            "grant_type": "password",
            "client_id": "test-client",
            "client_secret": "test-secret",
        },
    )
    assert r.status_code == 400


async def test_token_static_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    cfg = AppConfig(
        auth=AuthCfg(
            default_mode=AuthMode.STATIC_TOKEN,
            clients=[AuthClient(client_id="anything", client_secret="anything")],
        )
    )
    app = create_app(cfg)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://emulator") as client:
        r1 = await client.post(
            "/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "anything",
                "client_secret": "anything",
            },
        )
        r2 = await client.post(
            "/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "anything",
                "client_secret": "anything",
            },
        )
    assert r1.status_code == 200
    assert r2.status_code == 200
    # Same token both times — static mode returns the pre-minted JWT.
    assert r1.json()["access_token"] == r2.json()["access_token"]


async def test_discovery_shape(client: httpx.AsyncClient) -> None:
    r = await client.get("/.well-known/openid-configuration")
    assert r.status_code == 200
    body = r.json()
    assert "issuer" in body
    assert body["token_endpoint"].endswith("/oauth/token")
    assert body["jwks_uri"].endswith("/.well-known/jwks.json")


async def test_jwks_hs256_empty(client: httpx.AsyncClient) -> None:
    r = await client.get("/.well-known/jwks.json")
    assert r.status_code == 200
    assert r.json() == {"keys": []}


async def test_jwks_rs256_has_key(rs256_client: httpx.AsyncClient) -> None:
    r = await rs256_client.get("/.well-known/jwks.json")
    assert r.status_code == 200
    body = r.json()
    assert len(body["keys"]) == 1
    entry = body["keys"][0]
    assert entry["kty"] == "RSA"
    assert entry["alg"] == "RS256"


async def test_minted_token_decodes(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    r = await client.post(
        "/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "test-client",
            "client_secret": "test-secret",
        },
    )
    token = r.json()["access_token"]
    # Decode with the same shared secret used by create_app's lifespan.
    payload = pyjwt.decode(
        token,
        "x" * 32,
        algorithms=["HS256"],
        audience=AuthCfg().audience,
        issuer=AuthCfg().issuer,
    )
    assert payload["sub"] == "test-client"
