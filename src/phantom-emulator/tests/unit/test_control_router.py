"""Unit tests for :mod:`phantom_emulator.routers.control`."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from phantom_emulator.app import create_app
from phantom_emulator.auth.modes import AuthMode
from phantom_emulator.config import AppConfig, AuthCfg
from phantom_emulator.failure.injection import FailurePolicy, FailureScope
from phantom_emulator.state import EmulatorState

# A create-file body with nothing scenario-specific in it: these tests are
# about who is allowed to call, not about what the call carries.
_CREATE_PAYLOAD: dict[str, object] = {
    "domain": "D",
    "fileName": "f",
    "metadata": {"keyValueStore": {}},
}


@pytest.fixture
async def client(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[httpx.AsyncClient]:
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    app = create_app(AppConfig())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://emulator") as c:
        yield c


@pytest.fixture
async def client_and_state(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[httpx.AsyncClient, EmulatorState]]:
    """The default-mode client plus the state, for tests that mint out of band."""
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    app = create_app(AppConfig())
    state: EmulatorState = app.state.emulator_state
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://emulator") as c:
        yield c, state


@pytest.fixture
async def static_token_client(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[httpx.AsyncClient]:
    """A client whose emulator runs the ``static_token`` auth mode."""
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    app = create_app(AppConfig(auth=AuthCfg(default_mode=AuthMode.STATIC_TOKEN)))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://emulator") as c:
        yield c


async def _mint(client: httpx.AsyncClient) -> str:
    """Mint a bearer through the token endpoint and return it."""
    r = await client.post(
        "/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "test-client",
            "client_secret": "test-secret",
        },
    )
    return str(r.json()["access_token"])


async def _create_with(client: httpx.AsyncClient, bearer: str) -> httpx.Response:
    """Call the authenticated create endpoint with ``bearer``."""
    return await client.post(
        "/v1/files/create",
        json=_CREATE_PAYLOAD,
        headers={"Authorization": f"Bearer {bearer}"},
    )


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


async def test_revoke_tokens_makes_the_bearer_401_in_the_default_mode(
    client: httpx.AsyncClient,
) -> None:
    """Revoke -> the bearer that just worked 401s, and a fresh one works again.

    Objective: the emulator is the oracle for Phantom's auth-recovery path, so
    ``POST /control/revoke-tokens`` has to be observable in the DEFAULT
    ``oauth_client_credentials`` mode. It used to clear the ``issued_tokens``
    bookkeeping only, which that mode never reads, so a test that revoked and
    expected a 401 got a 200 and the whole auth_expired plus kicker-wake path
    went unexercised.

    Expected outcome: create 200 before the revoke, 401 with the identical
    credential after it, and 200 again on a newly minted credential, which is
    the recovery the parked row depends on.
    """
    token = await _mint(client)
    assert (await _create_with(client, token)).status_code == 200

    revoke = await client.post("/control/revoke-tokens")
    assert revoke.status_code == 204

    rejected = await _create_with(client, token)
    assert rejected.status_code == 401

    fresh = await _mint(client)
    assert (await _create_with(client, fresh)).status_code == 200


async def test_revoke_tokens_reaches_a_credential_the_emulator_never_issued(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """A bearer minted out of band, then accepted once, is revocable too.

    Objective: the e2e suite signs its own bearer with the shared secret and
    pushes it to Phantom, so that credential never passes through
    ``/oauth/token`` and never lands in ``issued_tokens``. Revocation keyed on
    the issuance bookkeeping alone would leave exactly the credential the
    auth-recovery scenario cares about untouched.

    Expected outcome: the out-of-band credential authenticates, and after the
    revoke the identical credential 401s, because the emulator revokes what it
    has ACCEPTED as well as what it has issued.
    """
    client, state = client_and_state
    assert state.jwt_minter is not None
    token, _expires_at = state.jwt_minter.mint(client_id="out-of-band-client")
    assert token not in state.issued_tokens

    assert (await _create_with(client, token)).status_code == 200

    assert (await client.post("/control/revoke-tokens")).status_code == 204

    assert (await _create_with(client, token)).status_code == 401


async def test_expire_all_now_makes_the_bearer_401_in_the_default_mode(
    client: httpx.AsyncClient,
) -> None:
    """Expire -> the bearer 401s even though its own ``exp`` is still ahead.

    Objective: ``POST /control/expire-all-now`` aged the emulator's recorded
    expiry, which the stateless JWT check never consults. The claim inside an
    issued token cannot be moved, so without the credential ledger this control
    was a no-op in the default mode and no test could stage an expired
    upstream credential.

    Expected outcome: create 200 before the call, 401 with the same credential
    after it, and 200 again once a fresh credential is minted.
    """
    token = await _mint(client)
    assert (await _create_with(client, token)).status_code == 200

    expire = await client.post("/control/expire-all-now")
    assert expire.status_code == 204

    assert (await _create_with(client, token)).status_code == 401
    assert (await _create_with(client, await _mint(client))).status_code == 200


async def test_expire_all_now_keeps_static_token_mode_serving(
    static_token_client: httpx.AsyncClient,
) -> None:
    """static_token mode: expire re-mints, and the re-issued token still works.

    Objective: the credential ledger must not disturb the mode that already
    answered to the control surface. ``static_token`` authenticates by
    comparing the bearer against the pre-minted JWT, so expiry there means
    "clear it and mint another", and a caller that re-fetches must be served.

    Expected outcome: the token endpoint hands out a working credential both
    before and after ``expire-all-now``.
    """
    before = await _mint(static_token_client)
    assert (await _create_with(static_token_client, before)).status_code == 200

    assert (await static_token_client.post("/control/expire-all-now")).status_code == 204

    after = await _mint(static_token_client)
    assert (await _create_with(static_token_client, after)).status_code == 200


async def test_idempotency_dedup_window_endpoint_sets_the_window(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """POST /control/idempotency-dedup-window -> 204 and the configured window moves.

    Objective: the dedup window was reachable only from the in-process oracle,
    which reached around ``EmulatorState`` into the config object, so the HTTP
    tier had no way to set it and the two control surfaces were not mirrors.

    Expected outcome: the endpoint answers 204 and the value the create path
    reads is the one it was given.
    """
    client, state = client_and_state
    r = await client.post("/control/idempotency-dedup-window", json={"seconds": 11})
    assert r.status_code == 204
    assert state.cfg.upstream.idempotency_dedup_window_seconds == 11


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
