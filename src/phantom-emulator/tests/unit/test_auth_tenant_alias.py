"""Unit pins for the T3 lifecycle emulator surface.

Three additions land together: the AAD tenant token alias
(``POST /{tenant}/oauth2/v2.0/token`` — azure-identity's direct target), the
secret-free ordered mint-attempt ledger, and the test-only AUTH_TOKEN
middleware gate. Each is pinned here at the unit tier; the T3 lifecycle E2E
drives them through the real subprocess stack.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest
from phantom_emulator.app import create_app
from phantom_emulator.config import AppConfig
from phantom_emulator.state import AuthTokenGate, EmulatorState

_TENANT = "11111111-2222-3333-4444-555555555555"
_CLIENT_ID = "test-client"
# The AppConfig DEFAULT clients entry (the e2e YAML overrides this suite-wide;
# a bare AppConfig() carries test-client / test-secret).
_GOOD_SECRET = "test-secret"
_BAD_SECRET = "t3-wrong-secret-b41c9d"

_GATE_BUDGET_SECONDS = 5.0


@pytest.fixture
async def client_and_state(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[httpx.AsyncClient, EmulatorState]]:
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    app = create_app(AppConfig())
    state: EmulatorState = app.state.emulator_state
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=transport, base_url="http://emulator") as client:
        yield client, state


def _grant(secret: str) -> dict[str, str]:
    return {
        "grant_type": "client_credentials",
        "client_id": _CLIENT_ID,
        "client_secret": secret,
    }


async def test_tenant_alias_mints_like_the_generic_route(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """Objective: the tenant alias is the same grant as /oauth/token.

    Expected: a valid client mints 200 with an access token that lands in
    ``issued_tokens``; the tenant segment is accepted and ignored.
    """
    client, state = client_and_state
    response = await client.post(f"/{_TENANT}/oauth2/v2.0/token", data=_grant(_GOOD_SECRET))
    assert response.status_code == 200
    token = response.json()["access_token"]
    assert token in state.issued_tokens


async def test_tenant_alias_rejects_bad_secret(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """Objective: alias validation is the generic route's validation.

    Expected: a wrong secret gets the AAD-shaped 401 invalid_client.
    """
    client, _state = client_and_state
    response = await client.post(f"/{_TENANT}/oauth2/v2.0/token", data=_grant(_BAD_SECRET))
    assert response.status_code == 401
    assert response.json()["error"] == "invalid_client"


async def test_mint_attempt_ledger_records_safe_slots_in_order(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """Objective: the armed ledger records order + slot + status, no secrets.

    Expected: a rejected primary then an accepted secondary yield exactly
    [(1, primary, 401), (2, secondary, 200)], and no secret substring appears
    anywhere in the ledger's repr (the audit's no-secret rule).
    """
    client, state = client_and_state
    state.mint_slot_secrets = {"primary": _BAD_SECRET, "secondary": _GOOD_SECRET}
    await client.post(f"/{_TENANT}/oauth2/v2.0/token", data=_grant(_BAD_SECRET))
    await client.post(f"/{_TENANT}/oauth2/v2.0/token", data=_grant(_GOOD_SECRET))

    shape = [(a.seq, a.slot, a.status) for a in state.mint_attempts]
    assert shape == [(1, "primary", 401), (2, "secondary", 200)]
    ledger_repr = repr(state.mint_attempts)
    assert _BAD_SECRET not in ledger_repr
    assert _GOOD_SECRET not in ledger_repr


async def test_disarmed_ledger_records_nothing(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """Objective: the ledger is inert unless a test arms the slot map.

    Expected: with no slot map, minting appends nothing.
    """
    client, state = client_and_state
    await client.post("/oauth/token", data=_grant(_GOOD_SECRET))
    assert state.mint_attempts == []


async def test_auth_token_gate_holds_until_released(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """Objective: the closed gate holds a token request before dispatch.

    Expected: the request task signals ``reached`` but does not complete
    while the gate is closed; opening ``release`` lets it mint 200. Non-auth
    paths are never held.
    """
    client, state = client_and_state
    state.auth_token_gate = AuthTokenGate()

    request_task = asyncio.create_task(
        client.post(f"/{_TENANT}/oauth2/v2.0/token", data=_grant(_GOOD_SECRET))
    )
    await asyncio.wait_for(state.auth_token_gate.reached.wait(), timeout=_GATE_BUDGET_SECONDS)
    assert not request_task.done(), "gated token request completed while the gate was closed"

    # A non-AUTH_TOKEN path proceeds while the gate holds.
    status = await client.get("/control/status")
    assert status.status_code == 200

    state.auth_token_gate.release.set()
    response = await asyncio.wait_for(request_task, timeout=_GATE_BUDGET_SECONDS)
    assert response.status_code == 200
