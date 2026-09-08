"""Unit tests for :mod:`phantom_emulator.failure.middleware`.

The middleware is exercised through a tiny FastAPI app + httpx
``ASGITransport`` — same pattern the rest of the test suite uses.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse
from phantom_emulator.config import AppConfig
from phantom_emulator.failure.injection import (
    FailureInjectionState,
    FailurePolicy,
    FailureScope,
)
from phantom_emulator.failure.middleware import make_failure_middleware
from phantom_emulator.state import EmulatorState


def _build_app(state: EmulatorState) -> FastAPI:
    app = FastAPI()
    app.middleware("http")(make_failure_middleware(state))

    @app.post("/v1/files/create")
    async def create() -> JSONResponse:
        return JSONResponse({"ok": True})

    @app.put("/v1/files/upload/{token}")
    async def upload(token: str) -> JSONResponse:
        return JSONResponse({"token": token, "body": "x" * 200})

    @app.get("/control/status")
    async def control_status() -> JSONResponse:
        return JSONResponse({"ok": True})

    return app


def _state() -> EmulatorState:
    cfg = AppConfig()
    state = EmulatorState(cfg=cfg, started_at=datetime.now(UTC))
    state.failure_state = FailureInjectionState(seed=0)
    return state


@pytest.fixture
async def client_and_state() -> AsyncIterator[tuple[httpx.AsyncClient, EmulatorState]]:
    state = _state()
    app = _build_app(state)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, state


async def test_503_when_unavailable(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    client, state = client_and_state
    assert state.failure_state is not None
    state.failure_state.set_policy(
        FailurePolicy(
            scope=FailureScope.UPSTREAM_FILES_CREATE,
            unavailable_until=datetime.now(UTC) + timedelta(hours=1),
        )
    )
    r = await client.post("/v1/files/create")
    assert r.status_code == 503
    assert r.headers.get("Retry-After") == "5"
    assert state.failure_state.error_rate_5xx_count(FailureScope.UPSTREAM_FILES_CREATE) == 0


async def test_401_after_n_calls(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    client, state = client_and_state
    assert state.failure_state is not None
    state.failure_state.set_policy(
        FailurePolicy(
            scope=FailureScope.UPSTREAM_FILES_CREATE,
            auth_401_after_n_calls=2,
        )
    )
    # First two calls pass.
    assert (await client.post("/v1/files/create")).status_code == 200
    assert (await client.post("/v1/files/create")).status_code == 200
    # Third call (N=2, count=3) returns 401.
    r = await client.post("/v1/files/create")
    assert r.status_code == 401
    assert r.json() == {"error": "invalid_token"}


async def test_latency_applies(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    client, state = client_and_state
    assert state.failure_state is not None
    state.failure_state.set_policy(
        FailurePolicy(
            scope=FailureScope.UPSTREAM_FILES_CREATE,
            latency_ms=150,
        )
    )
    t0 = time.monotonic()
    r = await client.post("/v1/files/create")
    elapsed_ms = (time.monotonic() - t0) * 1000
    assert r.status_code == 200
    # Allow a fuzz; we only need to confirm the sleep happened.
    assert elapsed_ms >= 100


async def test_body_cutoff(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    client, state = client_and_state
    assert state.failure_state is not None
    state.failure_state.set_policy(
        FailurePolicy(
            scope=FailureScope.UPSTREAM_FILES_UPLOAD,
            body_cutoff_at_bytes=10,
        )
    )
    r = await client.put("/v1/files/upload/abc")
    assert r.status_code == 200
    assert len(r.content) == 10


async def test_approximate_rst_closes_connection(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    client, state = client_and_state
    assert state.failure_state is not None
    state.failure_state.set_policy(
        FailurePolicy(
            scope=FailureScope.UPSTREAM_FILES_UPLOAD,
            tcp_rst_on_request=True,
        )
    )
    r = await client.put("/v1/files/upload/abc")
    # Body is truncated to empty and Connection: close advertised.
    assert r.content == b""
    assert r.headers.get("connection", "").lower() == "close"


async def test_5xx_with_full_probability(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    client, state = client_and_state
    assert state.failure_state is not None
    state.failure_state.set_policy(
        FailurePolicy(
            scope=FailureScope.UPSTREAM_FILES_CREATE,
            error_rate_5xx=1.0,
        )
    )
    r = await client.post("/v1/files/create")
    assert r.status_code == 503
    assert state.failure_state.error_rate_5xx_count(FailureScope.UPSTREAM_FILES_CREATE) == 1
    assert state.failure_state.call_counts == {}


async def test_response_modifiers_apply_to_a_synthetic_response(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """A policy pairing ``unavailable_until`` with a body cutoff -> a truncated 503.

    Objective: the five knobs are documented as ONE ordered pipeline, but the
    response modifiers only ever saw the real handler's response. A policy
    whose gate fired had its modifiers silently dropped, so the compound
    faults an upstream actually produces (a 503 whose body is cut off
    mid-frame) were unreachable, and a caller who installed both got no signal
    that half the policy was ignored.

    Expected outcome: the gate decides the status, and the modifier then
    shapes that same response.
    """
    client, state = client_and_state
    assert state.failure_state is not None
    state.failure_state.set_policy(
        FailurePolicy(
            scope=FailureScope.UPSTREAM_FILES_CREATE,
            unavailable_until=datetime.now(UTC) + timedelta(hours=1),
            body_cutoff_at_bytes=7,
        )
    )

    r = await client.post("/v1/files/create")

    assert r.status_code == 503
    assert len(r.content) == 7


async def test_latency_applies_to_a_synthetic_response(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    """A policy pairing the 401 gate with ``latency_ms`` -> a SLOW 401.

    Objective: the latency sleep is step 4 of the same pipeline and was
    likewise skipped whenever an earlier gate produced the response, so "the
    upstream is rejecting us and taking its time about it" could not be
    staged, which is the shape that exercises a client's timeout handling.

    Expected outcome: the 401 still comes back, and not before the configured
    delay has elapsed.
    """
    client, state = client_and_state
    assert state.failure_state is not None
    state.failure_state.set_policy(
        FailurePolicy(
            scope=FailureScope.UPSTREAM_FILES_CREATE,
            auth_401_after_n_calls=0,
            latency_ms=150,
        )
    )

    t0 = time.monotonic()
    r = await client.post("/v1/files/create")
    elapsed_ms = (time.monotonic() - t0) * 1000

    assert r.status_code == 401
    # Same fuzz allowance as test_latency_applies: we only need to confirm
    # the sleep happened at all.
    assert elapsed_ms >= 100


async def test_global_pause_short_circuits(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    client, state = client_and_state
    state.global_paused = True
    r = await client.post("/v1/files/create")
    assert r.status_code == 503
    # Control plane unaffected.
    r2 = await client.get("/control/status")
    assert r2.status_code == 200


async def test_global_scope_falls_through(
    client_and_state: tuple[httpx.AsyncClient, EmulatorState],
) -> None:
    client, state = client_and_state
    assert state.failure_state is not None
    state.failure_state.set_policy(FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=1.0))
    # Any upstream path picks up the global policy.
    r = await client.put("/v1/files/upload/abc")
    assert r.status_code == 503


def test_response_serialization_helper() -> None:
    # Sanity check: PlainTextResponse exposes .body as bytes so the
    # body-cutoff helper can splice it.
    r = PlainTextResponse("hello world")
    assert isinstance(r.body, bytes)
