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
