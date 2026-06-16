"""Unit tests for :mod:`phantom_emulator.server`."""

from __future__ import annotations

import httpx
import pytest
from phantom_emulator.config import AppConfig, ServerCfg
from phantom_emulator.failure.injection import FailurePolicy, FailureScope
from phantom_emulator.server import start_server


@pytest.fixture(autouse=True)
def _emulator_signing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)


async def test_start_ephemeral_port() -> None:
    server = await start_server(AppConfig(server=ServerCfg(port=0)))
    try:
        assert server.port > 0
        assert server.url().startswith("http://127.0.0.1:")
    finally:
        await server.stop()


async def test_url_returns_fully_qualified() -> None:
    server = await start_server(AppConfig(server=ServerCfg(port=0)))
    try:
        url = server.url()
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{url}/control/status")
        assert r.status_code == 200
    finally:
        await server.stop()


async def test_stop_terminates() -> None:
    server = await start_server(AppConfig(server=ServerCfg(port=0)))
    await server.stop()
    # After stop, the port should refuse connections (but Linux/macOS
    # might keep the socket in TIME_WAIT briefly — we only assert the
    # serve task is done).
    assert server._serve_task.done()


async def test_inject_failure_via_typed_surface() -> None:
    server = await start_server(AppConfig(server=ServerCfg(port=0)))
    try:
        policy = FailurePolicy(
            scope=FailureScope.UPSTREAM_FILES_CREATE,
            error_rate_5xx=1.0,
        )
        server.inject_failure(policy)

        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"{server.url()}/v1/files/create",
                json={},
            )
        assert r.status_code == 503

        server.clear_failures()
    finally:
        await server.stop()


async def test_pause_resume_via_typed_surface() -> None:
    server = await start_server(AppConfig(server=ServerCfg(port=0)))
    try:
        server.pause()
        async with httpx.AsyncClient() as client:
            r = await client.post(f"{server.url()}/v1/files/create", json={})
        assert r.status_code == 503
        server.resume()
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{server.url()}/control/status")
        assert r.status_code == 200
    finally:
        await server.stop()


async def test_received_empty_initially() -> None:
    server = await start_server(AppConfig(server=ServerCfg(port=0)))
    try:
        assert server.received() == []
    finally:
        await server.stop()


async def test_drain_returns_quickly() -> None:
    server = await start_server(AppConfig(server=ServerCfg(port=0)))
    try:
        await server.drain(timeout_seconds=0.01)
    finally:
        await server.stop()
