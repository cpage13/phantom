"""Unit tests for :mod:`phantom_emulator.app`."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from phantom_emulator.app import create_app
from phantom_emulator.auth.modes import AuthMode
from phantom_emulator.config import AppConfig, AuthCfg, AuthSigningCfg


def test_create_app_routes_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    app = create_app(AppConfig())
    assert isinstance(app, FastAPI)

    paths = {
        f"{','.join(sorted(r.methods))} {r.path}"
        for r in app.routes
        if hasattr(r, "methods") and hasattr(r, "path")
    }
    # Spot-check load-bearing routes.
    assert "POST /oauth/token" in paths
    assert "GET /.well-known/openid-configuration" in paths
    assert "GET /.well-known/jwks.json" in paths
    assert "POST /v1/files/create" in paths
    assert "PUT /v1/files/upload/{token}" in paths
    assert "GET /v1/files/{file_id}" in paths
    assert "POST /v1/files/search" in paths
    assert "GET /control/status" in paths
    assert "GET /control/received" in paths
    assert "POST /control/inject-failure" in paths
    assert "POST /control/clear-failures" in paths
    assert "POST /control/pause" in paths
    assert "POST /control/resume" in paths
    assert "POST /control/expire-all-now" in paths
    assert "POST /control/revoke-tokens" in paths
    assert "POST /control/auth/extra-claims" in paths
    assert "POST /control/auth/mode" in paths
    assert "POST /control/presigned-ttl" in paths
    assert "POST /control/seed" in paths
    assert "POST /control/clear-received" in paths
    assert "POST /control/shutdown" in paths


def test_lifespan_initializes_rs256_keypair_when_auto(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = AppConfig(auth=AuthCfg(signing=AuthSigningCfg(mode="RS256")))
    app = create_app(cfg)
    state = app.state.emulator_state
    assert state.rsa_keys is not None
    assert state.jwt_minter is not None


def test_lifespan_initializes_static_token_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EMULATOR_SIGNING_KEY", "x" * 32)
    cfg = AppConfig(auth=AuthCfg(default_mode=AuthMode.STATIC_TOKEN))
    app = create_app(cfg)
    state = app.state.emulator_state
    assert state.static_jwt is not None
    assert len(state.issued_tokens) >= 1
