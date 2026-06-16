"""Unit tests for :mod:`phantom_emulator.auth.modes`."""

from __future__ import annotations

from phantom_emulator.auth.jwt_minter import JwtMinter
from phantom_emulator.auth.modes import AuthMode, AuthModePolicy, authenticate
from phantom_emulator.config import AuthCfg, AuthSigningCfg

_LONG_SECRET: str = "x" * 32


def _minter() -> JwtMinter:
    cfg = AuthCfg(signing=AuthSigningCfg(mode="HS256"))
    return JwtMinter(cfg=cfg, hs256_secret=_LONG_SECRET, rsa_keys=None)


def test_none_mode_always_passes() -> None:
    policy = AuthModePolicy(mode=AuthMode.NONE)
    assert authenticate({}, policy, _minter()) is True


def test_plain_bearer_mode() -> None:
    policy = AuthModePolicy(
        mode=AuthMode.PLAIN_BEARER,
        plain_bearer_allowlist=frozenset({"allowed-token"}),
    )
    assert authenticate({"authorization": "Bearer allowed-token"}, policy, _minter()) is True
    assert authenticate({"authorization": "Bearer wrong-token"}, policy, _minter()) is False
    assert authenticate({}, policy, _minter()) is False


def test_api_key_mode() -> None:
    policy = AuthModePolicy(mode=AuthMode.API_KEY, api_key_secret="topsecret")
    assert authenticate({"x-api-key": "topsecret"}, policy, _minter()) is True
    assert authenticate({"x-api-key": "wrong"}, policy, _minter()) is False
    assert authenticate({}, policy, _minter()) is False


def test_static_token_mode() -> None:
    policy = AuthModePolicy(mode=AuthMode.STATIC_TOKEN, static_jwt="STATIC")
    assert authenticate({"authorization": "Bearer STATIC"}, policy, _minter()) is True
    assert authenticate({"authorization": "Bearer not-static"}, policy, _minter()) is False


def test_oauth_client_credentials_mode_accepts_valid_jwt() -> None:
    minter = _minter()
    token, _ = minter.mint(client_id="test-client")
    policy = AuthModePolicy(mode=AuthMode.OAUTH_CLIENT_CREDENTIALS)
    assert authenticate({"authorization": f"Bearer {token}"}, policy, minter) is True


def test_oauth_client_credentials_rejects_garbage() -> None:
    minter = _minter()
    policy = AuthModePolicy(mode=AuthMode.OAUTH_CLIENT_CREDENTIALS)
    assert authenticate({"authorization": "Bearer not-a-jwt"}, policy, minter) is False
    assert authenticate({}, policy, minter) is False


def test_header_lookup_is_case_insensitive() -> None:
    policy = AuthModePolicy(
        mode=AuthMode.PLAIN_BEARER,
        plain_bearer_allowlist=frozenset({"t"}),
    )
    assert authenticate({"Authorization": "Bearer t"}, policy, _minter()) is True
    assert authenticate({"AUTHORIZATION": "Bearer t"}, policy, _minter()) is True
