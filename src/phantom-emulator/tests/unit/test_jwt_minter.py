"""Unit tests for :mod:`phantom_emulator.auth.jwt_minter`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import jwt as pyjwt
import pytest
from phantom_emulator.auth.jwks import generate_keypair
from phantom_emulator.auth.jwt_minter import JwtMinter
from phantom_emulator.config import AuthCfg, AuthSigningCfg

# Use a 32+-char HS256 secret to keep pyjwt from emitting
# InsecureKeyLengthWarning on every encode/decode.
_LONG_SECRET: str = "x" * 32


def _hs256_cfg() -> AuthCfg:
    return AuthCfg(signing=AuthSigningCfg(mode="HS256"))


def _rs256_cfg() -> AuthCfg:
    return AuthCfg(signing=AuthSigningCfg(mode="RS256"))


def test_hs256_roundtrip() -> None:
    secret = _LONG_SECRET
    minter = JwtMinter(cfg=_hs256_cfg(), hs256_secret=secret, rsa_keys=None)
    token, exp = minter.mint(client_id="test-client")

    assert isinstance(token, str)
    assert exp > datetime.now(UTC)

    payload = pyjwt.decode(
        token,
        secret,
        algorithms=["HS256"],
        audience=_hs256_cfg().audience,
        issuer=_hs256_cfg().issuer,
    )
    assert payload["sub"] == "test-client"
    assert payload["iss"] == _hs256_cfg().issuer
    assert payload["aud"] == _hs256_cfg().audience
    assert payload["tid"] == _hs256_cfg().tenant_id


def test_rs256_roundtrip_with_jwks() -> None:
    keys = generate_keypair()
    minter = JwtMinter(cfg=_rs256_cfg(), hs256_secret=None, rsa_keys=keys)
    token, _exp = minter.mint(client_id="test-client")

    payload = pyjwt.decode(
        token,
        keys.public_pem,
        algorithms=["RS256"],
        audience=_rs256_cfg().audience,
        issuer=_rs256_cfg().issuer,
    )
    assert payload["sub"] == "test-client"

    # The minter's verify() also accepts the same token.
    assert minter.verify(token)["sub"] == "test-client"

    # The token carries the kid header so JWKS verifiers can pick it.
    headers = pyjwt.get_unverified_header(token)
    assert headers["kid"] == keys.kid


def test_extra_claims_merged() -> None:
    minter = JwtMinter(cfg=_hs256_cfg(), hs256_secret=_LONG_SECRET, rsa_keys=None)
    token, _ = minter.mint(
        client_id="anything",
        extra_claims={"oid": "operator-1", "roles": ["a", "b"]},
    )
    payload = pyjwt.decode(
        token,
        _LONG_SECRET,
        algorithms=["HS256"],
        audience=_hs256_cfg().audience,
        issuer=_hs256_cfg().issuer,
    )
    assert payload["oid"] == "operator-1"
    assert payload["roles"] == ["a", "b"]


def test_emulator_controlled_claims_not_overridable() -> None:
    minter = JwtMinter(cfg=_hs256_cfg(), hs256_secret=_LONG_SECRET, rsa_keys=None)
    token, _ = minter.mint(
        client_id="anything",
        extra_claims={
            "iss": "evil",
            "aud": "evil",
            "tid": "evil",
            "exp": 0,
            "iat": 0,
            "sub": "override-sub",  # sub IS overridable
        },
    )
    payload = pyjwt.decode(
        token,
        _LONG_SECRET,
        algorithms=["HS256"],
        audience=_hs256_cfg().audience,
        issuer=_hs256_cfg().issuer,
    )
    # Emulator-controlled claims unchanged.
    assert payload["iss"] == _hs256_cfg().issuer
    assert payload["aud"] == _hs256_cfg().audience
    assert payload["tid"] == _hs256_cfg().tenant_id
    assert payload["exp"] > 0
    assert payload["iat"] > 0
    # sub is overridable.
    assert payload["sub"] == "override-sub"


def test_expires_in_seconds_override() -> None:
    minter = JwtMinter(cfg=_hs256_cfg(), hs256_secret=_LONG_SECRET, rsa_keys=None)
    _, exp = minter.mint(client_id="c", expires_in_seconds=60)
    delta = exp - datetime.now(UTC)
    # Allow a generous fuzz for test scheduler jitter.
    assert timedelta(seconds=55) < delta <= timedelta(seconds=61)


def test_verify_rejects_bad_signature() -> None:
    minter = JwtMinter(cfg=_hs256_cfg(), hs256_secret=_LONG_SECRET, rsa_keys=None)
    token, _ = minter.mint(client_id="c")

    bad = JwtMinter(cfg=_hs256_cfg(), hs256_secret="different-" * 4, rsa_keys=None)
    with pytest.raises(pyjwt.InvalidTokenError):
        bad.verify(token)


def test_mint_requires_key_material_for_mode() -> None:
    minter = JwtMinter(cfg=_hs256_cfg(), hs256_secret=None, rsa_keys=None)
    with pytest.raises(ValueError):
        minter.mint(client_id="c")
