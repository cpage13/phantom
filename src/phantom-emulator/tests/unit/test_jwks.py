"""Unit tests for :mod:`phantom_emulator.auth.jwks`."""

from __future__ import annotations

import base64

import jwt as pyjwt
from cryptography.hazmat.primitives import serialization
from phantom_emulator.auth.jwks import build_jwks, generate_keypair


def test_keypair_roundtrip_pyjwt() -> None:
    keys = generate_keypair()
    assert keys.public_pem.startswith(b"-----BEGIN PUBLIC KEY-----")
    assert keys.private_pem.startswith(b"-----BEGIN PRIVATE KEY-----")
    assert keys.kid

    payload = {"sub": "test"}
    token = pyjwt.encode(payload, keys.private_pem, algorithm="RS256", headers={"kid": keys.kid})
    decoded = pyjwt.decode(token, keys.public_pem, algorithms=["RS256"])
    assert decoded["sub"] == "test"


def test_build_jwks_shape() -> None:
    keys = generate_keypair(kid="abc-123")
    doc = build_jwks(keys)
    assert "keys" in doc
    assert len(doc["keys"]) == 1
    entry = doc["keys"][0]
    assert entry["kty"] == "RSA"
    assert entry["kid"] == "abc-123"
    assert entry["use"] == "sig"
    assert entry["alg"] == "RS256"
    # n / e are base64url-encoded without padding.
    for field in ("n", "e"):
        raw = entry[field]
        assert "=" not in raw
        # Padding for decode test.
        decoded = base64.urlsafe_b64decode(raw + "=" * ((4 - len(raw) % 4) % 4))
        assert len(decoded) > 0


def test_jwks_modulus_matches_public_key() -> None:
    keys = generate_keypair()
    doc = build_jwks(keys)
    entry = doc["keys"][0]

    pub = serialization.load_pem_public_key(keys.public_pem)
    nums = pub.public_numbers()  # type: ignore[union-attr]

    def _decode(b64u: str) -> int:
        padded = b64u + "=" * ((4 - len(b64u) % 4) % 4)
        return int.from_bytes(base64.urlsafe_b64decode(padded), "big")

    assert _decode(entry["n"]) == nums.n
    assert _decode(entry["e"]) == nums.e
