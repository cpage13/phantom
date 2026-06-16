"""RSA keypair management and JWKS document generation.

Used only in RS256 signing mode. The emulator generates a 2048-bit
keypair at startup and publishes the public half as a JWKS document
at ``/.well-known/jwks.json`` so Phantom-side ``azure-identity`` runs
the same JWKS validation path it would against real AD.

See plan §4.4.
"""

from __future__ import annotations

import base64
import logging
import uuid
from dataclasses import dataclass
from typing import Any

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

logger = logging.getLogger(__name__)

# RSA key size for emulator-generated keys. 2048 is the AD-default.
RSA_KEY_SIZE: int = 2048

# Standard RSA public exponent. 65537 (0x10001) is universal.
RSA_PUBLIC_EXPONENT: int = 65537


@dataclass(frozen=True)
class RsaKeyPair:
    """An RSA keypair the emulator uses for RS256 signing.

    Attributes:
        public_pem: The public key in PEM (PKCS#1 SubjectPublicKeyInfo).
        private_pem: The private key in PEM (PKCS#8, unencrypted).
        kid: The ``kid`` claim used in JWT headers and JWKS entries.
    """

    public_pem: bytes
    private_pem: bytes
    kid: str


def generate_keypair(kid: str | None = None) -> RsaKeyPair:
    """Generate a fresh RSA keypair.

    Args:
        kid: Explicit key identifier. Defaults to a fresh UUID hex
            string so each generation is uniquely identifiable.

    Returns:
        A new :class:`RsaKeyPair`.
    """
    key = rsa.generate_private_key(public_exponent=RSA_PUBLIC_EXPONENT, key_size=RSA_KEY_SIZE)
    private_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    final_kid = kid if kid is not None else uuid.uuid4().hex
    logger.debug("Generated RSA keypair kid=%s", final_kid)
    return RsaKeyPair(public_pem=public_pem, private_pem=private_pem, kid=final_kid)


def build_jwks(keypair: RsaKeyPair) -> dict[str, Any]:
    """Build a JWKS document containing the keypair's public half.

    Args:
        keypair: The keypair whose public side is being advertised.

    Returns:
        A dict shaped like the JWKS spec's ``{"keys": [...]}`` document.
    """
    public_key = serialization.load_pem_public_key(keypair.public_pem)
    if not isinstance(public_key, RSAPublicKey):
        raise TypeError("RSAKeyPair.public_pem must encode an RSA public key")
    numbers = public_key.public_numbers()
    n = _b64url_uint(numbers.n)
    e = _b64url_uint(numbers.e)
    return {
        "keys": [
            {
                "kty": "RSA",
                "kid": keypair.kid,
                "use": "sig",
                "alg": "RS256",
                "n": n,
                "e": e,
            }
        ]
    }


def _b64url_uint(value: int) -> str:
    """Encode a non-negative integer in JWK's base64url-without-padding form."""
    byte_len = (value.bit_length() + 7) // 8 or 1
    raw = value.to_bytes(byte_len, "big")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
