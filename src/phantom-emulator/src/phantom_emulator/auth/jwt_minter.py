"""JWT minting helper used by ``/oauth/token`` and static-token mode.

The minter supports two signing algorithms - HS256 (shared secret)
and RS256 (RSA private key) - selected by configuration. Both
algorithms produce a compact-serialized JWT carrying the standard
OAuth2 claim set plus any extra claims a test scenario requests.

Plan §4.3 covers the contract: the only public entry points are
``mint`` (issue a fresh token and return the expires_at tuple) and
``verify`` (decode + validate for the incoming-request side).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import jwt

from phantom_emulator.auth.jwks import RsaKeyPair
from phantom_emulator.config import AuthCfg

logger = logging.getLogger(__name__)

# Claims the emulator owns: callers may not override these via
# extra_claims. Documented in plan §4.3 task 2.
_EMULATOR_CONTROLLED_CLAIMS: frozenset[str] = frozenset({"iss", "aud", "exp", "iat", "tid"})


@dataclass(frozen=True)
class JwtMinter:
    """Stateless JWT signer / verifier.

    Attributes:
        cfg: Authentication configuration; provides issuer, audience,
            tenant id, default expiry, and clock-skew tolerance.
        hs256_secret: Shared secret for HS256 mode; ``None`` in RS256
            mode.
        rsa_keys: RSA keypair for RS256 mode; ``None`` in HS256 mode.
    """

    cfg: AuthCfg
    hs256_secret: str | None
    rsa_keys: RsaKeyPair | None

    def mint(
        self,
        *,
        client_id: str,
        extra_claims: dict[str, Any] | None = None,
        expires_in_seconds: int | None = None,
    ) -> tuple[str, datetime]:
        """Sign and return a new JWT plus its absolute expiry.

        Args:
            client_id: The OAuth2 client_id; populates the ``sub`` claim
                unless the caller injects ``sub`` via ``extra_claims``.
            extra_claims: Additional claims to layer onto the token.
                Emulator-controlled claims (``iss``, ``aud``, ``exp``,
                ``iat``, ``tid``) are silently dropped if present.
            expires_in_seconds: Token lifetime in seconds. Defaults to
                :attr:`AuthCfg.default_expires_in_seconds`.

        Returns:
            A tuple ``(jwt, expires_at)``.

        Raises:
            ValueError: when the configured mode has no matching key
                material available.
        """
        now = datetime.now(UTC)
        lifetime = expires_in_seconds or self.cfg.default_expires_in_seconds
        expires_at = now + timedelta(seconds=lifetime)

        payload: dict[str, Any] = {
            "iss": self.cfg.issuer,
            "sub": client_id,
            "aud": self.cfg.audience,
            "exp": int(expires_at.timestamp()),
            "iat": int(now.timestamp()),
            "tid": self.cfg.tenant_id,
        }
        if extra_claims:
            for key, value in extra_claims.items():
                if key in _EMULATOR_CONTROLLED_CLAIMS:
                    logger.debug("Dropping emulator-controlled claim override: %s", key)
                    continue
                payload[key] = value

        mode = self.cfg.signing.mode
        if mode == "HS256":
            if self.hs256_secret is None:
                raise ValueError("HS256 mode requires hs256_secret")
            token = jwt.encode(payload, self.hs256_secret, algorithm="HS256")
        elif mode == "RS256":
            if self.rsa_keys is None:
                raise ValueError("RS256 mode requires rsa_keys")
            token = jwt.encode(
                payload,
                self.rsa_keys.private_pem,
                algorithm="RS256",
                headers={"kid": self.rsa_keys.kid},
            )
        else:  # pragma: no cover - Literal narrowed elsewhere
            raise ValueError(f"unknown signing mode: {mode}")

        logger.debug(
            "Minted JWT mode=%s client_id=%s exp=%s",
            mode,
            client_id,
            expires_at.isoformat(),
        )
        return token, expires_at

    def verify(self, token: str) -> dict[str, Any]:
        """Decode and validate a JWT against the configured signing key.

        Verifies signature, ``exp``, and audience claim. Issuer is
        checked when present.

        Args:
            token: The compact-serialized JWT.

        Returns:
            The decoded claim set.

        Raises:
            jwt.InvalidTokenError: when any check fails.
        """
        mode = self.cfg.signing.mode
        if mode == "HS256":
            if self.hs256_secret is None:
                raise ValueError("HS256 mode requires hs256_secret")
            return jwt.decode(
                token,
                self.hs256_secret,
                algorithms=["HS256"],
                audience=self.cfg.audience,
                issuer=self.cfg.issuer,
                leeway=self.cfg.clock_skew_seconds,
            )
        if mode == "RS256":
            if self.rsa_keys is None:
                raise ValueError("RS256 mode requires rsa_keys")
            return jwt.decode(
                token,
                self.rsa_keys.public_pem,
                algorithms=["RS256"],
                audience=self.cfg.audience,
                issuer=self.cfg.issuer,
                leeway=self.cfg.clock_skew_seconds,
            )
        raise ValueError(f"unknown signing mode: {mode}")
