"""Authentication-mode enumeration and per-mode policy.

The emulator can present several auth shapes to inbound requests:

- ``oauth_client_credentials`` - full OAuth2 client-credentials grant
  with JWT bearer authentication on protected endpoints.
- ``static_token`` - a single pre-minted JWT is accepted; clients may
  obtain it from ``POST /oauth/token`` (which always returns the
  static JWT in this mode) or from configuration.
- ``plain_bearer`` - accepts any value in ``Authorization: Bearer``
  that is on a configured allow-list. No JWT semantics.
- ``api_key`` - a shared secret in the ``X-API-Key`` header.
- ``none`` - no authentication at all.

The policy table threads the per-mode data; ``authenticate`` runs the
check for a single request against a single policy.

``oauth_client_credentials`` carries one piece of emulator state into an
otherwise stateless check: the set of credentials the control surface has
revoked or expired (``phantom_emulator.state.CredentialLedger``). A JWT's
signature and ``exp`` cannot be changed after issue, so without that set
``POST /control/revoke-tokens`` and ``POST /control/expire-all-now`` would
be silent no-ops in the default mode.

See plan §4.5.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

import jwt

if TYPE_CHECKING:  # pragma: no cover - typing-only import
    from phantom_emulator.auth.jwt_minter import JwtMinter

logger = logging.getLogger(__name__)


class AuthMode(StrEnum):
    """Enumeration of supported authentication modes."""

    OAUTH_CLIENT_CREDENTIALS = "oauth_client_credentials"
    STATIC_TOKEN = "static_token"
    PLAIN_BEARER = "plain_bearer"
    API_KEY = "api_key"
    NONE = "none"


@dataclass(frozen=True)
class AuthModePolicy:
    """Resolved policy for one auth mode at request-time.

    Attributes:
        mode: The active mode.
        plain_bearer_allowlist: Accepted bearer values in
            ``plain_bearer`` mode.
        api_key_secret: Required value of the ``X-API-Key`` header in
            ``api_key`` mode.
        static_jwt: The pre-minted JWT for ``static_token`` mode.
        invalidated_credentials: Snapshot of the credential ledger's
            revoked/expired set. Consulted in ``oauth_client_credentials``
            mode only: the other modes have their own control (the
            allowlist, the shared secret, the cleared ``static_jwt``) and
            are already answerable to the control surface.
    """

    mode: AuthMode
    plain_bearer_allowlist: frozenset[str] = field(default_factory=frozenset)
    api_key_secret: str | None = None
    static_jwt: str | None = None
    invalidated_credentials: frozenset[str] = field(default_factory=frozenset)


def authenticate(
    headers: Mapping[str, str],
    policy: AuthModePolicy,
    jwt_minter: JwtMinter,
) -> bool:
    """Check whether a request meets the policy.

    Args:
        headers: Mapping of request headers (case-sensitive - callers
            should pre-normalize for FastAPI which lowercases keys).
        policy: The resolved policy to evaluate.
        jwt_minter: Used for JWT decode/verify in
            ``oauth_client_credentials`` mode.

    Returns:
        ``True`` if the request satisfies the policy; ``False``
        otherwise.

    Raises:
        jwt.PyJWTError: when ``oauth_client_credentials`` verification fails
            for a reason that is not an invalid token, for example key
            material PyJWT refuses. A broken emulator configuration surfaces
            instead of masquerading as a rejected caller.
        ValueError: when the minter has no key material for its configured
            signing mode. Same reason.
    """
    mode = policy.mode
    if mode is AuthMode.NONE:
        return True

    auth_header = _header(headers, "authorization")
    bearer = _strip_bearer(auth_header)

    if mode is AuthMode.PLAIN_BEARER:
        return bearer is not None and bearer in policy.plain_bearer_allowlist

    if mode is AuthMode.API_KEY:
        return (
            policy.api_key_secret is not None
            and _header(headers, "x-api-key") == policy.api_key_secret
        )

    if mode is AuthMode.STATIC_TOKEN:
        return bearer is not None and bearer == policy.static_jwt

    if mode is AuthMode.OAUTH_CLIENT_CREDENTIALS:
        if bearer is None:
            return False
        if bearer in policy.invalidated_credentials:
            logger.debug("bearer rejected: revoked or expired by the control surface")
            return False
        try:
            jwt_minter.verify(bearer)
        except jwt.InvalidTokenError as exc:
            # Every PyJWT failure that means "this token is not acceptable"
            # derives from InvalidTokenError: bad signature, expired, wrong
            # audience or issuer, undecodable. Anything else (InvalidKeyError,
            # a TypeError from key material that is None) is a broken emulator
            # configuration and MUST NOT read as a rejected caller.
            logger.debug("JWT verify failed: %s", exc)
            return False
        return True

    return False  # pragma: no cover - exhaustive enum


def bearer_credential(headers: Mapping[str, str]) -> str | None:
    """Return the bearer credential a request presented, if any.

    The same extraction :func:`authenticate` performs, exposed so a caller
    that has just authenticated a request can record the credential on the
    ledger (:meth:`phantom_emulator.state.CredentialLedger.note_accepted`)
    without re-implementing the header parse.

    Args:
        headers: Mapping of request headers (case-insensitive lookup).

    Returns:
        The credential with any ``Bearer `` prefix stripped, or ``None``
        when the request carried no usable ``Authorization`` value.
    """
    return _strip_bearer(_header(headers, "authorization"))


def _header(headers: Mapping[str, str], name: str) -> str | None:
    """Return the named header value with case-insensitive lookup."""
    target = name.lower()
    for key, value in headers.items():
        if key.lower() == target:
            return value
    return None


def _strip_bearer(value: str | None) -> str | None:
    """Strip an optional ``Bearer `` prefix; return ``None`` if empty."""
    if value is None:
        return None
    parts = value.split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return value.strip() or None
