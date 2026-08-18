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

See plan §4.5.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

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
    """

    mode: AuthMode
    plain_bearer_allowlist: frozenset[str] = field(default_factory=frozenset)
    api_key_secret: str | None = None
    static_jwt: str | None = None


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
        try:
            jwt_minter.verify(bearer)
        except Exception as exc:  # decoding/verification errors
            logger.debug("JWT verify failed: %s", exc)
            return False
        return True

    return False  # pragma: no cover - exhaustive enum


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
