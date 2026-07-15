"""OAuth2 token endpoint + ``.well-known`` discovery + JWKS.

Implements the AD-shaped client-credentials grant Phantom's
``ad_client_credentials`` refresh strategy talks to. The same endpoint
also services ``static_token`` mode by returning the pre-minted token.

See plan §4.10.
"""

from __future__ import annotations

import hmac
import logging
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import JSONResponse

from phantom_emulator.auth.jwks import build_jwks
from phantom_emulator.auth.modes import AuthMode
from phantom_emulator.routers._deps import get_state
from phantom_emulator.state import EmulatorState, IssuedToken, MintAttempt

logger = logging.getLogger(__name__)

router = APIRouter()

# Reusable typed dependency. FastAPI inspects the Annotated metadata.
StateDep = Annotated[EmulatorState, Depends(get_state)]


def _record_mint_attempt(state: EmulatorState, client_secret: str, status: int) -> None:
    """Append one ordered, secret-free entry to the T3 mint-attempt ledger.

    Inert unless a test armed :attr:`EmulatorState.mint_slot_secrets`. The
    presented secret is resolved to its SAFE slot tag via
    ``hmac.compare_digest`` and immediately discarded; the ledger never
    carries a secret, token, header, or form body.
    """
    if not state.mint_slot_secrets:
        return
    slot = "unknown"
    for tag, secret in state.mint_slot_secrets.items():
        if hmac.compare_digest(client_secret, secret):
            slot = tag
            break
    state.mint_attempts.append(
        MintAttempt(
            seq=len(state.mint_attempts) + 1,
            slot=slot,
            status=status,
            at=datetime.now(UTC),
        )
    )


@router.post("/oauth/token")
async def token_endpoint(
    grant_type: Annotated[str, Form()],
    client_id: Annotated[str, Form()],
    client_secret: Annotated[str, Form()],
    state: StateDep,
    scope: Annotated[str | None, Form()] = None,
) -> JSONResponse:
    """Mint an OAuth2 access token.

    Accepts a form-encoded body shaped like Microsoft AD's
    client-credentials grant. Returns the standard token response. In
    ``static_token`` mode the emulator returns the pre-minted JWT
    regardless of the credentials supplied.

    Args:
        grant_type: Must be ``client_credentials``.
        client_id: OAuth2 client identifier.
        client_secret: OAuth2 client secret.
        state: Shared emulator state.
        scope: Requested scope (accepted but ignored — the emulator
            issues a token for the configured audience regardless).

    Returns:
        ``200`` with ``{"access_token": ..., "token_type": "Bearer",
        "expires_in": int}``; ``400`` on bad grant; ``401`` on bad
        credentials in modes that validate them.
    """
    del scope  # accepted for protocol parity but otherwise ignored
    if grant_type != "client_credentials":
        raise HTTPException(status_code=400, detail="grant_type must be client_credentials")

    if state.jwt_minter is None:
        raise HTTPException(status_code=500, detail="emulator JWT minter not initialized")

    mode_for_scope = state.cfg.auth.default_mode
    if mode_for_scope is AuthMode.STATIC_TOKEN and state.static_jwt is not None:
        # Static-token mode: ignore credentials, return the cached JWT
        # with the time remaining on its expiry.
        token = state.static_jwt
        # We don't track the static token's expiry separately; return
        # the configured default lifetime so clients have a reasonable
        # cache window. Real AD also issues fresh `expires_in` on each
        # mint, so this is faithful enough.
        return JSONResponse(
            {
                "access_token": token,
                "token_type": "Bearer",
                "expires_in": state.cfg.auth.default_expires_in_seconds,
            }
        )

    # All other modes: validate (client_id, client_secret) against the
    # configured list and mint.
    matching = [
        c
        for c in state.cfg.auth.clients
        if c.client_id == client_id and c.client_secret == client_secret
    ]
    if not matching:
        logger.info("oauth/token rejected: unknown client_id %s", client_id)
        _record_mint_attempt(state, client_secret, 401)
        return JSONResponse(
            {"error": "invalid_client"},
            status_code=401,
        )

    # Pull pending extra claims, consume them, and mint.
    extra = dict(state.extra_claims) if state.extra_claims else None
    token, expires_at = state.jwt_minter.mint(client_id=client_id, extra_claims=extra)
    state.extra_claims.clear()

    state.issued_tokens[token] = IssuedToken(
        client_id=client_id,
        expires_at=expires_at,
        jwt=token,
        extra_claims=dict(extra) if extra else {},
    )

    expires_in = state.cfg.auth.default_expires_in_seconds
    _record_mint_attempt(state, client_secret, 200)
    return JSONResponse(
        {
            "access_token": token,
            "token_type": "Bearer",
            "expires_in": expires_in,
        }
    )


@router.post("/{tenant_id}/oauth2/v2.0/token")
async def tenant_token_alias(
    tenant_id: str,
    grant_type: Annotated[str, Form()],
    client_id: Annotated[str, Form()],
    client_secret: Annotated[str, Form()],
    state: StateDep,
    scope: Annotated[str | None, Form()] = None,
) -> JSONResponse:
    """The AAD tenant token path — azure-identity's direct POST target.

    ``azure-identity`` (locked 1.25.3) POSTs ``/{tenant}/oauth2/v2.0/token``
    on the configured authority with NO discovery round-trip, so the generic
    ``/oauth/token`` alias alone cannot serve an autonomous-mint scenario.
    Same grant, same validation, same mint, same T3 attempt ledger: this
    delegates to :func:`token_endpoint` verbatim. The tenant segment is
    accepted and ignored (the emulator is single-tenant by configuration).

    Registration note: this parameterized route lives on the auth router,
    which registers BEFORE the S3 catch-all (``/{bucket}/{key:path}``), so
    first-match keeps it out of the sink; an S3 key literally shaped
    ``oauth2/v2.0/token`` under a POST would shadow into this route, which no
    scenario does.
    """
    del tenant_id
    return await token_endpoint(
        grant_type=grant_type,
        client_id=client_id,
        client_secret=client_secret,
        state=state,
        scope=scope,
    )


@router.get("/.well-known/openid-configuration")
async def discovery(
    request: Request,
    state: StateDep,
) -> JSONResponse:
    """OpenID Connect discovery document.

    Phantom's ``azure-identity`` uses this to locate ``token_endpoint``
    and ``jwks_uri``.
    """
    base = str(request.base_url).rstrip("/")
    return JSONResponse(
        {
            "issuer": state.cfg.auth.issuer,
            "token_endpoint": f"{base}/oauth/token",
            "jwks_uri": f"{base}/.well-known/jwks.json",
        }
    )


@router.get("/.well-known/jwks.json")
async def jwks(
    state: StateDep,
) -> JSONResponse:
    """JWKS document.

    Empty in HS256 mode (HS256 has no public side). Returns the
    emulator's RSA public key in RS256 mode.
    """
    if state.cfg.auth.signing.mode == "RS256" and state.rsa_keys is not None:
        return JSONResponse(build_jwks(state.rsa_keys))
    return JSONResponse({"keys": []})
