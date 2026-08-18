"""FastAPI application factory.

Wires the routers, mounts the failure-injection middleware above
them, and runs lifespan initialization (mint RSA keys in RS256 mode,
pre-mint the static-token JWT, record process start time).

See plan §4.13.
"""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import FastAPI

from phantom_emulator.auth.jwks import generate_keypair
from phantom_emulator.auth.jwt_minter import JwtMinter
from phantom_emulator.config import AppConfig
from phantom_emulator.failure.injection import FailureInjectionState
from phantom_emulator.failure.middleware import make_failure_middleware
from phantom_emulator.routers import auth as auth_router
from phantom_emulator.routers import control as control_router
from phantom_emulator.routers import raw_sink as raw_sink_router
from phantom_emulator.routers import s3 as s3_router
from phantom_emulator.routers import upstream as upstream_router
from phantom_emulator.state import EmulatorState

logger = logging.getLogger(__name__)

# Default HS256 secret used when the env var named by AuthSigningCfg
# is unset. 32 chars to satisfy pyjwt's insecure-key warning. The
# emulator is test infrastructure; this default is safe.
_FALLBACK_HS256_SECRET: str = "phantom-emulator-default-secret!"

# Startup banner emphasising this is NOT production infrastructure.
_STARTUP_BANNER: str = "phantom-emulator: TEST INFRASTRUCTURE ONLY — no auth on control plane"


def _initialize_state(cfg: AppConfig) -> EmulatorState:
    """Build an :class:`EmulatorState` ready for router use.

    Mints the JWT minter (with HS256 or RS256 key material as
    configured), pre-mints the static-token JWT when the default mode
    is ``static_token``, and primes the failure-injection state.

    Args:
        cfg: The emulator configuration.

    Returns:
        A fully-initialized :class:`EmulatorState`.
    """
    started_at = datetime.now(UTC)
    state = EmulatorState(cfg=cfg, started_at=started_at)
    state.failure_state = FailureInjectionState(seed=0)

    if cfg.auth.signing.mode == "RS256":
        state.rsa_keys = generate_keypair()
        state.jwt_minter = JwtMinter(cfg=cfg.auth, hs256_secret=None, rsa_keys=state.rsa_keys)
    else:
        secret = os.environ.get(cfg.auth.signing.hs256_secret_env, _FALLBACK_HS256_SECRET)
        state.jwt_minter = JwtMinter(cfg=cfg.auth, hs256_secret=secret, rsa_keys=None)

    # In static-token mode, pre-mint the JWT so /oauth/token can
    # return it without minting fresh on each call. The static_jwt is
    # also what the policy compares inbound bearers against.
    if cfg.auth.default_mode.value == "static_token":
        token, expires_at = state.jwt_minter.mint(client_id="static-client")
        state.static_jwt = token
        from phantom_emulator.state import IssuedToken

        state.issued_tokens[token] = IssuedToken(
            client_id="static-client",
            expires_at=expires_at,
            jwt=token,
            extra_claims={},
        )

    return state


def create_app(cfg: AppConfig, state: EmulatorState | None = None) -> FastAPI:
    """Build a FastAPI application bound to ``cfg``.

    Args:
        cfg: Emulator configuration.
        state: Optional pre-built state. When ``None``, a fresh state
            is initialized inline.

    Returns:
        A configured :class:`FastAPI` instance ready for uvicorn.
    """

    @asynccontextmanager
    async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Log the startup banner once, then serve for the process's lifetime."""
        logger.info(_STARTUP_BANNER)
        yield

    app = FastAPI(lifespan=_lifespan, title="phantom-emulator")
    bound_state = state if state is not None else _initialize_state(cfg)
    app.state.emulator_state = bound_state

    # Failure middleware must sit above the routers so it can wrap or
    # short-circuit every response.
    app.middleware("http")(make_failure_middleware(bound_state))

    app.include_router(auth_router.router)
    app.include_router(upstream_router.router)
    app.include_router(control_router.router)
    # The auth-free /raw/{path:path} sink is registered BEFORE the s3 catch-all
    # and AFTER the literal routers. Both /raw/{path:path} and the s3
    # /{bucket}/{key:path} are :path catch-alls, and a PUT /raw/foo matches
    # BOTH; Starlette resolves by registration-order first-match, so only
    # because raw_sink is registered first does /raw/* reach the auth-free sink
    # (200) instead of the SigV4 validator (which 403s an unsigned PUT). See
    # plan TASK 0.5 Step C.
    app.include_router(raw_sink_router.router)
    # The bare /{bucket}/{key:path} S3 catch-all is the most-greedy route, so
    # it MUST be the FINAL include_router: Starlette matching is
    # registration-order first-match, so every literal route above (and the
    # literal-prefixed /raw/... router) wins and the catch-all only fires for
    # paths no earlier router claimed. If this is registered earlier (or
    # another router is added after it), the catch-all swallows /v1/files/abc
    # as {bucket: "v1", key: "files/abc"} and silently breaks every literal
    # route.
    app.include_router(s3_router.router)

    return app
