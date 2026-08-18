"""Control-plane HTTP surface for runtime test orchestration.

Every endpoint here is a tool for E2E tests: inject and clear failure
policies, pause/resume upstream endpoints, expire JWTs, swap auth
modes, shorten presigned URL TTLs, reseed the RNG, and read back the
in-memory log of accepted bodies. The control plane is unauthenticated
and is expected to be bound to loopback in shared environments.
"""

from __future__ import annotations

import logging
import os
import signal
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, BackgroundTasks, Depends, Response
from pydantic import BaseModel, ConfigDict, Field

from phantom_emulator.auth.modes import AuthMode
from phantom_emulator.control_models import (
    ControlStatusResponse,
    ReceivedResponse,
)
from phantom_emulator.failure.injection import FailurePolicy
from phantom_emulator.routers._deps import get_state
from phantom_emulator.state import EmulatorState

logger = logging.getLogger(__name__)

router = APIRouter()

StateDep = Annotated[EmulatorState, Depends(get_state)]


# -- Request / response models ------------------------------------------------


class ExtraClaimsBody(BaseModel):
    """Body for ``POST /control/auth/extra-claims``."""

    model_config = ConfigDict(extra="forbid")

    claims: dict[str, Any] = Field(
        ...,
        description=(
            "Extra claims to merge into the next minted JWT. "
            "Emulator-controlled claims (iss/aud/exp/iat/tid) are dropped."
        ),
    )


class AuthModeBody(BaseModel):
    """Body for ``POST /control/auth/mode``."""

    model_config = ConfigDict(extra="forbid")

    mode: AuthMode = Field(..., description="Auth mode to apply.")
    scope: str = Field(
        "*",
        description="URL path scope, or '*' for global.",
    )


class PresignedTtlBody(BaseModel):
    """Body for ``POST /control/presigned-ttl``."""

    model_config = ConfigDict(extra="forbid")

    seconds: int = Field(..., ge=0, description="New default TTL.")


class SeedBody(BaseModel):
    """Body for ``POST /control/seed``."""

    model_config = ConfigDict(extra="forbid")

    seed: int = Field(..., description="New RNG seed.")


# -- Routes -------------------------------------------------------------------


@router.get("/control/status")
async def status(
    state: StateDep,
) -> ControlStatusResponse:
    """Snapshot of emulator state."""
    uptime = max(0, int((datetime.now(UTC) - state.started_at).total_seconds()))
    fs = state.failure_state
    policies = list(fs.policies.values()) if fs is not None else []
    return ControlStatusResponse(
        uptime_seconds=uptime,
        issued_tokens_count=len(state.issued_tokens),
        accepted_bodies_count=len(state.accepted_bodies),
        pending_uploads_count=len(state.pending_uploads),
        global_paused=state.global_paused,
        policies=policies,
        auth_mode_default=state.cfg.auth.default_mode,
        auth_mode_overrides=dict(state.auth_mode_overrides),
    )


@router.get("/control/received")
async def received(
    state: StateDep,
) -> ReceivedResponse:
    """In-memory log of accepted upload bodies.

    The projection lives on :meth:`EmulatorState.received` so the in-process
    oracle reads the same one (U3).
    """
    return ReceivedResponse(received=state.received())


@router.post("/control/inject-failure", status_code=204)
async def inject_failure(
    policy: FailurePolicy,
    state: StateDep,
) -> Response:
    """Install or replace a failure policy."""
    if state.failure_state is None:
        raise RuntimeError("failure_state not initialized")
    state.failure_state.set_policy(policy)
    logger.info("inject_failure scope=%s", policy.scope)
    return Response(status_code=204)


@router.post("/control/clear-failures", status_code=204)
async def clear_failures(
    state: StateDep,
) -> Response:
    """Drop every installed failure policy."""
    if state.failure_state is None:
        raise RuntimeError("failure_state not initialized")
    state.failure_state.clear_all()
    logger.info("clear_failures")
    return Response(status_code=204)


@router.post("/control/pause", status_code=204)
async def pause(
    state: StateDep,
) -> Response:
    """Refuse all upstream requests with 503 until resumed."""
    state.pause()
    logger.info("global_paused=True")
    return Response(status_code=204)


@router.post("/control/resume", status_code=204)
async def resume(
    state: StateDep,
) -> Response:
    """Restore normal upstream serving."""
    state.resume()
    logger.info("global_paused=False")
    return Response(status_code=204)


@router.post("/control/shutdown", status_code=204)
async def shutdown(
    background: BackgroundTasks,
) -> Response:
    """Send SIGTERM to the emulator process.

    Used to simulate "upstream goes away mid-traffic" in Docker mode.
    In wheel mode, ``Server.stop()`` is the cleaner path.
    """

    def _terminate() -> None:
        logger.info("shutdown: SIGTERM to self")
        os.kill(os.getpid(), signal.SIGTERM)

    background.add_task(_terminate)
    return Response(status_code=204)


@router.post("/control/expire-all-now", status_code=204)
async def expire_all_now(
    state: StateDep,
) -> Response:
    """Age every issued JWT past its ``exp``.

    In HS256 mode this is observability-only (Phantom won't see a
    server-side ``exp`` change without re-decoding); but if the test
    drives the request immediately, the emulator's verify path returns
    401 because the cached expires_at is now in the past.
    """
    state.expire_all_now()
    logger.info("expire_all_now: aged %d tokens", len(state.issued_tokens))
    return Response(status_code=204)


@router.post("/control/revoke-tokens", status_code=204)
async def revoke_tokens(
    state: StateDep,
) -> Response:
    """Drop every issued JWT."""
    state.revoke_tokens()
    logger.info("revoke_tokens")
    return Response(status_code=204)


@router.post("/control/auth/extra-claims", status_code=204)
async def set_extra_claims(
    body: ExtraClaimsBody,
    state: StateDep,
) -> Response:
    """Stage extra claims for the next minted JWT."""
    state.set_extra_claims(body.claims)
    logger.info("set_extra_claims keys=%s", list(body.claims))
    return Response(status_code=204)


@router.post("/control/auth/mode", status_code=204)
async def set_auth_mode(
    body: AuthModeBody,
    state: StateDep,
) -> Response:
    """Set the default auth mode (scope=='*') or a per-scope override."""
    state.set_auth_mode(body.mode, body.scope)
    logger.info("set_auth_mode mode=%s scope=%s", body.mode, body.scope)
    return Response(status_code=204)


@router.post("/control/presigned-ttl", status_code=204)
async def set_presigned_ttl(
    body: PresignedTtlBody,
    state: StateDep,
) -> Response:
    """Set the default presigned URL lifetime for new mints."""
    state.set_presigned_ttl(body.seconds)
    logger.info("set_presigned_ttl seconds=%d", body.seconds)
    return Response(status_code=204)


@router.post("/control/seed", status_code=204)
async def set_seed(
    body: SeedBody,
    state: StateDep,
) -> Response:
    """Reseed the failure-injection RNG."""
    if state.failure_state is None:
        raise RuntimeError("failure_state not initialized")
    state.failure_state.set_seed(body.seed)
    logger.info("set_seed seed=%d", body.seed)
    return Response(status_code=204)


@router.post("/control/clear-received", status_code=204)
async def clear_received(
    state: StateDep,
) -> Response:
    """Drop latest accepted bodies and append-only upstream events."""
    state.clear_received()
    logger.info("clear_received")
    return Response(status_code=204)
