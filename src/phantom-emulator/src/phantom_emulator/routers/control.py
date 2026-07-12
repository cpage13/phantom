"""Control-plane HTTP surface for runtime test orchestration.

Every endpoint here is a tool for E2E tests: inject and clear failure
policies, pause/resume upstream endpoints, expire JWTs, swap auth
modes, shorten presigned URL TTLs, reseed the RNG, and read back the
in-memory log of accepted bodies. The control plane is unauthenticated
and is expected to be bound to loopback in shared environments.
"""

from __future__ import annotations

import hashlib
import logging
import os
import signal
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, Response
from pydantic import BaseModel, ConfigDict, Field

from phantom_emulator.auth.modes import AuthMode
from phantom_emulator.failure.injection import FailurePolicy
from phantom_emulator.routers._deps import get_state
from phantom_emulator.state import EmulatorState

logger = logging.getLogger(__name__)

router = APIRouter()

StateDep = Annotated[EmulatorState, Depends(get_state)]


# -- Request / response models ------------------------------------------------


class ReceivedEntry(BaseModel):
    """One row of the ``/control/received`` log."""

    model_config = ConfigDict(extra="forbid")

    upload_token: str = Field(..., description="The opaque token from the upload URL.")
    file_id: UUID = Field(..., description="The emulator-minted FileInformation.id.")
    metadata_kvs: dict[str, str] = Field(
        ...,
        description=(
            "The metadata.keyValueStore the metadata POST carried (preserves phantom_local_uuid)."
        ),
    )
    x_amz_meta_headers: dict[str, str] = Field(
        ..., description="The x-amz-meta-* headers the PUT carried."
    )
    headers: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Every inbound HTTP header on the PUT, lowercased keys with "
            "original values. Captures the full request envelope so "
            "transparent-proxy tests can audit byte-equality of "
            "``Authorization``, absence of ``X-Phantom-*``, preservation "
            "of ``User-Agent`` and custom producer headers. Multi-value "
            'headers join on ``", "`` per Starlette\'s header-dict '
            "semantics. Authorization values are recorded verbatim — "
            "tests opt-in to assert against them."
        ),
    )
    body_size: int = Field(..., ge=0, description="Bytes accepted on the PUT.")
    body_hash: str = Field(
        ...,
        description=(
            "SHA-256 hex of the received body bytes — used by "
            "transparent-proxy E2E tests to assert byte-identity against "
            "the agent's pre-submit hash."
        ),
    )
    content_encoding: str | None = Field(
        None,
        description=(
            "Value of the PUT's ``Content-Encoding`` header (or ``None`` "
            "if the header was unset). Lets transparent-proxy tests "
            "assert header preservation alongside byte-identity."
        ),
    )
    accepted_at: datetime = Field(..., description="Server-side acceptance time.")
    idempotency_key: str | None = Field(
        None,
        description="Idempotency-Key from the create call (if any).",
    )


class ReceivedResponse(BaseModel):
    """Envelope for the received-log response."""

    model_config = ConfigDict(extra="forbid")

    received: list[ReceivedEntry] = Field(..., description="Accepted bodies, oldest first.")


class ControlStatusResponse(BaseModel):
    """``/control/status`` payload."""

    model_config = ConfigDict(extra="forbid")

    uptime_seconds: int = Field(
        ...,
        ge=0,
        description="Seconds since this emulator process started.",
    )
    issued_tokens_count: int = Field(
        ...,
        ge=0,
        description="Number of JWTs issued by ``POST /oauth/token`` since boot.",
    )
    accepted_bodies_count: int = Field(
        ...,
        ge=0,
        description="Number of upload bodies the emulator has accepted on the PUT path.",
    )
    pending_uploads_count: int = Field(
        ...,
        ge=0,
        description="Number of presigned URLs minted but not yet PUT-completed.",
    )
    global_paused: bool = Field(
        ...,
        description="True when ``POST /control/pause`` has been called and resume hasn't fired.",
    )
    policies: list[FailurePolicy] = Field(
        ...,
        description="Currently-installed policies (per ``POST /control/inject-failure``).",
    )
    auth_mode_default: AuthMode = Field(
        ...,
        description="Default auth mode applied when no per-path override matches.",
    )
    auth_mode_overrides: dict[str, AuthMode] = Field(
        ...,
        description="Path-prefix overrides of the default auth mode.",
    )


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
    """In-memory log of accepted upload bodies."""
    entries: list[ReceivedEntry] = []
    for token, body in sorted(state.accepted_bodies.items(), key=lambda kv: kv[1].accepted_at):
        pending = state.pending_uploads.get(token)
        kvs = pending.metadata_kvs if pending is not None else {}
        file_id = pending.file_id if pending is not None else UUID(int=0)
        entries.append(
            ReceivedEntry(
                upload_token=token,
                file_id=file_id,
                metadata_kvs=kvs,
                x_amz_meta_headers=body.headers,
                headers=body.all_headers,
                body_size=len(body.body),
                body_hash=hashlib.sha256(body.body).hexdigest(),
                content_encoding=body.content_encoding,
                accepted_at=body.accepted_at,
                idempotency_key=state.accepted_idempotency_keys.get(token),
            )
        )
    return ReceivedResponse(received=entries)


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
    state.global_paused = True
    logger.info("global_paused=True")
    return Response(status_code=204)


@router.post("/control/resume", status_code=204)
async def resume(
    state: StateDep,
) -> Response:
    """Restore normal upstream serving."""
    state.global_paused = False
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
    past = datetime.fromtimestamp(0, tz=UTC)
    for issued in state.issued_tokens.values():
        issued.expires_at = past
    state.static_jwt = None
    # In static-token mode, re-mint a fresh static token so subsequent
    # mints succeed.
    if state.cfg.auth.default_mode is AuthMode.STATIC_TOKEN and state.jwt_minter is not None:
        token, expires_at = state.jwt_minter.mint(client_id="static-client")
        state.static_jwt = token
        from phantom_emulator.state import IssuedToken

        state.issued_tokens[token] = IssuedToken(
            client_id="static-client",
            expires_at=expires_at,
            jwt=token,
            extra_claims={},
        )
    logger.info("expire_all_now: aged %d tokens", len(state.issued_tokens))
    return Response(status_code=204)


@router.post("/control/revoke-tokens", status_code=204)
async def revoke_tokens(
    state: StateDep,
) -> Response:
    """Drop every issued JWT."""
    state.issued_tokens.clear()
    state.static_jwt = None
    logger.info("revoke_tokens")
    return Response(status_code=204)


@router.post("/control/auth/extra-claims", status_code=204)
async def set_extra_claims(
    body: ExtraClaimsBody,
    state: StateDep,
) -> Response:
    """Stage extra claims for the next minted JWT."""
    state.extra_claims = dict(body.claims)
    logger.info("set_extra_claims keys=%s", list(body.claims))
    return Response(status_code=204)


@router.post("/control/auth/mode", status_code=204)
async def set_auth_mode(
    body: AuthModeBody,
    state: StateDep,
) -> Response:
    """Set the default auth mode (scope=='*') or a per-scope override."""
    if body.scope == "*":
        state.cfg.auth.default_mode = body.mode
    else:
        state.auth_mode_overrides[body.scope] = body.mode
    logger.info("set_auth_mode mode=%s scope=%s", body.mode, body.scope)
    return Response(status_code=204)


@router.post("/control/presigned-ttl", status_code=204)
async def set_presigned_ttl(
    body: PresignedTtlBody,
    state: StateDep,
) -> Response:
    """Set the default presigned URL lifetime for new mints."""
    state.cfg.upstream.presigned_ttl_seconds = body.seconds
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
    state.accepted_bodies.clear()
    state.accepted_idempotency_keys.clear()
    state.upstream_events.clear()
    logger.info("clear_received")
    return Response(status_code=204)
