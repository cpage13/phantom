"""FastAPI middleware that applies failure-injection policies.

The middleware sits above every router (mounted by
:func:`phantom_emulator.app.create_app`). It performs the following
checks for each inbound request:

1. **Global pause.** When ``state.global_paused`` is True, every
   request to an upstream endpoint returns 503 Retry-After. Control
   endpoints (``/control/*``) bypass the pause to remain reachable.
2. **Per-scope policy.** Map the URL path to a :class:`FailureScope`,
   resolve the policy, and apply its knobs in order: unavailable_until
   gate, 401-after-N gate, 5xx coin flip, latency sleep, and response
   modifiers (body cutoff, slow trickle, approximate RST).

The middleware never modifies request bodies; it injects failures
either before the handler runs or by wrapping the outbound response.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime

from fastapi import Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from phantom_emulator.failure.injection import (
    FailurePolicy,
    FailureScope,
    LatencyRange,
)
from phantom_emulator.state import EmulatorState

logger = logging.getLogger(__name__)

# Retry-After advertised on the global-pause 503. Short value so tests
# don't wait an unreasonable amount of time and so real clients see a
# clear "back off briefly" signal.
PAUSE_RETRY_AFTER_SECONDS: int = 5


def _scope_for_path(path: str) -> FailureScope:
    """Map a URL path to a :class:`FailureScope`.

    The path prefix table here is the emulator's source of truth for
    which failure scope applies to which endpoint. Adding a new
    upstream-shaped endpoint means adding a prefix branch here.
    """
    if path.startswith("/v1/files/upload/"):
        return FailureScope.UPSTREAM_FILES_UPLOAD
    # The create-file endpoint is served at BOTH /v1/files/create and /v2/files
    # (the alias the upstream adapter actually targets; see routers/upstream.py). Both
    # map to the create scope so an isolated UPSTREAM_FILES_CREATE-scoped fault
    # (e.g. a truncated create-response) fires on the real path instead of
    # silently no-op'ing as GLOBAL (aggressor R7 emulator-tooling-gap note).
    if path.startswith("/v1/files/create") or path.startswith("/v2/files"):
        return FailureScope.UPSTREAM_FILES_CREATE
    if path.startswith("/v1/files/"):
        return FailureScope.UPSTREAM_FILES_GET
    if path.startswith("/oauth/token") or path.startswith("/.well-known/"):
        return FailureScope.AUTH_TOKEN
    return FailureScope.GLOBAL


def _is_upstream_path(path: str) -> bool:
    """True if the path should respect ``global_paused`` and policies.

    Control-plane endpoints (``/control/*``) must remain reachable so
    tests can unpause and inspect state.
    """
    return not path.startswith("/control")


def _pick_latency(
    policy_latency: int | LatencyRange | None,
    state: EmulatorState,
) -> int | None:
    """Resolve the configured latency in milliseconds, if any."""
    if policy_latency is None:
        return None
    if isinstance(policy_latency, int):
        return policy_latency
    fs = state.failure_state
    rng = fs.rng if fs is not None else state.rng
    return rng.randint(policy_latency.min_ms, policy_latency.max_ms)


async def _maybe_inject_failure(
    request: Request,
    state: EmulatorState,
    scope: FailureScope,
    policy: FailurePolicy | None,
) -> Response | None:
    """Return a synthetic Response if the policy demands one.

    Args:
        request: The inbound request (used only for ``url.path``).
        state: Shared emulator state.
        scope: The matched scope.
        policy: The resolved policy (None means "no injection").

    Returns:
        A Response to short-circuit the request, or None to let the
        handler run normally.
    """
    if policy is None:
        return None

    now = datetime.now(UTC)
    if policy.unavailable_until is not None and now < policy.unavailable_until:
        logger.info("failure_inject: unavailable_until at %s", request.url.path)
        return JSONResponse(
            {"error": "service_unavailable"},
            status_code=503,
            headers={"Retry-After": str(PAUSE_RETRY_AFTER_SECONDS)},
        )

    # 401-after-N — increment first, then compare.
    fs = state.failure_state
    if policy.auth_401_after_n_calls is not None and fs is not None:
        count = fs.record_call(scope)
        if count > policy.auth_401_after_n_calls:
            logger.info(
                "failure_inject: 401 (call %d > N=%d) at %s",
                count,
                policy.auth_401_after_n_calls,
                request.url.path,
            )
            return JSONResponse(
                {"error": "invalid_token"},
                status_code=401,
            )

    if policy.error_rate_5xx > 0 and fs is not None and fs.rng.random() < policy.error_rate_5xx:
        logger.info("failure_inject: 5xx coin flip at %s", request.url.path)
        return JSONResponse(
            {"error": "service_unavailable"},
            status_code=503,
        )

    latency = _pick_latency(policy.latency_ms, state)
    if latency is not None and latency > 0:
        logger.debug("failure_inject: sleep %dms at %s", latency, request.url.path)
        await asyncio.sleep(latency / 1000.0)

    return None


async def _apply_response_modifiers(
    response: Response,
    policy: FailurePolicy | None,
) -> Response:
    """Wrap the outbound response per policy knobs.

    Handles ``body_cutoff_at_bytes``, ``slow_trickle_bytes_per_sec``,
    and ``tcp_rst_on_request``. Returns the (possibly wrapped) response.

    When the inbound ``response`` is a streaming response (FastAPI's
    BaseHTTPMiddleware always wraps handler returns as such), we drain
    its body iterator before applying modifiers.
    """
    if policy is None:
        return response

    if policy.tcp_rst_on_request:
        # Approximate RST: tell the client to close immediately and
        # truncate the body to zero. Not a real socket RST — see
        # plan §10.1.
        logger.info("failure_inject: approximate_rst (Connection: close, empty body)")
        return Response(
            content=b"",
            status_code=response.status_code,
            headers={"Connection": "close"},
        )

    if policy.body_cutoff_at_bytes is not None:
        full_body = await _drain_body(response)
        truncated_body = full_body[: policy.body_cutoff_at_bytes]
        logger.info("failure_inject: body_cutoff at %d bytes", policy.body_cutoff_at_bytes)
        return Response(
            content=truncated_body,
            status_code=response.status_code,
            headers=_passthrough_headers(response.headers),
            media_type=response.media_type,
        )

    if policy.slow_trickle_bytes_per_sec is not None:
        rate = policy.slow_trickle_bytes_per_sec
        body = await _drain_body(response)
        logger.info("failure_inject: slow_trickle at %d B/s", rate)

        async def _trickle() -> AsyncIterator[bytes]:
            yield_chunk_bytes = max(1, rate // 10)
            chunk_delay = yield_chunk_bytes / rate
            offset = 0
            while offset < len(body):
                chunk = body[offset : offset + yield_chunk_bytes]
                offset += yield_chunk_bytes
                yield chunk
                await asyncio.sleep(chunk_delay)

        return StreamingResponse(
            _trickle(),
            status_code=response.status_code,
            headers=_passthrough_headers(response.headers),
            media_type=response.media_type,
        )

    return response


async def _drain_body(response: Response) -> bytes:
    """Drain a (possibly streaming) response body into bytes."""
    if hasattr(response, "body_iterator"):
        chunks: list[bytes] = []
        async for chunk in response.body_iterator:
            if isinstance(chunk, str):
                chunks.append(chunk.encode("utf-8"))
            else:
                chunks.append(chunk)
        return b"".join(chunks)
    body = getattr(response, "body", b"")
    if isinstance(body, bytes):
        return body
    if isinstance(body, memoryview):
        return body.tobytes()
    raise TypeError(f"unsupported response body type: {type(body)}")


def _passthrough_headers(headers: object) -> dict[str, str]:
    """Copy response headers into a plain dict, stripping Content-Length.

    FastAPI's MutableHeaders behaves like a dict; we strip
    ``content-length`` so the rewrapped Response computes a fresh one
    based on the modified body.
    """
    out: dict[str, str] = {}
    for k, v in headers.items():  # type: ignore[attr-defined]
        if k.lower() == "content-length":
            continue
        out[k] = v
    return out


def make_failure_middleware(
    state: EmulatorState,
) -> Callable[[Request, Callable[[Request], Awaitable[Response]]], Awaitable[Response]]:
    """Build a FastAPI HTTP middleware bound to ``state``.

    Args:
        state: The shared emulator state.

    Returns:
        An ``async`` middleware callable suitable for
        ``app.middleware("http")``.
    """

    async def _middleware(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        path = request.url.path
        scope = _scope_for_path(path)

        if state.global_paused and _is_upstream_path(path):
            logger.debug("global_paused: short-circuit %s", path)
            return JSONResponse(
                {"error": "service_unavailable", "reason": "paused"},
                status_code=503,
                headers={"Retry-After": str(PAUSE_RETRY_AFTER_SECONDS)},
            )

        policy: FailurePolicy | None = None
        if state.failure_state is not None and _is_upstream_path(path):
            policy = state.failure_state.resolve(scope)

        synthetic = await _maybe_inject_failure(request, state, scope, policy)
        if synthetic is not None:
            return synthetic

        response = await call_next(request)
        return await _apply_response_modifiers(response, policy)

    return _middleware
