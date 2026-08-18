"""Programmatic in-process emulator server.

The dominant test mode. A pytest fixture calls
``await start_server(cfg)`` to get back a :class:`Server` with a
``.url()`` and a typed control surface that mirrors the
``/control/*`` HTTP endpoints one-to-one.

See plan §4.14.
"""

from __future__ import annotations

import asyncio
import logging
import socket
from typing import Any

import uvicorn

from phantom_emulator.auth.modes import AuthMode
from phantom_emulator.config import AppConfig
from phantom_emulator.control_models import ReceivedEntry
from phantom_emulator.failure.injection import (
    ErrorRate5xxEvent,
    FailurePolicy,
    FailureScope,
)
from phantom_emulator.state import EmulatorState, UpstreamEvent

logger = logging.getLogger(__name__)

# How long start_server waits for uvicorn to come up before raising.
# Generous enough for slow CI hosts; aborts fast on a real problem.
SERVER_STARTUP_TIMEOUT_SECONDS: float = 10.0

# Brief grace period for drain() before the caller is told the server
# has settled. Tests can override.
DEFAULT_DRAIN_TIMEOUT_SECONDS: float = 5.0


class Server:
    """Handle to a running in-process emulator.

    Construct via :func:`start_server`. Stop with :meth:`stop`. The
    typed control methods mirror ``/control/*`` HTTP endpoints
    one-to-one so readers can grep by either side.
    """

    def __init__(
        self,
        *,
        config: AppConfig,
        state: EmulatorState,
        uv_server: uvicorn.Server,
        serve_task: asyncio.Task[None],
        port: int,
    ) -> None:
        """Bind a fully-running server. Use :func:`start_server` instead."""
        self.config = config
        self.state = state
        self._uv_server = uv_server
        self._serve_task = serve_task
        self._port = port

    @property
    def port(self) -> int:
        """Active listen port (resolved if cfg.server.port was 0)."""
        return self._port

    def url(self) -> str:
        """Fully-qualified base URL (e.g., ``http://127.0.0.1:54321``)."""
        host = self.config.server.host
        if host == "0.0.0.0":
            host = "127.0.0.1"
        return f"http://{host}:{self._port}"

    async def stop(self) -> None:
        """Shut the server down and wait for the serve task to finish."""
        self._uv_server.should_exit = True
        try:
            await asyncio.wait_for(self._serve_task, timeout=SERVER_STARTUP_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.warning("server.stop: serve task did not exit; cancelling")
            self._serve_task.cancel()

    # ------------------------------------------------------------------
    # Typed control surface - mirrors /control/* endpoint paths 1:1.
    # ------------------------------------------------------------------

    def inject_failure(self, policy: FailurePolicy) -> None:
        """Install or replace a failure policy.

        Mirrors ``POST /control/inject-failure``.

        NOT hoisted, and this is a recorded divergence rather than an
        oversight (U3): this oracle guards the uninitialised failure state
        with ``assert``, which ``-O`` strips, while the route raises
        ``RuntimeError``, which it does not. Hoisting either body would
        change the OTHER surface's failure behaviour, so both bodies stay
        and the difference is carried to the human instead.
        """
        assert self.state.failure_state is not None
        self.state.failure_state.set_policy(policy)

    def clear_failures(self) -> None:
        """Drop every installed failure policy.

        Mirrors ``POST /control/clear-failures``.

        NOT hoisted: the same assert-versus-raise divergence recorded on
        :meth:`inject_failure`.
        """
        assert self.state.failure_state is not None
        self.state.failure_state.clear_all()

    def error_rate_5xx_count(self, scope: FailureScope) -> int:
        """Return the error-rate-generated 503 count for one request scope."""
        assert self.state.failure_state is not None
        return self.state.failure_state.error_rate_5xx_count(scope)

    def error_rate_5xx_events(self, scope: FailureScope) -> tuple[ErrorRate5xxEvent, ...]:
        """Return immutable error-rate-generated 503 events for one scope."""
        assert self.state.failure_state is not None
        return self.state.failure_state.error_rate_5xx_for_scope(scope)

    def pause(self) -> None:
        """Refuse upstream requests until :meth:`resume`.

        The in-process face of ``POST /control/pause``; both delegate to
        :meth:`EmulatorState.pause`.
        """
        self.state.pause()

    def resume(self) -> None:
        """Restore normal upstream serving.

        The in-process face of ``POST /control/resume``; both delegate to
        :meth:`EmulatorState.resume`.
        """
        self.state.resume()

    def expire_all_now(self) -> None:
        """Age every issued JWT past its ``exp``.

        The in-process face of ``POST /control/expire-all-now``; both delegate
        to :meth:`EmulatorState.expire_all_now`, including its static-token
        re-mint.
        """
        self.state.expire_all_now()

    def revoke_tokens(self) -> None:
        """Drop every issued JWT.

        The in-process face of ``POST /control/revoke-tokens``; both delegate
        to :meth:`EmulatorState.revoke_tokens`.
        """
        self.state.revoke_tokens()

    def set_extra_claims(self, claims: dict[str, Any]) -> None:
        """Stage extra claims for the next minted JWT.

        The in-process face of ``POST /control/auth/extra-claims``; both
        delegate to :meth:`EmulatorState.set_extra_claims`.
        """
        self.state.set_extra_claims(claims)

    def set_auth_mode(self, mode: AuthMode, scope: str = "*") -> None:
        """Set the default auth mode (scope=='*') or a per-scope override.

        The in-process face of ``POST /control/auth/mode``; both delegate to
        :meth:`EmulatorState.set_auth_mode`.
        """
        self.state.set_auth_mode(mode, scope)

    def set_presigned_ttl(self, seconds: int) -> None:
        """Set the default presigned URL lifetime.

        The in-process face of ``POST /control/presigned-ttl``; both delegate
        to :meth:`EmulatorState.set_presigned_ttl`.
        """
        self.state.set_presigned_ttl(seconds)

    def set_idempotency_dedup_window(self, seconds: int) -> None:
        """Set the create-response idempotency cache lifetime for new entries."""
        self.state.cfg.upstream.idempotency_dedup_window_seconds = seconds

    def set_seed(self, seed: int) -> None:
        """Reseed the failure-injection RNG.

        Mirrors ``POST /control/seed``.

        NOT hoisted: the same assert-versus-raise divergence recorded on
        :meth:`inject_failure`.
        """
        assert self.state.failure_state is not None
        self.state.failure_state.set_seed(seed)

    def received(self) -> list[ReceivedEntry]:
        """Return the token-keyed latest accepted-body view.

        The in-process face of ``GET /control/received``. THE projection now
        has one implementation, :meth:`EmulatorState.received`, which this
        oracle and the HTTP route both read; before U3 each assembled its
        own, so the conformance tier and the e2e tier could disagree about
        what the emulator saw.
        """
        return self.state.received()

    def upstream_events(self) -> list[UpstreamEvent]:
        """Return a snapshot of the append-only two-step upstream event log."""
        return list(self.state.upstream_events)

    def clear_received(self) -> None:
        """Drop latest accepted bodies and append-only upstream events.

        The in-process face of ``POST /control/clear-received``; both delegate
        to :meth:`EmulatorState.clear_received`.
        """
        self.state.clear_received()

    async def drain(self, *, timeout_seconds: float = DEFAULT_DRAIN_TIMEOUT_SECONDS) -> None:
        """Wait briefly for in-flight requests to settle.

        Uses a small ``asyncio.sleep`` since uvicorn doesn't expose a
        per-request settle hook publicly. Tests that need strict
        ordering should await every individual request explicitly.
        """
        # The serve loop is not directly inspectable; a short sleep
        # gives any in-flight call a chance to finish writing.
        await asyncio.sleep(min(timeout_seconds, 0.05))


def _pick_port(host: str) -> int:
    """Allocate an ephemeral OS port bound to ``host``."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind((host, 0))
        return int(s.getsockname()[1])
    finally:
        s.close()


async def start_server(cfg: AppConfig) -> Server:
    """Start an in-process emulator and return its handle.

    When ``cfg.server.port == 0``, an ephemeral OS port is allocated
    and bound; the resulting :attr:`Server.port` reflects the actual
    port.

    Args:
        cfg: Emulator configuration.

    Returns:
        A :class:`Server` ready for tests to drive.

    Raises:
        TimeoutError: when uvicorn fails to come up within
            :data:`SERVER_STARTUP_TIMEOUT_SECONDS`.
    """
    # Local import to avoid pulling FastAPI into module load time for
    # callers that only need the AppConfig dataclass.
    from phantom_emulator.app import create_app

    port = cfg.server.port if cfg.server.port != 0 else _pick_port(cfg.server.host)
    host = cfg.server.host

    # Re-build cfg with the resolved port so ``Server.url()`` and any
    # downstream references see the actual port.
    resolved_cfg = cfg.model_copy(deep=True)
    resolved_cfg.server.port = port

    app = create_app(resolved_cfg)
    state: EmulatorState = app.state.emulator_state

    uv_config = uvicorn.Config(
        app=app,
        host=host,
        port=port,
        log_level=resolved_cfg.logging.level.lower(),
        lifespan="on",
        access_log=False,
    )
    uv_server = uvicorn.Server(config=uv_config)
    # NOTE: older uvicorn required `install_signal_handlers = lambda: None`
    # here so a non-main-thread server didn't try to register SIGINT.
    # Modern uvicorn (>=0.30) auto-disables signal capture off the main
    # thread inside `capture_signals` - the override is no longer needed.

    serve_task = asyncio.create_task(uv_server.serve())

    # Wait until the server reports it is up. uvicorn sets
    # ``started`` after the startup event lifecycle completes.
    deadline = asyncio.get_event_loop().time() + SERVER_STARTUP_TIMEOUT_SECONDS
    while not uv_server.started:
        if asyncio.get_event_loop().time() > deadline:
            uv_server.should_exit = True
            await serve_task
            raise TimeoutError("phantom-emulator failed to start within timeout")
        await asyncio.sleep(0.01)

    logger.info("phantom-emulator started on %s:%d", host, port)
    return Server(
        config=resolved_cfg,
        state=state,
        uv_server=uv_server,
        serve_task=serve_task,
        port=port,
    )
