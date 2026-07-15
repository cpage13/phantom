"""Shared harness for the V1/V2 real-OS-process SIGKILL reproducers.

The existing crash-recovery suite (``tests/e2e/crash_recovery/``) drives
recovery at the *storage-component* level, and E2E-24 only ever does a
``serve_task.cancel()`` / ``force_exit`` on the in-process uvicorn server —
which STILL runs uvicorn's lifespan teardown (closing aiosqlite cleanly,
flushing the WAL). E2E-24's own docstring says a true SIGKILL "would require
running phantom in a subprocess (out of scope for in-process E2E mode)".

This harness closes exactly that gap. It runs **Phantom as a real OS
subprocess** (``python -m phantom -c <config>``) so the parent can deliver a
genuine ``SIGKILL`` — no lifespan teardown, no clean WAL flush, the database
interrupted mid-write exactly as a power-loss or OOM-kill on a Pi would leave
it. The **emulator stays alive in the parent process** (it is the upstream;
V2 mandates the upstream survive Phantom's death), so the post-restart
backlog has somewhere to deliver and delivery is observable.

Lifecycle a reproducer drives:

1. ``boot_emulator()`` — in-process upstream on a loopback port (survives).
2. ``write_phantom_config()`` — a real YAML pinned to a fixed port + data_dir,
   routes pointed at the live emulator host.
3. ``PhantomSubprocess.start()`` — ``Popen`` the real service; wait for
   ``/v1/healthz`` (R12-1: public liveness on the ingress port).
4. submit a realistic burst over HTTP.
5. ``PhantomSubprocess.sigkill()`` — ``os.kill(pid, SIGKILL)`` mid-flight.
6. ``open_store_readonly()`` + ``integrity_check()`` — open the on-disk DB the
   way the composition root would, assert ``PRAGMA integrity_check`` passes and
   walk the surviving rows.
7. ``PhantomSubprocess.start()`` again on the same data_dir → recovery sweep →
   observe the emulator receive the backlog.

Imported by the killable-subprocess e2e pins under ``tests/e2e/`` (crash
recovery, all-RAM, ingress-abort, db-contention, and the retry/external-lock
regressions); they reuse this launch + on-disk integrity logic rather than
duplicating it.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Final
from uuid import UUID

import httpx
import jwt
import yaml
from phantom_emulator.state import UpstreamEvent

logger = logging.getLogger("e2e.subprocess_harness")


def _find_repo_root() -> Path:
    """Resolve the repo root by walking up to the dir holding ``pyproject.toml``.

    This harness spawns the venv's ``python -m phantom`` with the repo root
    as ``cwd`` and reads the suite's pinned configs under ``tests/e2e/``, so
    it needs the worktree root regardless of how deep the harness file is
    nested. Anchoring on ``pyproject.toml`` is robust to future relocation,
    unlike a brittle ``parents[N]`` count.
    """
    here = Path(__file__).resolve()
    for candidate in (here, *here.parents):
        if (candidate / "pyproject.toml").is_file():
            return candidate
    # Fallback: tests/e2e/_harness/subprocess_harness.py → repo root is parents[3].
    return here.parents[3]


# Repo root (the worktree containing pyproject.toml); see _find_repo_root.
_REPO_ROOT: Path = _find_repo_root()

# Shared HS256 secret — same value the emulator config's env var defaults to.
SIGNING_SECRET: str = "e2e-test-secret"
_SIGNING_KEY_ENV: str = "EMULATOR_SIGNING_KEY"

# Loopback only.
HOST: str = "127.0.0.1"

# Default fake sub claim (v1 sub UUID shape).
DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

# Health-poll budget for the subprocess boot.
HEALTH_TIMEOUT_SECONDS: float = 30.0
HEALTH_POLL_SECONDS: float = 0.1

# Transient httpx errors expected while polling the admin health endpoint
# before the subprocess has bound its socket. Bound to a module-level constant
# rather than an inline ``except (A, B, C):`` clause: ruff 0.15.x reformats a
# parenthesized no-``as`` except-tuple by STRIPPING the parens under Python
# 3.14, so ``ruff format --check`` fails on the inline form (the project's
# established workaround). The constant sidesteps the reformat and is also
# clearer at the call site.
_HEALTH_POLL_RETRYABLE_ERRORS: Final[tuple[type[BaseException], ...]] = (
    httpx.ConnectError,
    httpx.ReadError,
    httpx.RemoteProtocolError,
)

# Grace the session reaper gives SIGTERM before escalating to SIGKILL:
# generous enough for a clean lifespan teardown (worker drain + WAL flush),
# short enough not to stall session end noticeably.
_REAP_TERM_GRACE_SECONDS: Final[float] = 5.0
# Bound on the post-SIGKILL zombie-reap wait. SIGKILL cannot be ignored, so
# this only covers the kernel actually tearing the process down.
_REAP_KILL_GRACE_SECONDS: Final[float] = 5.0


# ---------------------------------------------------------------------------
# Session-wide daemon registry (the cross-session leak fix)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReapedDaemon:
    """Record of one leaked daemon the session finalizer had to put down."""

    pid: int
    label: str


@dataclass
class PhantomDaemonRegistry:
    """Session-wide registry of every spawned ``python -m phantom`` daemon.

    The prior cycle's suite leaked orphaned daemons whenever a test failed
    before its own teardown (or skipped teardown outright). Every spawn site
    under ``tests/e2e/`` now registers its ``Popen`` handle here at spawn
    time, and the session finalizer in ``tests/e2e/conftest.py`` calls
    :meth:`reap_all` after the last test, so no tracked daemon can outlive
    the pytest session.
    """

    _tracked: list[tuple[subprocess.Popen[bytes], str]] = field(default_factory=list)

    def register(self, proc: subprocess.Popen[bytes], *, label: str) -> None:
        """Track one spawned daemon process for session-end reaping.

        Args:
            proc: The live ``Popen`` handle of the daemon.
            label: Human-readable provenance (config path / spawn site) used
                in the reap log so a leak is attributable to its test.
        """
        self._tracked.append((proc, label))

    def live(self) -> list[tuple[int, str]]:
        """Return ``(pid, label)`` for every tracked process still alive."""
        return [(proc.pid, label) for proc, label in self._tracked if proc.poll() is None]

    def reap_all(self) -> list[ReapedDaemon]:
        """Terminate (then kill) every still-alive tracked daemon.

        Idempotent: processes that already exited (the normal case, when
        every test tore down properly) are skipped silently. Each reaped
        daemon is logged with its pid and provenance label.

        Returns:
            One :class:`ReapedDaemon` record per process that was still
            alive and had to be reaped; empty when nothing leaked.
        """
        reaped: list[ReapedDaemon] = []
        for proc, label in self._tracked:
            if proc.poll() is not None:
                continue
            logger.warning("reaping leaked phantom daemon pid=%s (%s): SIGTERM", proc.pid, label)
            proc.terminate()
            try:
                proc.wait(timeout=_REAP_TERM_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                logger.warning(
                    "leaked phantom daemon pid=%s ignored SIGTERM; escalating to SIGKILL",
                    proc.pid,
                )
                proc.kill()
                try:
                    proc.wait(timeout=_REAP_KILL_GRACE_SECONDS)
                except subprocess.TimeoutExpired:
                    logger.error(
                        "leaked phantom daemon pid=%s survived SIGKILL wait; "
                        "kernel has not reaped it yet",
                        proc.pid,
                    )
                    continue
            reaped.append(ReapedDaemon(pid=proc.pid, label=label))
        return reaped


# Process-global registry. Deliberately module-level rather than a fixture:
# the spawn sites (PhantomSubprocess.start and the no-health-wait boot in the
# external-lock regression) are plain helpers with no access to pytest
# fixtures, and one pytest session is one process, so module scope IS session
# scope here. The conftest session finalizer is the sole reaper.
DAEMON_REGISTRY: PhantomDaemonRegistry = PhantomDaemonRegistry()


def allocate_port() -> int:
    """Allocate an ephemeral OS port (brief SO_REUSEADDR window)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind((HOST, 0))
        return int(s.getsockname()[1])
    finally:
        s.close()


# ---------------------------------------------------------------------------
# Emulator (in-process, parent — the surviving upstream)
# ---------------------------------------------------------------------------


@dataclass
class EmulatorHandle:
    """An in-process emulator kept alive in the parent test process."""

    server: Any
    url: str

    def received(self) -> list[Any]:
        """Return the emulator's token-keyed latest accepted-body view."""
        return self.server.received()

    def upstream_events(self) -> list[UpstreamEvent]:
        """Return the append-only metadata-create/body-PUT event log."""
        return self.server.upstream_events()

    def clear_received(self) -> None:
        """Drop latest accepted bodies and append-only upstream events."""
        self.server.clear_received()

    def inject_failure(self, policy: Any) -> None:
        """Install a failure policy on the upstream."""
        self.server.inject_failure(policy)

    def clear_failures(self) -> None:
        """Drop every installed failure policy."""
        self.server.clear_failures()

    def pause(self) -> None:
        """Refuse upstream requests with 503 until resume()."""
        self.server.pause()

    def resume(self) -> None:
        """Restore normal upstream serving."""
        self.server.resume()

    async def stop(self) -> None:
        """Stop the in-process emulator server."""
        await self.server.stop()


async def boot_emulator(*, tls: tuple[str, str] | None = None) -> EmulatorHandle:
    """Boot one phantom-emulator in-process on a loopback port.

    Mirrors ``tests/e2e/helpers/stack._boot_one_emulator`` but standalone so
    the reproducers do not depend on the pytest conftest.

    Args:
        tls: Optional ``(cert_path, key_path)`` PEM pair. When set, the
            in-process uvicorn serves HTTPS and the handle's ``url`` (and the
            emulator's JWT issuer) become ``https://`` — the T3 trusted-HTTPS
            authority lane (azure-identity refuses plaintext authorities).
            Default ``None``: plaintext HTTP, byte-for-byte prior behavior.
    """
    os.environ.setdefault(_SIGNING_KEY_ENV, SIGNING_SECRET)
    import uvicorn
    from phantom_emulator.app import create_app as emulator_create_app
    from phantom_emulator.config import load_config as load_emulator_config
    from phantom_emulator.server import Server as EmulatorServer

    emu_port = allocate_port()
    cfg_path = _REPO_ROOT / "tests" / "e2e" / "emulator-config.yml"
    emu_cfg = load_emulator_config(cfg_path)
    scheme = "https" if tls is not None else "http"
    emu_cfg.server.host = HOST
    emu_cfg.server.port = emu_port
    emu_cfg.auth.issuer = f"{scheme}://{HOST}:{emu_port}"

    app = emulator_create_app(emu_cfg)
    state = app.state.emulator_state
    ssl_kwargs: dict[str, str] = (
        {} if tls is None else {"ssl_certfile": tls[0], "ssl_keyfile": tls[1]}
    )
    uv_config = uvicorn.Config(
        app=app,
        host=HOST,
        port=emu_port,
        log_level="warning",
        lifespan="on",
        access_log=False,
        **ssl_kwargs,  # type: ignore[arg-type]
    )
    uv_server = uvicorn.Server(config=uv_config)
    serve_task = asyncio.create_task(uv_server.serve())
    deadline = time.monotonic() + HEALTH_TIMEOUT_SECONDS
    while not uv_server.started:
        if time.monotonic() > deadline:
            uv_server.should_exit = True
            await serve_task
            raise RuntimeError("emulator failed to start")
        await asyncio.sleep(HEALTH_POLL_SECONDS)  # pre-commit-allow: sleep
    server = EmulatorServer(
        config=emu_cfg, state=state, uv_server=uv_server, serve_task=serve_task, port=emu_port
    )
    url = server.url()
    if tls is not None:
        url = url.replace("http://", "https://", 1)
    logger.info("emulator up at %s", url)
    return EmulatorHandle(server=server, url=url)


def fake_security_token(
    emulator_handle: EmulatorHandle,
    *,
    sub: str = DEFAULT_SUB,
    expires_in_seconds: int = 3600,
) -> str:
    """Mint a fake bearer JWT the emulator will accept (HS256, shared secret)."""
    cfg = emulator_handle.server.config
    now = datetime.now(UTC)
    payload: dict[str, Any] = {
        "iss": cfg.auth.issuer,
        "sub": sub,
        "aud": cfg.auth.audience,
        "exp": int((now + timedelta(seconds=expires_in_seconds)).timestamp()),
        "iat": int(now.timestamp()),
        "tid": cfg.auth.tenant_id,
    }
    return jwt.encode(payload, SIGNING_SECRET, algorithm="HS256")


# ---------------------------------------------------------------------------
# Phantom subprocess (the killable service-under-test)
# ---------------------------------------------------------------------------


def write_phantom_config(
    *,
    data_dir: Path,
    bind_port: int,
    config_overrides: dict[str, Any] | None = None,
) -> Path:
    """Write a standalone Phantom YAML config to ``data_dir/phantom.yml``.

    Starts from the E2E suite's pinned ``phantom-config.yml`` (so route
    table + retention semantics match the suite), then pins the bind port +
    data_dir and applies any per-reproducer overrides via deep merge.

    Args:
        data_dir: The instance storage tree root.
        bind_port: The PUBLIC ingress port.
        config_overrides: Per-reproducer deep-merge overlay.

    Returns:
        The path to the written YAML config. The single listener serves
        intake + admin + health on ``bind_port``.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    base_path = _REPO_ROOT / "tests" / "e2e" / "phantom-config.yml"
    raw = yaml.safe_load(base_path.read_text())
    assert isinstance(raw, dict)
    # One listener serves intake + admin + health on the allocated port
    # (the same-machine-only single-listener composition).
    raw.setdefault("server", {})["bind_tcp"] = f"{HOST}:{bind_port}"
    raw.setdefault("storage", {})["data_dir"] = str(data_dir)
    if config_overrides:
        _deep_merge(raw, config_overrides)
    out = data_dir / "phantom.yml"
    out.write_text(yaml.safe_dump(raw))
    return out


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``overlay`` into ``base`` (lists replace wholesale)."""
    for key, value in overlay.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


@dataclass
class PhantomSubprocess:
    """A real OS subprocess running ``python -m phantom -c <config>``.

    The whole point: ``sigkill()`` delivers a genuine ``SIGKILL`` to the
    process, so no lifespan teardown runs and the SQLite WAL is interrupted
    mid-write — the realistic power-loss / OOM-kill on a Pi.
    """

    config_path: Path
    bind_port: int
    url: str
    # The single listener serves intake + admin + health on one socket, so
    # ``admin_url`` is the SAME URL as ``url`` (admin rides the one base
    # URL). Kept as an alias so tests reaching admin endpoints can read
    # ``proc.admin_url`` without caring about the collapse.
    admin_url: str = ""
    argv: tuple[str, ...] | None = None
    env_overrides: dict[str, str] = field(default_factory=dict)
    # CA-bundle path for a TLS-enabled child. None (the default) keeps the
    # plaintext-HTTP behavior byte-for-byte; set, the harness reaches the
    # child over https and the health poll VERIFIES against this bundle
    # (never ``verify=False`` — the T5 operator-TLS posture).
    tls_verify: str | None = None
    _proc: subprocess.Popen[bytes] | None = field(default=None)
    _log_path: Path | None = field(default=None)

    @classmethod
    def make(
        cls,
        config_path: Path,
        bind_port: int,
        *,
        argv: tuple[str, ...] | None = None,
        env_overrides: dict[str, str] | None = None,
        tls_verify: str | None = None,
    ) -> PhantomSubprocess:
        """Construct (not yet started) against a config + port.

        The single listener serves intake + admin + health on ``bind_port``,
        so ``admin_url`` equals ``url`` (admin rides the one base URL).

        Args:
            config_path: The written YAML config.
            bind_port: The listener port (intake + admin + health).
            argv: Optional complete child argv. The default is the production
                ``python -m phantom`` entry point.
            env_overrides: Optional child-only environment additions or
                replacements. The parent process is never mutated.
            tls_verify: Optional CA-bundle path for a child whose config sets
                ``server.tls.enabled``. When set, ``url`` (and ``admin_url``)
                become ``https://`` and the health poll verifies the listener
                against exactly this bundle. Default ``None``: plaintext HTTP,
                byte-for-byte the prior behavior.

        Returns:
            An unstarted :class:`PhantomSubprocess`.
        """
        scheme = "https" if tls_verify is not None else "http"
        url = f"{scheme}://{HOST}:{bind_port}"
        return cls(
            config_path=config_path,
            bind_port=bind_port,
            url=url,
            admin_url=url,
            argv=argv,
            env_overrides={} if env_overrides is None else dict(env_overrides),
            tls_verify=tls_verify,
        )

    def spawn(self, *, label: str | None = None) -> None:
        """Popen the service child WITHOUT waiting for health.

        The launch half of :meth:`start`, exposed for the lanes that must not
        health-poll: an expected-early-exit child (wrong TLS key password), a
        child that may legitimately block boot past the health budget (held
        external lock), or a non-TCP listener the poll cannot reach (UDS).
        Callers own their readiness (or exit) observation, typically via
        :meth:`wait_for_expected_exit` or a lane-specific probe.

        Args:
            label: Optional provenance label for the daemon registry (leak
                attribution). Defaults to the config path.
        """
        log_path = self.config_path.parent / f"phantom-{int(time.time() * 1000)}.log"
        self._log_path = log_path
        env = dict(os.environ)
        env.setdefault(_SIGNING_KEY_ENV, SIGNING_SECRET)
        env.update(self.env_overrides)
        # Spawn the venv interpreter DIRECTLY (sys.executable is the venv's
        # python inside the test session; phantom is installed editable).
        # The previous `uv run python -m phantom` indirection left a resident
        # `uv` wrapper as the Popen pid: signals the suite sent to that pid
        # only reached the real daemon when uv could forward them, so a
        # SIGKILL (unforwardable) killed the wrapper, ORPHANED the live
        # daemon to launchd/init, and broke the reproducers' whole premise.
        # Direct spawn makes _proc.pid the actual daemon: SIGKILL is genuine,
        # terminate() needs no forwarding, and the registry tracks the truth.
        with open(log_path, "wb") as logf:
            self._proc = subprocess.Popen(
                self.argv
                if self.argv is not None
                else (sys.executable, "-m", "phantom", "-c", str(self.config_path)),
                cwd=str(_REPO_ROOT),
                stdout=logf,
                stderr=subprocess.STDOUT,
                env=env,
            )
        # Register at spawn time, before any wait: a daemon that hangs
        # mid-boot when its test dies is exactly the leak the session reaper
        # exists for.
        DAEMON_REGISTRY.register(
            self._proc, label=label if label is not None else f"config={self.config_path}"
        )

    async def start(self) -> None:
        """Popen the service and wait for ``/v1/healthz`` to answer 200."""
        self.spawn()
        await self._await_health()

    async def _await_health(self) -> None:
        """Poll the admin health endpoint until it answers or the budget expires."""
        deadline = time.monotonic() + HEALTH_TIMEOUT_SECONDS
        # verify is inert for the default plaintext http URL; for a TLS child
        # it pins trust to the caller-supplied bundle (never verify=False).
        verify: str | bool = self.tls_verify if self.tls_verify is not None else True
        async with httpx.AsyncClient(timeout=2.0, verify=verify) as client:
            while time.monotonic() < deadline:
                if self._proc is not None and self._proc.poll() is not None:
                    tail = self._read_log_tail()
                    raise RuntimeError(
                        f"phantom subprocess exited early (code={self._proc.returncode}); "
                        f"log tail:\n{tail}"
                    )
                try:
                    r = await client.get(f"{self.url}/v1/healthz")
                    if r.status_code == 200:
                        logger.info("phantom subprocess healthy at %s (pid=%s)", self.url, self.pid)
                        return
                except _HEALTH_POLL_RETRYABLE_ERRORS:
                    pass
                await asyncio.sleep(HEALTH_POLL_SECONDS)  # pre-commit-allow: sleep
        raise RuntimeError(
            f"phantom subprocess did not become healthy within {HEALTH_TIMEOUT_SECONDS}s"
        )

    @property
    def pid(self) -> int | None:
        """The OS pid, or ``None`` if not started."""
        return self._proc.pid if self._proc is not None else None

    @property
    def returncode(self) -> int | None:
        """The child exit status, or ``None`` while it is still running."""
        return self._proc.poll() if self._proc is not None else None

    async def wait_for_expected_exit(self, *, timeout_seconds: float = 30.0) -> int:
        """Wait for the child to exit and require a non-zero status.

        Args:
            timeout_seconds: Maximum wait for TaskGroup failure to stop the
                process.

        Returns:
            The observed non-zero process exit status.

        Raises:
            AssertionError: If no process is running, it times out, or exits
                successfully instead of surfacing the expected fault.
        """
        if self._proc is None:
            raise AssertionError("phantom subprocess was not started")
        try:
            returncode = await asyncio.to_thread(self._proc.wait, timeout=timeout_seconds)
        except subprocess.TimeoutExpired as exc:
            raise AssertionError(
                f"phantom subprocess stayed alive for {timeout_seconds}s after expected fault; "
                f"log tail:\n{self._read_log_tail()}"
            ) from exc
        if returncode == 0:
            raise AssertionError(
                "phantom subprocess exited successfully after an expected unknown fault; "
                f"log tail:\n{self._read_log_tail()}"
            )
        return returncode

    def read_full_log(self) -> str:
        """Return the complete child log, or a sentinel when unavailable."""
        if self._log_path is None or not self._log_path.exists():
            return "<no log>"
        return self._log_path.read_text(errors="replace")

    def sigkill(self) -> None:
        """Deliver a genuine ``SIGKILL`` — no lifespan teardown, WAL mid-write.

        This is the load-bearing difference from E2E-24's ``force_exit`` (which
        still runs the lifespan and flushes the WAL cleanly).
        """
        if self._proc is None:
            return
        if self._proc.poll() is not None:
            return
        logger.info("delivering SIGKILL to phantom subprocess pid=%s", self._proc.pid)
        os.kill(self._proc.pid, signal.SIGKILL)
        try:
            self._proc.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            logger.warning("phantom subprocess did not reap after SIGKILL")

    def terminate(self) -> None:
        """Best-effort clean stop (SIGTERM) used in teardown."""
        if self._proc is None or self._proc.poll() is not None:
            return
        self._proc.send_signal(signal.SIGTERM)
        try:
            self._proc.wait(timeout=10.0)
        except subprocess.TimeoutExpired:
            os.kill(self._proc.pid, signal.SIGKILL)
            self._proc.wait(timeout=5.0)

    def _read_log_tail(self, n: int = 40) -> str:
        """Return the last ``n`` lines of the subprocess log for diagnostics."""
        if self._log_path is None or not self._log_path.exists():
            return "<no log>"
        lines = self._log_path.read_text(errors="replace").splitlines()
        return "\n".join(lines[-n:])


# ---------------------------------------------------------------------------
# Restart-side verification (open the on-disk DB the way recovery does)
# ---------------------------------------------------------------------------


# The multi-instance ``app.py`` nests per-instance storage under
# ``<data_dir>/<instance.data_dir>/`` (app.py:162). The suite's pinned config
# declares a single instance with ``data_dir: "primary"``, so the real on-disk
# DB is ``<data_dir>/primary/uploads.db`` and bodies live under
# ``<data_dir>/primary/bodies``.
DEFAULT_INSTANCE: str = "primary"


def instance_dir(data_dir: Path, instance_id: str = DEFAULT_INSTANCE) -> Path:
    """Return the per-instance storage subdirectory (app.py:162 layout)."""
    return data_dir / instance_id


def db_path_for(data_dir: Path, instance_id: str = DEFAULT_INSTANCE) -> Path:
    """Return the on-disk ``uploads.db`` path for ``instance_id``."""
    return instance_dir(data_dir, instance_id) / "uploads.db"


async def integrity_check(data_dir: Path, instance_id: str = DEFAULT_INSTANCE) -> tuple[bool, str]:
    """Run ``PRAGMA integrity_check`` against the on-disk uploads.db.

    Opens a fresh aiosqlite connection — the same surface the composition
    root's IntegrityChecker uses — so a WAL torn mid-write by the SIGKILL
    would surface here as a non-"ok" result.

    Returns:
        ``(ok, message)`` — ``ok`` True when integrity_check returns "ok".
    """
    import aiosqlite

    db_path = db_path_for(data_dir, instance_id)
    if not db_path.exists():
        return False, f"uploads.db missing at {db_path}"
    async with (
        aiosqlite.connect(str(db_path)) as conn,
        conn.execute("PRAGMA integrity_check") as cur,
    ):
        rows = await cur.fetchall()
    msg = ";".join(str(r[0]) for r in rows)
    return (msg.strip().lower() == "ok"), msg


async def open_store_readonly(data_dir: Path, instance_id: str = DEFAULT_INSTANCE) -> Any:
    """Open a fresh :class:`SqliteUploadStore` against the persisted data_dir.

    Caller is responsible for ``await store.stop()``. This is exactly how the
    composition root re-opens the store on restart, so reading rows here is a
    faithful "what does the next process see" probe.
    """
    from phantom.storage.sqlite_store import SqliteUploadStore

    store = SqliteUploadStore(str(db_path_for(data_dir, instance_id)))
    await store.start()
    return store


async def count_rows_by_state(
    data_dir: Path, instance_id: str = DEFAULT_INSTANCE
) -> dict[str, int]:
    """Return a ``{state: count}`` census of the on-disk uploads table."""
    import aiosqlite

    db_path = db_path_for(data_dir, instance_id)
    counts: dict[str, int] = {}
    async with (
        aiosqlite.connect(str(db_path)) as conn,
        conn.execute("SELECT state, COUNT(*) FROM uploads GROUP BY state") as cur,
    ):
        async for row in cur:
            counts[str(row[0])] = int(row[1])
    return counts


async def submit_one(
    client: Any,
    *,
    emulator_url: str,
    bearer: str,
    body: bytes,
    chain_id: UUID,
    file_prefix: str = "v1",
) -> None:
    """Submit one upload-shaped chain through the phantom-client SDK."""
    from tests.e2e._driver import build_in_memory_upload_envelope
    from tests.e2e.helpers.payloads import build_create_file_request

    request = build_create_file_request(file_name=f"{file_prefix}-{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    await client.submit_chain(
        envelope,
        body_refs={"body": body},
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )


def configure_root_logging() -> None:
    """Configure logging once for a reproducer entrypoint."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
