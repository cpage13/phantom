"""Boot/teardown of the cross-package E2E stack.

The stack is the three-package composition that every E2E test runs
against: phantom (service), phantom-emulator (upstream), and
phantom-client (SDK). :class:`E2EStack` exposes the boots
URLs and a typed :class:`EmulatorControl` that mirrors the emulator's
``/control/*`` surface one-to-one, so tests can drive failure
injection without crafting HTTP control calls.

The stack boots phantom and phantom-emulator as in-process
:class:`uvicorn.Server` instances on ephemeral ports (fast local
iteration, < 60 s suite target). The container topology has its own
dedicated lane and is NOT a stack mode:
``tests/e2e/test_docker_volume_replacement.py`` builds both images and
drives the compose file at ``tests/e2e/docker/compose.yml``.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import httpx
import jwt
import pytest
import uvicorn
import yaml  # type: ignore[import-untyped]  # types-PyYAML not in workspace dev deps
from phantom.app import create_app as phantom_create_app
from phantom.config.settings import Settings, load_settings
from phantom.runtime.tls_cert import resolve_tls_paths
from phantom_client import PhantomClient
from phantom_emulator import AppConfig as EmulatorAppConfig
from phantom_emulator import Server as EmulatorServer
from phantom_emulator.app import create_app as emulator_create_app
from phantom_emulator.auth.modes import AuthMode
from phantom_emulator.config import load_config as load_emulator_config
from phantom_emulator.failure.injection import (
    ErrorRate5xxEvent,
    FailurePolicy,
    FailureScope,
)
from phantom_emulator.routers.control import ReceivedEntry
from phantom_emulator.state import EmulatorState, RawBody, S3Object, UpstreamEvent

logger = logging.getLogger(__name__)

# HS256 secret env var shared between the driver's `get_security_token`
# fake and the emulator's JWT minter. Both sides read the same value
# from the environment so a JWT minted in one verifies in the other.
ENV_EMULATOR_SIGNING_KEY: str = "EMULATOR_SIGNING_KEY"

# AD client-secret env vars consulted by Phantom's
# `ad_client_credentials` refresh strategy. E2E-1 uses `wait` strategy
# so these are unused, but inprocess mode sets them anyway so a future
# test that flips to `ad_client_credentials` doesn't fail on a missing
# env.
ENV_PHANTOM_AD_PRIMARY: str = "PHANTOM_AD_PRIMARY"
ENV_PHANTOM_AD_SECONDARY: str = "PHANTOM_AD_SECONDARY"

# Default value for the HS256 secret when the env var is unset. Tests
# that depend on a known secret (e.g., for forging arbitrary JWTs)
# read this constant via :data:`E2EStack.signing_secret`.
DEFAULT_SIGNING_SECRET: str = "e2e-test-secret"

# Bind host for inprocess mode. Loopback only — admin endpoints are
# loopback-only (ADR-004) and the suite has no need to reach them
# from off-host.
INPROCESS_BIND_HOST: str = "127.0.0.1"

# Budget for the darwin loopback-alias preflight's non-interactive sudo
# attempt; `sudo -n` answers immediately (it never prompts), so this only
# bounds a pathological hang.
_SUDO_ALIAS_TIMEOUT_SECONDS: float = 10.0

# Operator-facing fixer script named by the preflight failure message.
_LOOPBACK_ALIAS_SCRIPT: str = "scripts/dev/ensure_loopback_alias.sh"


def _can_bind_loopback(host: str) -> bool:
    """Return True when a TCP socket can bind ``host`` on this machine."""
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind((host, 0))
        return True
    except OSError:
        return False
    finally:
        probe.close()


def ensure_second_loopback(host: str) -> None:
    """Preflight for multi-instance fixtures that need a second loopback IP.

    Linux binds all of 127.0.0.0/8 natively, so this returns immediately
    there. macOS exposes only 127.0.0.1 on lo0 until an alias is added; on
    darwin a non-bindable ``host`` triggers ONE non-interactive alias
    attempt (``sudo -n /sbin/ifconfig lo0 alias <host> up``, which succeeds
    on boxes with cached or passwordless sudo), and if the address STILL
    does not bind the test fails (never skips) with a one-line message
    naming the fixer script. Non-darwin platforms keep the historical skip
    semantics, so Linux CI behavior is unchanged.

    Args:
        host: The loopback IP the fixtures must bind (e.g. ``127.0.0.2``).

    Raises:
        pytest.fail.Exception: On darwin when ``host`` stays unbindable.
        pytest.skip.Exception: On non-darwin hosts that cannot bind.
    """
    if _can_bind_loopback(host):
        return
    if sys.platform != "darwin":
        pytest.skip(
            f"loopback IP {host} is not bindable on this non-darwin host; "
            "Linux CI is the authoritative lane"
        )
    logger.info("loopback %s not bindable; attempting non-interactive lo0 alias (sudo -n)", host)
    try:
        subprocess.run(
            ["sudo", "-n", "/sbin/ifconfig", "lo0", "alias", host, "up"],
            capture_output=True,
            timeout=_SUDO_ALIAS_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired:
        logger.warning("non-interactive sudo lo0-alias attempt for %s timed out", host)
    if _can_bind_loopback(host):
        logger.info("lo0 alias %s added non-interactively", host)
        return
    pytest.fail(
        f"second loopback {host} is not bindable; run `bash {_LOOPBACK_ALIAS_SCRIPT}` "
        "once on this Mac, then re-run",
        pytrace=False,
    )


# Seconds to wait for uvicorn to come up before raising. Generous
# enough for slow CI hosts; aborts fast on a real problem.
SERVER_STARTUP_TIMEOUT_SECONDS: float = 15.0

# Polling interval while uvicorn is starting up. Matches the
# phantom-emulator constant for consistency.
SERVER_STARTUP_POLL_INTERVAL_SECONDS: float = 0.01

# Default JWT expiration on the fake security token. Long enough that
# a single E2E test run never sees the token expire mid-suite.
FAKE_TOKEN_EXPIRY_SECONDS: int = 3600

# Default `sub` claim for the fake security token. v1 sub shape —
# UUID-formatted app object id. Tests that exercise the v0 path
# (numeric `sub`) mint their own token.
DEFAULT_FAKE_SUB: str = "00000000-0000-0000-0000-000000000001"

# Paths to the YAML configs used by both modes. Resolved relative to
# this file's location so the stack works regardless of the caller's
# working directory.
_HELPERS_DIR: Path = Path(__file__).resolve().parent
_E2E_DIR: Path = _HELPERS_DIR.parent
PHANTOM_CONFIG_PATH: Path = _E2E_DIR / "phantom-config.yml"
EMULATOR_CONFIG_PATH: Path = _E2E_DIR / "emulator-config.yml"


class StackBootError(RuntimeError):
    """Raised when the stack fails to boot."""


@dataclass
class E2EStack:
    """One full stack for the E2E suite.

    Constructed by :func:`boot_stack` and torn down by
    :meth:`tear_down`. Tests should never construct this directly —
    use the ``stack`` pytest fixture in ``conftest.py``.

    Attributes:
        phantom_url: HTTP base URL for Phantom's ingress endpoint.
        phantom_admin_url: HTTP base URL for Phantom's admin endpoint -
            the SAME URL as ``phantom_url`` (admin rides the one listener;
            the deployment is same-machine-only).
        emulator_url: HTTP base URL for the emulator.
        phantom_client: An open :class:`PhantomClient` pointed at
            Phantom's ingress + admin URL.
        emulator: Typed control wrapper for the emulator.
        signing_secret: The HS256 secret used by both the driver's
            fake security token and the emulator's JWT minter.
        data_dir: Phantom's storage root (the on-disk path under which
            per-instance ``bodies/`` trees live). Tests that need to
            inspect or mutate persisted bodies compute their paths from
            this root.
        settings: The validated :class:`Settings` Phantom booted with.
            Helpers consult it for per-instance ``data_dir`` and the
            global ``shard_prefix_chars`` to compute body file paths.
        settings_path: When hot reload is enabled, the on-disk path to
            the merged YAML config Phantom was constructed against.
            Tests rewrite this file then trigger SIGHUP or
            ``POST /v1/admin/reload`` to exercise the hot-reload path.
            ``None`` when hot reload is disabled (the default).
    """

    phantom_url: str
    phantom_admin_url: str
    emulator_url: str
    phantom_client: PhantomClient
    emulator: EmulatorControl
    signing_secret: str
    data_dir: Path
    settings: Settings
    extra_emulator_urls: list[str] = field(default_factory=list)
    extra_emulators: list[EmulatorControl] = field(default_factory=list)
    settings_path: Path | None = None
    _phantom_uv_server: uvicorn.Server | None = None
    _phantom_serve_task: asyncio.Task[None] | None = None
    _emulator_server: EmulatorServer | None = None
    _extra_emulator_servers: list[EmulatorServer] = field(default_factory=list)
    _tmp_paths: list[Path] = field(default_factory=list)

    def get_instance(self, instance_id: str = "primary") -> Any:
        """Return the live :class:`InstanceContext` for ``instance_id``.

        Useful for tests that need to insert into the storage layer
        directly (storage-corruption seeds, recovery scenarios). The
        return type is left dynamic because :class:`InstanceContext`
        is internal to ``phantom`` and tests should not import it.

        Args:
            instance_id: Which instance to return. Defaults to ``"primary"``.

        Raises:
            RuntimeError: When called outside ``inprocess`` mode.
            KeyError: When no instance with ``instance_id`` is wired.
        """
        if self._phantom_uv_server is None:
            raise RuntimeError("get_instance() requires a live in-process stack")
        # The uvicorn config's `app` field is typed as a broad union
        # (callable / asgi / str) so mypy can't see `.state`. We know
        # we passed a FastAPI app in `_boot_inprocess`, so an explicit
        # cast keeps the typing strict-clean.
        app: Any = self._phantom_uv_server.config.app
        for inst in app.state.instances:
            if inst.cfg.id == instance_id:
                return inst
        raise KeyError(f"no instance with id={instance_id!r}")

    def rewrite_yaml(self, overrides: Mapping[str, Any]) -> None:
        """Deep-merge ``overrides`` into the live YAML on disk.

        Helper for hot-reload tests. Reads the YAML at
        :attr:`settings_path`, applies a deep merge of ``overrides`` over
        the existing keys, then writes the result back to the same path.
        The merge follows the same semantics as ``config_overrides`` on
        :func:`boot_stack` — mappings merge key-by-key, non-mappings
        (including lists) are replaced wholesale.

        After this call, the file is freshly readable by
        :meth:`Settings.reload_from_yaml`; either SIGHUP or
        ``POST /v1/admin/reload`` will pick up the change on next fire.

        Args:
            overrides: Mapping to deep-merge into the on-disk YAML.

        Raises:
            RuntimeError: When hot reload was not enabled at boot
                (``settings_path is None``).
        """
        if self.settings_path is None:
            raise RuntimeError(
                "rewrite_yaml() requires enable_hot_reload=True at boot_stack(); "
                "settings_path is None"
            )
        raw = yaml.safe_load(self.settings_path.read_text())
        if not isinstance(raw, dict):
            raise StackBootError(
                f"YAML at {self.settings_path} no longer a mapping; cannot merge overrides"
            )
        _deep_merge_dict(raw, overrides)
        self.settings_path.write_text(yaml.safe_dump(raw))

    def body_path(
        self,
        chain_id: UUID,
        body_ref_name: str,
        *,
        instance_id: str = "primary",
    ) -> Path:
        """Return the on-disk body file path for ``chain_id`` / ``body_ref_name``.

        Mirrors :meth:`FileBodyStore._path_for`: shard prefix from the
        first ``shard_prefix_chars`` of the chain_id's hex, then the
        chain_id directory, then the named body_ref file.

        Args:
            chain_id: The chain whose body file to locate.
            body_ref_name: The named body_ref (e.g., ``"body"``).
            instance_id: Which instance owns the row. Defaults to
                ``"primary"`` (the suite's default instance id).
        """
        instance_cfg = next(i for i in self.settings.instances if i.id == instance_id)
        shard_chars = self.settings.storage.shard_prefix_chars
        shard = str(chain_id)[:shard_chars]
        return (
            self.data_dir / instance_cfg.data_dir / "bodies" / shard / str(chain_id) / body_ref_name
        )

    def fake_security_token(
        self,
        *,
        sub: str = DEFAULT_FAKE_SUB,
        expires_in_seconds: int = FAKE_TOKEN_EXPIRY_SECONDS,
        extra_claims: dict[str, Any] | None = None,
    ) -> str:
        """Mint a fake bearer JWT that the emulator will accept.

        Args:
            sub: The ``sub`` claim. Used by the driver's
                :func:`derive_uid` to derive ``X-Phantom-Uid``.
            expires_in_seconds: JWT lifetime relative to now.
            extra_claims: Additional claims merged into the payload.

        Returns:
            The compact-serialized JWT (without ``Bearer`` prefix).
        """
        from datetime import UTC, datetime, timedelta

        cfg = self._emulator_cfg
        now = datetime.now(UTC)
        payload: dict[str, Any] = {
            "iss": cfg.auth.issuer,
            "sub": sub,
            "aud": cfg.auth.audience,
            "exp": int((now + timedelta(seconds=expires_in_seconds)).timestamp()),
            "iat": int(now.timestamp()),
            "tid": cfg.auth.tenant_id,
        }
        if extra_claims:
            payload.update(extra_claims)
        return jwt.encode(payload, self.signing_secret, algorithm="HS256")

    @property
    def _emulator_cfg(self) -> EmulatorAppConfig:
        """Return the live emulator's configuration."""
        if self._emulator_server is None:
            raise RuntimeError("emulator not started")
        return self._emulator_server.config

    async def tear_down(self) -> None:
        """Stop both servers, close the client, remove temp dirs."""
        await self.phantom_client.aclose()
        if self._phantom_uv_server is not None and self._phantom_serve_task is not None:
            self._phantom_uv_server.should_exit = True
            try:
                await asyncio.wait_for(
                    self._phantom_serve_task,
                    timeout=SERVER_STARTUP_TIMEOUT_SECONDS,
                )
            except TimeoutError:
                logger.warning("phantom serve task did not exit; cancelling")
                self._phantom_serve_task.cancel()
        if self._emulator_server is not None:
            await self._emulator_server.stop()
        for extra in self._extra_emulator_servers:
            await extra.stop()
        for tmp in self._tmp_paths:
            # ignore_errors=True so a half-removed tree (e.g., from a
            # crashed prior run) doesn't mask the real test failure.
            import shutil

            shutil.rmtree(tmp, ignore_errors=True)


class EmulatorControl:
    """Typed wrapper around the emulator's control surface.

    Inprocess mode delegates to the :class:`Server` object directly
    (no HTTP). Docker mode would issue HTTP control calls; that path
    is stubbed pending CI infrastructure.

    Method names mirror ``/control/*`` paths one-to-one so readers
    can grep either side.
    """

    def __init__(self, *, server: EmulatorServer) -> None:
        """Bind the wrapper to a running emulator server."""
        self._server = server

    def inject_failure(self, policy: FailurePolicy) -> None:
        """Install or replace a failure policy."""
        self._server.inject_failure(policy)

    def clear_failures(self) -> None:
        """Drop every installed failure policy."""
        self._server.clear_failures()

    def error_rate_5xx_count(self, scope: FailureScope) -> int:
        """Return the error-rate-generated 503 count for one request scope."""
        return self._server.error_rate_5xx_count(scope)

    def error_rate_5xx_events(self, scope: FailureScope) -> tuple[ErrorRate5xxEvent, ...]:
        """Return immutable error-rate-generated 503 events for one scope."""
        return self._server.error_rate_5xx_events(scope)

    def pause(self) -> None:
        """Refuse upstream requests with 503 until :meth:`resume`."""
        self._server.pause()

    def resume(self) -> None:
        """Restore normal upstream serving."""
        self._server.resume()

    def expire_all_now(self) -> None:
        """Age every issued JWT past its ``exp``."""
        self._server.expire_all_now()

    def revoke_tokens(self) -> None:
        """Drop every issued JWT."""
        self._server.revoke_tokens()

    def set_extra_claims(self, claims: dict[str, Any]) -> None:
        """Stage extra claims for the next minted JWT."""
        self._server.set_extra_claims(claims)

    def set_auth_mode(self, mode: AuthMode, scope: str = "*") -> None:
        """Set the default auth mode (scope='*') or a per-scope override."""
        self._server.set_auth_mode(mode, scope=scope)

    def set_presigned_ttl(self, seconds: int) -> None:
        """Set the default presigned URL lifetime."""
        self._server.set_presigned_ttl(seconds)

    def set_idempotency_dedup_window(self, seconds: int) -> None:
        """Set the create-response idempotency cache lifetime."""
        self._server.set_idempotency_dedup_window(seconds)

    def set_seed(self, seed: int) -> None:
        """Reseed the failure-injection RNG."""
        self._server.set_seed(seed)

    def received(self) -> list[ReceivedEntry]:
        """Return the token-keyed latest accepted-body view."""
        return self._server.received()

    def upstream_events(self) -> list[UpstreamEvent]:
        """Return the append-only metadata-create/body-PUT event log."""
        return self._server.upstream_events()

    def s3_object(self, bucket: str, key: str) -> S3Object | None:
        """Return the stored S3 object for ``(bucket, key)``, or ``None``.

        The e2e read seam for the SigV4 sink: byte-identity assertions read
        ``stack.emulator.s3_object(bucket, key).body`` instead of reaching
        into :class:`EmulatorState` (which is held on
        :class:`EmulatorServer`, not on :class:`E2EStack`). Mirrors
        :meth:`received` over the path-style store.
        """
        return self._server.state.s3_objects.get((bucket, key))

    def raw_body(self, path: str) -> RawBody | None:
        """Return the stored ``RawBody`` for the forwarded ``path``, or ``None``.

        The e2e read seam for the auth-free ``/raw`` sink — the forward-as-is
        analogue of :meth:`s3_object`. A test reads
        ``stack.emulator.raw_body(path).method`` / ``.body`` instead of reaching
        into :class:`EmulatorState.raw_bodies` directly. The key is the full
        forwarded path (no leading slash, as the sink keys it).
        """
        return self._server.state.raw_bodies.get(path)

    def clear_received(self) -> None:
        """Drop latest accepted bodies and append-only upstream events."""
        self._server.clear_received()

    async def drain(self) -> None:
        """Wait briefly for in-flight requests to settle."""
        await self._server.drain()


async def boot_stack(
    *,
    tmp_path: Path | None = None,
    config_overrides: Mapping[str, Any] | None = None,
    extra_emulators: int = 0,
    extra_emulator_hosts: Sequence[str] | None = None,
    enable_hot_reload: bool = False,
) -> E2EStack:
    """Boot the full E2E stack and return its handle.

    Args:
        tmp_path: Optional override for Phantom's storage data
            directory. When ``None``, a temporary directory is
            created and cleaned up on :meth:`E2EStack.tear_down`.
        config_overrides: Optional mapping deep-merged into the
            ``phantom-config.yml`` base before Phantom boots.
            Per-test config overlay (e.g., saturation caps, retention
            windows, codec mode) for tests that need behavior diverging
            from the suite default. Use ``None`` (the default) to boot
            from the pinned YAML unchanged. A string value of
            ``"{EMULATOR_URL}"`` (optionally with a suffix, e.g.
            ``"{EMULATOR_URL}/raw"``) is rewritten to the live in-process
            emulator base URL before Phantom boots — the seam (TASK 1.3a)
            by which an emulator-targeting route gets the ephemeral
            emulator host:port.
        extra_emulators: Number of additional emulator instances to
            boot, each on its own ephemeral port. Used by tests that
            assert multi-instance dispatch correctness (E2E-20). Their
            URLs are exposed at :attr:`E2EStack.extra_emulator_urls`
            and their typed control wrappers at
            :attr:`E2EStack.extra_emulators`.
        extra_emulator_hosts: Optional per-extra-emulator bind host
            (loopback IP), one entry per ``extra_emulators``. When
            ``None`` (the default) every extra emulator binds
            ``127.0.0.1`` like the primary. The §9.3 hostname-dispatch
            e2e passes distinct loopback IPs (e.g. ``["127.0.0.2"]``)
            so the production no-header dispatcher resolves each instance
            by the URL hostname — the emulator's ``url()`` reports the
            distinct IP, which flows into the chain's first-step URL.
            Must match ``extra_emulators`` in length when provided.
        enable_hot_reload: When ``True``, wires the merged YAML path
            into ``create_app`` so SIGHUP and ``POST /v1/admin/reload``
            both work. :attr:`E2EStack.settings_path` is populated on
            the returned stack; tests rewrite that file via
            :meth:`E2EStack.rewrite_yaml` then fire the reload trigger.
            Defaults to ``False`` so tests that don't exercise hot
            reload keep their existing semantics.

    Returns:
        A fully-running :class:`E2EStack`.

    Raises:
        StackBootError: When boot fails before both servers are ready.
    """
    if extra_emulator_hosts is not None and len(extra_emulator_hosts) != extra_emulators:
        raise StackBootError(
            f"extra_emulator_hosts has {len(extra_emulator_hosts)} entries but "
            f"extra_emulators={extra_emulators}; they must match"
        )
    return await _boot_inprocess(
        tmp_path=tmp_path,
        config_overrides=config_overrides,
        extra_emulators=extra_emulators,
        extra_emulator_hosts=extra_emulator_hosts,
        enable_hot_reload=enable_hot_reload,
    )


def _deep_merge_dict(
    base: dict[str, Any],
    overlay: Mapping[str, Any],
) -> dict[str, Any]:
    """Recursively merge ``overlay`` into ``base`` and return ``base``.

    Mapping values merge key-by-key; non-mapping values from the
    overlay replace the base value (no list concatenation, no
    set union). Lists are overlay-wins because the YAML schema has a
    handful of ordered fields (e.g., ``intervals_seconds``) for which
    elementwise merge produces a nonsense result. The caller passes a
    full replacement list when it wants to change those fields.
    """
    for key, value in overlay.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, Mapping):
            _deep_merge_dict(base[key], value)
        else:
            base[key] = value
    return base


def _substitute_emulator_url(node: Any, emulator_url: str) -> None:
    """Replace the literal ``"{EMULATOR_URL}"`` token in every string leaf in place.

    The token-substitution seam for emulator-targeting e2e (TASK 1.3a): a
    per-test ``config_overrides`` value such as
    ``phantom_default_target = "{EMULATOR_URL}"`` (or ``"{EMULATOR_URL}/raw"``)
    is rewritten to the live, post-allocation emulator base URL after the
    deep-merge and before the merged YAML is serialized. Plain ``str.replace``
    so a suffix (``/raw``, ``/{bucket}/{key}``) concatenates naturally.
    Recurses through dicts and lists; leaves non-string scalars untouched.

    Args:
        node: The merged-YAML subtree to rewrite in place (dict, list, or
            scalar). Non-container, non-string scalars are no-ops.
        emulator_url: The live emulator base URL the token resolves to.
    """
    if isinstance(node, dict):
        for k, v in node.items():
            if isinstance(v, str):
                node[k] = v.replace("{EMULATOR_URL}", emulator_url)
            else:
                _substitute_emulator_url(v, emulator_url)
    elif isinstance(node, list):
        for i, v in enumerate(node):
            if isinstance(v, str):
                node[i] = v.replace("{EMULATOR_URL}", emulator_url)
            else:
                _substitute_emulator_url(v, emulator_url)


async def _boot_inprocess(
    *,
    tmp_path: Path | None,
    config_overrides: Mapping[str, Any] | None = None,
    extra_emulators: int = 0,
    extra_emulator_hosts: Sequence[str] | None = None,
    enable_hot_reload: bool = False,
) -> E2EStack:
    """Boot phantom + emulator as in-process uvicorn servers.

    Both servers bind to ephemeral OS ports on loopback. The instance's
    refresh strategy uses ``wait`` (no AD mint), so the only auth path is
    the bearer header the driver sends on each submit_chain. The
    emulator's HS256 secret is read from
    :data:`ENV_EMULATOR_SIGNING_KEY`; if unset, defaults to
    :data:`DEFAULT_SIGNING_SECRET` for both sides.
    """
    _ensure_env_defaults()
    signing_secret = os.environ[ENV_EMULATOR_SIGNING_KEY]
    tmp_paths: list[Path] = []

    # 1. Boot the primary emulator on an ephemeral port.
    primary_emu = await _boot_one_emulator()
    # Its ephemeral port is already allocated, so the live base URL is
    # known HERE — before _build_phantom_settings runs at step 2. This is
    # the value the `{EMULATOR_URL}` substitution token (TASK 1.3a) resolves
    # to, the seam by which an emulator-targeting route gets the ephemeral
    # host:port that the caller's pre-boot config_overrides dict cannot see
    # (the later `emulator_url = emulator_server.url()` is the same value —
    # emulator_server IS primary_emu — but it is computed after the build).
    primary_emulator_url = primary_emu.url()

    # 1b. Boot any extra emulators for tests that need a second
    #     upstream (E2E-20 multi-instance dispatch). The §9.3
    #     hostname-dispatch e2e binds each extra emulator to a distinct
    #     loopback IP via extra_emulator_hosts so the no-header
    #     dispatcher can resolve instances by URL hostname.
    extra_servers: list[EmulatorServer] = []
    for idx in range(extra_emulators):
        host = (
            extra_emulator_hosts[idx] if extra_emulator_hosts is not None else INPROCESS_BIND_HOST
        )
        extra_servers.append(await _boot_one_emulator(bind_host=host))

    # 2. Build phantom Settings from YAML + per-test overrides.
    phantom_port = _allocate_port(INPROCESS_BIND_HOST)
    data_dir = tmp_path if tmp_path is not None else _make_temp_data_dir()
    if tmp_path is None:
        tmp_paths.append(data_dir)
    settings, settings_path = _build_phantom_settings(
        bind_host=INPROCESS_BIND_HOST,
        bind_port=phantom_port,
        data_dir=data_dir,
        config_overrides=config_overrides,
        emulator_url=primary_emulator_url,  # the live URL for `{EMULATOR_URL}` substitution
    )
    emulator_server = primary_emu
    emulator_url = emulator_server.url()

    # 3. Boot phantom via uvicorn on the pre-allocated port.
    # Hot-reload-enabled tests wire the merged YAML path into create_app
    # so SIGHUP + POST /v1/admin/reload both work. Other tests get the
    # no-reload path so unit-style introspection (app.state) stays
    # minimal.
    # create_app returns ONE FastAPI app serving intake + admin + health on
    # one socket (the same-machine-only single-listener composition); the
    # harness serves that app directly.
    phantom_app = phantom_create_app(
        settings,
        settings_path=settings_path if enable_hot_reload else None,
    )
    # TLS: when a test sets ``server.tls.enabled`` via config_overrides, the
    # ONE listener serves HTTPS — mirroring production's ``__main__.py`` shape
    # (``resolve_tls_paths`` -> ssl_certfile/ssl_keyfile; both ``None`` when off,
    # so the plaintext path is byte-identical). Auto-gen mode (no cert/key paths)
    # needs no ssl_keyfile_password. Passed as explicit (``None``-defaulted)
    # ``uvicorn.Config`` args rather than a splat so the types check cleanly.
    tls_enabled = settings.server.tls.enabled
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None
    if tls_enabled:
        ssl_certfile, ssl_keyfile = resolve_tls_paths(settings.server.tls, str(data_dir))
    uv_config = uvicorn.Config(
        app=phantom_app,
        host=INPROCESS_BIND_HOST,
        port=phantom_port,
        log_level=settings.observability.log_level.lower(),
        lifespan="on",
        access_log=False,
        ssl_certfile=ssl_certfile,
        ssl_keyfile=ssl_keyfile,
    )
    phantom_uv_server = uvicorn.Server(config=uv_config)
    phantom_serve_task = asyncio.create_task(phantom_uv_server.serve())
    await _await_uvicorn_started(phantom_uv_server, phantom_serve_task, "phantom")

    # Reach the listener over HTTPS when TLS is enabled (else plaintext, as before).
    scheme = "https" if tls_enabled else "http"
    phantom_url = f"{scheme}://{INPROCESS_BIND_HOST}:{phantom_port}"

    # 4. Open the SDK client. One PhantomClient covers both intake and admin
    #    (they ride the same single listener). The auto-gen TLS cert is
    #    self-signed, so inject a real httpx transport with ``verify=False`` —
    #    a test-harness seam only (the SDK needs no HTTPS field; ``phantom_url``
    #    already accepts ``https://``).
    client_transport = httpx.AsyncHTTPTransport(verify=False) if tls_enabled else None
    phantom_client = PhantomClient(phantom_url, transport=client_transport)
    await phantom_client.__aenter__()

    emulator = EmulatorControl(server=emulator_server)
    extra_controls = [EmulatorControl(server=s) for s in extra_servers]
    extra_urls = [s.url() for s in extra_servers]

    logger.info(
        "E2E stack ready (inprocess): phantom=%s emulator=%s extras=%s data_dir=%s",
        phantom_url,
        emulator_url,
        extra_urls,
        data_dir,
    )

    return E2EStack(
        phantom_url=phantom_url,
        phantom_admin_url=phantom_url,
        emulator_url=emulator_url,
        phantom_client=phantom_client,
        emulator=emulator,
        signing_secret=signing_secret,
        data_dir=data_dir,
        settings=settings,
        extra_emulator_urls=extra_urls,
        extra_emulators=extra_controls,
        settings_path=settings_path if enable_hot_reload else None,
        _phantom_uv_server=phantom_uv_server,
        _phantom_serve_task=phantom_serve_task,
        _emulator_server=emulator_server,
        _extra_emulator_servers=extra_servers,
        _tmp_paths=tmp_paths,
    )


async def _boot_one_emulator(*, bind_host: str = INPROCESS_BIND_HOST) -> EmulatorServer:
    """Boot one phantom-emulator on a freshly allocated port.

    Used both by the primary boot path and by tests requesting
    additional emulators (E2E-20 multi-instance dispatch).

    Args:
        bind_host: Loopback address to bind to. Defaults to
            ``127.0.0.1``. The §9.3 hostname-dispatch e2e binds extra
            emulators to a DISTINCT loopback IP (e.g. ``127.0.0.2``) so
            the production no-header dispatcher can resolve them by
            hostname — the emulator's ``url()`` then reports that IP,
            which flows into the chain's first-step URL.

    Returns:
        A started :class:`EmulatorServer` whose ``url()`` is
        immediately reachable on ``bind_host``.
    """
    emu_port = _allocate_port(bind_host)
    emu_cfg = load_emulator_config(EMULATOR_CONFIG_PATH)
    emu_cfg.server.host = bind_host
    emu_cfg.server.port = emu_port
    emu_cfg.auth.issuer = f"http://{bind_host}:{emu_port}"

    emulator_app = emulator_create_app(emu_cfg)
    emulator_state: EmulatorState = emulator_app.state.emulator_state

    emu_uv_config = uvicorn.Config(
        app=emulator_app,
        # Bind uvicorn to the requested loopback IP — NOT a hardcoded
        # 127.0.0.1. The port is allocated on `bind_host`, `url()` reports
        # `bind_host`, and the §9.3 hostname-dispatch e2e binds extras to a
        # distinct IP (127.0.0.2); using INPROCESS_BIND_HOST here left uvicorn
        # listening on 127.0.0.1 while the advertised URL said 127.0.0.2, so
        # the forward to the "staging" emulator hit a dead address. For the
        # default path bind_host == INPROCESS_BIND_HOST, so this is a no-op.
        host=bind_host,
        port=emu_port,
        log_level=emu_cfg.logging.level.lower(),
        lifespan="on",
        access_log=False,
    )
    emu_uv_server = uvicorn.Server(config=emu_uv_config)
    emu_serve_task = asyncio.create_task(emu_uv_server.serve())
    await _await_uvicorn_started(emu_uv_server, emu_serve_task, "emulator")

    return EmulatorServer(
        config=emu_cfg,
        state=emulator_state,
        uv_server=emu_uv_server,
        serve_task=emu_serve_task,
        port=emu_port,
    )


def _ensure_env_defaults() -> None:
    """Populate the HS256 signing-secret env var when unset.

    Tests that depend on a specific secret value override the env var
    themselves; this just guarantees the suite boots when no env var
    is provided.

    The AD client-secret env vars (``PHANTOM_AD_PRIMARY`` /
    ``PHANTOM_AD_SECONDARY``) are deliberately removed if present —
    Phantom's Pydantic Settings uses ``PHANTOM_`` as the env prefix
    and ``__`` as the nested delimiter, so a naked ``PHANTOM_AD_PRIMARY``
    is interpreted as top-level field ``ad_primary`` (and rejected
    with ``extra="forbid"``). The smoke test uses the ``wait`` refresh
    strategy, which doesn't mint, so no AD secret is needed. Tests
    that exercise ``ad_client_credentials`` should use env-var names
    that don't start with ``PHANTOM_`` (e.g., ``E2E_AD_PRIMARY``) and
    reference them from a per-test config.
    """
    os.environ.setdefault(ENV_EMULATOR_SIGNING_KEY, DEFAULT_SIGNING_SECRET)
    os.environ.pop(ENV_PHANTOM_AD_PRIMARY, None)
    os.environ.pop(ENV_PHANTOM_AD_SECONDARY, None)


def _allocate_port(host: str) -> int:
    """Allocate an ephemeral OS port bound briefly to ``host``.

    Mirrors the pattern in :mod:`phantom_emulator.server`. A short
    SO_REUSEADDR window remains between this allocation and uvicorn's
    bind — acceptable for tests on a single host.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind((host, 0))
        return int(s.getsockname()[1])
    finally:
        s.close()


def _make_temp_data_dir() -> Path:
    """Allocate a temp directory for Phantom's storage tree."""
    import tempfile

    return Path(tempfile.mkdtemp(prefix="phantom-e2e-"))


def _build_phantom_settings(
    *,
    bind_host: str,
    bind_port: int,
    data_dir: Path,
    config_overrides: Mapping[str, Any] | None = None,
    emulator_url: str | None = None,
) -> tuple[Settings, Path]:
    """Build a phantom :class:`Settings` from YAML with runtime overrides.

    Reads ``phantom-config.yml``, applies the bind-host / port / data-
    dir overrides for the live inprocess run, deep-merges any
    per-test ``config_overrides``, then routes through
    :func:`load_settings`. The YAML on disk is never modified — every
    overlay lands on an in-memory copy serialized to a temp file. The
    temp file's path is returned alongside the validated Settings so
    callers (specifically the hot-reload-enabled boot path) can wire
    it into ``create_app(..., settings_path=...)``.

    Args:
        bind_host: Phantom's bind host (loopback in inprocess mode).
        bind_port: Pre-allocated TCP port for Phantom.
        data_dir: Storage data directory.
        config_overrides: Optional deep-merge overlay applied after the
            host / port / data_dir mutations. Per-test config customization
            (saturation caps, retention windows, codec mode, etc.) lives
            here so the base YAML stays the canonical default.
        emulator_url: The live in-process emulator base URL (host:port).
            When provided, the literal ``"{EMULATOR_URL}"`` token in every
            merged-YAML string leaf is rewritten to this value AFTER the
            deep-merge and BEFORE serialization (TASK 1.3a) — the seam by
            which an emulator-targeting route gets the ephemeral host:port.
            ``None`` (the default) leaves the merged YAML untouched.

    Returns:
        ``(settings, merged_path)``. ``merged_path`` is the on-disk
        location of the temp YAML; it remains readable for the lifetime
        of the test (the tempfile is created with ``delete=False`` and
        cleaned up only when the OS reclaims its tmp tree).
    """
    raw = yaml.safe_load(PHANTOM_CONFIG_PATH.read_text())
    if not isinstance(raw, dict):
        raise StackBootError(f"phantom-config.yml root must be a mapping, got {type(raw).__name__}")
    # One listener serves intake + admin + health on the ephemeral port.
    raw.setdefault("server", {})["bind_tcp"] = f"{bind_host}:{bind_port}"
    raw.setdefault("storage", {})["data_dir"] = str(data_dir)
    if config_overrides is not None:
        _deep_merge_dict(raw, config_overrides)
    # Resolve the `{EMULATOR_URL}` token (TASK 1.3a) on the post-merge tree so
    # an emulator-targeting overlay value (e.g. phantom_default_target =
    # "{EMULATOR_URL}/raw") points at the live ephemeral emulator. Runs before
    # serialization so load_settings sees the resolved URL; _pin_probe_defaults
    # touches only numeric fields, which never carry the token.
    if emulator_url is not None:
        _substitute_emulator_url(raw, emulator_url)
    # Write to a tmp file so load_settings runs the full validator path.
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False, encoding="utf-8") as f:
        yaml.safe_dump(raw, f)
        merged_path = Path(f.name)
    settings = load_settings(merged_path)
    # Pin every probe-derived value back into the on-disk YAML. Since
    # R7-1 the real reload re-runs the probe, so unpinned fields would
    # re-resolve fine; pinning keeps every hot-reload test's capacity
    # values byte-stable across rewrites (machine facts cannot drift a
    # derived knob mid-test) regardless of which block the test rewrites.
    _pin_probe_defaults(raw, settings)
    merged_path.write_text(yaml.safe_dump(raw))
    return settings, merged_path


def _pin_probe_defaults(raw: dict[str, Any], settings: Settings) -> None:
    """Write every probe-resolved default back into ``raw`` in place.

    Mirrors :meth:`Settings._resolve_defaults` field-by-field: any field
    the probe filled is mirrored back into the raw YAML so a later
    reload rebuilds an identical Settings whatever the machine facts do
    mid-test (the real reload re-probes since R7-1; pinned values win).

    No-op for fields the operator already pinned (the validator wrote
    the operator-supplied value over its probe candidate).
    """
    sat = raw.setdefault("saturation", {})
    sat["max_in_flight"] = settings.saturation.max_in_flight
    sat["max_in_flight_bytes"] = settings.saturation.max_in_flight_bytes
    sat["max_disk_bytes"] = settings.saturation.max_disk_bytes
    sat["large_body_threshold_bytes"] = settings.saturation.large_body_threshold_bytes
    sat["max_large_in_flight"] = settings.saturation.max_large_in_flight
    storage = raw.setdefault("storage", {})
    # Phase 1 Slice 1.A: in_memory_max_bytes renamed to body_store.ram_ceiling_bytes
    # and lives under the new body_store: block.
    body_store = storage.setdefault("body_store", {})
    body_store["ram_ceiling_bytes"] = settings.storage.body_store.ram_ceiling_bytes
    persist_trigger = storage.setdefault("persist_trigger", {})
    persist_trigger["body_size_threshold_bytes"] = (
        settings.storage.persist_trigger.body_size_threshold_bytes
    )
    retry = raw.setdefault("retry", {})
    retry["worker_count"] = settings.retry.worker_count


async def _await_uvicorn_started(
    server: uvicorn.Server,
    serve_task: asyncio.Task[None],
    name: str,
) -> None:
    """Block until ``server`` reports ``started`` or the timeout fires."""
    deadline = asyncio.get_event_loop().time() + SERVER_STARTUP_TIMEOUT_SECONDS
    while not server.started:
        if asyncio.get_event_loop().time() > deadline:
            server.should_exit = True
            await serve_task
            raise StackBootError(f"{name} failed to start within {SERVER_STARTUP_TIMEOUT_SECONDS}s")
        await asyncio.sleep(SERVER_STARTUP_POLL_INTERVAL_SECONDS)  # pre-commit-allow: sleep


def _hostname_of(url: str) -> str:
    """Return the lowercase hostname of ``url``."""
    parsed = urlparse(url)
    return (parsed.hostname or url).lower()
