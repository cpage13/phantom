"""Automatic public-SDK UDS path over the production listener (audit T6 / G9).

The service has always advertised the Unix-domain-socket deployment
(``server.bind_uds``; the connections table documents the client side as
``unix:/path``), but no E2E ever bound it, and the public
``PhantomClient("unix:/path")`` bare-string form was unusable until the
``unix:`` transport routing landed in ``phantom_client.transport`` (before
that, the bare string fell through to httpx as an unsupported URL scheme).
This module is the end-to-end proof of the advertised contract over the real
``python -m phantom`` listener.

What it proves:

* The CLI boots with ``server.bind_uds`` and serves health, intake, and admin
  on the one UDS listener. Readiness is observed through a TEST-owned
  ``httpx.AsyncHTTPTransport(uds=...)`` (the fixture half); the behavioral
  half constructs ONLY the public bare-string client.
* UDS precedence: the config's ``bind_tcp`` port is never opened. The proof is
  attributed to the child pid via ``psutil.Process(pid).net_connections`` (no
  TCP LISTEN entries), not by probing an unrelated free port.
* The dependency contract is pinned: uvicorn's ``Config.bind_socket`` creates
  the socket path owned by the child's effective UID and chmods it ``0o666``.
  If Phantom ever intentionally hardens that mode, the documented
  access-control contract and this oracle must change together.
* A missing socket maps to ``PhantomConnectError`` at the public client, never
  an unsupported-protocol error (the parse itself is unit-pinned in
  ``src/phantom-client/tests/unit/test_transport.py``).

Platform: POSIX-only by explicit mark (UDS has no Windows analogue here).
The socket path deliberately lives under a fresh ``tempfile.mkdtemp()`` dir,
NOT pytest's ``tmp_path``: macOS ``sun_path`` caps at 104 bytes and this
runner's pytest tmp roots already run ~100 characters deep.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
import stat
import tempfile
import time
from pathlib import Path
from uuid import uuid4

import httpx
import psutil
import pytest
from phantom_client import PhantomClient, PhantomConnectError

from tests.e2e._harness.subprocess_harness import (
    _HEALTH_POLL_RETRYABLE_ERRORS,
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    fake_security_token,
    submit_one,
    write_phantom_config,
)
from tests.e2e.helpers.assertions import assert_chain_reaches_state

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.e2e,
    pytest.mark.skipif(os.name != "posix", reason="UDS requires a POSIX socket namespace"),
]

# High bytes included so the delivered body also exercises byte-transparency.
_PAYLOAD = b"phantom-t6-uds-public-client-body\x00\xff\xfe-byte-identity"
# Budget for the buffered chain to reach terminal success after submission.
_SUCCEEDED_BUDGET_SECONDS = 20.0
# Budget for the UDS listener to become healthy after spawn.
_HEALTH_BUDGET_SECONDS = 30.0
# macOS sun_path is 104 bytes (Linux 108); stay comfortably below.
_SUN_PATH_SAFE_BYTES = 100
# The uvicorn bind_socket contract this module pins (see module docstring).
_UDS_SOCKET_MODE = 0o666
# Synthetic authority for the fixture-owned UDS health client (the UDS
# transport owns routing; the host token is never resolved).
_FIXTURE_BASE_URL = "http://phantom"


def _short_socket_dir() -> Path:
    """Create a socket dir under the SYSTEM tempdir (short enough for sun_path)."""
    return Path(tempfile.mkdtemp(prefix="phantom-uds-"))


async def _await_uds_health(
    proc: PhantomSubprocess, sock_path: Path, *, budget_seconds: float
) -> None:
    """Poll ``/v1/healthz`` over a test-owned UDS transport until 200.

    The fixture half of the audit's lane: readiness must not depend on the
    public client under test. Raises on child early-exit or budget expiry.
    """
    deadline = time.monotonic() + budget_seconds
    transport = httpx.AsyncHTTPTransport(uds=str(sock_path))
    async with httpx.AsyncClient(
        transport=transport, base_url=_FIXTURE_BASE_URL, timeout=2.0
    ) as client:
        while time.monotonic() < deadline:
            if proc.returncode is not None:
                raise RuntimeError(
                    f"phantom subprocess exited early (code={proc.returncode}); "
                    f"log:\n{proc.read_full_log()}"
                )
            if sock_path.exists():
                try:
                    response = await client.get("/v1/healthz")
                    if response.status_code == 200:
                        return
                except _HEALTH_POLL_RETRYABLE_ERRORS:
                    pass
            await asyncio.sleep(0.1)  # pre-commit-allow: sleep
    raise RuntimeError(
        f"phantom UDS listener did not become healthy within {budget_seconds}s; "
        f"log:\n{proc.read_full_log()}"
    )


async def test_public_bare_string_client_over_production_uds_listener(
    tmp_path: Path,
) -> None:
    """The advertised ``PhantomClient("unix:/path")`` contract works end to end.

    Objective: boot the real CLI on ``server.bind_uds``, prove the socket
    ownership/mode dependency contract, prove NO TCP listener exists on the
    child pid (UDS precedence), then drive submission and admin polling
    through ONLY the public bare-string client and require byte-identical
    emulator delivery.
    """
    data_dir = tmp_path / "data"
    socket_dir = _short_socket_dir()
    sock_path = socket_dir / "u.sock"
    assert len(str(sock_path).encode()) <= _SUN_PATH_SAFE_BYTES, (
        f"socket path too long for portable sun_path: {sock_path}"
    )

    emulator = await boot_emulator()
    tcp_port = allocate_port()
    config_path = write_phantom_config(
        data_dir=data_dir,
        bind_port=tcp_port,
        config_overrides={"server": {"bind_uds": str(sock_path)}},
    )
    proc = PhantomSubprocess.make(config_path, tcp_port)
    try:
        # spawn() (not start()): the TCP-shaped health poll cannot reach a
        # UDS listener; readiness is owned by the fixture transport below.
        proc.spawn(label=f"t6-uds config={proc.config_path}")
        await _await_uds_health(proc, sock_path, budget_seconds=_HEALTH_BUDGET_SECONDS)

        # Dependency contract pin: uvicorn.Config.bind_socket creates the
        # path owned by the child euid (same-user test session) and chmods
        # it 0o666. A deliberate future hardening must update the documented
        # access-control contract and this oracle together.
        socket_stat = os.stat(sock_path)
        assert stat.S_ISSOCK(socket_stat.st_mode), f"{sock_path} is not a socket"
        assert socket_stat.st_uid == os.geteuid(), (
            "UDS socket is not owned by the child's effective UID"
        )
        assert stat.S_IMODE(socket_stat.st_mode) == _UDS_SOCKET_MODE, (
            f"UDS socket mode {oct(stat.S_IMODE(socket_stat.st_mode))} != "
            f"{oct(_UDS_SOCKET_MODE)} (uvicorn bind_socket contract drifted)"
        )

        # UDS precedence: the configured bind_tcp port was never opened.
        # Attributed to the child pid, not an unrelated free-port probe.
        assert proc.pid is not None
        tcp_listens = [
            conn
            for conn in psutil.Process(proc.pid).net_connections(kind="tcp")
            if conn.status == psutil.CONN_LISTEN
        ]
        assert tcp_listens == [], f"UDS-configured child holds TCP LISTEN sockets: {tcp_listens}"

        # Behavioral half: ONLY the public bare-string client from here on.
        bearer = fake_security_token(emulator)
        chain_id = uuid4()
        async with PhantomClient(f"unix:{sock_path}") as client:
            await submit_one(
                client,
                emulator_url=emulator.url,
                bearer=bearer,
                body=_PAYLOAD,
                chain_id=chain_id,
            )
            detail = await assert_chain_reaches_state(
                client,
                chain_id,
                state="succeeded",
                timeout_seconds=_SUCCEEDED_BUDGET_SECONDS,
            )
        assert detail.state == "succeeded"

        # Upstream delivery: exactly one accepted body, ours, byte-identical
        # (ReceivedEntry.body_hash is the sink-side SHA-256 of the raw bytes).
        received = emulator.received()
        assert len(received) == 1, (
            f"expected exactly one accepted upstream body, got {len(received)}"
        )
        entry = received[0]
        assert entry.metadata_kvs.get("phantom_local_uuid") == str(chain_id), (
            "accepted upstream body does not correlate to the submitted chain"
        )
        assert entry.body_hash == hashlib.sha256(_PAYLOAD).hexdigest(), (
            "byte round-trip broke over the UDS listener"
        )
    finally:
        proc.terminate()
        shutil.rmtree(socket_dir, ignore_errors=True)
        await emulator.stop()


async def test_missing_socket_maps_to_connect_error(tmp_path: Path) -> None:
    """A ``unix:`` URL whose socket does not exist raises ``PhantomConnectError``.

    Objective: the public-client half of the audit's typed-failure
    requirement. No subprocess is booted; the socket path simply does not
    exist, and the failure must be the same typed CONNECT error a refused
    TCP port produces (the scheme-parse half is unit-pinned in the client
    suite).
    """
    absent = tmp_path / "absent-phantom.sock"
    async with PhantomClient(f"unix:{absent}") as client:
        with pytest.raises(PhantomConnectError):
            await client.get_upload(uuid4())
