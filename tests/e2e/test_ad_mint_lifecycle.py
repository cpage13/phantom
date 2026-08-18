"""Autonomous AD-mint FULL lifecycle through the real subprocess stack (audit T3).

The T3 mechanism guard (:mod:`tests.e2e.test_ad_mint_real_transport`) proved
the minter's OAuth wire and cache write in isolation. This module is the
deferred lifecycle arm: the real ``python -m phantom`` child, configured with
an ``ad_mint`` block and NO pushed token, parks a submitted row for auth,
autonomously mints against a trusted-HTTPS authority (invalid primary secret,
valid secondary), wakes the row through the bearer kicker, delivers, refreshes to
a DIFFERENT token, and delivers again — plus the two failure-posture arms the
audit names (fail-fast supervision with an empty outage schedule; bounded
retry without false success with a schedule).

Topology: TWO in-process emulators.

* The AUTHORITY serves HTTPS (azure-identity refuses plaintext authorities)
  with the tenant token alias, the closed AUTH_TOKEN gate (installed before
  the child boots, so the eager minter cannot win the race against the
  park-first phase), the secret-to-slot map, and the ordered mint-attempt
  ledger (safe slot tags only; never a secret).
* The UPSTREAM serves plaintext for the chain steps (no TLS constraint on
  that hop); its JWT issuer string is aligned to the authority's https origin
  so tokens minted THERE validate HERE (same HS256 signing secret).

Trust: the child receives ONLY ``SSL_CERT_FILE`` / ``REQUESTS_CA_BUNDLE``
pointing at the test certificate. Parent and global trust stores are never
touched. The mint secrets ride child-only env names (never ``PHANTOM_``-
prefixed: pydantic-settings would parse those as Settings fields and abort
boot). Token-cache reads select non-secret provenance columns plus the bearer
ONLY for a value-suppressed inequality (the refresh proof); no secret or
token value is ever asserted against a literal or logged.
"""

from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_emulator.config import AuthClient
from phantom_emulator.state import AuthTokenGate

from tests.e2e._driver import build_in_memory_upload_envelope
from tests.e2e._harness.subprocess_harness import (
    DEFAULT_SUB,
    EmulatorHandle,
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    write_phantom_config,
)
from tests.e2e.helpers.payloads import build_create_file_request
from tests.e2e.helpers.timing import await_until
from tests.e2e.test_ad_mint_real_transport import _write_temp_cert

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]

_TENANT = "t3-lifecycle-tenant"
_CLIENT_ID = "t3-lc-client"
_SCOPE = "api://phantom-t3-lifecycle/.default"
# Child-only env names for the two synthetic secrets. NEVER PHANTOM_-prefixed.
_PRIMARY_ENV = "T3_LC_PRIMARY_SECRET"
_SECONDARY_ENV = "T3_LC_SECONDARY_SECRET"
_BAD_SECRET = "t3-lc-primary-secret-WRONG-4a1f"
_GOOD_SECRET = "t3-lc-secondary-secret-CORRECT-9c2e"

_PAYLOAD_FIRST = b"phantom-t3-lifecycle-first\x00\xff\xfe-parked-then-minted"
_PAYLOAD_SECOND = b"phantom-t3-lifecycle-second\x00\xfd-on-refreshed-token"

# Short token lifetime + early refresh so the refresh arm runs in seconds.
_TOKEN_LIFETIME_SECONDS = 8
_REFRESH_BEFORE_SECONDS = 6

_PARK_BUDGET_SECONDS = 20.0
_MINT_BUDGET_SECONDS = 20.0
_SUCCEEDED_BUDGET_SECONDS = 30.0
_EXIT_BUDGET_SECONDS = 30.0
_BOUNDED_ATTEMPTS_BUDGET_SECONDS = 30.0


def _ad_mint_overrides(
    upstream_url: str, authority_url: str, *, outage_retry: list[int]
) -> dict[str, object]:
    """The full instance block (lists replace wholesale) plus the ad_mint arm."""
    hosts = ["emulator", "127.0.0.1", "localhost"]
    return {
        "instances": [
            {
                "id": "primary",
                "host_prefixes": hosts,
                "data_dir": "primary",
                "capture_reexecution": False,
                "routes": [{"name": "emulator", "hosts": hosts, "auth_mode": "phantom_bearer"}],
                "ad_mint": {
                    "tenant_id": _TENANT,
                    "client_id": _CLIENT_ID,
                    "primary_client_secret_env": _PRIMARY_ENV,
                    "secondary_client_secret_env": _SECONDARY_ENV,
                    "authority_url": authority_url,
                    "scope": _SCOPE,
                    "refresh_seconds_before_expiry": _REFRESH_BEFORE_SECONDS,
                    "refresh_jitter_seconds": 0.0,
                    "ad_outage_retry_seconds": outage_retry,
                    # The cache slot the minter writes MUST be the slot the
                    # sender reads for the row: endpoint is the upstream URL's
                    # hostname; uid is the submission's X-Phantom-Uid.
                    "endpoint": "127.0.0.1",
                    "uid": DEFAULT_SUB,
                },
            }
        ],
    }


async def _boot_authority(tmp_path: Path) -> tuple[EmulatorHandle, Path]:
    """Boot the HTTPS authority emulator, armed for the lifecycle scenario."""
    cert_dir = tmp_path / "authority-cert"
    cert_dir.mkdir()
    cert_path, key_path = _write_temp_cert(cert_dir)
    authority = await boot_emulator(tls=(str(cert_path), str(key_path)))
    authority.server.config.auth.clients.append(
        AuthClient(client_id=_CLIENT_ID, client_secret=_GOOD_SECRET)
    )
    authority.server.config.auth.default_expires_in_seconds = _TOKEN_LIFETIME_SECONDS
    authority.server.state.mint_slot_secrets = {
        "primary": _BAD_SECRET,
        "secondary": _GOOD_SECRET,
    }
    return authority, cert_path


def _child_env(cert_path: Path) -> dict[str, str]:
    """Child-only trust + secret env (parent never mutated)."""
    return {
        "SSL_CERT_FILE": str(cert_path),
        "REQUESTS_CA_BUNDLE": str(cert_path),
        _PRIMARY_ENV: _BAD_SECRET,
        _SECONDARY_ENV: _GOOD_SECRET,
    }


async def _submit_unauthenticated(
    client: PhantomClient, upstream_url: str, chain_id: UUID, body: bytes
) -> None:
    """Submit one chain WITHOUT any Authorization (the autonomous-mint posture)."""
    request = build_create_file_request(file_name=f"t3lc-{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=upstream_url,
        local_uuid=chain_id,
    )
    await client.submit_chain(envelope, body_refs={"body": body}, uid=DEFAULT_SUB, auth_token=None)


def _token_cache_row(data_dir: Path) -> tuple[str, str, str] | None:
    """Read the (source, status, bearer) of the minter's slot.

    The bearer is read ONLY for a value-suppressed inequality comparison in
    the refresh proof; it is never asserted against a literal or printed.
    """
    database = data_dir / "primary" / "token_cache.db"
    if not database.exists():
        return None
    with closing(sqlite3.connect(database)) as connection:
        row = connection.execute(
            "SELECT source, status, bearer FROM token_cache WHERE endpoint = ? AND uid = ?",
            ("127.0.0.1", DEFAULT_SUB),
        ).fetchone()
    if row is None:
        return None
    return (str(row[0]), str(row[1]), str(row[2]))


def _mint_shape(authority: EmulatorHandle) -> list[tuple[str, int]]:
    """The ordered (slot, status) view of the authority's attempt ledger."""
    return [(a.slot, a.status) for a in authority.server.state.mint_attempts]


async def test_park_mint_wake_deliver_refresh_deliver(tmp_path: Path) -> None:
    """The full autonomous lifecycle, park-first, with a refresh second act.

    Objective: an unauthenticated submission parks (no token anywhere, no
    push); releasing the authority gate lets the real minter run
    rejected-primary-then-accepted-secondary; the cache slot appears with
    source=plugin_mint; the bearer kicker wakes the row and it delivers; the
    refresh cycle mints a DIFFERENT token and a second upload rides it.
    """
    authority, cert_path = await _boot_authority(tmp_path)
    gate = AuthTokenGate()
    authority.server.state.auth_token_gate = gate
    upstream = await boot_emulator()
    # Tokens are minted by the AUTHORITY: align the upstream's expected
    # issuer to the authority's https origin (same HS256 signing secret).
    upstream.server.config.auth.issuer = authority.server.config.auth.issuer

    data_dir = tmp_path / "data"
    port = allocate_port()
    config_path = write_phantom_config(
        data_dir=data_dir,
        bind_port=port,
        config_overrides=_ad_mint_overrides(upstream.url, authority.url, outage_retry=[]),
    )
    proc = PhantomSubprocess.make(config_path, port, env_overrides=_child_env(cert_path))
    try:
        await proc.start()
        first_chain = uuid4()
        async with PhantomClient(proc.url) as client:
            await _submit_unauthenticated(client, upstream.url, first_chain, _PAYLOAD_FIRST)

            # Park-first: the row must reach auth_expired while the gate still
            # holds the authority (no mint has completed).
            async def _parked() -> bool:
                detail = await client.get_upload(first_chain)
                return str(detail.state) == "auth_expired"

            await await_until(_parked, timeout_seconds=_PARK_BUDGET_SECONDS)
            detail = await client.get_upload(first_chain)
            assert detail.last_error == "auth_401"
            assert _mint_shape(authority) == [], "a mint completed before the gate released"
            assert _token_cache_row(data_dir) is None, "a token appeared before any mint"

            # Release: the held first mint proceeds. Primary rejected,
            # secondary accepted — the exact ordered wire the audit demands.
            gate.release.set()

            async def _first_mint_done() -> bool:
                return len(authority.server.state.mint_attempts) >= 2

            await await_until(_first_mint_done, timeout_seconds=_MINT_BUDGET_SECONDS)
            assert _mint_shape(authority)[:2] == [("primary", 401), ("secondary", 200)]

            cache_row = _token_cache_row(data_dir)
            assert cache_row is not None, "the accepted mint did not write the cache slot"
            first_source, first_status, first_bearer = cache_row
            assert (first_source, first_status) == ("plugin_mint", "fresh"), (
                f"unexpected slot provenance: source={first_source} status={first_status}"
            )

            # Wake + deliver: the kicker re-queues the parked row on the
            # cache write; delivery lands byte-identical at the upstream.
            async def _first_delivered() -> bool:
                current = await client.get_upload(first_chain)
                return str(current.state) == "succeeded"

            await await_until(_first_delivered, timeout_seconds=_SUCCEEDED_BUDGET_SECONDS)
            received = upstream.received()
            assert len(received) == 1
            assert received[0].metadata_kvs.get("phantom_local_uuid") == str(first_chain)
            assert received[0].body_hash == hashlib.sha256(_PAYLOAD_FIRST).hexdigest()

            # Refresh act: the short lifetime forces a second mint cycle
            # (primary rejected again, secondary accepted again) and the
            # cached bearer must CHANGE (compared, never printed).
            async def _refreshed() -> bool:
                shape = _mint_shape(authority)
                return shape.count(("secondary", 200)) >= 2

            await await_until(_refreshed, timeout_seconds=_MINT_BUDGET_SECONDS)

            async def _bearer_rotated() -> bool:
                row = _token_cache_row(data_dir)
                return row is not None and row[2] != first_bearer

            await await_until(_bearer_rotated, timeout_seconds=_MINT_BUDGET_SECONDS)

            second_chain = uuid4()
            await _submit_unauthenticated(client, upstream.url, second_chain, _PAYLOAD_SECOND)

            async def _second_delivered() -> bool:
                current = await client.get_upload(second_chain)
                return str(current.state) == "succeeded"

            await await_until(_second_delivered, timeout_seconds=_SUCCEEDED_BUDGET_SECONDS)
            received = upstream.received()
            assert len(received) == 2
            hashes = {e.body_hash for e in received}
            assert hashlib.sha256(_PAYLOAD_SECOND).hexdigest() in hashes

        # No secret ever reaches the child log (the audit's no-leak rule).
        log_text = proc.read_full_log()
        assert _BAD_SECRET not in log_text
        assert _GOOD_SECRET not in log_text
    finally:
        gate.release.set()
        proc.terminate()
        await upstream.stop()
        await authority.stop()


async def test_both_secrets_invalid_fail_fast_supervision(tmp_path: Path) -> None:
    """Empty outage schedule + both secrets invalid kills the child (fail-fast).

    Objective: the documented supervision outcome. AuthUnavailableError
    escapes the minter, the composition root's TaskGroup aborts, and the CLI
    fatal-worker bridge terminates the process non-zero. No secret in the
    log; the authority saw only rejected attempts.
    """
    authority, cert_path = await _boot_authority(tmp_path)
    upstream = await boot_emulator()
    upstream.server.config.auth.issuer = authority.server.config.auth.issuer

    child_env = _child_env(cert_path)
    child_env[_SECONDARY_ENV] = "t3-lc-secondary-also-WRONG-7d3b"

    data_dir = tmp_path / "data"
    port = allocate_port()
    config_path = write_phantom_config(
        data_dir=data_dir,
        bind_port=port,
        config_overrides=_ad_mint_overrides(upstream.url, authority.url, outage_retry=[]),
    )
    proc = PhantomSubprocess.make(config_path, port, env_overrides=child_env)
    try:
        # spawn() (not start()): the eager minter can kill the child before
        # health ever answers; the expected exit IS the assertion.
        proc.spawn(label=f"t3-fail-fast config={proc.config_path}")
        returncode = await proc.wait_for_expected_exit(timeout_seconds=_EXIT_BUDGET_SECONDS)
        assert returncode != 0

        shape = _mint_shape(authority)
        assert shape, "the child died without ever reaching the authority"
        assert all(status == 401 for _slot, status in shape), (
            "an attempt unexpectedly succeeded in the fail-fast arm"
        )
        assert _token_cache_row(data_dir) is None, "a token was cached despite double rejection"

        log_text = proc.read_full_log()
        assert "supervised worker failed" in log_text, (
            "the CLI fatal-worker bridge did not report the supervision path"
        )
        assert _BAD_SECRET not in log_text
        assert _GOOD_SECRET not in log_text
    finally:
        proc.terminate()
        await upstream.stop()
        await authority.stop()


async def test_both_secrets_invalid_bounded_retry_no_false_success(tmp_path: Path) -> None:
    """A retry schedule keeps the child alive, retrying without false success.

    Objective: with ad_outage_retry_seconds=[1, 1] and both secrets invalid,
    the minter retries on the schedule (ledger keeps growing, every attempt
    rejected), the process stays healthy, and no token is ever cached.
    """
    authority, cert_path = await _boot_authority(tmp_path)
    upstream = await boot_emulator()
    upstream.server.config.auth.issuer = authority.server.config.auth.issuer

    child_env = _child_env(cert_path)
    child_env[_SECONDARY_ENV] = "t3-lc-secondary-also-WRONG-7d3b"

    data_dir = tmp_path / "data"
    port = allocate_port()
    config_path = write_phantom_config(
        data_dir=data_dir,
        bind_port=port,
        config_overrides=_ad_mint_overrides(upstream.url, authority.url, outage_retry=[1, 1]),
    )
    proc = PhantomSubprocess.make(config_path, port, env_overrides=child_env)
    try:
        await proc.start()

        # Two full outage cycles (primary+secondary rejected each time).
        async def _two_cycles() -> bool:
            return len(authority.server.state.mint_attempts) >= 4

        await await_until(_two_cycles, timeout_seconds=_BOUNDED_ATTEMPTS_BUDGET_SECONDS)
        assert all(status == 401 for _slot, status in _mint_shape(authority))
        assert proc.returncode is None, "the child died despite a retry schedule"
        assert _token_cache_row(data_dir) is None, "a token was cached from rejected mints"

        log_text = proc.read_full_log()
        assert _BAD_SECRET not in log_text
        assert _GOOD_SECRET not in log_text
    finally:
        proc.terminate()
        await upstream.stop()
        await authority.stop()
