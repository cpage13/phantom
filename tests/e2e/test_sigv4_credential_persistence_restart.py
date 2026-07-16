"""SigV4 destination credential survives a real process restart (audit T4 / G10).

The existing SigV4 E2Es prove the re-sign path within one process (a pushed
static credential, wrong-credential parking, the refresh loop). None restarts
Phantom, so credential-store persistence and reopen are unproven end to end:
a mapping seam could drop the stored credential on reopen and every same-process
test would still pass.

This test provisions a static SigV4 credential into a real ``python -m phantom``
process, confirms the row is durable in ``credential_store.db`` (reading only
non-secret metadata, never ``cred_json``), stops the process, starts a fresh
process on the SAME data root with NO credential re-push, then drives a stock
S3-style upload through the catch-all. Delivery succeeds only if the restarted
process reopened the store and re-signed with the persisted credential: the
emulator's SigV4 sink stores the object solely on a faithful signature match.
"""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from phantom_client import PhantomClient

from tests.e2e._harness.subprocess_harness import (
    EmulatorHandle,
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    write_phantom_config,
)
from tests.e2e.helpers.assertions import assert_chain_reaches_state

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio, pytest.mark.e2e]

# The AWS documentation example pair the emulator's SigV4 sink validates against.
_ACCESS_KEY_ID = "AKIAIOSFODNN7EXAMPLE"
_SECRET_ACCESS_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
_REGION = "us-east-1"
_BUCKET = "mybucket"
_KEY = "restart/persisted-credential-object.bin"
_OBJECT_PATH = f"{_BUCKET}/{_KEY}"
_PAYLOAD = b"phantom-sigv4-restart-persistence\x00\xff\xfe-byte-identity"
_SUCCEEDED_BUDGET_SECONDS = 20.0


@dataclass(frozen=True)
class CredentialMetadata:
    """Non-secret credential-store row fields. ``cred_json`` is never read."""

    dest_host: str
    kind: str
    source: str
    status: str


def _sigv4_config_overrides(emulator_url: str) -> dict[str, object]:
    """Config overlay for the aws_sigv4 catch-all path, real emulator URL bound."""
    return {
        "phantom_default_target": emulator_url,
        "instances": [
            {
                "id": "primary",
                "host_prefixes": ["emulator", "127.0.0.1", "localhost"],
                "data_dir": "primary",
                "capture_reexecution": False,
                "routes": [
                    {
                        "name": "emulator",
                        "hosts": ["emulator", "127.0.0.1", "localhost"],
                        "auth_mode": "aws_sigv4",
                    },
                ],
            },
        ],
    }


def _emulator_host(emulator: EmulatorHandle) -> str:
    """The destination host the signer keys the credential on (port-stripped)."""
    host = httpx.URL(emulator.url).host
    assert host, f"emulator url has no host: {emulator.url!r}"
    return host


async def _push_static_credential(admin_url: str, dest_host: str) -> None:
    """Provision the static SigV4 credential through the loopback admin API."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.put(
            f"{admin_url}/v1/admin/credentials/{dest_host}",
            json={
                "kind": "sigv4_static",
                "access_key_id": _ACCESS_KEY_ID,
                "secret_access_key": _SECRET_ACCESS_KEY,
                "region": _REGION,
                "service": "s3",
                "session_token": None,
            },
        )
    assert response.status_code == 204, (
        f"admin credential push expected 204, got {response.status_code}: {response.text!r}"
    )


def _read_credential_metadata(data_dir: Path, dest_host: str) -> CredentialMetadata | None:
    """Read one credential row's NON-SECRET metadata via a read-only connection.

    Deliberately never selects ``cred_json``: the secret material stays
    untouched, matching the audit's constraint on inspecting the store.
    """
    database = data_dir / "primary" / "credential_store.db"
    if not database.exists():
        return None
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        row = connection.execute(
            "SELECT dest_host, kind, source, status FROM credential_store WHERE dest_host = ?",
            (dest_host,),
        ).fetchone()
    if row is None:
        return None
    return CredentialMetadata(
        dest_host=str(row[0]), kind=str(row[1]), source=str(row[2]), status=str(row[3])
    )


async def _raw_intake_put(phantom_url: str, path: str, body: bytes) -> UUID:
    """Drive a stock PUT through the catch-all; return the minted chain id."""
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.put(f"{phantom_url}/{path}", content=body)
    assert response.status_code == 202, (
        f"raw intake expected 202 ack, got {response.status_code}: {response.text!r}"
    )
    upload_id = response.headers.get("X-Phantom-Upload-Id")
    assert upload_id, "raw-intake ack must carry X-Phantom-Upload-Id"
    return UUID(upload_id)


async def test_sigv4_credential_survives_restart_and_resigns(tmp_path: Path) -> None:
    """Provision, restart on the same data root, and deliver seed-free."""
    data_dir = tmp_path / "data"
    emulator = await boot_emulator()
    port = allocate_port()
    config_path = write_phantom_config(
        data_dir=data_dir,
        bind_port=port,
        config_overrides=_sigv4_config_overrides(emulator.url),
    )
    dest_host = _emulator_host(emulator)

    first = PhantomSubprocess.make(config_path, port)
    second: PhantomSubprocess | None = None
    try:
        # First process: provision the credential and confirm it is durable.
        await first.start()
        await _push_static_credential(first.url, dest_host)
        metadata = _read_credential_metadata(data_dir, dest_host)
        assert metadata is not None, "credential row was not persisted before restart"
        assert metadata.dest_host == dest_host
        assert metadata.kind == "sigv4_static"
        assert metadata.source == "admin_push"
        assert metadata.status == "fresh", (
            f"freshly pushed credential status was {metadata.status!r}"
        )

        # Stop the first process. The store must survive on disk.
        first.terminate()
        assert _read_credential_metadata(data_dir, dest_host) is not None, (
            "credential row vanished after the first process stopped"
        )

        # Second process on the SAME data root, with NO credential re-push. Any
        # successful re-sign now is attributable only to store reopen.
        second = PhantomSubprocess.make(config_path, port)
        await second.start()

        chain_id = await _raw_intake_put(second.url, _OBJECT_PATH, _PAYLOAD)
        async with PhantomClient(second.url) as client:
            detail = await assert_chain_reaches_state(
                client, chain_id, state="succeeded", timeout_seconds=_SUCCEEDED_BUDGET_SECONDS
            )
        assert detail.state == "succeeded"

        # The SigV4 sink stored the object ONLY because the re-signed signature,
        # produced from the PERSISTED credential, recomputed and matched.
        stored = emulator.server.state.s3_objects.get((_BUCKET, _KEY))
        assert stored is not None, (
            "no S3 object stored: the restarted process did not re-sign with the persisted "
            "credential (store reopen or mapping seam dropped it)"
        )
        assert stored.body == _PAYLOAD, "byte round-trip broke across the restart re-sign"
        expected_sha256 = hashlib.sha256(_PAYLOAD).hexdigest()
        assert stored.all_headers.get("x-amz-content-sha256") == expected_sha256, (
            "re-signed PUT must carry x-amz-content-sha256 == the real body hash"
        )
    finally:
        first.terminate()
        if second is not None:
            second.terminate()
        await emulator.stop()
