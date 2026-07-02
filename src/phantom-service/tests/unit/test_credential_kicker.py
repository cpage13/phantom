"""Unit tests for phantom.workers.credential_kicker + the two-kicker auth_mode guard.

Proves (plan §2.3 / §2.5):

* the ``CredentialKicker`` wakes a parked ``aws_sigv4`` row when its cred slot
  goes fresh, re-queuing ``auth_expired → queued`` through the saturation gate;
* the two kickers do NOT fight — the ``AuthKicker`` skips ``aws_sigv4`` rows and
  the ``CredentialKicker`` skips ``phantom_bearer`` rows;
* an un-routable parked row is SKIPPED (not aborting the rescan pass), so a
  routable row ordered behind it is still processed;
* the ``CredentialKicker`` is an inert no-op when ``signer_creds is None``.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.models.credential import HostCredKey, SigningService, SigV4StaticCreds
from phantom.models.upload import UploadRow
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.credential_store import SqliteCredentialStore
from phantom.workers.auth_kicker import AuthKicker
from phantom.workers.credential_kicker import CredentialKicker
from phantom.workers.saturation import SaturationGate

from .conftest import make_snapshot, snapshot_thunk, track_instance, track_started

# Distinct hosts so the route table maps each to a different auth_mode.
_SIGV4_HOST = "s3.us-east-1.amazonaws.com"
_BEARER_HOST = "files.example.com"


def _sigv4_creds() -> SigV4StaticCreds:
    """A static SigV4 key-pair for the cred store."""
    return SigV4StaticCreds(
        access_key_id="AKIAEXAMPLE",
        secret_access_key="secret",
        region="us-east-1",
        service=SigningService.S3,
    )


async def _build(
    tmp_path: Path,
    *,
    saturation: SaturationGate | None = None,
    with_signer_creds: bool = True,
) -> tuple[InstanceContext, SqliteCredentialStore | None]:
    """Build an InstanceContext with an aws_sigv4 + a phantom_bearer route.

    When ``with_signer_creds`` the context carries a started
    :class:`SqliteCredentialStore` (tracked for teardown, since the conftest
    ``track_instance`` only tracks the fixed component set, not ``signer_creds``).
    Returns ``(instance, signer_creds_or_None)``.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    from phantom.storage.hybrid_body_store import HybridBodyStore

    body_store = HybridBodyStore(ram=ram, disk=fbs)
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await store.start()
    await body_store.start()
    await tokens.start()

    signer_creds: SqliteCredentialStore | None = None
    if with_signer_creds:
        signer_creds = SqliteCredentialStore(str(tmp_path / "credential_store.db"))
        await signer_creds.start()
        track_started(signer_creds)

    cfg = InstanceCfg(
        id="primary",
        host_prefixes=["*"],
        data_dir="primary",
        routes=[
            RouteCfg(name="sigv4", hosts=[_SIGV4_HOST], auth_mode="aws_sigv4"),
            RouteCfg(name="bearer", hosts=[_BEARER_HOST], auth_mode="phantom_bearer"),
        ],
    )
    sat = saturation or SaturationGate(
        max_in_flight=100,
        max_in_flight_bytes=10_000_000,
        max_disk_bytes=1_000_000_000,
    )
    instance = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=None,
        token_cache=tokens,
        minter=None,
        retry_strategy=MagicMock(),
        upstream_client=MagicMock(),
        executor=MagicMock(),
        saturation=sat,
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(make_snapshot()),
        signer_creds=signer_creds,
    )
    return track_instance(instance), signer_creds


def _parked_row(*, endpoint: str, body_size_bytes: int = 500) -> UploadRow:
    """An ``auth_expired`` row pinned to ``endpoint`` (the current-step host)."""
    now = datetime.now(tz=UTC)
    row_uid = uuid4()
    return UploadRow(
        chain_id=row_uid,
        instance_id="primary",
        group_id=row_uid,
        multifile_id=row_uid,
        send_order=0,
        route_name="r",
        state="auth_expired",
        body_location="ram",
        body_size_bytes=body_size_bytes,
        received_at=now,
        updated_at=now,
        endpoint=endpoint,
        uid="user-1",
        chain_envelope_json="{}",
        idempotency_key=str(row_uid),
        capture_reexecution_active=False,
    )


async def _drain_until_queued(
    instance: InstanceContext, chain_id: object, *, expect_queued: bool
) -> str:
    """Spin briefly; return the row's terminal-for-this-test state."""
    last = ""
    for _ in range(50):
        await asyncio.sleep(0.02)
        fresh = await instance.store.get(chain_id)  # type: ignore[arg-type]
        assert fresh is not None
        last = fresh.state
        if expect_queued and last == "queued":
            break
    return last


@pytest.mark.asyncio
async def test_credential_set_wakes_parked_sigv4_row(tmp_path: Path) -> None:
    """A cred ``set()`` wakes a parked ``aws_sigv4`` row (auth_expired → queued)."""
    sat = SaturationGate(max_in_flight=10, max_in_flight_bytes=10_000, max_disk_bytes=1_000_000)
    instance, signer_creds = await _build(tmp_path, saturation=sat)
    assert signer_creds is not None
    row = _parked_row(endpoint=_SIGV4_HOST)
    await instance.store.insert(row)
    assert sat.in_flight == 0

    cred_kicker = CredentialKicker(instance=instance)
    stop_event = asyncio.Event()
    task = asyncio.create_task(cred_kicker.run(stop_event))
    # Fresh creds land for the SigV4 host → fires the wake handler.
    await signer_creds.set(HostCredKey(_SIGV4_HOST), _sigv4_creds(), source="admin_push")

    state = await _drain_until_queued(instance, row.chain_id, expect_queued=True)
    stop_event.set()
    await asyncio.gather(task, return_exceptions=True)

    assert state == "queued"
    # Re-admitted through the gate (the sender released it at park time).
    assert sat.in_flight == 1
    assert sat.in_flight_bytes == 500


@pytest.mark.asyncio
async def test_credential_kicker_skips_bad_cred_slot(tmp_path: Path) -> None:
    """A parked ``aws_sigv4`` row with a ``bad`` cred slot is NOT woken."""
    instance, signer_creds = await _build(tmp_path)
    assert signer_creds is not None
    row = _parked_row(endpoint=_SIGV4_HOST)
    await instance.store.insert(row)
    # Slot exists but is bad — the executor flipped it on the 401.
    await signer_creds.set(HostCredKey(_SIGV4_HOST), _sigv4_creds(), source="admin_push")
    await signer_creds.mark_bad(HostCredKey(_SIGV4_HOST))

    cred_kicker = CredentialKicker(instance=instance)
    await cred_kicker._rescan()

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "auth_expired"


@pytest.mark.asyncio
async def test_credential_kicker_skips_phantom_bearer_row(tmp_path: Path) -> None:
    """The CredentialKicker does NOT wake a ``phantom_bearer`` row (no fight).

    Even with a fresh cred slot present (and reachable), the guard skips the
    bearer-routed row — that row is the AuthKicker's to wake.
    """
    instance, signer_creds = await _build(tmp_path)
    assert signer_creds is not None
    bearer_row = _parked_row(endpoint=_BEARER_HOST)
    await instance.store.insert(bearer_row)
    # A fresh cred slot for the bearer host would let a guard-less kicker wake
    # it; the guard must reject on auth_mode, not on slot absence.
    await signer_creds.set(HostCredKey(_BEARER_HOST), _sigv4_creds(), source="admin_push")

    cred_kicker = CredentialKicker(instance=instance)
    await cred_kicker._rescan()

    fresh = await instance.store.get(bearer_row.chain_id)
    assert fresh is not None
    assert fresh.state == "auth_expired"


@pytest.mark.asyncio
async def test_auth_kicker_skips_sigv4_row(tmp_path: Path) -> None:
    """The AuthKicker does NOT wake an ``aws_sigv4`` row (no fight).

    Even with a fresh BEARER token cached for the SigV4 host, the guard skips
    the SigV4-routed row — that row is the CredentialKicker's to wake.
    """
    instance, _ = await _build(tmp_path)
    sigv4_row = _parked_row(endpoint=_SIGV4_HOST)
    await instance.store.insert(sigv4_row)
    # A fresh bearer token for the SigV4 host would let a guard-less AuthKicker
    # wake it; the guard must reject on auth_mode.
    await instance.token_cache.set(_SIGV4_HOST, "user-1", "Bearer abc", source="inbound_request")

    kicker = AuthKicker(instance=instance)
    await kicker._rescan()

    fresh = await instance.store.get(sigv4_row.chain_id)
    assert fresh is not None
    assert fresh.state == "auth_expired"


@pytest.mark.asyncio
async def test_two_kickers_do_not_fight(tmp_path: Path) -> None:
    """A bearer push wakes ONLY the bearer row; a cred push wakes ONLY the SigV4 row."""
    instance, signer_creds = await _build(tmp_path)
    assert signer_creds is not None
    bearer_row = _parked_row(endpoint=_BEARER_HOST)
    sigv4_row = _parked_row(endpoint=_SIGV4_HOST)
    await instance.store.insert(bearer_row)
    await instance.store.insert(sigv4_row)

    auth_kicker = AuthKicker(instance=instance)
    cred_kicker = CredentialKicker(instance=instance)

    # Push a fresh BEARER token for the bearer host, then run BOTH rescans.
    await instance.token_cache.set(_BEARER_HOST, "user-1", "Bearer abc", source="inbound_request")
    await auth_kicker._rescan()
    await cred_kicker._rescan()

    bearer_after = await instance.store.get(bearer_row.chain_id)
    sigv4_after = await instance.store.get(sigv4_row.chain_id)
    assert bearer_after is not None and bearer_after.state == "queued"
    # The SigV4 row was claimed by NEITHER (no cred yet) — stays parked.
    assert sigv4_after is not None and sigv4_after.state == "auth_expired"

    # Now push fresh CREDS for the SigV4 host, then run BOTH rescans.
    await signer_creds.set(HostCredKey(_SIGV4_HOST), _sigv4_creds(), source="admin_push")
    await auth_kicker._rescan()
    await cred_kicker._rescan()

    sigv4_after = await instance.store.get(sigv4_row.chain_id)
    assert sigv4_after is not None and sigv4_after.state == "queued"


@pytest.mark.asyncio
async def test_unroutable_row_skipped_not_aborting_pass(tmp_path: Path) -> None:
    """An un-routable parked row is SKIPPED; a routable row behind it still processes.

    The copied loop's per-row ``resolve_route`` raises ``ValueError`` on a host
    with no matching route (a route removed by hot-reload between park and
    wake). The guard wraps it per row and ``continue``s — the rescan pass must
    NOT abort behind the un-routable row (which would strand every later row).
    """
    instance, signer_creds = await _build(tmp_path)
    assert signer_creds is not None
    # Row 1: a host that matches NO route → resolve_route raises.
    unroutable = _parked_row(endpoint="nowhere.invalid")
    # Row 2: a normal aws_sigv4 row that SHOULD wake.
    sigv4_row = _parked_row(endpoint=_SIGV4_HOST)
    # Insert the un-routable FIRST so it is ordered ahead in the rescan.
    await instance.store.insert(unroutable)
    await instance.store.insert(sigv4_row)
    await signer_creds.set(HostCredKey(_SIGV4_HOST), _sigv4_creds(), source="admin_push")

    cred_kicker = CredentialKicker(instance=instance)
    # A single rescan: must not raise out, must process the second row.
    await cred_kicker._rescan()

    unroutable_after = await instance.store.get(unroutable.chain_id)
    sigv4_after = await instance.store.get(sigv4_row.chain_id)
    # Un-routable row skipped — still parked.
    assert unroutable_after is not None and unroutable_after.state == "auth_expired"
    # The pass did NOT abort behind it — the routable row was processed.
    assert sigv4_after is not None and sigv4_after.state == "queued"


@pytest.mark.asyncio
async def test_credential_kicker_noop_when_no_signer_creds(tmp_path: Path) -> None:
    """With ``signer_creds is None`` the kicker registers no handler and no-ops.

    A parked ``aws_sigv4`` row stays parked: there is no cred store to wake on,
    and ``_rescan`` returns early. Proves the bearer-only deployment is inert.
    """
    instance, signer_creds = await _build(tmp_path, with_signer_creds=False)
    assert signer_creds is None
    row = _parked_row(endpoint=_SIGV4_HOST)
    await instance.store.insert(row)

    cred_kicker = CredentialKicker(instance=instance)
    # Direct rescan returns early — no store, no work.
    await cred_kicker._rescan()

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "auth_expired"
