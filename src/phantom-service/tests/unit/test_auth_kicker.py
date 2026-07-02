"""Unit tests for phantom.workers.auth_kicker."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadRow
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.workers.auth_kicker import AuthKicker
from phantom.workers.saturation import SaturationGate

from .conftest import make_snapshot, snapshot_thunk, track_instance


async def _build(
    tmp_path: Path,
    *,
    saturation: SaturationGate | None = None,
) -> InstanceContext:
    """Build a single-store InstanceContext for the auth-kicker tests.

    Phase 1 Slice 1.E (plan § 2.3.6 / § 2.3.17): one persistent SQLite
    on the InstanceContext; workers, admin, and admission all read
    through ``instance.store``.
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
    cfg = InstanceCfg(
        id="primary",
        host_prefixes=["*"],
        data_dir="primary",
        # phantom_bearer: these tests exercise the bearer-wake path (a token
        # set() re-queues a parked row). With the §2.5 auth_mode guard the
        # AuthKicker only wakes phantom_bearer rows, so the route the parked
        # row resolves to must be phantom_bearer for the scenario to be valid
        # (a "none" route would never have parked via the bearer path).
        routes=[RouteCfg(name="r", hosts=["*"], auth_mode="phantom_bearer")],
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
    )
    return track_instance(instance)


@pytest.mark.asyncio
async def test_set_wakes_matching_rows(tmp_path: Path) -> None:
    """A cache set() wakes rows in ``auth_expired`` matching (endpoint, uid)."""
    instance = await _build(tmp_path)
    now = datetime.now(tz=UTC)
    row_uid = uuid4()
    row = UploadRow(
        chain_id=row_uid,
        instance_id="primary",
        group_id=row_uid,
        multifile_id=row_uid,
        send_order=0,
        route_name="r",
        state="auth_expired",
        body_location="ram",
        received_at=now,
        updated_at=now,
        endpoint="files.example.com",
        uid="user-1",
        chain_envelope_json="{}",
        idempotency_key="k",
        capture_reexecution_active=False,
    )
    await instance.store.insert(row)
    kicker = AuthKicker(instance=instance)
    stop_event = asyncio.Event()
    task = asyncio.create_task(kicker.run(stop_event))
    await instance.token_cache.set(
        "files.example.com",
        "user-1",
        "Bearer abc",
        source="inbound_request",
    )
    # Wait for the kicker to consume the event.
    for _ in range(50):
        await asyncio.sleep(0.02)
        fresh = await instance.store.get(row_uid)
        if fresh is not None and fresh.state == "queued":
            break
    stop_event.set()
    await asyncio.gather(task, return_exceptions=True)
    fresh = await instance.store.get(row_uid)
    assert fresh is not None
    assert fresh.state == "queued"


@pytest.mark.asyncio
async def test_kick_admits_on_saturation_gate(tmp_path: Path) -> None:
    """The wake path re-admits the row's bytes through the gate (§3.1).

    The sender released the row's bytes when parking into auth_expired;
    waking the row back to queued must re-admit so the in-flight counter
    matches reality.
    """
    sat = SaturationGate(
        max_in_flight=10,
        max_in_flight_bytes=10_000,
        max_disk_bytes=1_000_000,
    )
    instance = await _build(tmp_path, saturation=sat)
    now = datetime.now(tz=UTC)
    row_uid = uuid4()
    row = UploadRow(
        chain_id=row_uid,
        instance_id="primary",
        group_id=row_uid,
        multifile_id=row_uid,
        send_order=0,
        route_name="r",
        state="auth_expired",
        body_location="ram",
        body_size_bytes=500,
        received_at=now,
        updated_at=now,
        endpoint="files.example.com",
        uid="user-1",
        chain_envelope_json="{}",
        idempotency_key="k",
        capture_reexecution_active=False,
    )
    await instance.store.insert(row)
    # Gate starts clean — no in-flight bytes.
    assert sat.in_flight == 0
    assert sat.in_flight_bytes == 0

    kicker = AuthKicker(instance=instance)
    stop_event = asyncio.Event()
    task = asyncio.create_task(kicker.run(stop_event))
    await instance.token_cache.set(
        "files.example.com",
        "user-1",
        "Bearer abc",
        source="inbound_request",
    )
    for _ in range(50):
        await asyncio.sleep(0.02)
        fresh = await instance.store.get(row_uid)
        if fresh is not None and fresh.state == "queued":
            break
    stop_event.set()
    await asyncio.gather(task, return_exceptions=True)

    fresh = await instance.store.get(row_uid)
    assert fresh is not None
    assert fresh.state == "queued"
    # Gate now reflects the woken row's bytes.
    assert sat.in_flight == 1
    assert sat.in_flight_bytes == 500


@pytest.mark.asyncio
async def test_kick_keeps_row_parked_on_saturation_refusal(tmp_path: Path) -> None:
    """If the gate refuses the wake (saturated), the row stays auth_expired (§3.1)."""
    sat = SaturationGate(
        max_in_flight=1,
        max_in_flight_bytes=10_000,
        max_disk_bytes=1_000_000,
    )
    instance = await _build(tmp_path, saturation=sat)
    # Pre-saturate the row-count cap with one in-flight charge from elsewhere.
    from phantom.workers.saturation import AdmissionGranted

    result = await sat.admit(1)
    assert isinstance(result, AdmissionGranted)
    assert sat.in_flight == 1

    now = datetime.now(tz=UTC)
    row_uid = uuid4()
    row = UploadRow(
        chain_id=row_uid,
        instance_id="primary",
        group_id=row_uid,
        multifile_id=row_uid,
        send_order=0,
        route_name="r",
        state="auth_expired",
        body_location="ram",
        body_size_bytes=500,
        received_at=now,
        updated_at=now,
        endpoint="files.example.com",
        uid="user-1",
        chain_envelope_json="{}",
        idempotency_key="k",
        capture_reexecution_active=False,
    )
    await instance.store.insert(row)

    kicker = AuthKicker(instance=instance)
    stop_event = asyncio.Event()
    task = asyncio.create_task(kicker.run(stop_event))
    await instance.token_cache.set(
        "files.example.com",
        "user-1",
        "Bearer abc",
        source="inbound_request",
    )
    # Let the kicker make its admission attempt.
    await asyncio.sleep(0.2)
    stop_event.set()
    await asyncio.gather(task, return_exceptions=True)

    fresh = await instance.store.get(row_uid)
    assert fresh is not None
    # Wake refused; row stays parked for the next cache-set or reaper sweep.
    assert fresh.state == "auth_expired"
    # The gate's in_flight count did NOT grow.
    assert sat.in_flight == 1
