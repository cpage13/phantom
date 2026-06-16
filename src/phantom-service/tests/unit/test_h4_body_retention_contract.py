"""H4 audit closure — body-retention contract reconciliation (Phase 2 § 3.2.4).

Two regressions land here:

1. **Sender ``_on_succeeded`` honors ``succeeded_body_seconds``.** When
   the operator configures a non-zero retention window the sender must
   NOT delete the body on success; the reaper owns deletion at the
   configured time. Pre-Phase-2 the sender always deleted, breaking the
   Slice 1.F yellow E2E tests (F-Slice1F-A).
2. **Recovery's ``body_discarded_at IS NOT NULL`` carve-out.** Rows
   whose bodies the reaper already discarded must not be re-quarantined
   by recovery's body-presence integrity guard. The carve-out lives in
   ``workers/recovery.py:78-82``; Phase 2 verifies the regression.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from phantom.chain.executor import ChainExecutor, Succeeded
from phantom.config.settings import (
    InstanceCfg,
    PersistTriggerCfg,
    RetentionCfg,
    RouteCfg,
)
from phantom.instances.context import InstanceContext
from phantom.models.upload import BodyHash, BodyHashes, CapturedValues, StorageHash, UploadRow
from phantom.routing import resolve_route
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.transport import UpstreamRequest, UpstreamResponse
from phantom.workers.recovery import run_recovery
from phantom.workers.saturation import SaturationGate
from phantom.workers.sender import Sender

from .conftest import make_snapshot, snapshot_thunk, track_instance


class _FakeUpstream:
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


_TEST_RETENTION_DEFAULTS: dict[str, int] = {
    "succeeded_metadata_seconds": 300,
    "failed_body_seconds": 14 * 86_400,
    "auth_expired_body_seconds": 60 * 86_400,
    "stored_body_seconds": 60 * 86_400,
}


async def _build_instance(
    tmp_path: Path,
    *,
    retention: RetentionCfg | None = None,
) -> InstanceContext:
    """Single-store instance for the H4 sender + recovery regressions."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await store.start()
    await body_store.start()
    await tokens.start()
    cfg = InstanceCfg(
        id="primary",
        host_prefixes=["files.example.com"],
        data_dir="primary",
        routes=[RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer")],
    )
    upstream = _FakeUpstream()
    executor = ChainExecutor(
        token_cache=tokens,
        upstream_client=upstream,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=cfg,
    )
    saturation = SaturationGate(
        max_in_flight=10, max_in_flight_bytes=10_000_000, max_disk_bytes=10_000_000
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
        retry_strategy=FixedIntervalsStrategy([1, 5]),
        upstream_client=upstream,
        executor=executor,
        saturation=saturation,
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(
            make_snapshot(
                persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=0),
                retention=retention,
            )
        ),
    )
    return track_instance(instance)


def _row(
    chain_id: Any,
    *,
    body: bytes,
    body_hashes: dict[str, BodyHashes],
    storage_encoding: str = "original",
) -> UploadRow:
    """Build a minimal UploadRow with body_hashes set for verification."""
    now = datetime.now(tz=UTC)
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": chain_id,
            "multifile_id": chain_id,
            "send_order": 0,
            "route_name": "files",
            "state": "attempting",
            "body_location": "ram",
            "attempts": 0,
            "next_attempt_at": now,
            "received_at": now,
            "updated_at": now,
            "endpoint": "files.example.com",
            "uid": "user-1",
            "chain_envelope_json": "{}",
            "idempotency_key": "k",
            "capture_reexecution_active": False,
            "storage_encoding": storage_encoding,
            "body_size_bytes": len(body),
            "body_hashes": body_hashes,
        },
    )


def _seed_hashes(body: bytes) -> dict[str, BodyHashes]:
    """Return body_hashes for a passthrough-coded body."""
    digest = hashlib.sha256(body).hexdigest()
    return {
        "body": BodyHashes(
            body_hash=BodyHash(digest),
            storage_hash=StorageHash(digest),
        )
    }


# -----------------------------------------------------------------------------
# Sender body-retention contract (H4 reconciliation).
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sender_drops_body_immediately_when_retention_zero(tmp_path: Path) -> None:
    """``succeeded_body_seconds == 0`` (default) — sender drops the body."""
    # Default retention has succeeded_body_seconds=0.
    instance = await _build_instance(tmp_path)
    chain_id = uuid4()
    body = b"payload"
    row = _row(chain_id, body=body, body_hashes=_seed_hashes(body))
    await instance.store.insert(row)
    await instance.ram_body_store.put(chain_id, {"body": body})
    # Take a saturation slot so the release path on success doesn't underflow.
    sat_result = await instance.saturation.admit(len(body))
    assert sat_result.__class__.__name__ == "AdmissionGranted", sat_result

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    result = Succeeded(
        captured=CapturedValues(),
        next_step_index=1,
        chain_done=True,
        step_name="step",
        upstream_status=200,
        upstream_headers={},
    )
    await sender._on_succeeded(instance.store, row, result)

    # The body is gone — default contract preserved.
    assert not await instance.body_store.has_body_ref(chain_id, "body")


@pytest.mark.asyncio
async def test_sender_retains_body_when_retention_nonzero(tmp_path: Path) -> None:
    """``succeeded_body_seconds > 0`` — sender keeps the body for reaper.

    H4 audit closure regression. Without this fix the sender always
    deleted, contradicting any non-zero retention configured by the
    operator and breaking the F-Slice1F-A yellow E2E tests.
    """
    retention = RetentionCfg(
        succeeded_body_seconds=60,  # 60 seconds — reaper owns deletion
        **_TEST_RETENTION_DEFAULTS,  # type: ignore[arg-type]
    )
    instance = await _build_instance(tmp_path, retention=retention)
    chain_id = uuid4()
    body = b"payload"
    row = _row(chain_id, body=body, body_hashes=_seed_hashes(body))
    await instance.store.insert(row)
    await instance.ram_body_store.put(chain_id, {"body": body})
    sat_result = await instance.saturation.admit(len(body))
    assert sat_result.__class__.__name__ == "AdmissionGranted", sat_result

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    result = Succeeded(
        captured=CapturedValues(),
        next_step_index=1,
        chain_done=True,
        step_name="step",
        upstream_status=200,
        upstream_headers={},
    )
    await sender._on_succeeded(instance.store, row, result)

    # The body STAYS — the reaper will delete it after the configured
    # retention window elapses. Admin GET /chains/{id}/body must still
    # succeed for that window.
    assert await instance.body_store.has_body_ref(chain_id, "body")


# -----------------------------------------------------------------------------
# Recovery body_discarded_at carve-out (H4 part 1).
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recovery_skips_quarantine_when_body_discarded_at_set(tmp_path: Path) -> None:
    """Recovery's body-presence guard skips rows the reaper already discarded.

    The reaper's body-discard pass sets ``body_discarded_at`` on each
    affected row; the body file is then gone. Without the carve-out,
    recovery would re-mark every such row ``corrupted`` on the next
    boot. The H4 closure is a one-line skip in workers/recovery.py.
    """
    instance = await _build_instance(tmp_path)
    chain_id = uuid4()
    body = b"payload"
    body_hashes = _seed_hashes(body)
    # Build a succeeded terminal row with body_discarded_at set.
    now = datetime.now(tz=UTC)
    row = UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": chain_id,
            "multifile_id": chain_id,
            "send_order": 0,
            "route_name": "files",
            "state": "succeeded",
            "body_location": "file",
            "received_at": now - timedelta(hours=2),
            "updated_at": now - timedelta(hours=1),
            "endpoint": "files.example.com",
            "uid": "user-1",
            "chain_envelope_json": "{}",
            "idempotency_key": "k",
            "capture_reexecution_active": False,
            "body_size_bytes": len(body),
            "body_hashes": body_hashes,
            "body_discarded_at": now - timedelta(minutes=30),  # reaper-discarded
        },
    )
    await instance.store.insert(row)
    # No body in the body store — reaper already deleted it.

    await run_recovery(instance.store, instance.body_store)

    fresh = await instance.store.get(chain_id)
    assert fresh is not None
    # Not quarantined — the carve-out spared this row.
    assert fresh.state == "succeeded"
    assert fresh.last_error is None


@pytest.mark.asyncio
async def test_recovery_quarantines_when_body_discarded_at_null(tmp_path: Path) -> None:
    """Counter-test: a DELIVERABLE row with ``body_discarded_at IS NULL`` must
    have its body present, else recovery quarantines it.

    Without this counter-test the ``body_discarded_at`` carve-out could
    regress to "always skip", which would silently hide real corruption. A
    DELIVERABLE (non-terminal) row with no discarded-at timestamp but missing
    body files is the corruption case Slice 1.D's recovery integrity guard
    catches.

    NOTE (finding R9-PM-3): the carrier state must be NON-terminal. A
    ``succeeded`` (terminal) row whose body is gone is the legitimate
    delivered-then-reaped state — recovery now correctly EXEMPTS it (the
    terminal carve-out), so it can no longer stand in for "real corruption"
    here. We use ``queued`` (deliverable — its body MUST exist) to express the
    genuine corruption case this counter-test guards.
    """
    instance = await _build_instance(tmp_path)
    chain_id = uuid4()
    body = b"payload"
    body_hashes = _seed_hashes(body)
    now = datetime.now(tz=UTC)
    # body_discarded_at is None (the field defaults to None) and the
    # body store is empty. A deliverable (queued) row MUST have its body —
    # recovery should mark this corrupted.
    row = UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": chain_id,
            "multifile_id": chain_id,
            "send_order": 0,
            "route_name": "files",
            "state": "queued",
            "body_location": "file",
            "received_at": now,
            "updated_at": now,
            "endpoint": "files.example.com",
            "uid": "user-1",
            "chain_envelope_json": "{}",
            "idempotency_key": "k",
            "capture_reexecution_active": False,
            "body_size_bytes": len(body),
            "body_hashes": body_hashes,
        },
    )
    await instance.store.insert(row)

    await run_recovery(instance.store, instance.body_store)

    fresh = await instance.store.get(chain_id)
    assert fresh is not None
    assert fresh.state == "corrupted"


# Suppress unused-import warnings on UUID — used by typing annotations only.
_ = UUID
