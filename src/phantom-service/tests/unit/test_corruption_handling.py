"""Tests for storage-corruption + codec-round-trip-drift handling in the sender.

Slice 1.D rewrite: the dual-store fixture is gone; the sender reads
bodies via :attr:`InstanceContext.body_store` (per plan § 2.3.18).
The single persistent store carries every row regardless of
``body_location``.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from phantom.chain.executor import ChainExecutor
from phantom.compression import build_codec_for_algorithm
from phantom.config.settings import (
    InstanceCfg,
    PersistTriggerCfg,
    RouteCfg,
)
from phantom.instances.context import InstanceContext
from phantom.models.upload import BodyHash, BodyHashes, StorageHash, UploadRow
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
from phantom.workers.saturation import SaturationGate
from phantom.workers.sender import Sender

from .conftest import make_snapshot, snapshot_thunk, track_instance


class _FakeUpstream:
    """Minimal upstream client — never reached in these tests."""

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


async def _build_instance(tmp_path: Path) -> InstanceContext:
    """Build a single-store + hybrid body store InstanceContext."""
    disk = SqliteUploadStore(str(tmp_path / "disk.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await disk.start()
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
        store=disk,
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
            make_snapshot(persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=0))
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
    """Build a minimal UploadRow with body_hashes set for verification.

    State defaults to ``attempting`` to match the post-claim_due
    invariant the sender's record_attempt_result M-W4-F7 predicate
    (Phase 2 § 3.2.8) checks. Tests that exercise ``_drive_one``
    directly (bypassing the sender's worker_loop + claim_due) need
    this so the terminal-state UPDATE fires.
    """
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


@pytest.mark.asyncio
async def test_storage_corruption_transitions_to_corrupted(tmp_path: Path) -> None:
    """A stored byte that doesn't match storage_hash trips StorageCorruptionError → corrupted."""
    instance = await _build_instance(tmp_path)
    chain_id = uuid4()
    body = b"hello-world"
    correct_storage_hash = hashlib.sha256(body).hexdigest()
    body_hash = hashlib.sha256(body).hexdigest()
    row = _row(
        chain_id,
        body=body,
        body_hashes={
            "body": BodyHashes(
                body_hash=BodyHash(body_hash),
                storage_hash=StorageHash(correct_storage_hash),
            )
        },
    )
    await instance.store.insert(row)
    # Store mutated bytes via RAM half — the storage_hash will not match.
    await instance.ram_body_store.put(chain_id, {"body": b"mutated-body"})

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._drive_one(instance.store, row)

    fresh = await instance.store.get(chain_id)
    assert fresh is not None
    assert fresh.state == "corrupted"
    assert fresh.last_error is not None
    assert fresh.last_error.startswith("storage_corruption:")


@pytest.mark.asyncio
async def test_codec_drift_transitions_to_corrupted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Codec decode returning wrong bytes → corrupted state w/ codec_round_trip_drift."""
    instance = await _build_instance(tmp_path)
    chain_id = uuid4()
    body = b"hello-world"
    storage_bytes = body  # passthrough — stored = original
    storage_hash = hashlib.sha256(storage_bytes).hexdigest()
    body_hash = hashlib.sha256(body).hexdigest()
    row = _row(
        chain_id,
        body=body,
        body_hashes={
            "body": BodyHashes(
                body_hash=BodyHash(body_hash),
                storage_hash=StorageHash(storage_hash),
            )
        },
    )
    await instance.store.insert(row)
    await instance.ram_body_store.put(chain_id, {"body": storage_bytes})

    # Force decode to return bytes whose hash differs from body_hash.
    real_codec = build_codec_for_algorithm("original")

    class _DriftingCodec:
        algorithm_name = real_codec.algorithm_name

        def encode(self, b: bytes) -> bytes:
            return real_codec.encode(b)

        def decode(self, b: bytes) -> bytes:
            return b + b"-drifted"

    monkeypatch.setattr(
        "phantom.workers.sender.build_codec_for_algorithm",
        lambda _name: _DriftingCodec(),
    )

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._drive_one(instance.store, row)

    fresh = await instance.store.get(chain_id)
    assert fresh is not None
    assert fresh.state == "corrupted"
    assert fresh.last_error is not None
    assert fresh.last_error.startswith("codec_round_trip_drift:")


@pytest.mark.asyncio
async def test_corrupted_not_retried(tmp_path: Path) -> None:
    """``claim_due`` skips corrupted rows (terminal, never re-attempted)."""
    instance = await _build_instance(tmp_path)
    chain_id = uuid4()
    now = datetime.now(tz=UTC)
    row = UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": chain_id,
            "multifile_id": chain_id,
            "send_order": 0,
            "route_name": "files",
            "state": "corrupted",
            "body_location": "ram",
            "attempts": 0,
            # Even with next_attempt_at in the past, ``claim_due`` should NOT
            # pick it up — only ``state='queued'`` rows are due.
            "next_attempt_at": now,
            "received_at": now,
            "updated_at": now,
            "endpoint": "files.example.com",
            "uid": "user-1",
            "chain_envelope_json": "{}",
            "idempotency_key": "k",
            "capture_reexecution_active": False,
        },
    )
    await instance.store.insert(row)

    claimed = await instance.store.claim_due(now, limit=10)
    assert claimed == []
