"""H8 audit closure — missing body in sender routes the row to ``corrupted``.

Phase 2 § 3.2.6. Before this fix sender's ``_load_body_refs`` caught
``KeyError`` from ``body_store.get_all`` and returned an empty dict —
making a row with declared body_hashes but absent body store entries
look identical to a row with no body declared. The sender then
forwarded zero bytes upstream, an effective data loss disguised as a
successful upload.

The fix raises :class:`BodyMissingError` from
:meth:`Sender._load_body_refs` when the body store doesn't have the
chain's bodies. ``_drive_one`` catches it and routes the row to
``corrupted`` via the existing ``_on_corrupted`` path, matching the
behavior for storage_hash mismatches per ADR-014.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from phantom.chain.executor import ChainExecutor
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
from phantom.storage.errors import BodyMissingError
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.transport import UpstreamRequest, UpstreamResponse
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


async def _build_instance(tmp_path: Path) -> InstanceContext:
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
            make_snapshot(persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=0))
        ),
    )
    return track_instance(instance)


def _row_with_body(body: bytes) -> UploadRow:
    """Build a row whose body_hashes declare ``body`` is present.

    State is ``attempting`` — that's the post-claim_due invariant the
    sender's M-W4-F7 predicate (Phase 2 § 3.2.8) checks.
    """
    digest = hashlib.sha256(body).hexdigest()
    now = datetime.now(tz=UTC)
    return UploadRow.model_validate(
        {
            "chain_id": uuid4(),
            "instance_id": "primary",
            "group_id": uuid4(),
            "multifile_id": uuid4(),
            "send_order": 0,
            "route_name": "files",
            "state": "attempting",
            "body_location": "ram",
            "received_at": now,
            "updated_at": now,
            "endpoint": "files.example.com",
            "uid": "user-1",
            "chain_envelope_json": "{}",
            "idempotency_key": "k",
            "capture_reexecution_active": False,
            "body_size_bytes": len(body),
            "storage_encoding": "original",
            "body_hashes": {
                "body": BodyHashes(
                    body_hash=BodyHash(digest),
                    storage_hash=StorageHash(digest),
                )
            },
        },
    )


@pytest.mark.asyncio
async def test_load_body_refs_raises_when_body_store_missing(tmp_path: Path) -> None:
    """``_load_body_refs`` now raises BodyMissingError instead of returning {}.

    Pre-Phase-2 regression: the absent body store entry was silently
    treated as "no body declared" and the sender forwarded an empty
    payload upstream.
    """
    instance = await _build_instance(tmp_path)
    row = _row_with_body(b"payload")
    await instance.store.insert(row)
    # Body store is intentionally empty for ``row.chain_id``.

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    with pytest.raises(BodyMissingError) as exc_info:
        await sender._load_body_refs(row)
    assert exc_info.value.chain_id == row.chain_id
    assert exc_info.value.missing == ["body"]


@pytest.mark.asyncio
async def test_drive_one_marks_corrupted_on_missing_body(tmp_path: Path) -> None:
    """``_drive_one`` routes a row with missing bodies to ``corrupted``.

    The full pipeline regression: from ``_load_body_refs`` raising
    ``BodyMissingError`` through ``_drive_one``'s exception cascade to
    the row's terminal-state UPDATE. ``last_error`` carries the H8
    contract token so operator logs can distinguish it from a hash
    mismatch.
    """
    instance = await _build_instance(tmp_path)
    row = _row_with_body(b"payload")
    await instance.store.insert(row)
    # Take a saturation slot so the on-corrupted release path is
    # exercised end-to-end.
    sat_result = await instance.saturation.admit(len(b"payload"))
    assert sat_result.__class__.__name__ == "AdmissionGranted", sat_result

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._drive_one(instance.store, row)

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "corrupted"
    assert fresh.last_error is not None
    assert fresh.last_error.startswith("storage_corruption:")
    # H8 contract: detail token names the body_missing_in_sender case.
    assert "body_missing_in_sender" in fresh.last_error


@pytest.mark.asyncio
async def test_load_body_refs_returns_empty_for_bodyless_chain(tmp_path: Path) -> None:
    """A row with empty ``body_hashes`` returns ``{}`` — not BodyMissingError.

    Counter-test: the H8 fix must not break legitimate metadata-only
    POSTs where no body_refs were declared at admission. Such rows
    have ``body_hashes = {}`` and skip the body store entirely — the
    sender returns empty bytes for upstream forwarding.
    """
    instance = await _build_instance(tmp_path)
    now = datetime.now(tz=UTC)
    row = UploadRow.model_validate(
        {
            "chain_id": uuid4(),
            "instance_id": "primary",
            "group_id": uuid4(),
            "multifile_id": uuid4(),
            "send_order": 0,
            "route_name": "files",
            "state": "queued",
            "body_location": "ram",
            "received_at": now,
            "updated_at": now,
            "endpoint": "files.example.com",
            "uid": "user-1",
            "chain_envelope_json": "{}",
            "idempotency_key": "k",
            "capture_reexecution_active": False,
            "body_size_bytes": 0,
            "storage_encoding": "original",
            "body_hashes": {},  # No bodies declared — legitimate bodyless chain.
        },
    )
    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    refs = await sender._load_body_refs(row)
    assert refs == {}


@pytest.mark.asyncio
async def test_drive_one_no_retry_after_corrupted(tmp_path: Path) -> None:
    """``corrupted`` is terminal — the row's next_attempt_at is cleared."""
    instance = await _build_instance(tmp_path)
    row = _row_with_body(b"payload")
    await instance.store.insert(row)
    sat_result = await instance.saturation.admit(len(b"payload"))
    assert sat_result.__class__.__name__ == "AdmissionGranted", sat_result

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._drive_one(instance.store, row)

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    # next_attempt_at is None on terminal states.
    assert fresh.next_attempt_at is None


@pytest.mark.asyncio
async def test_drive_one_marks_corrupted_when_body_directory_vanishes(
    tmp_path: Path,
) -> None:
    """P1: a deleted on-disk body directory routes the row to ``corrupted``.

    The exact e2e ``test_multipart_corrupted`` scenario, exercised
    through the real :class:`FileBodyStore` (behind
    :class:`HybridBodyStore`): the body is written to disk, then the
    whole-chain directory is deleted out from under the sender. Before
    the P1 fix ``FileBodyStore.get_all`` let a raw ``FileNotFoundError``
    escape ``_load_body_refs`` (which only catches ``KeyError``), so
    ``_drive_one`` raised and the worker loop logged "failed to drive
    row" without ever resolving the row — a wedge. The fix maps the
    filesystem-absence error to ``KeyError`` → ``BodyMissingError`` →
    the ``corrupted`` terminal state.
    """
    instance = await _build_instance(tmp_path)
    body = b"payload-on-disk"
    row = _row_with_body(body)
    await instance.store.insert(row)
    sat_result = await instance.saturation.admit(len(body))
    assert sat_result.__class__.__name__ == "AdmissionGranted", sat_result

    # Write the body to the disk half, then delete the whole-chain
    # directory — leaving the row's declared body_hashes pointing at a
    # body directory that no longer exists.
    await instance.file_body_store.put(row.chain_id, {"body": body})
    await instance.file_body_store.delete(row.chain_id)

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._drive_one(instance.store, row)

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "corrupted"
    assert fresh.last_error is not None
    assert fresh.last_error.startswith("storage_corruption:")
    assert "body_missing_in_sender" in fresh.last_error
