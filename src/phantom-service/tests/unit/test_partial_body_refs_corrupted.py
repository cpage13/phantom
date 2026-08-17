"""F2: a PARTIAL body is a missing body, caught at the sender's load boundary.

``Sender._load_body_refs`` reads the body store and then iterates only what the
store RETURNED. Nothing checked that every ref declared in ``row.body_hashes``
came back. ``FileBodyStore.get_all`` lists whatever the chain directory holds and
returns a partial dict without raising: its inner traversal raises ``KeyError``
only for a missing directory or a file that vanishes mid-traversal, so a
directory that was ALREADY incomplete when listed returned quietly.

Two hole shapes reach the sender through different code paths, and both are
covered here:

1. **Partial**: the chain directory holds some of the declared refs.
2. **Empty**: the directory exists and holds nothing, so the traversal iterates
   to zero entries and returns ``{}``. The sender's bodyless early return does
   NOT mask this, because that return is keyed on the ROW declaring no hashes,
   not on the store returning nothing.

Consequences before F2: a step using only the surviving ref forwarded truncated
content stamped ``succeeded``, and a step using the missing ref returned
``TemplateUnresolved`` and terminated ``failed``, blaming the producer's template
for a storage fault. Either way ADR-014's missing-body path to ``corrupted``
never fired.

F2 asserts at send time the rule the boot recovery sweep already applies per
declared ref, as one set comparison. The reverse direction (a returned ref the
row never declared) was already covered by ``StorageCorruptionError`` and is
pinned here so a later refactor cannot collapse the two directions into one.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from phantom.chain.executor import ChainExecutor
from phantom.config.settings import InstanceCfg, PersistTriggerCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.models.upload import BodyHash, BodyHashes, StorageHash, UploadRow
from phantom.routing import resolve_route
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.errors import BodyMissingError, StorageCorruptionError
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.transport import UpstreamRequest, UpstreamResponse
from phantom.workers.saturation import SaturationGate
from phantom.workers.sender import Sender

from .conftest import make_snapshot, snapshot_thunk, track_instance

pytestmark = pytest.mark.asyncio

# The two declared refs of the multipart row under test.
PART_ONE = "part1"
PART_TWO = "part2"

# Their bytes. Distinct so a truncated forward would be observable.
PART_ONE_BYTES = b"phantom-f2-part-one"
PART_TWO_BYTES = b"phantom-f2-part-two-longer"

# An undeclared ref name for the reverse-direction test.
STOWAWAY = "stowaway"


class _FakeUpstream:
    """Stub upstream client. No test here should ever reach the transport."""

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


async def _build_instance(tmp_path: Path) -> InstanceContext:
    """Build a real-store instance over a real RAM plus file hybrid body store.

    Args:
        tmp_path: pytest's per-test temporary directory.

    Returns:
        The tracked :class:`InstanceContext`.
    """
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


def _hashes_for(data: bytes) -> BodyHashes:
    """Return the dual hash for ``data`` under the ``original`` storage encoding.

    Args:
        data: The raw body bytes.

    Returns:
        The :class:`BodyHashes` pair. Both halves are the same digest because the
        row declares ``storage_encoding="original"``, so the stored bytes are the
        raw bytes.
    """
    digest = hashlib.sha256(data).hexdigest()
    return BodyHashes(body_hash=BodyHash(digest), storage_hash=StorageHash(digest))


def _multipart_row(declared: dict[str, bytes]) -> UploadRow:
    """Build an ``attempting`` row declaring one ``BodyHashes`` entry per ref.

    Args:
        declared: The refs the ROW claims exist, keyed by name, with the bytes
            those hashes are computed over. What the STORE actually holds is set
            up separately by each test; the gap between the two is F2's subject.

    Returns:
        The persisted-shape :class:`UploadRow`.
    """
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
            "body_location": "file",
            "received_at": now,
            "updated_at": now,
            "endpoint": "files.example.com",
            "uid": "user-1",
            "chain_envelope_json": "{}",
            "idempotency_key": "k",
            "capture_reexecution_active": False,
            "body_size_bytes": sum(len(v) for v in declared.values()),
            "storage_encoding": "original",
            "body_hashes": {name: _hashes_for(data) for name, data in declared.items()},
        },
    )


def _both_parts() -> dict[str, bytes]:
    """Return the full two-ref declaration used by most tests."""
    return {PART_ONE: PART_ONE_BYTES, PART_TWO: PART_TWO_BYTES}


async def test_load_body_refs_raises_when_one_declared_ref_is_absent(tmp_path: Path) -> None:
    """The load boundary detects a PARTIAL body.

    Objective: a chain directory holding one of two declared refs must raise
    rather than return a short dict. Success: ``BodyMissingError`` naming the
    row's chain_id and exactly the absent ref.
    """
    instance = await _build_instance(tmp_path)
    row = _multipart_row(_both_parts())
    await instance.store.insert(row)
    # Only part1 is written: the chain directory exists and is INCOMPLETE.
    await instance.file_body_store.put(row.chain_id, {PART_ONE: PART_ONE_BYTES})

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    with pytest.raises(BodyMissingError) as exc_info:
        await sender._load_body_refs(row)

    assert exc_info.value.chain_id == row.chain_id
    assert exc_info.value.missing == [PART_TWO]


async def test_empty_chain_directory_is_treated_as_a_missing_body(tmp_path: Path) -> None:
    """The second hole shape: an intact but EMPTY chain directory.

    Objective: cover the zero-entry traversal path, which is distinct from the
    partial-listing path and equally silent before F2. Success:
    ``BodyMissingError`` whose missing list is the full sorted declared set.
    """
    instance = await _build_instance(tmp_path)
    row = _multipart_row(_both_parts())
    await instance.store.insert(row)
    # The directory exists and holds nothing, so the traversal returns {}.
    instance.file_body_store.path_for(row.chain_id, PART_ONE).parent.mkdir(
        parents=True, exist_ok=True
    )

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    with pytest.raises(BodyMissingError) as exc_info:
        await sender._load_body_refs(row)

    assert exc_info.value.missing == [PART_ONE, PART_TWO]


async def test_drive_one_routes_a_partial_body_to_corrupted(tmp_path: Path) -> None:
    """A partial body reaches ADR-014's terminal state, not a truncated delivery.

    Objective: the shortfall must land the row in ``corrupted`` through the
    existing ``BodyMissingError`` handler, not forward short bytes as
    ``succeeded`` and not terminate ``failed`` blaming the producer's template.
    Success: the re-read row is ``corrupted``, its ``last_error`` carries the
    storage-corruption prefix, the in-sender token, and the missing ref's name,
    and no retry is scheduled.
    """
    instance = await _build_instance(tmp_path)
    row = _multipart_row(_both_parts())
    await instance.store.insert(row)
    await instance.file_body_store.put(row.chain_id, {PART_ONE: PART_ONE_BYTES})

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._drive_one(instance.store, row)

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "corrupted"
    assert fresh.last_error is not None
    assert fresh.last_error.startswith("storage_corruption:")
    assert "body_missing_in_sender" in fresh.last_error
    assert PART_TWO in fresh.last_error
    assert fresh.next_attempt_at is None


async def test_partial_body_releases_the_saturation_slot(tmp_path: Path) -> None:
    """The corrupted transition returns the slot.

    Objective: ``corrupted`` is not in ``SLOT_HOLDING_STATES``, so the existing
    ``_on_corrupted`` release must run on this path too. Success: the gate is
    back to zero rows and zero bytes after the drive.
    """
    instance = await _build_instance(tmp_path)
    row = _multipart_row(_both_parts())
    await instance.store.insert(row)
    await instance.file_body_store.put(row.chain_id, {PART_ONE: PART_ONE_BYTES})
    admitted = await instance.saturation.admit(row.body_size_bytes)
    assert admitted.__class__.__name__ == "AdmissionGranted", admitted

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._drive_one(instance.store, row)

    assert instance.saturation.in_flight == 0
    assert instance.saturation.in_flight_bytes == 0


async def test_all_declared_refs_present_is_not_flagged(tmp_path: Path) -> None:
    """Counter-test: a COMPLETE body must not be flagged.

    Objective: the new check must be a shortfall check, not a blanket refusal.
    Success: both refs load and decode back to their original bytes.
    """
    instance = await _build_instance(tmp_path)
    row = _multipart_row(_both_parts())
    await instance.store.insert(row)
    await instance.file_body_store.put(
        row.chain_id, {PART_ONE: PART_ONE_BYTES, PART_TWO: PART_TWO_BYTES}
    )

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    refs = await sender._load_body_refs(row)

    assert refs == {PART_ONE: PART_ONE_BYTES, PART_TWO: PART_TWO_BYTES}


async def test_extra_returned_ref_still_raises_storage_corruption(tmp_path: Path) -> None:
    """Pin the OTHER direction of the comparison so a refactor cannot drop it.

    Objective: a ref the store returns but the row never declared has no
    expected hash, so it cannot be verified and must terminate the row. That
    branch predates F2 and must survive it; collapsing the two directions into
    one comparison would silently remove it. Success: ``StorageCorruptionError``.
    """
    instance = await _build_instance(tmp_path)
    row = _multipart_row({PART_ONE: PART_ONE_BYTES})
    await instance.store.insert(row)
    await instance.file_body_store.put(
        row.chain_id, {PART_ONE: PART_ONE_BYTES, STOWAWAY: b"undeclared"}
    )

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    with pytest.raises(StorageCorruptionError):
        await sender._load_body_refs(row)
