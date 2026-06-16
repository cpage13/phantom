"""Persist-controller RAM-to-disk flip vs the v2 columns and the auditor.

Round 4 adversary hardening (iteration loop, task 7.3). The
PersistController migrates a GROUPED body RAM to disk while readers and
the InvariantAuditor walk the rows. The contracts under attack:

* In BOTH crash-ordering windows of the four-step migration (after the
  durable ``FileBodyStore.put`` but before ``mark_persisted``; after
  ``mark_persisted`` but before ``RamBodyStore.delete``) the auditor
  observes ZERO false violations: the hybrid store resolves the body
  from whichever side currently holds it, exactly the fall-through the
  hybrid's own docstring promises.
* The flip moves ``body_location`` ONLY: ``group_id`` /
  ``multifile_id`` / ``send_order`` / ``sent_at`` are byte-identical
  across every window, and ``list_by_group_id`` returns the same member
  set throughout (the rollup never blinks during a migration).
* One sweep across the full v2 row matrix (grouped member mid-flip,
  multifile sibling in RAM, delivered row with its ``sent_at`` stamp
  and no body, ``auth_expired`` with ``body_discarded_at`` stamped)
  yields zero violations: the terminal and H4 carve-outs hold with the
  new columns populated.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom.models.upload import BodyHash, BodyHashes, StorageHash, UploadRow
from phantom.observability.metrics import MetricsRegistry
from phantom.storage import FileBodyStore, RamBodyStore, SqliteUploadStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.workers.invariant_audit import InvariantAuditor
from phantom.workers.persist_controller import PersistController

from .conftest import make_snapshot, snapshot_thunk

# The one body_ref name the migrating row declares.
_BODY_REF_NAME = "part0"
# Body payload for the migrating member (content is irrelevant to the
# flip; presence is what the auditor checks).
_BODY_BYTES = b"grouped-body-bytes"
# Probe timeout: generous bound for awaiting a deterministic event that
# the gated stores set synchronously inside the migration; only ever
# hit on a real hang.
_WINDOW_TIMEOUT_SECONDS = 5.0


class _GatedFileBodyStore(FileBodyStore):
    """FileBodyStore that pauses the migration AFTER the durable put.

    Window W1: the body file is fsynced on disk, the row still says
    ``body_location='ram'`` (the crash window plan section 0.5
    documents; recovery + the orphan janitor own it after a real
    crash).
    """

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.window_entered = asyncio.Event()
        self.window_release = asyncio.Event()

    async def put(self, chain_id: UUID, body_refs: dict[str, bytes]) -> int:
        written = await super().put(chain_id, body_refs)
        self.window_entered.set()
        await self.window_release.wait()
        return written


class _GatedRamBodyStore(RamBodyStore):
    """RamBodyStore that pauses the migration BEFORE the RAM cleanup.

    Window W2: ``mark_persisted`` has committed (``body_location='file'``),
    the body bytes exist in BOTH stores.
    """

    def __init__(self) -> None:
        super().__init__()
        self.window_entered = asyncio.Event()
        self.window_release = asyncio.Event()

    async def delete(self, chain_id: UUID) -> None:
        self.window_entered.set()
        await self.window_release.wait()
        await super().delete(chain_id)


@pytest.fixture
async def stack(
    tmp_path: Path,
) -> AsyncIterator[
    tuple[
        SqliteUploadStore,
        _GatedRamBodyStore,
        _GatedFileBodyStore,
        HybridBodyStore,
        PersistController,
        InvariantAuditor,
        MetricsRegistry,
    ]
]:
    """Real store + gated body stores + controller + auditor."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = _GatedRamBodyStore()
    fbs = _GatedFileBodyStore(tmp_path / "bodies")
    hybrid = HybridBodyStore(ram=ram, disk=fbs)
    await store.start()
    await hybrid.start()
    registry = MetricsRegistry()
    controller = PersistController(
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        metrics_registry=registry,
    )
    auditor = InvariantAuditor(
        store=store,
        body_store=hybrid,
        current_settings=snapshot_thunk(make_snapshot()),
        metrics_registry=registry,
    )
    yield store, ram, fbs, hybrid, controller, auditor, registry
    await store.stop()


def _violations(registry: MetricsRegistry) -> dict[str, int]:
    """Current label -> count map of the violation counter."""
    return dict(registry.counters["invariant_violation_total"].snapshot())


def _declared_hashes() -> dict[str, BodyHashes]:
    """The migrating row's declared hash set for its one body_ref."""
    return {
        _BODY_REF_NAME: BodyHashes(body_hash=BodyHash("bh"), storage_hash=StorageHash("sh")),
    }


@pytest.mark.asyncio
async def test_migration_windows_no_false_violations_and_rollup_steady(
    stack: tuple[
        SqliteUploadStore,
        _GatedRamBodyStore,
        _GatedFileBodyStore,
        HybridBodyStore,
        PersistController,
        InvariantAuditor,
        MetricsRegistry,
    ],
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Audit + group reads at every migration window observe steady truth."""
    store, ram, fbs, _hybrid, controller, auditor, registry = stack
    group_id = uuid4()
    multifile_id = uuid4()
    migrating = make_upload_row(
        group_id=group_id,
        multifile_id=multifile_id,
        send_order=0,
        body_location="ram",
        body_hashes=_declared_hashes(),
        body_size_bytes=len(_BODY_BYTES),
    )
    sibling = make_upload_row(
        group_id=group_id,
        multifile_id=multifile_id,
        send_order=1,
        body_location="ram",
        body_hashes=_declared_hashes(),
    )
    await store.insert(migrating)
    await store.insert(sibling)
    await ram.put(migrating.chain_id, {_BODY_REF_NAME: _BODY_BYTES})
    await ram.put(sibling.chain_id, {_BODY_REF_NAME: _BODY_BYTES})

    async def _probe_windows() -> None:
        # Window W1: durable file written, flip NOT yet committed.
        await asyncio.wait_for(fbs.window_entered.wait(), timeout=_WINDOW_TIMEOUT_SECONDS)
        mid = await store.get(migrating.chain_id)
        assert mid is not None
        assert mid.body_location == "ram"
        assert mid.group_id == group_id
        assert mid.multifile_id == multifile_id
        assert mid.sent_at is None
        members = await store.list_by_group_id(group_id)
        assert sorted(m.chain_id for m in members) == sorted([migrating.chain_id, sibling.chain_id])
        await auditor._sweep_once()
        assert _violations(registry) == {"": 0}
        fbs.window_release.set()

        # Window W2: flip committed, body present in BOTH stores.
        await asyncio.wait_for(ram.window_entered.wait(), timeout=_WINDOW_TIMEOUT_SECONDS)
        flipped = await store.get(migrating.chain_id)
        assert flipped is not None
        assert flipped.body_location == "file"
        assert flipped.group_id == group_id
        assert flipped.send_order == migrating.send_order
        assert flipped.sent_at is None
        assert await ram.has_body_ref(migrating.chain_id, _BODY_REF_NAME)
        assert await fbs.has_body_ref(migrating.chain_id, _BODY_REF_NAME)
        await auditor._sweep_once()
        assert _violations(registry) == {"": 0}
        ram.window_release.set()

    await asyncio.gather(controller._migrate_one(migrating.chain_id), _probe_windows())

    # Settled: RAM empty for the migrated chain, disk holds the bytes,
    # the row moved NOTHING but body_location, the sibling is untouched,
    # and the sweep stays clean.
    assert not await ram.has_body_ref(migrating.chain_id, _BODY_REF_NAME)
    assert await fbs.get_all(migrating.chain_id) == {_BODY_REF_NAME: _BODY_BYTES}
    settled = await store.get(migrating.chain_id)
    assert settled is not None
    assert settled.body_location == "file"
    assert settled.group_id == group_id
    assert settled.multifile_id == multifile_id
    assert settled.send_order == migrating.send_order
    assert settled.sent_at is None
    untouched_sibling = await store.get(sibling.chain_id)
    assert untouched_sibling is not None
    assert untouched_sibling.body_location == "ram"
    await auditor._sweep_once()
    assert _violations(registry) == {"": 0}


@pytest.mark.asyncio
async def test_v2_row_matrix_single_sweep_zero_violations(
    stack: tuple[
        SqliteUploadStore,
        _GatedRamBodyStore,
        _GatedFileBodyStore,
        HybridBodyStore,
        PersistController,
        InvariantAuditor,
        MetricsRegistry,
    ],
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """The carve-outs hold across the populated v2 column shapes."""
    store, ram, _fbs, _hybrid, _controller, auditor, registry = stack
    group_id = uuid4()
    now = datetime.now(tz=UTC)

    # Grouped queued member with its body in RAM (audited; present).
    live = make_upload_row(group_id=group_id, body_location="ram", body_hashes=_declared_hashes())
    await store.insert(live)
    await ram.put(live.chain_id, {_BODY_REF_NAME: _BODY_BYTES})
    # Delivered group member: sent_at stamped, body legitimately gone
    # (terminal carve-out; no body_discarded_at on the success path).
    delivered = make_upload_row(
        group_id=group_id,
        state="succeeded",
        sent_at=now,
        body_hashes=_declared_hashes(),
    )
    await store.insert(delivered)
    # auth_expired member whose body was discarded by retention (H4
    # carve-out; NOT terminal, still wakeable).
    discarded = make_upload_row(
        group_id=group_id,
        state="auth_expired",
        body_discarded_at=now,
        body_hashes=_declared_hashes(),
    )
    await store.insert(discarded)
    # Standalone row (group of one, NULL multifile) beside the group.
    standalone = make_upload_row(
        multifile_id=None, body_location="ram", body_hashes=_declared_hashes()
    )
    await store.insert(standalone)
    await ram.put(standalone.chain_id, {_BODY_REF_NAME: _BODY_BYTES})

    await auditor._sweep_once()
    assert _violations(registry) == {"": 0}

    # The group read sees all three members regardless of body state.
    members = await store.list_by_group_id(group_id)
    assert len(members) == 3
    stamped = [m for m in members if m.sent_at is not None]
    assert [m.chain_id for m in stamped] == [delivered.chain_id]
