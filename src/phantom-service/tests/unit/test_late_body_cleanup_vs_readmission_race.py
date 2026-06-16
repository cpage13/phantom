"""Late body cleanup after row removal re-checks for a new owner (R10-D1).

Invariant #17 (architecture-intent section 5) and the R8-3 adjudication:
once a row DELETE commits, a same-chain_id re-POST is legal at ANY later
instant (``chain_id_in_use`` refuses only live rows; producers deriving
stable chain_ids re-submit naturally), so no later effect keyed on that
chain_id may assume it still owns the key. The persist controller's undo
learned this in R8-3 ("no check can make a RAM delete here safe");
the orphan janitor re-reads the live table immediately before every
delete (R6-1). Two row-removal paths kept an UNGUARDED late body delete:

* ``bulk_delete_uploads`` (``routes/admin.py``): the row DELETE commits
  in one transaction, then the per-entry loop deletes bodies (the C1
  complement) - with awaits between the transaction and each entry's
  delete;
* the reaper's ``max_rows`` eviction pass (``workers/reaper.py``):
  ``evict_terminal_over_limit`` removes rows in-transaction, then the
  per-entry loop deletes bodies.

Finding history (R10-D1, found by the round-10 defender's sweep A,
pre-fix): a producer re-POST of a just-removed chain_id landing between
the row DELETE and that entry's body delete had its accepted bytes wiped
by the OLD row's cleanup - admission puts the body before the row
commits, so the late ``body_store.delete`` destroyed the new upload's
bodies in RAM and on disk alike, and the new live row corrupted on its
first claim (``BodyMissingError``, the north-star harm; exactly the
mechanism of R8-3, in the two cleanup loops the R8-3 fix did not touch).
The single-chain ``delete_upload`` route is NOT affected: it deletes
bodies BEFORE the row, while the live row still blocks any re-POST.

The fixed loops re-read the live table immediately before each body
delete (the janitor's R6-1 discipline) and step aside when a new owner
exists; the OLD row's slot release stays keyed on the accounting
captured atomically with its DELETE, which the new owner never touches.

Both tests drive the REAL route / reaper over a REAL SqliteUploadStore,
SaturationGate, and HybridBodyStore. A store wrapper lands the
re-admission (body put, then row insert - admission's documented order)
the instant the row-removal call returns, deterministically inside the
window between the row DELETE and the cleanup loop, the established
R7-2 / R8-3 / R9-5 hook technique. The guard narrows the race to the
single await between its live re-read and the delete - the same
irreducible sliver the janitor's R6-1 re-read carries (file deletion
cannot share the row store's transaction). Property: a live row is
never left bodiless by another row's cleanup.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from phantom.config.settings import (
    BodyStoreCfg,
    CompressionCfg,
    InstanceCfg,
    PersistTriggerCfg,
    RetentionCfg,
    RouteCfg,
    SaturationCfg,
)
from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.instances.snapshot import InstanceSettingsSnapshot
from phantom.models.admin import DeleteFilter
from phantom.models.upload import UploadRow
from phantom.routes import admin as admin_routes
from phantom.storage import FileBodyStore, RamBodyStore, SqliteUploadStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.workers.reaper import Reaper
from phantom.workers.saturation import SaturationGate

from .conftest import track_instance

pytestmark = pytest.mark.asyncio

# The new (re-admitted) upload's accepted bytes - the payload that must
# survive the old row's cleanup.
_NEW_BODY_BYTES: bytes = b"re-admitted-accepted-bytes"

# Declared sizes for the old and new rows.
_OLD_SIZE: int = 300
_NEW_SIZE: int = len(_NEW_BODY_BYTES)

# Generous caps so nothing in the scenario is refused for capacity.
_GATE_ROW_CAP: int = 10
_GATE_BYTE_CAP: int = 10_000_000
_GATE_DISK_CAP: int = 10_000_000

# How stale the old terminal row is (comfortably past any cutoff used).
_OLD_ROW_AGE_SECONDS: int = 3600


def _retention_evict_only() -> RetentionCfg:
    """A retention config whose ONLY active pass is the max_rows eviction."""
    return RetentionCfg(
        succeeded_metadata_seconds=-1,
        succeeded_body_seconds=-1,
        failed_metadata_seconds=-1,
        failed_body_seconds=-1,
        cancelled_metadata_seconds=-1,
        cancelled_body_seconds=-1,
        corrupted_metadata_seconds=-1,
        corrupted_body_seconds=-1,
        stored_metadata_seconds=-1,
        stored_body_seconds=-1,
        auth_expired_metadata_seconds=-1,
        auth_expired_body_seconds=-1,
        max_rows=0,
    )


def _snapshot(retention: RetentionCfg) -> InstanceSettingsSnapshot:
    """A snapshot carrying the test retention; other blocks at defaults."""
    return InstanceSettingsSnapshot(
        persist_trigger=PersistTriggerCfg(),
        body_store=BodyStoreCfg(),
        retention=retention,
        compression=CompressionCfg(),
        saturation=SaturationCfg(
            max_in_flight=_GATE_ROW_CAP,
            max_in_flight_bytes=_GATE_BYTE_CAP,
            max_disk_bytes=_GATE_DISK_CAP,
            large_body_threshold_bytes=0,
            max_large_in_flight=0,
        ),
        capture_reexecution=False,
    )


class _ReadmissionAfterRowRemoval:
    """Store wrapper landing a same-chain_id re-POST as the row removal returns.

    The row DELETE commits inside ``bulk_delete`` /
    ``evict_terminal_over_limit``; from that instant a re-POST of the
    removed chain_id is legal, and the cleanup loop has not run yet.
    This wrapper fires the re-admission ONCE, the moment the removal
    call returns non-empty - new bytes into the REAL body store, then
    the new row insert (admission's documented body-put-before-
    row-commit order) - deterministically inside that window.
    Everything else (including the guard's ``get`` re-read) proxies to
    the real store.
    """

    def __init__(self, real: SqliteUploadStore, body_store: HybridBodyStore, target: UUID) -> None:
        self._real = real
        self._body_store = body_store
        self._target = target
        self._fired = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)

    async def _land_readmission(self) -> None:
        """Put the new bytes, then insert the new row (admission's order)."""
        if self._fired:
            return
        self._fired = True
        await self._body_store.put(self._target, {"body": _NEW_BODY_BYTES})
        await self._real.insert(_row(self._target, state="queued", size=_NEW_SIZE))

    async def bulk_delete(self, **kwargs: Any) -> Any:
        """Run the real bulk delete, then land the re-POST once."""
        removed = await self._real.bulk_delete(**kwargs)
        if removed:
            await self._land_readmission()
        return removed

    async def evict_terminal_over_limit(self, max_rows: int) -> Any:
        """Run the real eviction, then land the re-POST once."""
        evicted = await self._real.evict_terminal_over_limit(max_rows)
        if evicted:
            await self._land_readmission()
        return evicted


def _row(chain_id: UUID, *, state: str, size: int, age_seconds: int = 0) -> UploadRow:
    """A row in ``state`` with one declared body_ref named ``body``."""
    stamp = datetime.now(tz=UTC) - timedelta(seconds=age_seconds)
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "emu",
            "group_id": chain_id,
            "multifile_id": None,
            "send_order": 0,
            "route_name": "files",
            "state": state,
            "body_location": "ram",
            "body_size_bytes": size,
            "received_at": stamp,
            "updated_at": stamp,
            "endpoint": "files.example.com",
            "uid": "user-1",
            "chain_envelope_json": "{}",
            "idempotency_key": str(uuid4()),
            "capture_reexecution_active": False,
            "body_hashes": {"body": {"body_hash": "x" * 64, "storage_hash": "y" * 64}},
        }
    )


async def _build_instance(tmp_path: Path, retention: RetentionCfg) -> InstanceContext:
    """A real-store instance whose cleanup loops must respect new owners."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    await store.start()
    await ram.start()
    await fbs.start()
    await body_store.start()
    cfg = InstanceCfg(
        id="emu",
        host_prefixes=["files.example.com"],
        data_dir="emu",
        routes=[RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer")],
    )
    snapshot = _snapshot(retention)
    instance = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=None,
        token_cache=MagicMock(),
        minter=None,
        retry_strategy=FixedIntervalsStrategy([1]),
        upstream_client=MagicMock(),
        executor=MagicMock(),
        saturation=SaturationGate(
            max_in_flight=_GATE_ROW_CAP,
            max_in_flight_bytes=_GATE_BYTE_CAP,
            max_disk_bytes=_GATE_DISK_CAP,
        ),
        codec_factory=MagicMock(),
        current_settings=lambda: snapshot,
    )
    return track_instance(instance)


async def _assert_new_owner_survived(instance: InstanceContext, chain_id: UUID) -> None:
    """The re-admitted live row must keep its accepted bytes."""
    final = await instance.store.get(chain_id)
    assert final is not None and final.state == "queued", (
        "precondition: the re-admitted row exists and is live (queued)"
    )
    body_present = await instance.ram_body_store.has_body_ref(chain_id, "body")
    assert body_present, (
        "the OLD row's late body cleanup wiped the re-admitted upload's "
        "accepted bytes: the live queued row is bodiless and its first claim "
        "takes BodyMissingError to corrupted (north star; the cleanup loop "
        "must re-read the live table immediately before each body delete and "
        "step aside for a new owner, the R6-1 / R8-3 discipline)"
    )


async def test_bulk_delete_body_cleanup_respects_a_new_owner(tmp_path: Path) -> None:
    """A re-POST landing inside bulk delete's cleanup loop keeps its bytes.

    Attack: a terminal ``failed`` row is bulk-deleted (routine operator
    cleanup). The row DELETE commits, legalizing a same-chain_id re-POST;
    the producer's re-admission lands the instant the cleanup loop
    reaches the old entry's body delete. The new accepted upload must
    come out deliverable.
    """
    instance = await _build_instance(tmp_path, RetentionCfg())
    chain_id = uuid4()
    real_store = instance.store
    await real_store.insert(_row(chain_id, state="failed", size=_OLD_SIZE))
    await instance.body_store.put(chain_id, {"body": b"old-bytes"})
    instance.store = _ReadmissionAfterRowRemoval(  # type: ignore[assignment]
        real_store,  # type: ignore[arg-type]
        instance.body_store,  # type: ignore[arg-type]
        chain_id,
    )
    dispatcher = InstanceDispatcher([instance])

    response = await admin_routes.bulk_delete_uploads(DeleteFilter(state="failed"), dispatcher)

    assert response.deleted == 1, "precondition: the old failed row was bulk-deleted"
    await _assert_new_owner_survived(instance, chain_id)


async def test_reaper_evict_body_cleanup_respects_a_new_owner(tmp_path: Path) -> None:
    """A re-POST landing inside the eviction's cleanup loop keeps its bytes.

    Attack: ``max_rows=0`` makes the reaper's count-cap backstop evict
    the old terminal row (every time-based pass is disabled with -1
    windows). The eviction DELETE commits, the producer's re-admission
    lands the instant the cleanup loop reaches the old entry's body
    delete, and the new accepted upload must come out deliverable.
    """
    instance = await _build_instance(tmp_path, _retention_evict_only())
    chain_id = uuid4()
    real_store = instance.store
    await real_store.insert(
        _row(chain_id, state="failed", size=_OLD_SIZE, age_seconds=_OLD_ROW_AGE_SECONDS)
    )
    await instance.body_store.put(chain_id, {"body": b"old-bytes"})
    instance.store = _ReadmissionAfterRowRemoval(  # type: ignore[assignment]
        real_store,  # type: ignore[arg-type]
        instance.body_store,  # type: ignore[arg-type]
        chain_id,
    )
    reaper = Reaper(instances=[instance])

    await reaper._sweep_instance(instance, datetime.now(tz=UTC))

    await _assert_new_owner_survived(instance, chain_id)


async def test_single_delete_blocks_readmission_by_ordering(tmp_path: Path) -> None:
    """The single-chain delete needs no guard: bodies go before the row.

    ``delete_upload`` deletes bodies while the row is still live, so a
    same-chain_id re-POST inside its window is refused by the
    ``chain_id_in_use`` pre-check; the row DELETE is the last effect.
    Pinned here so a future reordering of that route inherits the
    R10-D1 hazard loudly instead of silently.
    """
    instance = await _build_instance(tmp_path, RetentionCfg())
    chain_id = uuid4()
    await instance.store.insert(_row(chain_id, state="failed", size=_OLD_SIZE))
    await instance.body_store.put(chain_id, {"body": b"old-bytes"})
    observed: dict[str, bool] = {}

    real_body_store = instance.body_store

    class _RowStillLiveProbe:
        """Asserts the row is still live when the body delete fires."""

        def __getattr__(self, name: str) -> Any:
            return getattr(real_body_store, name)

        async def delete(self, probe_chain_id: UUID) -> None:
            row = await instance.store.get(probe_chain_id)
            observed["row_live_at_body_delete"] = row is not None
            await real_body_store.delete(probe_chain_id)

    instance.body_store = _RowStillLiveProbe()  # type: ignore[assignment]
    dispatcher = InstanceDispatcher([instance])

    response = await admin_routes.delete_upload(chain_id, dispatcher)

    assert response.status_code == 204
    assert observed.get("row_live_at_body_delete") is True, (
        "delete_upload must delete bodies BEFORE the row: the live row is "
        "what blocks a same-chain_id re-POST during the body delete (the "
        "ordering that exempts this route from the R10-D1 live-row re-read)"
    )
    assert await instance.store.get(chain_id) is None, "the row is removed last"
