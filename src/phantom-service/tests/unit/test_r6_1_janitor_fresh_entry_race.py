"""R6-1 regression: the orphan janitor must never destroy live chains' bodies.

Round 6 pre-round estate verification reproduced the parked round-5
observation three times with full tracebacks: the unit suite's two-phase
lifespan tests (a3 / f2 / r36) failed nondeterministically (about 1 run
in 3 on a quiet 16-core host) with the PersistController's migration
dying on ``os.replace`` ENOENT immediately after a
``BodyOrphanJanitor sweep removed 1 orphan body entries`` log line.

Mechanism (a stale-snapshot GC race, the same TOCTOU family R5-1 fixed
in the InvariantAuditor): ``_sweep_once`` snapshots the known set
(``UploadStore.list_chain_ids``), then walks the disk
(``BodyStore.list_orphans``), then deletes. Any chain whose row landed
after the snapshot but whose body files (or freshly created destination
directory) landed before the walk was reported as an orphan and
deleted:

- mid-migration, the janitor removed the just-created
  ``<shard>/<chain>/`` destination directory, so the migration's
  ``os.replace`` raised ENOENT (missing dst parent), the worker
  crashed, and the TaskGroup tore the process down; after restart the
  still-``ram`` row was quarantined to ``corrupted``: an accepted
  upload lost.
- in ``all_disk`` mode, admission writes body files moments before the
  row commits; a sweep straddling that window deleted the
  just-admitted bodies, and the first send attempt took
  ``BodyMissingError`` to ``corrupted``: same loss, no crash needed.

The fix (both guards janitor-owned, no clock or Protocol changes):

1. **Two-sweep confirmation.** A candidate is deletable only when it
   was also a candidate on the immediately preceding sweep; a fresh
   entry's row reaches the next sweep's fresh snapshot, so it drops
   out. Real orphans are collected one cadence later, which the
   schedule-driven invariant #4 tolerates by design.
2. **Live-row re-read.** Immediately before each irreversible delete
   the live table is re-read; a chain with a row is never deleted.

Each guard has its own falsifier below; both were committed as strict
xfails pinning the defect and flipped by the fix. The original
store-level falsifier (a fresh entry must not appear in
``list_orphans``) was retargeted to the janitor when the fix settled on
two-sweep confirmation: ``list_orphans`` semantics are unchanged, and
the protected property (fresh entries survive the sweep) is asserted
where the responsible owner enforces it.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom.models.upload import BodyHashes, CapturedValues, UploadRow
from phantom.storage.file_body_store import FileBodyStore
from phantom.workers.body_orphan_janitor import BodyOrphanJanitor

from .conftest import make_snapshot, snapshot_thunk

pytestmark = pytest.mark.asyncio

_BODY = b"0123456789"


def _row(chain_id: UUID) -> UploadRow:
    """A minimal live row proving the chain exists in ``uploads``."""
    now = datetime.now(tz=UTC)
    return UploadRow(  # type: ignore[call-arg]
        chain_id=chain_id,
        instance_id="primary",
        group_id=chain_id,
        multifile_id=None,
        send_order=0,
        route_name="files",
        state="queued",
        body_location="ram",
        next_attempt_at=now,
        received_at=now,
        updated_at=now,
        endpoint="e",
        uid="u",
        chain_envelope_json="{}",
        captured_values=CapturedValues(),
        current_step_index=0,
        idempotency_key=f"r6-1-{chain_id}",
        chain_id_at_ingress=None,
        capture_reexecution_active=False,
        body_size_bytes=len(_BODY),
        storage_encoding="original",
        body_hashes={
            "body": BodyHashes(  # type: ignore[call-arg]
                body_hash="0" * 64,
                storage_hash="0" * 64,
            ),
        },
    )


class _StaleSnapshotStore:
    """UploadStore stand-in frozen at a pre-insert known-set snapshot.

    ``list_chain_ids`` answers as the janitor's snapshot saw the world
    (empty), while ``get`` answers from the live table. This is exactly
    the window the sweep occupies when a chain is admitted between its
    snapshot and its disk walk.
    """

    def __init__(self, live: dict[UUID, UploadRow]) -> None:
        self._live = live

    async def list_chain_ids(self) -> list[UUID]:
        """The stale snapshot: taken before the chain was admitted."""
        return []

    async def get(self, chain_id: UUID) -> UploadRow | None:
        """The live table."""
        return self._live.get(chain_id)


async def test_first_sighting_never_deletes_then_real_orphan_collected(
    tmp_path: Path,
) -> None:
    """A candidate's first sighting must never delete; the second may.

    The entry has no row anywhere (a true orphan to every guard except
    recency), exactly what a mid-sweep admission looks like to the
    janitor before the row commits. Sweep one must leave it untouched
    (this deleted before the R6-1 fix and crashed live migrations);
    sweep two must collect it, proving real-orphan GC still works and
    the guard is a deferral, not a hole.
    """
    body_store = FileBodyStore(root=tmp_path / "bodies")
    await body_store.start()
    chain_id = uuid4()
    await body_store.put(chain_id, {"body": _BODY})

    janitor = BodyOrphanJanitor(
        store=_StaleSnapshotStore({}),  # type: ignore[arg-type]
        body_store=body_store,
        current_settings=snapshot_thunk(make_snapshot()),
    )

    await janitor._sweep_once()
    assert await body_store.has_body_ref(chain_id, "body"), (
        "first sighting deleted the entry; a chain admitted or migrating "
        "mid-sweep would lose its bodies (R6-1)"
    )

    await janitor._sweep_once()
    assert not await body_store.has_body_ref(chain_id, "body"), (
        "second consecutive sighting of a rowless entry must collect it; "
        "the guard must defer real-orphan GC, not disable it"
    )


async def test_sweep_rereads_live_row_before_delete(tmp_path: Path) -> None:
    """A chain with a live row survives even two consecutive sightings.

    The fake store's snapshot is permanently stale (``list_chain_ids``
    never returns the chain), so the candidate passes two-sweep
    confirmation; only the live-row re-read at delete time can save it.
    Before the R6-1 fix the sweep trusted its snapshot and removed the
    bodies of a chain whose row exists.
    """
    body_store = FileBodyStore(root=tmp_path / "bodies")
    await body_store.start()
    chain_id = uuid4()
    await body_store.put(chain_id, {"body": _BODY})

    janitor = BodyOrphanJanitor(
        store=_StaleSnapshotStore({chain_id: _row(chain_id)}),  # type: ignore[arg-type]
        body_store=body_store,
        current_settings=snapshot_thunk(make_snapshot()),
    )

    # Two sweeps: candidacy is confirmed, so deletion is attempted and
    # only the live-row re-read stands between the chain and data loss.
    await janitor._sweep_once()
    await janitor._sweep_once()

    assert await body_store.has_body_ref(chain_id, "body"), (
        "the sweep deleted the bodies of a chain whose uploads row is "
        "live; an accepted upload would reach corrupted on next read"
    )
