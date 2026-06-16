"""PersistController resurrects a body the reaper discarded mid-migration (R7-2).

The H4 carve-out (``body_discarded_at IS NOT NULL``) marks a row whose
body the reaper dropped by retention policy. Every consumer that could
act on a stale body guards the stamp - recovery skips it
(``workers/recovery.py``), the InvariantAuditor skips it
(``workers/invariant_audit.py``), ``replay`` refuses
(``storage/sqlite_store.py``), and R6-3 added the same guard to the
AuthKicker. :meth:`SqliteUploadStore.mark_persisted` - the
PersistController's RAM->disk commit point - is the lone writer that does
NOT: its ``WHERE`` clause guards ``body_location = 'ram'`` only, not
``body_discarded_at IS NULL``.

That gap is reachable because :meth:`list_oldest_ram_bodies` (the
RAM-pressure migration-candidate query) filters on ``body_location =
'ram'`` with NO state filter, so an ``auth_expired`` (or any terminal,
body-retained) RAM row is a migration candidate exactly while it is also
a reaper body-discard candidate - the oldest stuck rows satisfy both.
The PersistController migration is multi-step (read RAM bytes, write +
fsync to disk, flip the column, drop RAM), and the reaper's body-discard
(the guarded ``discard_body_and_zero_accounting`` stamp, then
``body_store.delete`` - stamp-first since R9-5; the original finding
raced the pre-R9-5 delete-then-stamp order, but the hazard is the same
either way) can land in the gap between the controller's read and its
disk write.

Interleaving (the damaging order):

1. controller reads the RAM body bytes (``get_all``);
2. reaper stamps ``body_discarded_at`` + zeroes ``body_size_bytes`` via
   the guarded discard, then deletes the body from the store
   (``body_location`` stays ``'ram'`` - the reaper never writes that
   column);
3. controller writes the captured bytes to disk (``file.put`` - the body
   re-materializes on disk) and calls ``mark_persisted``, whose
   ``body_location = 'ram'`` guard still matches, so it flips the column
   to ``'file'`` and refreshes ``updated_at``.

The row ends ``body_discarded_at``-stamped yet ``body_location='file'``
with the discarded bytes back on disk. Three harms:

* **Retention/privacy breach.** The operator configured
  ``<state>_body_seconds`` to DROP the body (space and privacy on
  Pi-class hardware); the bytes resurface on disk and now outlive that
  window.
* **Delayed file leak.** The reaper will not re-delete it
  (``list_terminal_older_than`` filters ``body_discarded_at IS NULL``)
  and the orphan janitor will not touch it (the row still exists), so the
  file lingers until the row's METADATA window reaps the row, only then
  becoming a collectable orphan.
* **Retention-clock reset.** ``mark_persisted`` bumps ``updated_at``,
  and the metadata-delete pass keys off ``updated_at``, so the row's
  metadata window restarts - extending the leak by a full window.

The fix mirrors every other H4 consumer: add ``AND body_discarded_at IS
NULL`` to ``mark_persisted``'s ``WHERE`` (so a late migration of a
discarded row is a no-op), and re-check the stamp before the disk write.
This test pins the load-bearing guarantee - a discarded row is never
flipped to ``body_location='file'`` by a migration that raced the
discard - through the REAL PersistController, SqliteUploadStore, and
HybridBodyStore. Falsifiability proven both ways in scratch: the real
code resurrects (flip to 'file' + file on disk); the guarded
``mark_persisted`` leaves the row ``'ram'`` so recovery + the janitor
converge it.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from pathlib import Path
from uuid import UUID, uuid4

from phantom.models.upload import UploadRow
from phantom.storage.file_body_store import FileBodyStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.storage.ram_body_store import RamBodyStore
from phantom.storage.sqlite_store import SqliteUploadStore
from phantom.workers.persist_controller import PersistController

from .conftest import track_started

# Bounded wait for the migration future so a regression cannot hang the
# suite; the migration is a handful of local SQLite + file ops.
_MIGRATION_TIMEOUT_SECONDS: float = 5.0

# The single body_ref the row declares; its bytes are what must not
# resurface on disk past the reaper's discard.
_BODY_NAME: str = "a"
_BODY_BYTES: bytes = b"sensitive-bytes"


class _ReaperDiscardOnReadRamBodyStore:
    """A RamBodyStore wrapper that fires the reaper's discard inside ``get_all``.

    The PersistController reads RAM bytes first via
    ``ram_body_store.get_all`` (migration step 1). This wrapper returns
    the real RAM bytes AND, on that same call, runs the reaper's
    body-discard sequence against the real store + the real hybrid body
    store - deterministically placing the discard in the gap between the
    controller's read and its disk write, exactly the production race the
    finding describes. ``delete`` (migration step 4) delegates to the
    real RAM store; nothing else on the RAM surface is touched by the
    controller.
    """

    def __init__(
        self,
        *,
        real_ram: RamBodyStore,
        real_hybrid: HybridBodyStore,
        store: SqliteUploadStore,
        target: UUID,
    ) -> None:
        self._real_ram = real_ram
        self._real_hybrid = real_hybrid
        self._store = store
        self._target = target
        self._discard_fired = False

    async def get_all(self, chain_id: UUID) -> dict[str, bytes]:
        """Return the real RAM bytes, then run the reaper discard once."""
        captured = await self._real_ram.get_all(chain_id)
        if chain_id == self._target and not self._discard_fired:
            self._discard_fired = True
            # Reaper body-discard pass: stamp the row via the guarded
            # discard, then delete the body through the instance
            # BodyStore (stamp-first, the reaper's R9-5 order).
            await self._store.discard_body_and_zero_accounting(
                chain_id, expected_state="auth_expired"
            )
            await self._real_hybrid.delete(chain_id)
        return captured

    async def delete(self, chain_id: UUID) -> None:
        """Delegate to the real RAM store (migration cleanup step)."""
        await self._real_ram.delete(chain_id)


def _auth_expired_ram_row(make_row: Callable[..., UploadRow], chain_id: UUID) -> UploadRow:
    """An ``auth_expired`` RAM-resident row with one declared body_ref."""
    return make_row(
        chain_id=chain_id,
        state="auth_expired",
        body_location="ram",
        body_size_bytes=len(_BODY_BYTES),
        body_hashes={_BODY_NAME: {"body_hash": "x" * 64, "storage_hash": "y" * 64}},
    )


async def test_persist_migration_does_not_resurrect_a_discarded_body(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """A body the reaper discarded mid-migration must not be flipped to disk.

    Attack: an ``auth_expired`` RAM row (a real RAM-pressure migration
    candidate via ``list_oldest_ram_bodies``, which has no state filter)
    is enqueued on the REAL PersistController. The body store wrapper
    fires the reaper's real body-discard sequence inside the controller's
    ``get_all`` - between the controller's read and its disk write - the
    exact production interleaving. After the migration, the row must NOT
    be ``body_location='file'`` with the discarded bytes back on disk: a
    discarded body is gone, and ``mark_persisted`` must refuse a discarded
    row the way every other H4 consumer does.

    Before the R7-2 fix the migration wrote the captured bytes to disk
    and ``mark_persisted`` (guarding only ``body_location='ram'``, which
    the reaper left untouched) flipped the column to ``'file'``,
    resurrecting the body. The fix adds the ``body_discarded_at IS
    NULL`` guard, returns the rowcount, and the controller undoes the
    raced disk write on rowcount 0 (plus skips already-discarded rows
    up front).
    """
    store = track_started(SqliteUploadStore(str(tmp_path / "uploads.db")))
    await store.start()
    ram = track_started(RamBodyStore())
    await ram.start()
    file_bs = track_started(FileBodyStore(tmp_path / "bodies", shard_prefix_chars=2))
    await file_bs.start()
    real_hybrid = HybridBodyStore(ram=ram, disk=file_bs)

    chain_id = uuid4()
    await store.insert(_auth_expired_ram_row(make_upload_row, chain_id))
    await ram.put(chain_id, {_BODY_NAME: _BODY_BYTES})

    racing_ram = _ReaperDiscardOnReadRamBodyStore(
        real_ram=ram,
        real_hybrid=real_hybrid,
        store=store,
        target=chain_id,
    )
    controller = PersistController(
        store=store,
        # The controller reads RAM bytes via this wrapper's get_all, which
        # fires the reaper discard in the read->write gap; the file store
        # and persistent store are the real components.
        ram_body_store=racing_ram,  # type: ignore[arg-type]  # duck-typed RAM-store surface
        file_body_store=file_bs,
    )

    handle = await controller.enqueue(chain_id)
    task = asyncio.create_task(controller.run(asyncio.Event()))
    try:
        await asyncio.wait_for(handle, timeout=_MIGRATION_TIMEOUT_SECONDS)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    final = await store.get(chain_id)
    assert final is not None
    discarded = final.body_discarded_at is not None
    file_present = await file_bs.has_body_ref(chain_id, _BODY_NAME)
    assert not (discarded and final.body_location == "file" and file_present), (
        "the reaper discarded this row's body (body_discarded_at stamped), "
        "yet the racing PersistController migration flipped body_location to "
        f"'file' (={final.body_location!r}) and re-materialized the bytes on "
        f"disk (present={file_present}) - mark_persisted must carry the H4 "
        "body_discarded_at guard every other consumer has so a discarded "
        "body is never resurrected"
    )
