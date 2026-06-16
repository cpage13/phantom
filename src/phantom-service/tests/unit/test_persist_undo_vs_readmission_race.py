"""The R7-2 undo path must not wipe a freshly re-admitted upload's body (R8-3).

The R7-2 fix gave the PersistController an undo leg: when
``mark_persisted`` returns rowcount 0 (the row was body-discarded or
deleted mid-migration), the controller deletes the raced disk write AND
the RAM remnant (``file.delete`` then ``ram.delete``,
``workers/persist_controller.py``). Both deletes key on the chain_id
alone - neither re-validates what the chain_id refers to NOW.

That is the same stale-identity TOCTOU family as R5-1/R6-1 (act on a
snapshot judgment without re-reading the live row before an
irreversible delete), opened one layer deeper by the fix itself:

1. A terminal body-retained RAM row (e.g. ``failed``) is a migration
   candidate (``list_oldest_ram_bodies`` has no state filter); the
   controller's pre-check passes and it captures the RAM bytes.
2. The row is deleted mid-migration - an admin bulk delete clearing
   failed rows is the routine trigger (it removes the row AND its body
   files; ``chain_id_in_use`` only refuses while a live row exists).
3. The controller's ``file.put`` + ``mark_persisted`` run; rowcount 0
   correctly reports the deletion.
4. The producer re-POSTs the SAME chain_id - legal the moment the row
   is gone, and natural for producers that derive stable chain_ids
   (re-submitting after an operator cleanup). Admission writes the new
   body bytes into RAM and acks 202: the upload is ACCEPTED. The whole
   migration stretch after step 2 (a disk fsync plus a SQLite write
   transaction plus the undo's file unlink) is the landing window.
5. The undo now runs ``ram.delete(chain_id)`` - and wipes the NEW
   upload's RAM bytes. The new row stays ``body_location='ram'`` with
   nothing in any store; the sender's first claim takes
   ``BodyMissingError`` to ``corrupted``
   (``last_error="storage_corruption:bodies_missing"``).

Why it matters: an upload Phantom acked with 202 is silently destroyed
by a janitorial code path - the north star ("never lose an accepted
upload") violated, surfaced as a misleading storage-corruption terminal
state on healthy hardware. R6-1 fixed exactly this shape in the orphan
janitor with a live-row re-read before each delete; the undo leg needs
the same discipline (re-read the row before undoing: only delete bytes
when the chain_id still refers to the raced-away row, or scope the RAM
delete to the captured generation).

The test drives the REAL PersistController, SqliteUploadStore,
RamBodyStore, FileBodyStore, and HybridBodyStore. Two hooks place the
production interleaving deterministically (the established R7-2
technique): the RAM read hook fires the bulk-delete mid-migration; the
file-store delete hook (the undo's first step) fires the producer's
re-admission, so the undo's ``ram.delete`` lands after the new bytes -
the exact ordering the landing window above makes routine.
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

# The single body_ref both generations declare.
_BODY_NAME: str = "a"

# First generation: the failed row's bytes the operator's bulk delete
# drops mid-migration.
_GEN1_BODY_BYTES: bytes = b"first-generation-bytes"

# Second generation: the re-admitted upload's bytes - the accepted body
# that must survive the undo.
_GEN2_BODY_BYTES: bytes = b"second-generation-accepted-bytes"

# R8-3 (fixed): the undo removes only its own disk artifact and never
# touches RAM; a re-admitted upload's accepted body survives.


class _BulkDeleteOnReadRamBodyStore:
    """RamBodyStore wrapper firing an admin bulk delete inside ``get_all``.

    Step 1 of the migration reads RAM bytes through this wrapper; on
    that call it runs the bulk-delete sequence (body files via the
    hybrid store, then the row) against the real components -
    deterministically placing the deletion in the gap between the
    controller's pre-check and its disk write. ``delete`` delegates to
    the real RAM store: it IS the undo step under test.
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
        self._fired = False

    async def get_all(self, chain_id: UUID) -> dict[str, bytes]:
        """Return the real RAM bytes, then run the admin removal once."""
        captured = await self._real_ram.get_all(chain_id)
        if chain_id == self._target and not self._fired:
            self._fired = True
            # Admin removal mid-migration, body-then-row (the single
            # delete route's order; the bulk route removes rows first
            # with the C1 body deletes following, R10-D1-guarded).
            # Either order lands both effects inside the controller's
            # read->write gap, which is all this hook needs.
            await self._real_hybrid.delete(chain_id)
            await self._store.delete(chain_id)
        return captured

    async def delete(self, chain_id: UUID) -> None:
        """Delegate to the real RAM store (the undo's RAM step)."""
        await self._real_ram.delete(chain_id)


class _ReadmissionOnDeleteFileBodyStore:
    """FileBodyStore wrapper firing the producer's re-POST inside ``delete``.

    The undo's first step is ``file.delete``; on that call this wrapper
    first lands the re-admission (new row INSERT + RAM body put -
    admission's documented order is body put before the row commit,
    ``routes/admission.py``), then delegates. The controller's next
    undo step (``ram.delete``) therefore lands AFTER the new bytes,
    exactly the production ordering whenever the re-POST arrives inside
    the migration's post-deletion stretch.
    """

    def __init__(
        self,
        *,
        real_file: FileBodyStore,
        real_ram: RamBodyStore,
        store: SqliteUploadStore,
        target: UUID,
        readmitted_row: UploadRow,
    ) -> None:
        self._real_file = real_file
        self._real_ram = real_ram
        self._store = store
        self._target = target
        self._readmitted_row = readmitted_row
        self._fired = False

    async def put(self, chain_id: UUID, body_refs: dict[str, bytes]) -> int:
        """Delegate to the real file store (migration step 2)."""
        return await self._real_file.put(chain_id, body_refs)

    async def delete(self, chain_id: UUID) -> None:
        """Land the re-admission, then delegate the undo's file delete."""
        if chain_id == self._target and not self._fired:
            self._fired = True
            # The producer's re-POST: RAM body put, then the row commit
            # (admission's order). 202 acked - the upload is accepted.
            await self._real_ram.put(chain_id, {_BODY_NAME: _GEN2_BODY_BYTES})
            await self._store.insert(self._readmitted_row)
        await self._real_file.delete(chain_id)


def _ram_row(
    make_row: Callable[..., UploadRow],
    chain_id: UUID,
    *,
    state: str,
    body_bytes: bytes,
) -> UploadRow:
    """A RAM-resident row with one declared body_ref of ``body_bytes``."""
    return make_row(
        chain_id=chain_id,
        state=state,
        body_location="ram",
        body_size_bytes=len(body_bytes),
        body_hashes={_BODY_NAME: {"body_hash": "x" * 64, "storage_hash": "y" * 64}},
    )


async def test_persist_undo_does_not_wipe_a_readmitted_uploads_body(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """A re-admitted upload's RAM body must survive the migration undo.

    Attack: a ``failed`` RAM row (a real migration candidate) is
    enqueued on the REAL PersistController. The RAM read hook fires an
    admin bulk delete mid-migration (row + body files gone, rowcount 0
    at the commit point - the undo leg engages, correctly). The
    file-store delete hook lands a same-chain_id re-admission inside
    the undo window. After the migration, the re-admitted row must
    still have its RAM body: the undo may only remove bytes belonging
    to the raced-away generation, never a body admission just accepted.

    Today the undo's unconditional ``ram.delete(chain_id)`` wipes the
    new bytes: the row sits at ``body_location='ram'`` with every store
    empty, and the sender's first claim drives the accepted upload to
    ``corrupted`` - data loss reported as hardware corruption.
    """
    store = track_started(SqliteUploadStore(str(tmp_path / "uploads.db")))
    await store.start()
    ram = track_started(RamBodyStore())
    await ram.start()
    file_bs = track_started(FileBodyStore(tmp_path / "bodies", shard_prefix_chars=2))
    await file_bs.start()
    real_hybrid = HybridBodyStore(ram=ram, disk=file_bs)

    chain_id = uuid4()
    await store.insert(
        _ram_row(make_upload_row, chain_id, state="failed", body_bytes=_GEN1_BODY_BYTES)
    )
    await ram.put(chain_id, {_BODY_NAME: _GEN1_BODY_BYTES})

    readmitted = _ram_row(make_upload_row, chain_id, state="queued", body_bytes=_GEN2_BODY_BYTES)
    racing_ram = _BulkDeleteOnReadRamBodyStore(
        real_ram=ram,
        real_hybrid=real_hybrid,
        store=store,
        target=chain_id,
    )
    racing_file = _ReadmissionOnDeleteFileBodyStore(
        real_file=file_bs,
        real_ram=ram,
        store=store,
        target=chain_id,
        readmitted_row=readmitted,
    )
    controller = PersistController(
        store=store,
        ram_body_store=racing_ram,  # type: ignore[arg-type]  # duck-typed RAM-store surface
        file_body_store=racing_file,  # type: ignore[arg-type]  # duck-typed file-store surface
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
    assert final is not None, "the re-admitted row must exist (it was acked 202)"
    assert final.body_location == "ram", "the re-admitted row is RAM-resident by admission"
    body_survives = await ram.has_body_ref(chain_id, _BODY_NAME)
    assert body_survives, (
        "the undo leg wiped the re-admitted upload's RAM body: the row is "
        "body_location='ram' with no bytes in any store, so the sender's "
        "first claim will take BodyMissingError to corrupted - an ACCEPTED "
        "upload destroyed by the migration undo; the undo must re-validate "
        "the live row (R6-1 discipline) before its irreversible deletes"
    )
