"""HybridBodyStore — RAM-first writes, RAM-presence-authoritative reads.

Production-default :class:`BodyStore` binding (Settings
``storage.body_store.mode='hybrid'``). Composes :class:`RamBodyStore`
and :class:`FileBodyStore`. The migration from RAM to disk is owned
exclusively by the :class:`PersistController` (plan § 0.5 single-writer
manifest invariant #6) — HybridBodyStore reads runtime authority off
:class:`RamBodyStore` presence; it never inspects ``body_location`` on
the SQLite row.

Plan § 2.3.9. The :class:`BodyStore` Protocol surface this
class implements is the one narrowed in
``storage/interface.py``: no ``tier`` property; ``list_orphans`` present.
"""

from __future__ import annotations

import contextlib
import logging
from uuid import UUID

from phantom.storage.file_body_store import FileBodyStore
from phantom.storage.ram_body_store import RamBodyStore

logger = logging.getLogger(__name__)


class HybridBodyStore:
    """Compose :class:`RamBodyStore` + :class:`FileBodyStore`.

    Writes are RAM-first. The :class:`PersistController` (plan § 2.3.11)
    is the SOLE component that migrates bodies to disk; it reads from
    the RAM half, writes to the file half, and flips the SQLite
    ``body_location`` column.

    Reads consult RAM first; on RAM miss, fall through to disk. The
    runtime authority is body presence on the underlying stores — this
    class does NOT consult the SQLite ``body_location`` column at read
    time. Per plan § 2.3.9: a row's ``body_location`` is the
    *persistence-side* truth, but body bytes can transiently be on
    either side of the migration window, and the in-flight reader
    needs the bytes regardless of which side currently owns them.

    Atomicity note: between :meth:`has_body_ref` and :meth:`get`, the
    persist controller could (in principle) drop the RAM body. In
    practice the persist controller (plan § 2.3.11) calls
    :meth:`FileBodyStore.put` (which fsyncs body file + parent
    directory) BEFORE deleting from :class:`RamBodyStore`, so the disk
    fallback always succeeds when the race triggers.
    """

    def __init__(self, *, ram: RamBodyStore, disk: FileBodyStore) -> None:
        """Construct the hybrid store.

        Args:
            ram: The RAM-tier component. Receives every :meth:`put` by
                default; produces :meth:`total_bytes` for the
                saturation gate / RAM-pressure watcher.
            disk: The file-tier component. Receives bodies only via
                the persist controller's migration; produces
                :meth:`list_orphans` for the body-orphan janitor.
        """
        self._ram = ram
        self._disk = disk

    async def start(self) -> None:
        """Start both underlying stores."""
        await self._ram.start()
        await self._disk.start()

    async def stop(self) -> None:
        """Stop both underlying stores."""
        await self._ram.stop()
        await self._disk.stop()

    async def put(self, chain_id: UUID, body_refs: dict[str, bytes]) -> int:
        """RAM-first write.

        The persist controller (plan § 2.3.11) is the SOLE migrator to
        disk. Admission's size-threshold path (plan § 2.3.10
        ``body_size_threshold_bytes``) routes large bodies to direct
        disk-write by enqueueing against the persist controller
        immediately after this :meth:`put` returns — never by branching
        inside HybridBodyStore.

        Namespace semantics (R11-a): this put REPLACES the RAM half's
        chain entry (:class:`RamBodyStore.put`) and leaves the DISK
        half untouched, so stale disk files for a reused chain_id are
        masked only while the fresh entry is RAM-resident and would
        resurface once the body migrates off RAM. Admission's R11-1
        namespace clear (:meth:`delete`, which fans to BOTH halves)
        runs before every admission put precisely so that disk residue
        cannot exist for a newly admitted row.

        Args:
            chain_id: The owning upload's chain identifier.
            body_refs: Mapping of body_ref name → bytes.

        Returns:
            Total bytes stored (RAM, pre-migration).
        """
        return await self._ram.put(chain_id, body_refs)

    async def get(self, chain_id: UUID, name: str) -> bytes:
        """Read one named body_ref.

        Checks RAM first; on RAM miss falls through to disk. See class
        docstring for the atomicity argument that keeps this race-free
        against the persist controller's migration window.

        A :class:`KeyError` raised by :meth:`RamBodyStore.get` is the
        race window: the persist controller cleared the RAM entry
        between :meth:`has_body_ref` and :meth:`get`. The file is
        durable by then (commit-last-column ordering — plan § 0.5), so
        falling through to disk gives the bytes. The
        :func:`contextlib.suppress` makes the documented fall-through
        explicit (not a "silent KeyError swallow" — H8 closure pattern).
        """
        if await self._ram.has_body_ref(chain_id, name):
            with contextlib.suppress(KeyError):
                return await self._ram.get(chain_id, name)
        return await self._disk.get(chain_id, name)

    async def get_all(self, chain_id: UUID) -> dict[str, bytes]:
        """Read every body_ref for ``chain_id``.

        Same atomicity argument as :meth:`get`. :class:`FileBodyStore`
        is the durable fallback once the persist controller's
        commit-last-column flip lands. RAM-side :class:`KeyError` (no
        entry for ``chain_id``) is the explicit fall-through-to-disk
        path — see :func:`contextlib.suppress` rationale on :meth:`get`.
        """
        with contextlib.suppress(KeyError):
            ram_refs = await self._ram.get_all(chain_id)
            if ram_refs:
                return ram_refs
        return await self._disk.get_all(chain_id)

    async def has_body_ref(self, chain_id: UUID, name: str) -> bool:
        """Return whether either underlying store has the body_ref."""
        if await self._ram.has_body_ref(chain_id, name):
            return True
        return await self._disk.has_body_ref(chain_id, name)

    async def delete(self, chain_id: UUID) -> None:
        """Drop the body from BOTH stores. Idempotent.

        Per plan § 2.3.9: RAM-first delete keeps the invariant — a
        racing reader sees the RAM body gone first and falls through
        to disk before disk is also gone. ``RamBodyStore.delete`` and
        ``FileBodyStore.delete`` are both idempotent so a missing
        entry on either side is fine.
        """
        await self._ram.delete(chain_id)
        await self._disk.delete(chain_id)

    async def total_bytes(self) -> int:
        """Return RAM-tier bytes only — the saturation / RAM-pressure metric.

        Disk-tier bytes are bounded by the filesystem and tracked
        separately by :class:`DiskPressureProbe` (per
        ``saturation.max_disk_bytes``); they are not relevant to the
        in-flight saturation gate.
        """
        return await self._ram.total_bytes()

    async def list_chain_ids(self) -> list[UUID]:
        """Return the union of RAM and disk chain_ids.

        Used by the body-orphan janitor (plan § 2.3.14)
        when it builds the "known chain_ids" set against
        :meth:`list_orphans`. The union is sorted-deduplicated so
        callers iterating it cannot accidentally double-count a chain
        held in both halves (during the migration window).
        """
        ram_ids = await self._ram.list_chain_ids()
        disk_ids = await self._disk.list_chain_ids()
        return sorted(set(ram_ids) | set(disk_ids))

    async def list_orphans(self, known_chain_ids: set[UUID]) -> list[UUID]:
        """Return file-tier orphans.

        RAM-tier orphans are impossible by construction (see
        :meth:`RamBodyStore.list_orphans`); disk-tier orphans are the
        body-orphan janitor's target (plan § 2.3.14).
        """
        return await self._disk.list_orphans(known_chain_ids)
