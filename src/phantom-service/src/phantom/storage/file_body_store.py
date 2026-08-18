"""Atomic-rename, fsync'd body store for the persisted body files.

Layout: ``<root>/<sharded-prefix>/<chain_id>/<name>``. Each named
body_ref is written via ``aiofiles`` to a tmp file, fsync'd off-loop
via ``asyncio.to_thread(os.fsync)``, then renamed into place. A
parent-directory fsync (also off-loop) follows the rename on Linux —
NTFS journals metadata so this is a no-op on Windows.

The full durability contract, which callers rely on for the
commit-last-column invariant (F10): every directory level this store
CREATES has its own parent fsynced while the leaf is new, and each chain
directory is fsynced after its body files are renamed in. So the entire
link chain from the store root down to each body file is durable before
``put()`` returns, which is before admission commits
``body_location='file'``. A directory entry is durable only once the
directory holding it has been fsynced, and ``makedirs`` leaves new
entries in their parents' dirty page cache, so creating the levels is
not enough on its own.

The ancestor sweep in ``_makedirs_durable`` is UNCONDITIONAL whenever
the leaf is new, rather than filtered to the levels the call observed as
missing, because that observation is a time-of-check window: two
concurrent puts run in separate ``asyncio.to_thread`` workers, so one
can create a shard and be descheduled before its root fsync while the
other sees the shard already present, fsyncs one level, writes its
bodies and returns. A power cut then loses the shard's entry in the root
and every body underneath it. Sweeping unconditionally means whichever
caller goes on to write bodies has itself made the whole ancestor chain
durable, depending on no other caller's fsync.

All blocking file system calls (``os.fsync``, parent-dir fsync) run
via ``asyncio.to_thread``; ``aiofiles`` covers open/write/close.

Plan § 2.3.8 dropped the ``tier`` property — the
Protocol no longer carries it. :meth:`list_orphans` is unchanged.

Phase 4 § 5.2.3 (WS-4 Finding 9): :meth:`start` purges ``.tmp/``
orphan files left behind by a crash mid-write so subsequent
admissions stage into a clean directory.

The stored-byte total is a RUNNING COUNTER (CL6), not a query and not a
per-read tree walk. :meth:`start` seeds it with one walk immediately after
that purge, so it counts orphaned body files a SQL sum over ``uploads``
would miss, and :meth:`put` and :meth:`delete` then adjust it by what the
tree actually gained or lost.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path
from uuid import UUID

import aiofiles  # type: ignore[import-untyped]  # types-aiofiles not in workspace dev deps
import aiofiles.os  # type: ignore[import-untyped]  # types-aiofiles not in workspace dev deps

logger = logging.getLogger(__name__)

# Chunk size for streaming body writes. 1 MiB balances throughput
# against memory headroom for large uploads.
_CHUNK_BYTES = 1_048_576


def _sync_directory(path: Path) -> None:
    """Fsync ``path`` (a directory) on Linux; no-op on Windows.

    NTFS journals metadata immediately, so an explicit dir-fsync is
    unnecessary on Windows. Linux ext4/F2FS need this to make a rename
    durable across power loss.
    """
    if sys.platform.startswith("win"):
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _makedirs_durable(leaf: Path, boundary: Path) -> None:
    """Create ``leaf`` and fsync the parent of every level down from ``boundary``.

    A directory entry is durable only once the directory HOLDING it has been
    fsynced. ``os.makedirs`` creates each missing level but leaves the new
    entries in their parents' dirty page cache, so a power cut after the body
    files themselves are fsynced can still lose the links that make them
    reachable.

    When ``leaf`` did not exist on entry, this fsyncs the parent of EVERY
    level between ``boundary`` (exclusive) and ``leaf`` (inclusive),
    shallowest first, without conditioning on which levels this call
    happened to observe as missing. That unconditional sweep is what makes
    the helper safe under concurrent puts; see the module docstring. When
    ``leaf`` already existed, no level was created by anyone in this call and
    nothing is fsynced.

    Args:
        leaf: The directory to create. Must be under ``boundary``.
        boundary: The deepest directory the caller guarantees already exists.
            This helper fsyncs the boundary directory itself when it creates
            the first level beneath it, and it never creates or modifies
            anything above the boundary.

    Raises:
        ValueError: If ``leaf`` is not under ``boundary``.
    """
    relative = leaf.relative_to(boundary)
    levels = [boundary.joinpath(*relative.parts[: i + 1]) for i in range(len(relative.parts))]
    leaf_existed = leaf.exists()
    os.makedirs(leaf, exist_ok=True)
    if leaf_existed:
        return
    for level in levels:
        _sync_directory(level.parent)


def _fsync_file(fd: int) -> None:
    """Fsync a single file descriptor (blocking)."""
    os.fsync(fd)


def _replace(src: Path, dst: Path) -> None:
    """Atomic-rename ``src`` -> ``dst`` (blocking)."""
    os.replace(src, dst)


def _rm_rf(path: Path) -> int:
    """Recursively remove ``path``; tolerant of partial trees.

    Returns:
        The summed size of the files actually unlinked, which is what
        :meth:`FileBodyStore.delete` subtracts from the running total (CL6).
        A file that vanished between the ``stat`` and the ``unlink`` counts
        as zero rather than raising, matching the walk's own tolerance.
    """
    if not path.exists():
        return 0
    if path.is_file():
        try:
            removed = path.stat().st_size
        except OSError:  # vanished under us
            removed = 0
        path.unlink(missing_ok=True)
        return removed
    freed = 0
    for entry in path.iterdir():
        freed += _rm_rf(entry)
    path.rmdir()
    return freed


def _purge_tmp_orphans(tmp_dir: Path) -> None:
    """Delete every entry under ``tmp_dir`` (the .tmp staging directory).

    The directory itself stays — new admissions stage atomic writes
    into it. Per-entry removal is tolerant of mid-purge vanishing
    (another startup raced through, or an operator hand-deleted) and
    logs (does not raise) on permission errors.

    Phase 4 § 5.2.3 / WS-4 Finding 9.
    """
    if not tmp_dir.exists():
        return
    for entry in tmp_dir.iterdir():
        try:
            if entry.is_dir():
                # The freed size is discarded deliberately: this purge runs
                # BEFORE the counter is seeded, and .tmp/ is outside the
                # counter's scope in any case (CL6).
                _rm_rf(entry)
            else:
                entry.unlink()
        except OSError:
            logger.exception("failed to purge .tmp/ orphan: %s", entry)


class FileBodyStore:
    """Disk-backed body store with atomic-rename + fsync semantics.

    No ``tier`` property; admin / sender / persist code
    that used to branch on ``store.tier`` now branches on
    ``UploadRow.body_location`` instead.
    """

    def __init__(self, root: Path | str, *, shard_prefix_chars: int = 2) -> None:
        """Construct a store rooted at ``root``.

        Args:
            root: Directory under which per-upload body trees live.
            shard_prefix_chars: Number of leading hex chars of the
                chain_id used as a directory shard (e.g., ``2`` →
                ``ab/<chain_id>/``).
        """
        self._root = Path(root)
        self._shard_chars = shard_prefix_chars
        # Bytes under _root, excluding .tmp/. Seeded by start() from one walk
        # and maintained by the two writers; see total_bytes for why the walk
        # is the seed rather than the reader (CL6).
        self._total_bytes: int = 0

    async def start(self) -> None:
        """Create the root and tmp directories if missing; purge .tmp/ orphans.

        Phase 4 § 5.2.3 — WS-4 Finding 9. The atomic-write helper
        ``_put_one`` stages partial body files in ``<root>/.tmp/`` before
        renaming to the canonical layout. A crash mid-write or a power
        loss leaves a partially-written staged file behind; left alone,
        they accumulate on disk-full or repeated crashes. This one-shot
        startup purge clears the staging directory before any new
        admission can stage a write into it. The canonical body tree is
        NOT touched — that's the body-orphan janitor's steady-state
        responsibility (plan § 2.3.14).
        """
        await asyncio.to_thread(_makedirs_durable, self._root, self._root.parent)
        tmp_dir = self._tmp_dir()
        await asyncio.to_thread(_makedirs_durable, tmp_dir, self._root)
        await asyncio.to_thread(_purge_tmp_orphans, tmp_dir)
        # Seed the running total AFTER the purge, so the staged files it just
        # deleted are not counted. This walk is the counter's whole
        # orphan-awareness: a body file with no row is still occupying the
        # disk the ENOSPC gate protects, and only a walk can see it.
        self._total_bytes = await asyncio.to_thread(self._walk_total_bytes)

    async def stop(self) -> None:
        """No-op."""

    def _tmp_dir(self) -> Path:
        """Per-store tmp directory for atomic-rename staging."""
        return self._root / ".tmp"

    def _path_for(self, chain_id: UUID, name: str | None = None) -> Path:
        """Return the per-upload directory or per-name body path."""
        shard = str(chain_id)[: self._shard_chars]
        upload_dir = self._root / shard / str(chain_id)
        if name is None:
            return upload_dir
        return upload_dir / name

    def path_for(self, chain_id: UUID, name: str) -> Path:
        """Return the on-disk body file path for ``chain_id`` / ``name``.

        Public counterpart to :meth:`_path_for`. Used by recovery and
        E2E test helpers that need to inspect or manipulate files
        directly. The path is structural — its existence is not
        guaranteed.
        """
        return self._path_for(chain_id, name)

    async def has_body_ref(self, chain_id: UUID, name: str) -> bool:
        """Return whether the on-disk body file for ``name`` exists."""
        exists: bool = await aiofiles.os.path.isfile(self._path_for(chain_id, name))
        return exists

    async def put(self, chain_id: UUID, body_refs: dict[str, bytes]) -> int:
        """Write every named body_ref to disk atomically.

        ADDITIVE semantics (R11-a): each ref in ``body_refs`` is
        written (atomic-rename, overwriting a same-named file), but
        the chain directory is NOT cleared - a pre-existing file whose
        name is absent from ``body_refs`` survives. This is the
        additive end of the :class:`BodyStore` put contract
        (:class:`RamBodyStore.put` replaces the whole entry); a caller
        that needs a clean namespace deletes first, as admission's
        R11-1 namespace clear does.

        DURABILITY (F10): on return, every link from the store root down to
        each body file written here is durable. The directory levels are
        created and their parents fsynced first, then each body file is
        written, fsynced, and renamed into place, then the chain directory is
        fsynced once so the renamed FILE entries are durable too. Callers rely
        on this for the commit-last-column invariant: admission flips
        ``body_location='file'`` and acks the producer only after ``put()``
        returns, so anything left unsynced here is an acknowledged upload that
        a power cut can lose.

        Returns:
            Total bytes written.
        """
        upload_dir = self._path_for(chain_id)
        await asyncio.to_thread(_makedirs_durable, upload_dir, self._root.parent)
        total = 0
        grew = 0
        for name, data in body_refs.items():
            written, delta = await self._put_one(chain_id, name, data)
            total += written
            grew += delta
        # Parent-dir fsync once per upload, not per body_ref.
        await asyncio.to_thread(_sync_directory, upload_dir)
        # The running total moves by the tree's GROWTH, not by the bytes
        # written: put is additive and overwrites a same-named file, so a
        # re-put of an existing ref adds nothing to the disk (CL6).
        self._total_bytes += grew
        return total

    async def _put_one(self, chain_id: UUID, name: str, data: bytes) -> tuple[int, int]:
        """Write one named body_ref via tmp + fsync + atomic rename.

        Returns:
            ``(written, delta)``: the bytes this call wrote, and the change in
            the tree's size, which differ whenever the rename displaces an
            existing file of the same name (CL6). The displaced size is read
            with one extra ``stat`` on a path this method already touches.
        """
        tmp_path = self._tmp_dir() / f"{chain_id}-{name}.tmp"
        final_path = self._path_for(chain_id, name)
        async with aiofiles.open(tmp_path, "wb") as fh:
            # Stream in chunks (data is in memory; we still chunk so that the
            # loop yields periodically on very large bodies).
            for start in range(0, len(data), _CHUNK_BYTES):
                await fh.write(data[start : start + _CHUNK_BYTES])
            await fh.flush()
            fd = fh.fileno()
            await asyncio.to_thread(_fsync_file, fd)
        try:
            displaced = final_path.stat().st_size
        except OSError:  # no file at that name yet
            displaced = 0
        await asyncio.to_thread(_replace, tmp_path, final_path)
        return len(data), len(data) - displaced

    async def get(self, chain_id: UUID, name: str) -> bytes:
        """Read one named body_ref.

        A missing file (the upload directory or the named file is gone)
        raises :class:`KeyError`, never a raw ``FileNotFoundError`` /
        ``NotADirectoryError``. ``KeyError`` is the body-missing contract
        the sender's body-load path already maps to ``BodyMissingError``
        → the ``corrupted`` terminal state (the H8 / ADR-014 path). This
        matches :meth:`RamBodyStore.get`, which raises ``KeyError`` for
        an absent body, so :class:`HybridBodyStore` sees one uniform
        body-missing signal across both halves.
        """
        path = self._path_for(chain_id, name)
        try:
            async with aiofiles.open(path, "rb") as fh:
                data: bytes = await fh.read()
                return data
        except (FileNotFoundError, NotADirectoryError) as exc:
            raise KeyError(f"No body_ref {name!r} for chain_id={chain_id}") from exc

    async def get_all(self, chain_id: UUID) -> dict[str, bytes]:
        """Read every body_ref for ``chain_id`` as ``{name: bytes}``.

        This returns whatever the chain directory HOLDS. It does not
        guarantee completeness against the row's declared ``body_hashes``,
        and it cannot: it takes only a ``chain_id`` and never sees the row.
        Two absence shapes surface as :class:`KeyError` here, never as a raw
        ``FileNotFoundError`` / ``NotADirectoryError``: a missing upload
        directory, and a file that vanishes mid-traversal. A directory that
        was ALREADY partial or ALREADY empty when it was listed returns a
        short dict quietly. Proving the return covers every declared ref is
        the SENDER's check, in ``Sender._load_body_refs`` (F2), which raises
        :class:`BodyMissingError` on a shortfall.

        The sender's ``_load_body_refs`` catches ``KeyError`` and re-raises
        :class:`BodyMissingError`, which ``_drive_one`` routes to the
        ``corrupted`` terminal state (the H8 / ADR-014 path) — so a
        vanished body directory quarantines the row rather than crashing
        the worker's drive loop.

        The directory scan and every per-file read run inside one
        synchronous worker via :func:`asyncio.to_thread`. Doing the whole
        traversal in one shot (rather than interleaving ``await`` points
        between the directory listing and each ``open``) closes the
        time-of-check/time-of-use window in which a concurrent
        :meth:`delete` could remove the directory or a file *after* an
        existence check passed but *before* the read — exactly the race
        that previously surfaced as a raw ``FileNotFoundError`` escaping
        to the sender (e2e ``test_multipart_corrupted``). Any filesystem-
        absence error raised mid-traversal is mapped to ``KeyError`` for
        the same reason.
        """
        upload_dir = self._path_for(chain_id)

        def _read_all() -> dict[str, bytes]:
            result: dict[str, bytes] = {}
            try:
                entries = list(upload_dir.iterdir())
            except (FileNotFoundError, NotADirectoryError) as exc:
                raise KeyError(f"No body refs for chain_id={chain_id}") from exc
            for entry in entries:
                try:
                    if not entry.is_file():
                        continue
                    with entry.open("rb") as fh:
                        result[entry.name] = fh.read()
                except (FileNotFoundError, NotADirectoryError) as exc:
                    # A constituent body file vanished mid-traversal (a
                    # concurrent whole-chain delete). The body is an
                    # atomic unit (ADR-014), so a partial directory is a
                    # missing body — surface the same KeyError the
                    # all-gone case raises.
                    raise KeyError(f"No body refs for chain_id={chain_id}") from exc
            return result

        return await asyncio.to_thread(_read_all)

    async def delete(self, chain_id: UUID) -> None:
        """Remove the per-upload directory tree. Idempotent.

        The running total is reduced by what the removal actually unlinked
        (CL6), so the janitor's and the reaper's deletions are accounted at
        the time they happen rather than at the next boot.
        """
        upload_dir = self._path_for(chain_id)
        freed = await asyncio.to_thread(_rm_rf, upload_dir)
        self._total_bytes -= freed

    def _walk_total_bytes(self) -> int:
        """Sum every file size under the root, excluding ``.tmp/``.

        The counter's BOOT SEED and nothing else: called once from
        :meth:`start` (CL6). It is a full tree walk with a ``stat`` per file,
        which is what :meth:`total_bytes` used to do on every disk-pressure
        probe tick and every admin status read. A file that vanishes mid-walk
        counts as zero rather than raising.
        """
        total = 0
        for dirpath, _dirnames, filenames in os.walk(self._root):
            # Skip the tmp staging directory.
            if Path(dirpath) == self._tmp_dir():
                continue
            for f in filenames:
                fp = Path(dirpath) / f
                try:
                    total += fp.stat().st_size
                except OSError:  # file vanished mid-walk
                    continue
        return total

    async def total_bytes(self) -> int:
        """Bytes stored under the root, excluding ``.tmp/``.

        Returns a RUNNING TOTAL rather than a tree walk (CL6). The counter is
        seeded by :meth:`start` from one walk, so it counts orphaned body
        files that no row claims, and is then adjusted by the two methods that
        can move bytes: :meth:`put` by the delta its rename produced, and
        :meth:`delete` by the size of the tree it unlinked. A SQL
        ``SUM(body_size_bytes)`` cannot substitute, because it misses crash
        orphans and rows already zeroed by a discard, and the consumer is an
        ENOSPC gate where under-counting is the unsafe direction.

        Every mutation is applied on the event loop AFTER the filesystem work
        returns from its thread, so ``+=`` cannot interleave and no lock is
        needed. Any drift a running process accumulates is corrected by the
        next boot's seed.
        """
        return self._total_bytes

    async def list_chain_ids(self) -> list[UUID]:
        """Return every chain_id that has at least one body_ref on disk."""

        def _scan() -> list[UUID]:
            chain_ids: list[UUID] = []
            for shard in self._root.iterdir():
                if not shard.is_dir() or shard.name.startswith("."):
                    continue
                for upload_dir in shard.iterdir():
                    if not upload_dir.is_dir():
                        continue
                    try:
                        chain_ids.append(UUID(upload_dir.name))
                    except ValueError:
                        logger.warning("Skipping non-UUID directory %s", upload_dir)
            return chain_ids

        return await asyncio.to_thread(_scan)

    async def list_orphans(self, known_chain_ids: set[UUID]) -> list[UUID]:
        """Return chain_ids present on disk but absent from ``known_chain_ids``."""
        on_disk = await self.list_chain_ids()
        return [u for u in on_disk if u not in known_chain_ids]
