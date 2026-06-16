"""Atomic-rename, fsync'd body store for the persisted body files.

Layout: ``<root>/<sharded-prefix>/<chain_id>/<name>``. Each named
body_ref is written via ``aiofiles`` to a tmp file, fsync'd off-loop
via ``asyncio.to_thread(os.fsync)``, then renamed into place. A
parent-directory fsync (also off-loop) follows the rename on Linux —
NTFS journals metadata so this is a no-op on Windows.

All blocking file system calls (``os.fsync``, parent-dir fsync) run
via ``asyncio.to_thread``; ``aiofiles`` covers open/write/close.

Plan § 2.3.8 dropped the ``tier`` property — the
Protocol no longer carries it. :meth:`list_orphans` is unchanged.

Phase 4 § 5.2.3 (WS-4 Finding 9): :meth:`start` purges ``.tmp/``
orphan files left behind by a crash mid-write so subsequent
admissions stage into a clean directory.
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


def _fsync_file(fd: int) -> None:
    """Fsync a single file descriptor (blocking)."""
    os.fsync(fd)


def _replace(src: Path, dst: Path) -> None:
    """Atomic-rename ``src`` -> ``dst`` (blocking)."""
    os.replace(src, dst)


def _rm_rf(path: Path) -> None:
    """Recursively remove ``path``; tolerant of partial trees."""
    if not path.exists():
        return
    if path.is_file():
        path.unlink()
        return
    for entry in path.iterdir():
        _rm_rf(entry)
    path.rmdir()


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
        await aiofiles.os.makedirs(self._root, exist_ok=True)
        tmp_dir = self._tmp_dir()
        await aiofiles.os.makedirs(tmp_dir, exist_ok=True)
        await asyncio.to_thread(_purge_tmp_orphans, tmp_dir)

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

        Returns:
            Total bytes written.
        """
        upload_dir = self._path_for(chain_id)
        await aiofiles.os.makedirs(upload_dir, exist_ok=True)
        total = 0
        for name, data in body_refs.items():
            total += await self._put_one(chain_id, name, data)
        # Parent-dir fsync once per upload, not per body_ref.
        await asyncio.to_thread(_sync_directory, upload_dir)
        return total

    async def _put_one(self, chain_id: UUID, name: str, data: bytes) -> int:
        """Write one named body_ref via tmp + fsync + atomic rename."""
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
        await asyncio.to_thread(_replace, tmp_path, final_path)
        return len(data)

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

        A missing upload directory (the body files for a row that is
        expected to have a body are gone) raises :class:`KeyError`, never
        a raw ``FileNotFoundError`` / ``NotADirectoryError``. The sender's
        ``_load_body_refs`` catches ``KeyError`` and re-raises
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
        """Remove the per-upload directory tree. Idempotent."""
        upload_dir = self._path_for(chain_id)
        await asyncio.to_thread(_rm_rf, upload_dir)

    async def total_bytes(self) -> int:
        """Sum of file sizes under the store. Walks the root tree."""

        def _walk() -> int:
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

        return await asyncio.to_thread(_walk)

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
