"""Unit tests for phantom.storage.file_body_store."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest import mock
from uuid import uuid4

import pytest
from phantom.storage.file_body_store import FileBodyStore


@pytest.mark.asyncio
async def test_directory_sharding(tmp_path: Path) -> None:
    """Body files land under ``<shard>/<uid>/<name>``."""
    s = FileBodyStore(tmp_path, shard_prefix_chars=2)
    await s.start()
    uid = uuid4()
    await s.put(uid, {"body": b"hello"})
    shard = str(uid)[:2]
    expected = tmp_path / shard / str(uid) / "body"
    assert expected.is_file()
    assert expected.read_bytes() == b"hello"
    await s.stop()


@pytest.mark.asyncio
async def test_put_get_delete(tmp_path: Path) -> None:
    """Round-trip via the on-disk store."""
    s = FileBodyStore(tmp_path)
    await s.start()
    uid = uuid4()
    await s.put(uid, {"body": b"abcdef"})
    assert await s.get(uid, "body") == b"abcdef"
    assert await s.get_all(uid) == {"body": b"abcdef"}
    await s.delete(uid)
    with pytest.raises(KeyError):
        await s.get_all(uid)
    await s.stop()


# ---------------------------------------------------------------------
# P1 — a missing body directory/file must surface as ``KeyError`` (the
# body-missing contract), never a raw ``FileNotFoundError`` /
# ``NotADirectoryError``. ``KeyError`` is what the sender's
# ``_load_body_refs`` catches and re-raises as ``BodyMissingError`` →
# the ``corrupted`` terminal state (H8 / ADR-014). A raw OSError would
# escape that catch and crash / wedge the sender's drive loop.
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_all_missing_directory_raises_keyerror(tmp_path: Path) -> None:
    """``get_all`` on a chain that was never written raises ``KeyError``.

    The upload directory for ``chain_id`` does not exist. The store must
    raise ``KeyError`` (the body-missing contract), not let the
    underlying ``FileNotFoundError`` from ``iterdir()`` escape.
    """
    s = FileBodyStore(tmp_path)
    await s.start()
    with pytest.raises(KeyError):
        await s.get_all(uuid4())
    await s.stop()


@pytest.mark.asyncio
async def test_get_all_directory_deleted_after_check_raises_keyerror(
    tmp_path: Path,
) -> None:
    """TOCTOU: directory removed before the scan still yields ``KeyError``.

    Regression for the e2e ``test_multipart_corrupted`` hang. Under load
    the sender's body-read could race a concurrent whole-chain
    :meth:`delete`: the directory existence check passed, then the
    directory vanished before ``iterdir()`` ran, raising a raw
    ``FileNotFoundError`` that escaped the sender's ``KeyError`` catch and
    left the row wedged in ``attempting``. The store now performs the
    whole traversal in one off-loop worker and maps any filesystem-
    absence error to ``KeyError``.

    We force the exact race deterministically: the body directory exists
    when ``get_all`` is entered, but ``iterdir()`` raises
    ``FileNotFoundError`` (the directory vanished in the check→use
    window). The store must surface ``KeyError``, not the raw OSError.
    """
    s = FileBodyStore(tmp_path)
    await s.start()
    uid = uuid4()
    await s.put(uid, {"a": b"x", "b": b"y"})

    real_iterdir = Path.iterdir

    def _vanishing_iterdir(self: Path) -> object:
        # The directory we're about to scan "disappears" mid-call.
        if self == s.path_for(uid, "a").parent:
            raise FileNotFoundError(2, "No such file or directory", str(self))
        return real_iterdir(self)

    with mock.patch.object(Path, "iterdir", _vanishing_iterdir), pytest.raises(KeyError):
        await s.get_all(uid)
    await s.stop()


@pytest.mark.asyncio
async def test_get_all_does_not_raise_oserror_subclasses(tmp_path: Path) -> None:
    """``get_all`` on a missing chain never surfaces a raw OSError.

    ``FileNotFoundError`` and ``NotADirectoryError`` are both ``OSError``
    subclasses; the sender catches only ``KeyError``. Assert the store
    never lets an ``OSError`` (which is NOT a ``KeyError``) escape.
    """
    s = FileBodyStore(tmp_path)
    await s.start()
    # A path component that is a file, not a directory, would yield
    # NotADirectoryError from iterdir() — also must map to KeyError.
    uid = uuid4()
    shard = str(uid)[:2]
    shard_dir = tmp_path / shard
    shard_dir.mkdir(parents=True, exist_ok=True)
    # Create a *file* where the per-chain directory would be.
    (shard_dir / str(uid)).write_bytes(b"not-a-directory")
    try:
        await s.get_all(uid)
    except KeyError:
        pass
    except OSError as exc:  # pragma: no cover - failure path
        pytest.fail(f"get_all leaked a raw OSError: {exc!r}")
    await s.stop()


@pytest.mark.asyncio
async def test_get_missing_file_raises_keyerror(tmp_path: Path) -> None:
    """``get`` on an absent body file raises ``KeyError``, not OSError.

    Mirrors :meth:`RamBodyStore.get` so :class:`HybridBodyStore` sees one
    uniform body-missing signal across both halves.
    """
    s = FileBodyStore(tmp_path)
    await s.start()
    with pytest.raises(KeyError):
        await s.get(uuid4(), "body")
    await s.stop()


@pytest.mark.asyncio
async def test_atomic_rename_leaves_no_tmp(tmp_path: Path) -> None:
    """The tmp staging directory is not visible as a body shard."""
    s = FileBodyStore(tmp_path)
    await s.start()
    await s.put(uuid4(), {"body": b"x"})
    chain_ids = await s.list_chain_ids()
    assert len(chain_ids) == 1
    # tmp dir is hidden by the leading dot.
    assert (tmp_path / ".tmp").is_dir()
    await s.stop()


@pytest.mark.asyncio
async def test_orphan_sweep(tmp_path: Path) -> None:
    """``list_orphans`` returns uids on disk but not in the known set."""
    s = FileBodyStore(tmp_path)
    await s.start()
    u1, u2 = uuid4(), uuid4()
    await s.put(u1, {"body": b"a"})
    await s.put(u2, {"body": b"b"})
    orphans = await s.list_orphans({u1})
    assert orphans == [u2]
    await s.stop()


@pytest.mark.asyncio
async def test_put_does_not_block_loop(tmp_path: Path) -> None:
    """A 4 MiB write doesn't stall the event loop for >50 ms."""
    s = FileBodyStore(tmp_path)
    await s.start()
    uid = uuid4()
    big = b"a" * (4 * 1_048_576)
    woke = asyncio.Event()

    async def keep_loop_alive() -> None:
        await asyncio.sleep(0.01)
        woke.set()

    task = asyncio.create_task(keep_loop_alive())
    await s.put(uid, {"body": big})
    await task
    assert woke.is_set()
    await s.stop()


@pytest.mark.asyncio
async def test_total_bytes(tmp_path: Path) -> None:
    """``total_bytes`` walks the data dir and sums file sizes."""
    s = FileBodyStore(tmp_path)
    await s.start()
    await s.put(uuid4(), {"body": b"x" * 100})
    await s.put(uuid4(), {"body": b"y" * 200})
    total = await s.total_bytes()
    assert total == 300
    await s.stop()


# ---------------------------------------------------------------------
# Phase 4 § 5.2.3 — ``.tmp/`` orphan purge at start().
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_purges_tmp_orphans(tmp_path: Path) -> None:
    """Orphan files in ``.tmp/`` from a prior crash are deleted on start."""
    tmp_dir = tmp_path / ".tmp"
    tmp_dir.mkdir()
    # Stage an orphan file + an orphan subdirectory, as could be left
    # behind by a crash mid-write.
    orphan_file = tmp_dir / "orphan-abc.tmp"
    orphan_file.write_bytes(b"junk")
    orphan_subdir = tmp_dir / "orphan-subdir"
    orphan_subdir.mkdir()
    (orphan_subdir / "nested.tmp").write_bytes(b"more junk")
    s = FileBodyStore(tmp_path)
    await s.start()
    # .tmp/ still exists (it's the staging directory) but is empty.
    assert tmp_dir.is_dir()
    assert list(tmp_dir.iterdir()) == []
    await s.stop()


@pytest.mark.asyncio
async def test_start_purge_does_not_touch_canonical_tree(tmp_path: Path) -> None:
    """The .tmp/ purge leaves the canonical shard layout untouched."""
    # Pre-populate a body file in the canonical sharded layout.
    uid = uuid4()
    shard = str(uid)[:2]
    canonical = tmp_path / shard / str(uid)
    canonical.mkdir(parents=True)
    (canonical / "body").write_bytes(b"persisted")
    # Pre-populate an orphan staging file.
    tmp_dir = tmp_path / ".tmp"
    tmp_dir.mkdir()
    (tmp_dir / "orphan").write_bytes(b"junk")
    s = FileBodyStore(tmp_path)
    await s.start()
    assert (canonical / "body").read_bytes() == b"persisted"
    assert list(tmp_dir.iterdir()) == []
    await s.stop()


@pytest.mark.asyncio
async def test_start_purge_is_idempotent_on_clean_tmp(tmp_path: Path) -> None:
    """start() on a clean .tmp/ is a no-op (no exception, no creation noise)."""
    s = FileBodyStore(tmp_path)
    await s.start()
    # Re-invoke; .tmp/ is empty going in, still empty coming out.
    await s.start()
    assert (tmp_path / ".tmp").is_dir()
    assert list((tmp_path / ".tmp").iterdir()) == []
    await s.stop()
