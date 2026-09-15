"""Unit tests for phantom.storage.file_body_store."""

from __future__ import annotations

import asyncio
import contextlib
import errno
from pathlib import Path
from unittest import mock
from uuid import UUID, uuid4

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
    """``total_bytes`` reports the bytes two puts added to the store.

    Objective: the running counter's put transition (CL6). Success is the
    sum of the two written bodies, which is the same number the pre-CL6 tree
    walk returned.
    """
    s = FileBodyStore(tmp_path)
    await s.start()
    await s.put(uuid4(), {"body": b"x" * 100})
    await s.put(uuid4(), {"body": b"y" * 200})
    total = await s.total_bytes()
    assert total == 300
    await s.stop()


# ---------------------------------------------------------------------
# The disk counter is the SOLE input to DiskPressureProbe and therefore to
# admission's ``max_disk_bytes`` gate, and it drifts in both directions.
#
# SP-3 / B2: ``put`` applied its increment only after every ref succeeded
# and after the parent-dir fsync, but each ``_put_one`` has ALREADY renamed
# its file into place. A multi-ref upload whose second ref hits ENOSPC left
# the first ref's bytes on disk with the counter unchanged, so the gate was
# fed a number low by exactly the leaked bytes, the producer's retries
# compounded it, and the janitor's later reclaim drove the counter negative.
#
# S8-5: ``delete`` applied its decrement only after ``_rm_rf`` returned, so
# a partial removal that raised dropped the accounting for every file it had
# already unlinked; the counter drifted permanently UPWARD and admission
# refused with 503 ``disk_pressure`` for space that was free.
#
# Every test below asserts the counter against the bytes ACTUALLY on disk,
# which is the property the gate depends on, rather than against a constant.
# ---------------------------------------------------------------------


def _on_disk_bytes(root: Path) -> int:
    """Sum the real file sizes under ``root``, excluding the ``.tmp/`` staging dir.

    The independent oracle for the running counter: an assertion against
    this catches drift in either direction, where an assertion against a
    hand-computed constant only catches the direction it was written for.

    Args:
        root: The body-store root to measure.

    Returns:
        Total bytes of every regular file under ``root`` outside ``.tmp/``.
    """
    tmp_dir = root / ".tmp"
    return sum(
        p.stat().st_size for p in root.rglob("*") if p.is_file() and tmp_dir not in p.parents
    )


def _fail_fsync_on_call(monkeypatch: pytest.MonkeyPatch, *, nth: int) -> None:
    """Make the ``nth`` body-file fsync raise ENOSPC, leaving earlier refs on disk.

    ``_put_one`` fsyncs exactly once per ref, before its atomic rename, and
    fsync is where a filesystem with delayed allocation actually reports
    ENOSPC. Failing the nth call therefore reproduces "ref n hit ENOSPC"
    exactly: refs 1..n-1 are renamed into place and stay there, ref n never
    lands, and ``put`` raises.

    Args:
        monkeypatch: pytest's monkeypatch fixture.
        nth: 1-based index of the fsync call that must fail.
    """
    from phantom.storage import file_body_store as module

    calls = 0
    real = module._fsync_file

    def _failing(fd: int) -> None:
        nonlocal calls
        calls += 1
        if calls == nth:
            raise OSError(errno.ENOSPC, "No space left on device")
        real(fd)

    monkeypatch.setattr(module, "_fsync_file", _failing)


@pytest.mark.asyncio
async def test_put_counts_the_refs_a_partial_upload_already_landed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A three-ref put whose second ref hits ENOSPC still counts the first ref.

    Objective: close SP-3 / B2. ``_put_one`` renames each ref into place one
    at a time, so the bytes of every ref that already landed are occupying
    the disk that the ENOSPC gate protects, whether or not the refs after it
    succeed.

    Success: ``put`` raises ``OSError``, ref "a" is on disk, and
    ``total_bytes()`` equals the bytes actually on disk. Before the fix the
    counter stayed at 0 while 100 bytes sat in the tree.
    """
    s = FileBodyStore(tmp_path)
    await s.start()
    chain_id = uuid4()
    _fail_fsync_on_call(monkeypatch, nth=2)

    with pytest.raises(OSError):
        await s.put(chain_id, {"a": b"x" * 100, "b": b"y" * 200, "c": b"z" * 400})

    assert s.path_for(chain_id, "a").is_file(), "ref 'a' was renamed into place"
    assert not s.path_for(chain_id, "b").exists(), "ref 'b' never landed"
    on_disk = _on_disk_bytes(tmp_path)
    assert on_disk == 100
    assert await s.total_bytes() == on_disk, (
        "the counter must match the bytes the partial put left on disk; "
        "under-counting over-admits straight into ENOSPC"
    )
    await s.stop()


@pytest.mark.asyncio
async def test_put_counts_a_landed_ref_when_the_parent_dir_fsync_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A single-ref put whose closing directory fsync raises still counts the ref.

    Objective: the second half of SP-3. Even a one-ref upload reaches the
    same hole, because the rename lands before the once-per-upload
    parent-directory fsync; an EIO there used to discard the whole put's
    accounting for bytes that are on disk.

    Success: ``put`` raises ``OSError`` and ``total_bytes()`` equals the
    bytes actually on disk. Before the fix the counter stayed at 0.
    """
    from phantom.storage import file_body_store as module

    s = FileBodyStore(tmp_path)
    await s.start()
    chain_id = uuid4()
    upload_dir = s.path_for(chain_id, "body").parent
    real = module._sync_directory

    def _failing(path: Path) -> None:
        # Only the closing per-upload fsync fails; the ancestor sweep that
        # runs before any rename must still work, or the test would prove
        # nothing about the post-rename window.
        if path == upload_dir:
            raise OSError(errno.EIO, "Input/output error")
        real(path)

    monkeypatch.setattr(module, "_sync_directory", _failing)

    with pytest.raises(OSError):
        await s.put(chain_id, {"body": b"x" * 100})

    on_disk = _on_disk_bytes(tmp_path)
    assert on_disk == 100
    assert await s.total_bytes() == on_disk
    await s.stop()


@pytest.mark.asyncio
async def test_delete_counts_the_files_it_unlinked_when_the_directory_removal_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A removal that unlinks every file and then fails to rmdir still decrements.

    Objective: close S8-5. ``rmdir`` raises ``ENOTEMPTY`` when a concurrent
    put lands a file mid-sweep, and ``FileNotFoundError`` when a concurrent
    delete won the race. Both arrive AFTER the body files have been
    unlinked, so the bytes are genuinely gone; a decrement applied only on
    the success path leaves the counter permanently high and admission then
    refuses with 503 ``disk_pressure`` for space that is free.

    Success: whatever ``delete`` does about the error, ``total_bytes()``
    equals the bytes actually on disk afterwards. Before the fix the counter
    stayed at 700 with an empty tree.
    """
    s = FileBodyStore(tmp_path)
    await s.start()
    chain_id = uuid4()
    await s.put(chain_id, {"a": b"x" * 100, "b": b"y" * 200, "c": b"z" * 400})
    assert await s.total_bytes() == 700
    upload_dir = s.path_for(chain_id, "a").parent
    real_rmdir = Path.rmdir

    def _failing_rmdir(self: Path) -> None:
        if self == upload_dir:
            raise OSError(errno.ENOTEMPTY, "Directory not empty")
        real_rmdir(self)

    monkeypatch.setattr(Path, "rmdir", _failing_rmdir)

    # Suppressed so this test asserts the ACCOUNTING claim alone; whether
    # delete propagates the error is the separate claim below.
    with contextlib.suppress(OSError):
        await s.delete(chain_id)

    on_disk = _on_disk_bytes(tmp_path)
    assert on_disk == 0, "every body file was unlinked before the rmdir failed"
    assert await s.total_bytes() == on_disk, (
        "the counter must drop by what the removal actually unlinked; "
        "over-counting refuses admission with 503 disk_pressure for free space"
    )
    await s.stop()


@pytest.mark.asyncio
async def test_delete_does_not_propagate_a_partial_removal_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An undeletable body file still leaves the counter matching the disk.

    Objective: pin the accounting half of S8-5 on the raising path. ``delete``
    signals the failure, because admission clears this namespace before
    writing a retry's body and the idempotency-collision rollback turns the
    failure into a 503; swallowing here would silence both. What must NOT
    happen is the old behaviour, where the decrement was applied only after a
    clean return, so a partial removal moved the counter by nothing and left
    it permanently high, refusing admission with 503 ``disk_pressure`` for
    space that is free.

    S8-5's other consequence, one stuck file abandoning the rest of a reaper
    tick, is pinned in ``test_reaper.py`` against ``Reaper._reclaim_bodies``,
    which is where the unguarded await actually lives.

    Success: the raise carries the chain_id, and the counter still equals the
    bytes genuinely on disk.
    """
    s = FileBodyStore(tmp_path)
    await s.start()
    chain_id = uuid4()
    await s.put(chain_id, {"a": b"x" * 100, "b": b"y" * 200})
    upload_dir = s.path_for(chain_id, "a").parent
    real_unlink = Path.unlink

    def _failing_unlink(self: Path, missing_ok: bool = False) -> None:
        if self.parent == upload_dir:
            raise PermissionError(errno.EACCES, "Permission denied")
        real_unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", _failing_unlink)

    with pytest.raises(OSError, match=str(chain_id)):
        await s.delete(chain_id)

    on_disk = _on_disk_bytes(tmp_path)
    assert on_disk == 300, "nothing could be unlinked, so nothing was reclaimed"
    assert await s.total_bytes() == on_disk, (
        "the counter must still match the tree after a failed removal; drifting "
        "high refuses admission with 503 disk_pressure for space that is free"
    )
    await s.stop()


# ---------------------------------------------------------------------
# T1: the BOOT SEED. CL6 turned ``total_bytes()`` from a live tree walk into
# a running counter seeded once by ``start()``, and the seed is the only
# thing that makes disk accounting survive a restart or notice body files no
# row claims. It had no assertion anywhere in the tree, because
# ``test_total_bytes`` constructs its store over an EMPTY tmp_path: the walk
# returns 0 vacuously there and the assertion is carried entirely by the two
# put increments.
#
# If the seed regresses (assignment dropped, walk rooted wrong, shards
# skipped, or the walk moved ahead of the ``.tmp/`` purge), ``total_bytes()``
# reports a number the tree does not support for the whole process lifetime.
# DiskPressureProbe writes it into ``SaturationGate.set_disk_usage_bytes``
# every tick and admission evaluates it against ``max_disk_bytes``, so a
# seed stuck at 0 NEVER REFUSES and the volume fills past the configured cap
# until an fsync raises ENOSPC mid-write - the F13 harm class, reached from
# the restart direction.
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_seeds_the_counter_from_body_files_it_did_not_write(
    tmp_path: Path,
) -> None:
    """A fresh store over a populated tree boots with the tree's real size.

    Objective: pin the boot seed. This is the restart case and the orphan
    case at once - the files are in the canonical sharded layout, spread
    across two shards, and no put in this process created any of them, which
    is exactly what a body file left by a previous boot (or by a row the
    janitor has not collected yet) looks like.

    Success: ``total_bytes()`` immediately after ``start()`` equals the bytes
    on disk, and a second store constructed over the same root reports the
    same number, so the accounting survives the restart. A seed that is
    dropped, rooted above or below the shards, or short-circuited to 0
    reports 0 here and admission's disk gate never refuses again.
    """
    first = UUID("ab000000-0000-4000-8000-000000000001")
    second = UUID("cd000000-0000-4000-8000-000000000002")
    for chain_id, name, payload in (
        (first, "body", b"x" * 300),
        (second, "body", b"y" * 500),
        (second, "extra", b"z" * 100),
    ):
        upload_dir = tmp_path / str(chain_id)[:2] / str(chain_id)
        upload_dir.mkdir(parents=True, exist_ok=True)
        (upload_dir / name).write_bytes(payload)

    s = FileBodyStore(tmp_path, shard_prefix_chars=2)
    await s.start()
    on_disk = _on_disk_bytes(tmp_path)
    assert on_disk == 900
    assert await s.total_bytes() == on_disk, (
        "start() must seed the counter from the tree; a seed of 0 over a "
        "populated volume makes the max_disk_bytes gate unable to refuse"
    )

    # The restart: a brand-new store instance over the same root, as a
    # process rebooting into its existing data directory would construct.
    restarted = FileBodyStore(tmp_path, shard_prefix_chars=2)
    await restarted.start()
    assert await restarted.total_bytes() == on_disk
    await restarted.stop()
    await s.stop()


@pytest.mark.asyncio
async def test_start_seeds_the_counter_after_the_tmp_purge_not_before(
    tmp_path: Path,
) -> None:
    """Staged files a crash left behind are purged first and never counted.

    Objective: pin the ORDER of the two things ``start()`` does. The purge
    deletes the ``.tmp/`` staging tree; seeding ahead of it would count bytes
    that are about to stop existing, and the counter would then over-report
    for the process lifetime and refuse admission for free space.

    Success: the counter equals the canonical body alone, with nothing from
    the nested staging orphan. The orphan is nested one directory deep on
    purpose: the walk skips the ``.tmp`` directory itself by exact path, so a
    top-level staged file would be excluded even by a seed that ran too
    early, and only a nested one can tell the two orderings apart.
    """
    chain_id = uuid4()
    upload_dir = tmp_path / str(chain_id)[:2] / str(chain_id)
    upload_dir.mkdir(parents=True)
    (upload_dir / "body").write_bytes(b"k" * 250)

    staging = tmp_path / ".tmp" / "orphan-subdir"
    staging.mkdir(parents=True)
    (staging / "nested.tmp").write_bytes(b"j" * 4000)

    s = FileBodyStore(tmp_path)
    await s.start()

    assert list((tmp_path / ".tmp").iterdir()) == [], "the staging tree was purged"
    assert await s.total_bytes() == 250, (
        "the seed must run after the purge; counting the 4000 staged bytes "
        "leaves the counter permanently high against an empty .tmp/"
    )
    await s.stop()


@pytest.mark.asyncio
async def test_start_does_the_boot_work_once_however_often_it_is_called(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A second ``start()`` on the same store repeats neither the walk nor the purge.

    Objective: close S8-8. In ``hybrid`` mode the composition root starts
    this store directly and then hands it to ``build_body_store``, whose
    :class:`HybridBodyStore` starts both halves again, so ``start()`` runs
    TWICE per boot. Unguarded, the second run re-walks the entire body tree
    with a stat per file, which at the 100k ``max_rows`` default doubles the
    walk portion of boot latency on SD-card-class hardware, and re-purges
    ``.tmp/``.

    Success: across two ``start()`` calls the seed walk and the staging
    purge each run exactly once, and the counter still reports the tree.
    Before the fix both ran twice.
    """
    from phantom.storage import file_body_store as module

    walks = 0
    purges = 0
    real_walk = FileBodyStore._walk_total_bytes
    real_purge = module._purge_tmp_orphans

    def _counting_walk(self: FileBodyStore) -> int:
        nonlocal walks
        walks += 1
        return real_walk(self)

    def _counting_purge(tmp_dir: Path) -> None:
        nonlocal purges
        purges += 1
        real_purge(tmp_dir)

    monkeypatch.setattr(FileBodyStore, "_walk_total_bytes", _counting_walk)
    monkeypatch.setattr(module, "_purge_tmp_orphans", _counting_purge)

    chain_id = uuid4()
    upload_dir = tmp_path / str(chain_id)[:2] / str(chain_id)
    upload_dir.mkdir(parents=True)
    (upload_dir / "body").write_bytes(b"q" * 750)

    s = FileBodyStore(tmp_path)
    await s.start()
    await s.start()  # what HybridBodyStore.start() does to an already-started half

    assert (walks, purges) == (1, 1), (
        f"the boot walk and the .tmp/ purge must not repeat on an already-started "
        f"store; observed {walks} walk(s) and {purges} purge(s)"
    )
    assert await s.total_bytes() == 750
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


# ---------------------------------------------------------------------
# F10: every directory level this store CREATES must have its parent
# fsynced before ``put()`` returns. A directory entry is durable only
# once the directory HOLDING it has been fsynced, and ``makedirs``
# leaves new entries in their parents' dirty page cache. Before F10 only
# the per-chain directory was fsynced (making the body FILE entries
# durable), so the entry linking that chain directory into its shard,
# and the entry linking a fresh shard into the root, were never made
# durable: in ``all_disk`` mode admission commits ``body_location='file'``
# and acks 202 immediately after the put, so a power cut could persist
# the database write and lose the directory entry, and recovery would
# then quarantine an acknowledged row to ``corrupted``.
#
# A real power cut cannot be staged in the suite, so these assert the
# fsync call SET at the seam, in the same spirit as
# ``scripts/check_persist_ordering.py`` asserting call order at the
# source level. ``_makedirs_durable`` must call the module-level
# ``_sync_directory`` rather than ``os.fsync`` directly precisely so this
# seam exists.
# ---------------------------------------------------------------------


def _record_syncs(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    """Patch ``_sync_directory`` with a recorder that still calls through.

    Both ``_makedirs_durable`` and ``put`` resolve the name as a module
    global, so patching the module attribute intercepts both.

    Args:
        monkeypatch: pytest's monkeypatch fixture.

    Returns:
        The live list of fsynced paths, in call order.
    """
    from phantom.storage import file_body_store as module

    recorded: list[Path] = []
    real = module._sync_directory

    def _recorder(path: Path) -> None:
        recorded.append(path)
        real(path)

    monkeypatch.setattr(module, "_sync_directory", _recorder)
    return recorded


@pytest.mark.asyncio
async def test_put_fsyncs_root_shard_and_chain_directories_on_first_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every newly created directory level's parent is made durable.

    Objective: close the F10 hole. On a first write the root, the shard, and the
    chain directory are all created, so all three must be fsynced (each one
    making its own new child entry durable), plus the root's parent.

    Success: the recorded fsync paths include the root's parent, the root, the
    shard, and the chain directory. Asserted on set membership rather than an
    exact call count, so a harmless duplicate fsync does not make this brittle.
    """
    root = tmp_path / "bodies"
    recorded = _record_syncs(monkeypatch)
    s = FileBodyStore(root, shard_prefix_chars=2)
    await s.start()
    uid = uuid4()
    await s.put(uid, {"body": b"x"})

    shard = root / str(uid)[:2]
    chain_dir = shard / str(uid)
    for expected in (root.parent, root, shard, chain_dir):
        assert expected in recorded, (
            f"{expected} was never fsynced; its child's directory entry is not durable. "
            f"Recorded: {recorded}"
        )
    await s.stop()


@pytest.mark.asyncio
async def test_put_fsyncs_parents_before_children(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Shallowest first: fsyncing a child before its parent's entry proves nothing.

    Objective: pin the ordering. A chain directory made durable inside a shard
    whose own entry is still in the root's dirty page cache is still reachable
    only by luck.

    Success: in the recorded sequence the root precedes the shard, and the shard
    precedes the chain directory.
    """
    root = tmp_path / "bodies"
    recorded = _record_syncs(monkeypatch)
    s = FileBodyStore(root, shard_prefix_chars=2)
    await s.start()
    uid = uuid4()
    await s.put(uid, {"body": b"x"})

    shard = root / str(uid)[:2]
    chain_dir = shard / str(uid)
    assert recorded.index(root) < recorded.index(shard)
    assert recorded.index(shard) < recorded.index(chain_dir)
    await s.stop()


@pytest.mark.asyncio
async def test_new_chain_directory_syncs_the_full_ancestor_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ancestor sweep is UNCONDITIONAL whenever the leaf is new.

    Objective: pin the property that makes the helper safe under concurrent
    creates. A form that filtered the sweep by a pre-``makedirs`` existence
    probe would pass every other test in this section and still lose the shard
    link under interleaving: put B creates the shard and is descheduled before
    its root fsync, put A then sees the shard present, concludes only its own
    chain directory was created, and returns after fsyncing one level. With
    ``shard_prefix_chars`` defaulting to 2 there are only 256 shards, so
    fresh-shard collisions on a cold store are common rather than exotic.

    Success: the second put, into an ALREADY-EXISTING shard, still records the
    root AND the shard AND its own new chain directory.
    """
    root = tmp_path / "bodies"
    s = FileBodyStore(root, shard_prefix_chars=2)
    await s.start()
    # Two chain ids sharing the first two hex characters, constructed
    # explicitly rather than generated until they collide.
    first = UUID("ab000000-0000-4000-8000-000000000001")
    second = UUID("ab000000-0000-4000-8000-000000000002")
    await s.put(first, {"body": b"x"})

    recorded = _record_syncs(monkeypatch)
    await s.put(second, {"body": b"y"})

    shard = root / "ab"
    second_dir = shard / str(second)
    for expected in (root, shard, second_dir):
        assert expected in recorded, (
            f"{expected} was not fsynced on the second put; the ancestor sweep must not be "
            f"filtered by a pre-makedirs existence probe. Recorded: {recorded}"
        )
    await s.stop()


@pytest.mark.asyncio
async def test_reput_into_an_existing_chain_directory_syncs_only_that_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The steady-state path stays cheap.

    Objective: the ancestor sweep runs only when the leaf is new. A re-put into
    an existing chain directory created no level, so it must fsync only that
    directory, which is the pre-existing per-chain fsync that makes the new body
    FILE entries durable.

    Success: the recorded paths for the second put are exactly the chain
    directory.
    """
    root = tmp_path / "bodies"
    s = FileBodyStore(root, shard_prefix_chars=2)
    await s.start()
    uid = uuid4()
    await s.put(uid, {"body": b"x"})

    recorded = _record_syncs(monkeypatch)
    await s.put(uid, {"second": b"y"})

    chain_dir = root / str(uid)[:2] / str(uid)
    assert recorded == [chain_dir], (
        f"a re-put creates no directory level, so only the chain directory may be fsynced; "
        f"recorded {recorded}"
    )
    await s.stop()


def test_makedirs_durable_rejects_a_leaf_outside_its_boundary(tmp_path: Path) -> None:
    """The boundary contract is enforced rather than silently ignored.

    Objective: ``_makedirs_durable`` promises never to create or fsync anything
    above its boundary, and it computes its level list with ``relative_to``. A
    leaf outside the boundary must be refused rather than producing an empty or
    nonsensical sweep.

    Success: ``ValueError``. The import is inside the test body on purpose: this
    module must stay collectible on a tree where the helper does not exist yet,
    so the witness test above can run and fail behaviourally.
    """
    from phantom.storage.file_body_store import _makedirs_durable

    with pytest.raises(ValueError):
        _makedirs_durable(Path("/some/other/tree"), tmp_path)
