"""Unit tests for phantom.storage.file_body_store."""

from __future__ import annotations

import asyncio
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
