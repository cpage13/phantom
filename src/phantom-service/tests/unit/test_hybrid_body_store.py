"""Unit tests for :class:`HybridBodyStore` (plan § 2.3.9)."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from phantom.storage.file_body_store import FileBodyStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.storage.ram_body_store import RamBodyStore


@pytest.fixture
async def hybrid(tmp_path: Path) -> HybridBodyStore:
    """Construct a started :class:`HybridBodyStore` rooted under ``tmp_path``."""
    ram = RamBodyStore()
    disk = FileBodyStore(tmp_path / "bodies", shard_prefix_chars=2)
    store = HybridBodyStore(ram=ram, disk=disk)
    await store.start()
    return store


async def test_put_writes_to_ram_only(hybrid: HybridBodyStore) -> None:
    """RAM-first :meth:`put`: bytes land in RAM; disk stays empty."""
    chain_id = uuid4()
    refs = {"a": b"hello", "b": b"world"}
    total = await hybrid.put(chain_id, refs)
    assert total == 10  # 5 + 5
    # Bytes are in RAM, not on disk.
    assert await hybrid._ram.has_body_ref(chain_id, "a")
    assert not await hybrid._disk.has_body_ref(chain_id, "a")


async def test_get_reads_from_ram_when_present(hybrid: HybridBodyStore) -> None:
    """:meth:`get` returns RAM bytes when RAM has the ref."""
    chain_id = uuid4()
    await hybrid.put(chain_id, {"a": b"ram-bytes"})
    assert await hybrid.get(chain_id, "a") == b"ram-bytes"


async def test_get_falls_through_to_disk_when_ram_misses(
    hybrid: HybridBodyStore,
) -> None:
    """:meth:`get` falls through to disk when RAM has nothing for ``chain_id``."""
    chain_id = uuid4()
    # Write only to disk (simulating a post-migration state where the
    # persist controller flipped body_location and dropped RAM).
    await hybrid._disk.put(chain_id, {"a": b"disk-bytes"})
    # RAM is empty for this chain_id.
    assert not await hybrid._ram.has_body_ref(chain_id, "a")
    assert await hybrid.get(chain_id, "a") == b"disk-bytes"


async def test_get_all_reads_ram_first(hybrid: HybridBodyStore) -> None:
    """:meth:`get_all` prefers RAM contents when present."""
    chain_id = uuid4()
    await hybrid.put(chain_id, {"a": b"ram", "b": b"refs"})
    # Even if disk somehow has the same chain_id (pre-cleanup window),
    # RAM wins.
    await hybrid._disk.put(chain_id, {"a": b"stale", "b": b"data"})
    refs = await hybrid.get_all(chain_id)
    assert refs == {"a": b"ram", "b": b"refs"}


async def test_get_all_falls_through_when_ram_missing(
    hybrid: HybridBodyStore,
) -> None:
    """:meth:`get_all` falls through to disk when RAM has no entry."""
    chain_id = uuid4()
    await hybrid._disk.put(chain_id, {"x": b"disk-only"})
    refs = await hybrid.get_all(chain_id)
    assert refs == {"x": b"disk-only"}


async def test_has_body_ref_checks_both_stores(hybrid: HybridBodyStore) -> None:
    """:meth:`has_body_ref` returns True if either store has the ref."""
    ram_chain = uuid4()
    disk_chain = uuid4()
    await hybrid.put(ram_chain, {"a": b"in-ram"})
    await hybrid._disk.put(disk_chain, {"a": b"on-disk"})
    assert await hybrid.has_body_ref(ram_chain, "a")
    assert await hybrid.has_body_ref(disk_chain, "a")
    assert not await hybrid.has_body_ref(uuid4(), "a")


async def test_delete_removes_from_both_stores(hybrid: HybridBodyStore) -> None:
    """:meth:`delete` is idempotent across both halves."""
    chain_id = uuid4()
    await hybrid.put(chain_id, {"a": b"ram"})
    await hybrid._disk.put(chain_id, {"a": b"disk"})
    await hybrid.delete(chain_id)
    assert not await hybrid._ram.has_body_ref(chain_id, "a")
    assert not await hybrid._disk.has_body_ref(chain_id, "a")
    # Second delete is a no-op (idempotent).
    await hybrid.delete(chain_id)


async def test_delete_missing_chain_id_is_noop(hybrid: HybridBodyStore) -> None:
    """:meth:`delete` of an unknown chain_id is silent (idempotent)."""
    await hybrid.delete(uuid4())


async def test_total_bytes_returns_ram_only(hybrid: HybridBodyStore) -> None:
    """:meth:`total_bytes` reports RAM bytes; disk bytes are tracked elsewhere."""
    chain_id = uuid4()
    await hybrid.put(chain_id, {"a": b"abcdefghij"})  # 10 bytes RAM
    await hybrid._disk.put(uuid4(), {"a": b"x" * 1000})  # disk-only
    assert await hybrid.total_bytes() == 10


async def test_list_chain_ids_unions_both_stores(hybrid: HybridBodyStore) -> None:
    """:meth:`list_chain_ids` returns the deduped union of RAM and disk ids."""
    ram_only = uuid4()
    disk_only = uuid4()
    both = uuid4()
    await hybrid._ram.put(ram_only, {"a": b"a"})
    await hybrid._disk.put(disk_only, {"a": b"a"})
    await hybrid._ram.put(both, {"a": b"a"})
    await hybrid._disk.put(both, {"a": b"a"})
    ids = await hybrid.list_chain_ids()
    assert sorted(ids) == sorted({ram_only, disk_only, both})
    # Sorted result with no duplicates.
    assert len(ids) == len(set(ids))


async def test_list_orphans_returns_disk_orphans(hybrid: HybridBodyStore) -> None:
    """:meth:`list_orphans` exposes the disk-side orphan set; RAM contributes []."""
    orphan = uuid4()
    known = uuid4()
    await hybrid._disk.put(orphan, {"a": b"orphan"})
    await hybrid._disk.put(known, {"a": b"keep"})
    # Only ``known`` is in the known-set; ``orphan`` is orphan.
    orphans = await hybrid.list_orphans({known})
    assert orphans == [orphan]


async def test_list_orphans_skips_ram_only_chains(hybrid: HybridBodyStore) -> None:
    """A RAM-only chain is NOT an orphan — the orphan janitor only sweeps disk."""
    ram_only = uuid4()
    await hybrid._ram.put(ram_only, {"a": b"ram"})
    # Empty known-set: every disk chain is an orphan; ram-only is not.
    orphans = await hybrid.list_orphans(set())
    assert ram_only not in orphans


async def test_start_stop_drives_both_components(tmp_path: Path) -> None:
    """:meth:`start`/:meth:`stop` are forwarded to both halves."""
    ram = RamBodyStore()
    disk = FileBodyStore(tmp_path / "bodies", shard_prefix_chars=2)
    store = HybridBodyStore(ram=ram, disk=disk)
    await store.start()
    # Both RAM and disk should be operable.
    chain_id = uuid4()
    await store.put(chain_id, {"a": b"works"})
    assert await store.get(chain_id, "a") == b"works"
    await store.stop()
