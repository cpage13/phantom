"""Unit tests for phantom.storage.ram_body_store."""

from __future__ import annotations

from uuid import uuid4

import pytest
from phantom.storage.ram_body_store import RamBodyStore


@pytest.fixture
async def store():
    """Started RAM body store."""
    s = RamBodyStore()
    await s.start()
    yield s
    await s.stop()


@pytest.mark.asyncio
async def test_put_get_delete(store: RamBodyStore) -> None:
    """Put/get/delete round-trip."""
    uid = uuid4()
    total = await store.put(uid, {"body": b"hello", "extra": b"world"})
    assert total == 10
    assert await store.get(uid, "body") == b"hello"
    assert await store.get_all(uid) == {"body": b"hello", "extra": b"world"}
    await store.delete(uid)
    with pytest.raises(KeyError):
        await store.get(uid, "body")


@pytest.mark.asyncio
async def test_total_bytes(store: RamBodyStore) -> None:
    """``total_bytes`` returns sum across uploads."""
    u1, u2 = uuid4(), uuid4()
    await store.put(u1, {"body": b"a" * 10})
    await store.put(u2, {"body": b"b" * 20})
    assert await store.total_bytes() == 30


@pytest.mark.asyncio
async def test_list_chain_ids(store: RamBodyStore) -> None:
    """``list_chain_ids`` returns the set of stored chain_ids."""
    chain_ids = {uuid4(), uuid4()}
    for c in chain_ids:
        await store.put(c, {"body": b"x"})
    assert set(await store.list_chain_ids()) == chain_ids


@pytest.mark.asyncio
async def test_delete_idempotent(store: RamBodyStore) -> None:
    """``delete`` on missing uid is a no-op."""
    await store.delete(uuid4())
