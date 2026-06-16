"""Unit tests for the idempotency-index cleanup behavior.

Slice 1.D rewrite (plan § 2.3.16). The single-store collapse removes
the dual-store ``preserve_chain_ids`` carve-out: every live chain_id
is in the persistent store's ``uploads`` table by construction, so
the cleanup pass drops every index row whose linked upload is absent
— no carve-out needed.

The pre-collapse ``test_cleanup_preserves_memory_tier_chain_ids`` and
``test_cleanup_mixed_preserve`` tests exercised the dual-store-only
carve-out semantic; they're deleted with a ledger row per plan § 0.9.
"""

from __future__ import annotations

from collections.abc import Callable
from uuid import uuid4

import pytest
from phantom.models.upload import UploadRow
from phantom.storage.sqlite_store import SqliteUploadStore


@pytest.fixture
async def store():
    """A started single persistent store on an in-memory DB."""
    s = SqliteUploadStore(":memory:")
    await s.start()
    yield s
    await s.stop()


@pytest.mark.asyncio
async def test_cleanup_drops_orphaned_index_rows(store: SqliteUploadStore) -> None:
    """An index row whose chain has been deleted from uploads IS reaped.

    The baseline: cleanup drops dedup metadata for rows the reaper
    already purged.
    """
    chain_id = uuid4()
    await store.claim_idempotency("key-orphan", chain_id)
    deleted = await store.cleanup_idempotency_index()
    assert deleted == 1


@pytest.mark.asyncio
async def test_find_by_chain_id_at_ingress(
    store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Admission's fallback scan finds the row by ingress key."""
    chain_id = uuid4()
    row = make_upload_row(chain_id=chain_id, chain_id_at_ingress="k-direct")
    await store.insert(row)
    found = await store.find_by_chain_id_at_ingress("k-direct")
    assert found == chain_id


@pytest.mark.asyncio
async def test_find_by_chain_id_at_ingress_no_match(
    store: SqliteUploadStore,
) -> None:
    """find_by_chain_id_at_ingress returns None when no row matches."""
    assert await store.find_by_chain_id_at_ingress("never-claimed") is None


@pytest.mark.asyncio
async def test_find_by_chain_id_at_ingress_ignores_null(
    store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Rows without a chain_id_at_ingress aren't matched on empty lookups."""
    row = make_upload_row(chain_id_at_ingress=None)
    await store.insert(row)
    assert await store.find_by_chain_id_at_ingress("anything") is None


@pytest.mark.asyncio
async def test_list_all_chain_ids(
    store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """list_all_chain_ids enumerates every row in any state."""
    a, b, c = uuid4(), uuid4(), uuid4()
    await store.insert(make_upload_row(chain_id=a, state="queued"))
    await store.insert(make_upload_row(chain_id=b, state="succeeded"))
    await store.insert(make_upload_row(chain_id=c, state="failed"))
    ids = set(await store.list_all_chain_ids())
    assert ids == {a, b, c}


@pytest.mark.asyncio
async def test_cleanup_preserves_live_rows(
    store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """The cleanup pass does NOT touch index rows whose upload is still live.

    Single-store collapse: every live chain_id is in this store's
    ``uploads`` table by construction. The cleanup pass drops only
    rows whose linked upload is absent. This locks the regression that
    the cleanup pass doesn't over-eagerly drop live rows.
    """
    live = uuid4()
    orphan = uuid4()
    await store.insert(make_upload_row(chain_id=live, state="queued"))
    await store.claim_idempotency("k-live", live)
    await store.claim_idempotency("k-orphan", orphan)
    deleted = await store.cleanup_idempotency_index()
    assert deleted == 1
    # Re-claim of live key returns the same chain_id (dedup survived).
    assert await store.claim_idempotency("k-live", uuid4()) == live
