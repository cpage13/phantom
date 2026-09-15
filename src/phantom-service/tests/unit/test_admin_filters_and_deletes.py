"""Four admin surfaces that answered wrongly rather than answering at all.

Review 2026-09-08:

* S1-4 the ``body_location`` filter was applied in Python AFTER the store had
  spent its LIMIT, so a page came back EMPTY with a non-null ``next_cursor``
  while matching rows sat further along. A client using the natural "stop on
  an empty page" idiom concluded there were no RAM-held chains.
* S1-1 ``bulk_delete_uploads`` accepted ``instance`` as a satisfying filter
  and never forwarded it to the store, so the store's own empty-filter guard
  raised an uncaught ``ValueError`` on the exact request the route's own 422
  message, ADR-017 and the SDK all advertise as valid.
* S1-2 the two single-chain body reads caught only the PARTIAL absence shape.
  The whole body namespace being gone, which is what the default
  ``retention.succeeded_body_seconds`` produces on every delivered chain,
  raised ``KeyError`` out of the handler.
* A4/D3 ``delete_upload`` deleted the BYTES before confirming the row delete,
  so a failing ``store.delete`` left a live row with no body: the sender then
  picked it up and drove it to ``corrupted``, turning an operator action plus
  a transient lock into a storage-fault diagnostic.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from phantom.instances.context import InstanceContext
from phantom.models.upload import BodyHashes

from ._admin_truth_app import build_admin_app, build_instance

# A syntactically valid SHA-256 hex digest. These rows are never sent, so no
# assertion depends on the digest matching any bytes; it only has to be a
# well-formed declaration for the row model.
_DIGEST = "a" * 64

# Receipt times one second apart, so the store's ``received_at ASC, chain_id
# ASC`` keyset order is the order the test writes rows in rather than a
# chain_id coin toss.
_EPOCH = datetime(2026, 9, 8, 12, 0, 0, tzinfo=UTC)


async def _seed(
    ctx: InstanceContext,
    make_upload_row: Any,
    locations: list[str],
) -> list[UUID]:
    """Insert one row per entry of ``locations``, one second apart.

    Args:
        ctx: The instance to insert into.
        make_upload_row: The row-factory fixture.
        locations: The ``body_location`` of each row, in receipt order.

    Returns:
        The chain_ids in receipt order.
    """
    ids: list[UUID] = []
    for index, location in enumerate(locations):
        chain_id = uuid4()
        received = _EPOCH + timedelta(seconds=index)
        await ctx.store.insert(
            make_upload_row(
                chain_id=chain_id,
                state="queued",
                body_location=location,
                received_at=received,
                updated_at=received,
            )
        )
        ids.append(chain_id)
    return ids


@pytest.mark.asyncio
async def test_body_location_page_is_not_empty_while_matches_remain(
    tmp_path: Path,
    make_upload_row: Any,
) -> None:
    """S1-4: the filtered page must carry the matches, not an empty list.

    Objective: the review's own reproduction - four rows, three on file and
    one in RAM, read with ``?body_location=ram&limit=2``. The store spent the
    LIMIT on the two file rows and the Python filter then emptied the page,
    while ``next_cursor`` stayed non-null. Success: the RAM row is on the
    page, and the walk that found it reports itself finished.
    """
    ctx = await build_instance(tmp_path, "primary")
    seeded = await _seed(ctx, make_upload_row, ["file", "file", "file", "ram"])
    client = TestClient(build_admin_app([ctx]))

    body = client.get("/v1/admin/chains", params={"body_location": "ram", "limit": 2}).json()

    assert [row["chain_id"] for row in body["uploads"]] == [str(seeded[3])], (
        "the only RAM-held chain was filtered off a page the store had already spent"
    )
    assert body["next_cursor"] is None


@pytest.mark.asyncio
async def test_body_location_pagination_walk_sees_every_match(
    tmp_path: Path,
    make_upload_row: Any,
) -> None:
    """S1-4: following the cursor must find every match exactly once.

    Objective: the harm is what a paginating client concludes, so walk the
    way a client does - request, consume, follow ``next_cursor``, stop when a
    page comes back empty. Success: all four RAM rows arrive, in receipt
    order, with no repeats, through a row layout whose pages do not divide
    evenly into the limit.

    The layout deliberately makes one page overflow the caller's limit
    (``ram, file, ram, ram``), which is the case where a naive
    keep-reading loop would skip the overflow rows.
    """
    ctx = await build_instance(tmp_path, "primary")
    seeded = await _seed(ctx, make_upload_row, ["ram", "file", "ram", "ram", "file", "ram", "file"])
    expected = [str(seeded[i]) for i in (0, 2, 3, 5)]
    client = TestClient(build_admin_app([ctx]))

    collected: list[str] = []
    cursor: str | None = None
    for _ in range(len(seeded) + 1):
        params: dict[str, Any] = {"body_location": "ram", "limit": 2}
        if cursor is not None:
            params["cursor"] = cursor
        page = client.get("/v1/admin/chains", params=params).json()
        if not page["uploads"]:
            break
        collected.extend(row["chain_id"] for row in page["uploads"])
        cursor = page["next_cursor"]
        if cursor is None:
            break

    assert collected == expected
    assert len(collected) == len(set(collected))


@pytest.mark.asyncio
async def test_unfiltered_listing_still_pages_one_store_call_at_a_time(
    tmp_path: Path,
    make_upload_row: Any,
) -> None:
    """S1-4: the unfiltered path must be untouched.

    Objective: the filtered walk shares its code path with the unfiltered
    listing, which is the overwhelmingly common request; a regression there
    would be far worse than the bug being fixed. Success: a limit-2 read over
    four rows returns the first two in receipt order with a non-null cursor,
    exactly as before.
    """
    ctx = await build_instance(tmp_path, "primary")
    seeded = await _seed(ctx, make_upload_row, ["file", "ram", "file", "ram"])
    client = TestClient(build_admin_app([ctx]))

    body = client.get("/v1/admin/chains", params={"limit": 2}).json()

    assert [row["chain_id"] for row in body["uploads"]] == [str(seeded[0]), str(seeded[1])]
    assert body["next_cursor"] is not None


@pytest.mark.asyncio
async def test_bulk_delete_by_instance_alone_deletes_that_instance(
    tmp_path: Path,
    make_upload_row: Any,
) -> None:
    """S1-1: an instance-only filter must delete, not raise.

    Objective: the route narrowed its targets by ``instance`` and then called
    the store with every filter None, so the store's empty-filter guard
    raised ``ValueError`` and the request failed. Success: the named
    instance's rows are deleted, the count is reported, and the other
    instance's rows are untouched.
    """
    primary = await build_instance(tmp_path, "primary")
    secondary = await build_instance(tmp_path, "secondary")
    for _ in range(2):
        await primary.store.insert(make_upload_row(chain_id=uuid4(), instance_id="primary"))
    await secondary.store.insert(make_upload_row(chain_id=uuid4(), instance_id="secondary"))
    client = TestClient(build_admin_app([primary, secondary]))

    response = client.request("DELETE", "/v1/admin/chains", json={"instance": "primary"})

    assert response.status_code == 200, response.text
    assert response.json()["deleted"] == 2
    assert await primary.store.list_non_terminal() == []
    assert len(await secondary.store.list_non_terminal()) == 1


@pytest.mark.asyncio
async def test_bulk_delete_with_no_filter_at_all_is_still_refused(
    tmp_path: Path,
) -> None:
    """S1-1: forwarding ``instance`` must not weaken the empty-filter guard.

    Objective: ADR-004 refuses an all-None filter because it would mean
    "delete every row"; the fix adds a filter field to the forwarded set, so
    pin that the refusal still fires. Success: the 422
    ``bulk_delete_filter_empty`` envelope.
    """
    ctx = await build_instance(tmp_path, "primary")
    client = TestClient(build_admin_app([ctx]))

    response = client.request("DELETE", "/v1/admin/chains", json={})

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "bulk_delete_filter_empty"


@pytest.mark.asyncio
async def test_body_reads_refuse_a_whole_missing_namespace(
    tmp_path: Path,
    make_upload_row: Any,
) -> None:
    """S1-2: the common absence shape must refuse like the partial one.

    Objective: ``BodyStore.get_all`` raises ``KeyError`` when the entire
    chain namespace is gone, which is what the sender does to every chain
    that succeeds at the default retention. Both single-chain reads let that
    escape the handler. Success: both answer the same
    ``storage_corruption`` envelope the partial shape already produced,
    naming the declared ref.
    """
    ctx = await build_instance(tmp_path, "primary")
    chain_id = uuid4()
    await ctx.store.insert(
        make_upload_row(
            chain_id=chain_id,
            state="succeeded",
            body_hashes={"body": BodyHashes(body_hash=_DIGEST, storage_hash=_DIGEST)},
        )
    )
    client = TestClient(build_admin_app([ctx]))

    body = client.get(f"/v1/admin/chains/{chain_id}/body")
    bundle = client.get(f"/v1/admin/chains/{chain_id}/bundle")

    for response in (body, bundle):
        assert response.status_code == 500, response.text
        assert response.json()["error"]["code"] == "storage_corruption"
        assert "body" in response.json()["error"]["message"]


@pytest.mark.asyncio
async def test_body_read_still_streams_a_complete_body(
    tmp_path: Path,
    make_upload_row: Any,
) -> None:
    """S1-2: a complete body must still be streamed.

    Objective: the refusal is new on one absence shape only; a chain whose
    declared refs are all present must be unaffected. Success: the bytes come
    back with a 200.
    """
    ctx = await build_instance(tmp_path, "primary")
    chain_id = uuid4()
    await ctx.store.insert(
        make_upload_row(
            chain_id=chain_id,
            body_hashes={"body": BodyHashes(body_hash=_DIGEST, storage_hash=_DIGEST)},
        )
    )
    await ctx.body_store.put(chain_id, {"body": b"the whole payload"})
    client = TestClient(build_admin_app([ctx]))

    response = client.get(f"/v1/admin/chains/{chain_id}/body")

    assert response.status_code == 200, response.text
    assert response.content == b"the whole payload"


@pytest.mark.asyncio
async def test_a_failed_row_delete_leaves_the_body_alone(
    tmp_path: Path,
    make_upload_row: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A4/D3: a half-landed delete must name itself, not drift into corrupted.

    Objective: the route deletes bytes first so that the live row structurally
    blocks a same-chain_id re-POST for the whole window. The cost of that
    ordering is this case: a ``store.delete`` that raises (a WAL write lock
    held past the busy timeout) leaves a live ``queued`` row with no bytes.

    A4 proposed inverting the order to avoid it. Inverting is worse: the row
    DELETE legalizes a re-POST immediately, and any guard on the later body
    delete is a time-of-check-to-time-of-use gap, so a re-POST landing inside
    it has its OWN accepted bytes deleted after answering 202. The ordering is
    pinned by ``test_single_delete_blocks_readmission_by_ordering``.

    So the remedy is diagnostic, and that is what this pins. Success: the
    operator gets a typed ``storage_unavailable`` envelope that says the bodies
    are gone and the DELETE should be retried, the row is still there to be
    retried against, and the failure never reaches the sender as a fabricated
    ``body_missing_in_sender`` storage fault.
    """
    ctx = await build_instance(tmp_path, "primary")
    chain_id = uuid4()
    await ctx.store.insert(make_upload_row(chain_id=chain_id, state="queued"))
    await ctx.body_store.put(chain_id, {"body": b"still deliverable"})

    async def _locked(_chain_id: UUID) -> None:
        """Stand in for the write lock held past the busy timeout."""
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(ctx.store, "delete", _locked)
    # raise_server_exceptions=False makes the client return the registered
    # handler's envelope instead of re-raising the fault into the test, which
    # is what a real operator's curl would see.
    client = TestClient(build_admin_app([ctx]), raise_server_exceptions=False)

    response = client.request("DELETE", f"/v1/admin/chains/{chain_id}")

    body = response.json()
    assert body["error"]["code"] == "storage_unavailable", response.text
    assert str(chain_id) in body["error"]["message"], (
        "the envelope does not name the chain whose delete half-landed, so the "
        "operator cannot tell which row to retry"
    )
    assert body["error"]["details"]["bodies_deleted"] is True, (
        "the envelope does not say the bytes are already gone, which is the one "
        "fact that distinguishes this from an ordinary failed delete"
    )
    assert await ctx.store.get(chain_id) is not None, (
        "the row is gone as well, so the operator has nothing left to retry "
        "against and the bytes are unrecoverable"
    )


@pytest.mark.asyncio
async def test_delete_upload_still_removes_the_row_and_its_body(
    tmp_path: Path,
    make_upload_row: Any,
) -> None:
    """A4/D3: the happy path must still delete both halves.

    Objective: reordering the two deletes must not turn the successful case
    into a body leak. Success: a 204, no row, and no body.
    """
    ctx = await build_instance(tmp_path, "primary")
    chain_id = uuid4()
    await ctx.store.insert(make_upload_row(chain_id=chain_id, state="failed"))
    await ctx.body_store.put(chain_id, {"body": b"payload"})
    client = TestClient(build_admin_app([ctx]))

    response = client.request("DELETE", f"/v1/admin/chains/{chain_id}")

    assert response.status_code == 204, response.text
    assert await ctx.store.get(chain_id) is None
    assert not await ctx.body_store.has_body_ref(chain_id, "body")
