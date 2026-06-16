"""SDK-against-real-app contract tests for the admin surface.

The other admin contract modules hit the raw routes with
``TestClient.get(...)`` and never drive the phantom-client SDK methods
against a real app. That blind spot is exactly why four client-facing
admin methods drifted from their server shapes undetected (R-EX1 ..
R-EX4): the response models on the two sides diverged and no test
exercised the round-trip. This module closes the gap by constructing a
real :class:`PhantomClient` over an ``httpx.ASGITransport`` pointed at
the same ``admin_app`` fixture the route contract tests use, then
calling the SDK admin methods and asserting the SDK parses the live
server response. Any future SDK<->server drift on these methods fails
here, at the contract tier, rather than only in the heavier e2e tier.

Covers the four methods the wire-contract pass fixed, plus the
malformed-input guard that proves R-EX1 did not weaken validation:

* ``extract`` with ``chain_ids`` + ``since`` filters (R-EX1).
* ``bulk_delete`` with a ``since`` filter (R-EX1, the DeleteFilter side).
* ``fetch_bundle`` -> ``UploadBundle.body_refs`` (R-EX2).
* ``invalidate_token`` -> slot persists ``status='bad'`` (R-EX3).
* ``list_instances`` -> envelope parsed to ``[InstanceSummary]`` (R-EX4).
"""

from __future__ import annotations

import io
import json
import tarfile
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from phantom.instances.context import InstanceContext
from phantom.models.upload import BodyHash, BodyHashes, StorageHash, UploadRow
from phantom_client import ExtractFilter, PhantomClient
from phantom_client.models.admin import DeleteFilter

# The body-ref name every seeded upload carries (matches the intake
# envelope's single "body" part).
BODY_REF_NAME: str = "body"

# A base URL the ASGITransport ignores for routing but httpx requires.
ASGI_BASE_URL: str = "http://phantom.test"


async def _insert_row_with_body(
    ctx: InstanceContext,
    *,
    body: bytes,
    received_at: datetime,
) -> UploadRow:
    """Insert one queued row + its body into the wired stores.

    Mirrors the seeding helper in ``test_chains_endpoints.py`` but also
    writes the body so the extract tar and the bundle carry real bytes.
    """
    chain_id = uuid4()
    now = datetime.now(tz=UTC)
    row = UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": chain_id,
            "multifile_id": None,
            "send_order": 0,
            "route_name": "files",
            "state": "auth_expired",
            "body_location": "ram",
            "received_at": received_at,
            "updated_at": now,
            "endpoint": "files.example.com",
            "uid": "test-uid",
            "chain_envelope_json": "{}",
            "idempotency_key": f"k-{chain_id}",
            "capture_reexecution_active": False,
            "body_hashes": {
                BODY_REF_NAME: BodyHashes(
                    body_hash=BodyHash("a" * 64),
                    storage_hash=StorageHash("b" * 64),
                ),
            },
            "body_size_bytes": len(body),
        },
    )
    await ctx.store.insert(row)
    await ctx.body_store.put(chain_id, {BODY_REF_NAME: body})
    return row


def _sdk_client(app: FastAPI) -> PhantomClient:
    """Build a PhantomClient that speaks to ``app`` in-process via ASGI."""
    return PhantomClient(ASGI_BASE_URL, transport=httpx.ASGITransport(app=app))


async def _drain(iter_: AsyncIterator[bytes]) -> bytes:
    """Concatenate every chunk of a streaming SDK response."""
    chunks: list[bytes] = []
    async for chunk in iter_:
        chunks.append(chunk)
    return b"".join(chunks)


def _manifest_chain_ids(tar_bytes: bytes) -> set[UUID]:
    """Return the set of chain_ids the extract tar's manifest lists."""
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:*") as tar:
        for member in tar.getmembers():
            if member.name in {"manifest.json", "./manifest.json"} and member.isfile():
                extracted = tar.extractfile(member)
                data = extracted.read() if extracted is not None else b""
                manifest = json.loads(data.decode())
                return {UUID(entry["chain_id"]) for entry in manifest}
    raise AssertionError(f"manifest.json missing from extract tar; bytes={len(tar_bytes)}")


@pytest.mark.asyncio
async def test_sdk_extract_chain_ids_and_since_narrow_the_tar(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """SDK ``extract`` submits chain_ids + since over the wire (R-EX1)."""
    app, ctx = admin_app
    base = datetime(2026, 1, 1, tzinfo=UTC)
    old = await _insert_row_with_body(ctx, body=b"old-body", received_at=base)
    mid = await _insert_row_with_body(ctx, body=b"mid-body", received_at=base + timedelta(hours=2))
    new = await _insert_row_with_body(ctx, body=b"new-body", received_at=base + timedelta(hours=4))

    client = _sdk_client(app)
    async with client:
        # chain_ids alone: exactly the named subset (mid excluded).
        subset = {old.chain_id, new.chain_id}
        tar_ids = _manifest_chain_ids(
            await _drain(await client.extract(ExtractFilter(chain_ids=list(subset))))
        )
        assert tar_ids == subset, f"chain_ids did not narrow the tar: {tar_ids} != {subset}"

        # chain_ids AND since compose: a cutoff between old and mid drops
        # old while keeping mid + new (the R8-5 compose, over the wire).
        cutoff = base + timedelta(hours=1)
        combined = _manifest_chain_ids(
            await _drain(
                await client.extract(
                    ExtractFilter(
                        chain_ids=[old.chain_id, mid.chain_id, new.chain_id], since=cutoff
                    )
                )
            )
        )
        assert combined == {mid.chain_id, new.chain_id}, (
            f"since did not further restrict within chain_ids: {combined}"
        )


@pytest.mark.asyncio
async def test_extract_malformed_uuid_still_422(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """A malformed chain_id still 422s at the boundary (R-EX1 not weakened).

    The fix relaxed strict only enough to coerce the natural string
    representation; a non-uuid must still be rejected. A valid
    ``ExtractFilter`` cannot hold a malformed uuid, so this drives the
    route with the raw bad body and asserts the server refuses it with the
    ``request_invalid`` envelope the SDK maps to
    :class:`PhantomValidationError`.
    """
    app, _ctx = admin_app
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=ASGI_BASE_URL
    ) as http:
        response = await http.post(
            "/v1/admin/chains/extract",
            content=json.dumps({"chain_ids": ["not-a-uuid"]}),
            headers={"Content-Type": "application/json"},
        )
    assert response.status_code == 422, response.text
    assert response.json()["error"]["code"] == "request_invalid", response.text


@pytest.mark.asyncio
async def test_sdk_bulk_delete_with_since_filter(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """SDK ``bulk_delete`` submits a since filter over the wire (R-EX1)."""
    app, ctx = admin_app
    base = datetime(2026, 1, 1, tzinfo=UTC)
    old = await _insert_row_with_body(ctx, body=b"old", received_at=base)
    new = await _insert_row_with_body(ctx, body=b"new", received_at=base + timedelta(hours=4))

    client = _sdk_client(app)
    async with client:
        # Delete everything received at/after a cutoff between the two rows:
        # only the newer row matches.
        deleted = await client.bulk_delete(DeleteFilter(since=base + timedelta(hours=1)))
        assert deleted == 1, f"since-filtered bulk_delete removed {deleted} rows, expected 1"

    # The old row survives, the new one is gone.
    assert await ctx.store.get(old.chain_id) is not None
    assert await ctx.store.get(new.chain_id) is None


@pytest.mark.asyncio
async def test_sdk_fetch_bundle_returns_body_refs(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """SDK ``fetch_bundle`` parses the server body_refs map (R-EX2)."""
    app, ctx = admin_app
    body = b"phantom-contract-bundle-distinct-body"
    row = await _insert_row_with_body(ctx, body=body, received_at=datetime(2026, 1, 1, tzinfo=UTC))

    client = _sdk_client(app)
    async with client:
        bundle = await client.fetch_bundle(row.chain_id)
    assert bundle.metadata.chain_id == row.chain_id
    assert bundle.body_refs.get(BODY_REF_NAME) == body, (
        f"bundle body_refs did not carry the body under {BODY_REF_NAME!r}: "
        f"{sorted(bundle.body_refs)}"
    )


@pytest.mark.asyncio
async def test_sdk_invalidate_token_marks_slot_bad(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """SDK ``invalidate_token`` leaves the slot present as bad (R-EX3 / ADR-003)."""
    app, _ctx = admin_app
    endpoint = "files.example.com"
    uid = "test-uid"

    client = _sdk_client(app)
    async with client:
        await client.push_token(endpoint=endpoint, uid=uid, token="Bearer push-me")
        before = [s for s in await client.list_tokens(endpoint=endpoint) if s.uid == uid]
        assert before and before[0].status in {"fresh", "unknown"}, (
            f"freshly pushed slot unexpected: {before}"
        )

        await client.invalidate_token(endpoint=endpoint, uid=uid)
        after = [s for s in await client.list_tokens(endpoint=endpoint) if s.uid == uid]
        assert after, "slot vanished after invalidate; ADR-003 preserves it as status='bad'"
        assert after[0].status == "bad", f"slot status after invalidate={after[0].status!r}"


@pytest.mark.asyncio
async def test_sdk_list_instances_parses_envelope(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """SDK ``list_instances`` parses the server envelope (R-EX4)."""
    app, _ctx = admin_app
    client = _sdk_client(app)
    async with client:
        instances = await client.list_instances()
    ids = {summary.id for summary in instances}
    assert ids == {"primary"}, f"list_instances did not surface the configured instance: {ids}"
