"""Contract tests for the /v1/admin/chains-family endpoints.

Covers the read-side of the chains surface:

- ``GET /v1/admin/chains`` — paginated list.
- ``GET /v1/admin/chains/{chain_id}`` — detail (200 + 404).

The write-side endpoints (replay / cancel / delete / bulk_delete /
extract / export / body / bundle) are exercised by the existing
test_admin_models_alignment.py / E2E tests; this module focuses on
the read-side wire format that the phantom-client SDK deserializes
into UploadRow / ListUploadsResponse / ChainAdminDetail.
"""

from __future__ import annotations

import io
import json
import tarfile
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.instances.context import InstanceContext
from phantom.models.upload import BodyHash, BodyHashes, StorageHash, UploadRow


async def _insert_test_row(
    ctx: InstanceContext,
    *,
    group_id: UUID | None = None,
    multifile_id: UUID | None = None,
    send_order: int = 0,
) -> UploadRow:
    """Insert one queued row + minimal body_hashes into the store.

    ``group_id`` defaults to the row's chain_id (admission's default
    semantics); ``multifile_id`` defaults to None (standalone).
    """
    chain_id = uuid4()
    now = datetime.now(tz=UTC)
    row = UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": group_id if group_id is not None else chain_id,
            "multifile_id": multifile_id,
            "send_order": send_order,
            "route_name": "files",
            "state": "queued",
            "body_location": "ram",
            "received_at": now,
            "updated_at": now,
            "endpoint": "files.example.com",
            "uid": "test-uid",
            "chain_envelope_json": "{}",
            "idempotency_key": f"k-{chain_id}",
            "capture_reexecution_active": False,
            "body_hashes": {
                "body": BodyHashes(
                    body_hash=BodyHash("a" * 64),
                    storage_hash=StorageHash("b" * 64),
                ),
            },
            "body_size_bytes": 100,
        },
    )
    await ctx.store.insert(row)
    return row


async def test_list_chains_empty_returns_empty_list(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """``GET /chains`` on an empty store returns ``{uploads: [], next_cursor: null}``."""
    app, _ctx = admin_app
    client = TestClient(app)
    response = client.get("/v1/admin/chains")
    assert response.status_code == 200, response.text
    body = response.json()
    assert "uploads" in body
    assert isinstance(body["uploads"], list)
    assert body["uploads"] == []
    assert body.get("next_cursor") is None


async def test_list_chains_with_one_row_returns_it(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """``GET /chains`` returns the inserted row in the uploads array."""
    app, ctx = admin_app
    row = await _insert_test_row(ctx)
    client = TestClient(app)
    response = client.get("/v1/admin/chains")
    assert response.status_code == 200, response.text
    body = response.json()
    assert len(body["uploads"]) == 1
    assert body["uploads"][0]["chain_id"] == str(row.chain_id)
    assert body["uploads"][0]["state"] == "queued"
    assert body["uploads"][0]["body_location"] == "ram"


async def test_list_chains_multifile_filter_orders_by_send_order(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """``GET /chains?multifile_id=`` returns ONLY the set, ordered by send_order."""
    app, ctx = admin_app
    multifile_id = uuid4()
    inserted: dict[int, UploadRow] = {}
    for position in (2, 0, 1):  # deliberately out of order
        inserted[position] = await _insert_test_row(
            ctx, multifile_id=multifile_id, send_order=position
        )
    await _insert_test_row(ctx)  # standalone row: excluded
    client = TestClient(app)
    response = client.get(f"/v1/admin/chains?multifile_id={multifile_id}")
    assert response.status_code == 200, response.text
    body = response.json()
    assert [u["send_order"] for u in body["uploads"]] == [0, 1, 2]
    assert [u["chain_id"] for u in body["uploads"]] == [
        str(inserted[i].chain_id) for i in (0, 1, 2)
    ]
    assert body.get("next_cursor") is None


async def test_list_chains_multifile_filter_rejects_cursor(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """Combining ``multifile_id`` with ``cursor`` is a 422 (not paginated)."""
    app, _ctx = admin_app
    client = TestClient(app)
    response = client.get(
        f"/v1/admin/chains?multifile_id={uuid4()}&cursor=opaque-token",
    )
    assert response.status_code == 422
    assert "multifile_id" in response.text


async def test_list_chains_group_filter_returns_group_only(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """``GET /chains?group_id=`` returns exactly the group's members."""
    app, ctx = admin_app
    group_id = uuid4()
    members = [await _insert_test_row(ctx, group_id=group_id) for _ in range(2)]
    outsider = await _insert_test_row(ctx)
    client = TestClient(app)
    response = client.get(f"/v1/admin/chains?group_id={group_id}")
    assert response.status_code == 200, response.text
    body = response.json()
    returned = {u["chain_id"] for u in body["uploads"]}
    assert returned == {str(m.chain_id) for m in members}
    assert str(outsider.chain_id) not in returned
    assert all(u["group_id"] == str(group_id) for u in body["uploads"])


async def test_get_chain_unknown_returns_404(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """``GET /chains/{chain_id}`` returns 404 for an unknown chain_id."""
    app, _ctx = admin_app
    client = TestClient(app)
    missing_id = uuid4()
    response = client.get(f"/v1/admin/chains/{missing_id}")
    assert response.status_code == 404


async def test_get_chain_known_returns_detail(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """``GET /chains/{chain_id}`` returns the ChainAdminDetail for a known row."""
    app, ctx = admin_app
    row = await _insert_test_row(ctx)
    client = TestClient(app)
    response = client.get(f"/v1/admin/chains/{row.chain_id}")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["chain_id"] == str(row.chain_id)
    assert body["state"] == "queued"
    assert body["body_location"] == "ram"
    # ChainAdminDetail exposes a set of admin-visible fields; assert
    # the load-bearing identification + state fields are present.
    assert "captured" in body
    assert "attempts" in body
    assert "metadata" in body or "steps" in body or "body_size_bytes" in body


def _parse_export_manifest(tar_bytes: bytes) -> list[dict[str, Any]]:
    """Extract and decode manifest.json from an export.tar payload."""
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r") as tf:
        member = tf.extractfile("manifest.json")
        assert member is not None, "export.tar carries no manifest.json"
        loaded = json.loads(member.read().decode("utf-8"))
    assert isinstance(loaded, list)
    return loaded


async def test_export_manifest_carries_group_id_and_sent_at_per_member(
    admin_app: tuple[FastAPI, InstanceContext],
) -> None:
    """The export manifest correlates every member to its group (task 4.6).

    Round 3 adversary (manifest-honesty seed): the cycle-7 export adds
    ``group_id`` and ``sent_at`` to each manifest entry so an operator
    can correlate exported bodies back to the group surface without a
    second lookup. The existing e2e pins chain_id + state only; this
    pins the two new fields for a real multi-member group. Every member
    of one group must carry the SAME ``group_id`` string, an
    undelivered member's ``sent_at`` must be JSON null (not omitted,
    not the empty string), and a standalone outsider must NOT borrow
    the group's id. Field scale is well under the per-instance cap, so
    no truncation applies; the contract being pinned is per-row
    group-correlation honesty.
    """
    app, ctx = admin_app
    group_id = uuid4()
    members = [
        await _insert_test_row(ctx, group_id=group_id, multifile_id=group_id, send_order=i)
        for i in range(3)
    ]
    outsider = await _insert_test_row(ctx)
    # Give each row a body so the exporter packs something; the rows are
    # queued/undelivered, so sent_at stays null across the board.
    for row in [*members, outsider]:
        await ctx.body_store.put(row.chain_id, {"body": b"export-body"})

    client = TestClient(app)
    with client.stream("GET", "/v1/admin/export.tar") as response:
        assert response.status_code == 200, response.text
        tar_bytes = b"".join(response.iter_bytes())
    manifest = _parse_export_manifest(tar_bytes)

    by_chain = {entry["chain_id"]: entry for entry in manifest}
    for member in members:
        entry = by_chain[str(member.chain_id)]
        assert entry["group_id"] == str(group_id), "a member lost its group correlation"
        assert entry["sent_at"] is None, "an undelivered member must report sent_at null"
    outsider_entry = by_chain[str(outsider.chain_id)]
    assert outsider_entry["group_id"] == str(outsider.chain_id), (
        "a standalone row's group_id defaults to its own chain_id, never the group's"
    )
