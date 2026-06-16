"""C1 audit closure: ``bulk_delete`` removes body files alongside rows.

Phase 2 § 3.2.1. Before this fix, ``bulk_delete_uploads`` deleted the
``uploads`` row only — body files in :class:`FileBodyStore` remained on
disk as orphans until the body-orphan janitor's next sweep. The
operator's "delete these uploads" expectation includes the bodies, so
the route now iterates the deleted chain_ids and calls
``body_store.delete`` for each.

This test exercises the route function directly: insert rows + bodies,
trigger ``bulk_delete_uploads`` via the FastAPI test client, assert the
body files are gone immediately (no janitor sweep needed).
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.chain.executor import ChainExecutor, default_clock
from phantom.compression import BodyCodec, select_codec
from phantom.config.settings import (
    CompressionCfg,
    InstanceCfg,
    PersistTriggerCfg,
    RouteCfg,
)
from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.routes import admin as admin_routes
from phantom.routing import resolve_route
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.transport import UpstreamRequest, UpstreamResponse
from phantom.workers.saturation import SaturationGate

from .conftest import make_snapshot, snapshot_thunk, track_instance


class _FakeUpstream:
    """Stub UpstreamClient — admin tests never call upstream."""

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


@pytest.fixture
async def app_and_ctx(tmp_path: Path) -> Iterable[tuple[FastAPI, InstanceContext]]:
    """Build an app exposing the admin router + a single instance ctx."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await store.start()
    await ram.start()
    await fbs.start()
    await tokens.start()
    cfg = InstanceCfg(
        id="primary",
        host_prefixes=["files.example.com"],
        data_dir="primary",
        routes=[
            RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer"),
        ],
    )
    upstream = _FakeUpstream()
    executor = ChainExecutor(
        token_cache=tokens,
        upstream_client=upstream,
        resolve_route=resolve_route,
        clock=default_clock,
        instance=cfg,
    )
    saturation = SaturationGate(
        max_in_flight=10, max_in_flight_bytes=1_000_000, max_disk_bytes=10_000_000
    )

    def _passthrough_factory() -> BodyCodec:
        return select_codec(CompressionCfg(algorithm="original"))

    persist_trigger = PersistTriggerCfg(body_size_threshold_bytes=0)
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    await body_store.start()
    ctx = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=None,
        token_cache=tokens,
        minter=None,
        retry_strategy=FixedIntervalsStrategy([1]),
        upstream_client=upstream,
        executor=executor,
        saturation=saturation,
        codec_factory=_passthrough_factory,
        current_settings=snapshot_thunk(make_snapshot(persist_trigger=persist_trigger)),
    )
    track_instance(ctx)
    dispatcher = InstanceDispatcher([ctx])
    app = FastAPI()
    app.include_router(admin_routes.router)
    # The ONE shared helper registers every admin typed-error handler so
    # this fixture cannot drift from production app.py (round 3 fix R3-1).
    admin_routes.register_admin_error_handlers(app)
    app.dependency_overrides[admin_routes.get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[admin_routes.get_version] = lambda: "0.1.0"

    yield app, ctx


@pytest.mark.asyncio
async def test_bulk_delete_removes_body_files_for_deleted_rows(
    app_and_ctx: tuple[FastAPI, InstanceContext],
    make_upload_row,
) -> None:
    """``bulk_delete`` deletes body files for every removed row (C1).

    Setup: 3 ``failed`` rows + 1 ``succeeded`` row, each with a body
    written into the instance's body store. Bulk-delete the ``failed``
    rows; assert the surviving row's body is still present, the deleted
    rows' bodies are gone, and the response reports the count.
    """
    app, ctx = app_and_ctx

    # Insert 3 failed rows + 1 succeeded row, each with a body in the
    # body store. The body store is the HybridBodyStore the route reads.
    deleted_ids = [uuid4() for _ in range(3)]
    survivor_id = uuid4()
    all_rows = [
        make_upload_row(chain_id=cid, state="failed", instance_id="primary") for cid in deleted_ids
    ]
    all_rows.append(make_upload_row(chain_id=survivor_id, state="succeeded", instance_id="primary"))

    for row in all_rows:
        await ctx.store.insert(row)
        await ctx.body_store.put(row.chain_id, {"body": b"payload"})

    # Sanity: every body present pre-delete.
    for cid in [*deleted_ids, survivor_id]:
        assert await ctx.body_store.has_body_ref(cid, "body")

    client = TestClient(app)
    response = client.request(
        "DELETE",
        "/v1/admin/chains",
        json={"state": "failed"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["deleted"] == 3

    # C1 assertion: deleted rows' bodies are gone from the body store.
    for cid in deleted_ids:
        assert not await ctx.body_store.has_body_ref(cid, "body"), (
            f"body for deleted chain_id={cid} should be gone (C1 closure)"
        )
    # The surviving row's body is still present.
    assert await ctx.body_store.has_body_ref(survivor_id, "body")


@pytest.mark.asyncio
async def test_bulk_delete_returns_zero_when_no_rows_match(
    app_and_ctx: tuple[FastAPI, InstanceContext],
    make_upload_row,
) -> None:
    """``bulk_delete`` returns ``deleted: 0`` when no rows match the filter.

    Verifies that no body-store delete is attempted on an empty match
    set — the route should not raise on missing chain_ids.
    """
    app, ctx = app_and_ctx
    # A single succeeded row; we filter for failed, so nothing matches.
    survivor_id = uuid4()
    await ctx.store.insert(
        make_upload_row(chain_id=survivor_id, state="succeeded", instance_id="primary")
    )
    await ctx.body_store.put(survivor_id, {"body": b"payload"})

    client = TestClient(app)
    response = client.request(
        "DELETE",
        "/v1/admin/chains",
        json={"state": "failed"},
    )
    assert response.status_code == 200
    assert response.json()["deleted"] == 0
    # Survivor's body is intact.
    assert await ctx.body_store.has_body_ref(survivor_id, "body")
