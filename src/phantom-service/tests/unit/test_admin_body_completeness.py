"""N2: the admin body reads refuse a short answer instead of returning one.

``BodyStore.get_all`` returns whatever the store HAS. A declared body_ref
whose file was never written, or was removed before the traversal began, is
simply absent from the returned mapping: no ``KeyError``, no signal. Before
N2 the three admin read surfaces streamed that shortfall as a 200.

The two tests here pin the two shapes the refusal takes, and the split is the
point. The single-chain reads can raise, because nothing has been written to
the wire yet. The tar stream cannot: it is an async generator handed to
``StreamingResponse``, so its 200 and headers are already sent when the
shortfall is discovered, and an exception would truncate a response the client
was told was fine. There the refusal is DATA, in the manifest.
"""

from __future__ import annotations

import io
import json
import tarfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.compression import BodyCodec, select_codec
from phantom.config.settings import CompressionCfg, InstanceCfg, PersistTriggerCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.models.upload import BodyHashes, UploadRow
from phantom.observability.metrics import MetricsRegistry
from phantom.routes import admin as admin_routes
from phantom.storage import FileBodyStore, RamBodyStore, SqliteTokenCache, SqliteUploadStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.workers.saturation import SaturationGate

from .conftest import make_snapshot, snapshot_thunk, track_instance

pytestmark = pytest.mark.asyncio

# The row declares two refs; the store is given only the first. That is the
# reachable shape of the defect: a partial chain directory on the file store.
_PRESENT_REF = "part-a"
_ABSENT_REF = "part-b"
_PRESENT_BYTES = b"the half that survived"


class _NoUpstream:
    """Upstream client stub; these tests never send."""

    async def start(self) -> None:
        """No-op."""

    async def stop(self) -> None:
        """No-op."""


async def _instance_with_a_partial_body(tmp_path: Path) -> InstanceContext:
    """Build one instance holding a row that declares two refs and stores one."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    await store.start()
    await ram.start()
    await fbs.start()
    await tokens.start()
    await body_store.start()

    cfg = InstanceCfg(
        id="primary",
        host_prefixes=["files.example.com"],
        data_dir="primary",
        routes=[
            RouteCfg(
                name="upstream-files",
                hosts=["files.example.com"],
                auth_mode="phantom_bearer",
            )
        ],
    )
    now = datetime.now(tz=UTC)
    row = UploadRow(
        chain_id=uuid4(),
        instance_id="primary",
        group_id=uuid4(),
        multifile_id=None,
        send_order=0,
        route_name="upstream-files",
        state="stored",
        body_location="file",
        received_at=now,
        updated_at=now,
        endpoint="files.example.com",
        uid="user-1",
        chain_envelope_json="{}",
        idempotency_key="k",
        capture_reexecution_active=False,
        body_hashes={
            _PRESENT_REF: BodyHashes(body_hash="a" * 64, storage_hash="a" * 64),
            _ABSENT_REF: BodyHashes(body_hash="b" * 64, storage_hash="b" * 64),
        },
        body_size_bytes=len(_PRESENT_BYTES),
    )
    await store.insert(row)
    # Only ONE of the two declared refs reaches the store.
    await fbs.put(row.chain_id, {_PRESENT_REF: _PRESENT_BYTES})

    def _passthrough_factory() -> BodyCodec:
        return select_codec(CompressionCfg(algorithm="original"))

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
        upstream_client=_NoUpstream(),
        executor=None,
        saturation=SaturationGate(
            max_in_flight=10, max_in_flight_bytes=1_000_000, max_disk_bytes=10_000_000
        ),
        codec_factory=_passthrough_factory,
        current_settings=snapshot_thunk(
            make_snapshot(persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=0))
        ),
    )
    track_instance(ctx)
    return ctx


def _client_for(ctx: InstanceContext, tmp_path: Path) -> TestClient:
    """Wire the admin router the way production does, including its handlers."""
    dispatcher = InstanceDispatcher([ctx])
    app = FastAPI()
    app.include_router(admin_routes.router)
    admin_routes.register_admin_error_handlers(app)
    app.dependency_overrides[admin_routes.get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[admin_routes.get_version] = lambda: "0.1.0"
    app.dependency_overrides[admin_routes.get_metrics_registry] = lambda: MetricsRegistry()
    app.dependency_overrides[admin_routes.get_data_root] = lambda: tmp_path
    return TestClient(app)


async def _one_row(ctx: InstanceContext) -> UploadRow:
    """Return the single seeded row."""
    rows = await ctx.store.list_non_terminal()
    if rows:
        return rows[0]
    listed, _ = await ctx.store.list_uploads(limit=1)
    return listed[0]


async def test_single_chain_body_reads_refuse_an_incomplete_body(tmp_path: Path) -> None:
    """A row whose store holds one of two declared refs is refused, not truncated.

    Objective: ``GET /chains/{id}/body`` and ``GET /chains/{id}/bundle`` compare
    what the store returned against what the row DECLARES, and refuse the read
    when they differ. Success is a ``storage_corruption`` error envelope naming
    the absent ref, on both routes.

    Before N2 both returned 200: the byte stream was short, and the bundle
    carried fewer ``body_refs`` than the row declared, with nothing anywhere
    saying so. The pre-fix assertion is that 200, so this test is red before
    and green after.
    """
    ctx = await _instance_with_a_partial_body(tmp_path)
    row = await _one_row(ctx)
    client = _client_for(ctx, tmp_path)

    body_response = client.get(f"/v1/admin/chains/{row.chain_id}/body")
    assert body_response.status_code == 500
    envelope = body_response.json()
    assert envelope["error"]["code"] == "storage_corruption"
    assert envelope["error"]["details"]["missing_body_refs"] == [_ABSENT_REF]

    bundle_response = client.get(f"/v1/admin/chains/{row.chain_id}/bundle")
    assert bundle_response.status_code == 500
    assert bundle_response.json()["error"]["code"] == "storage_corruption"


async def test_the_tar_export_records_the_shortfall_and_keeps_streaming(
    tmp_path: Path,
) -> None:
    """The archive still streams, and its manifest names the refs it lacks.

    Objective: the tar path reports incompleteness as DATA rather than as an
    HTTP error, because its response has already started. Success is a 200
    carrying a real tar, the present ref packed, and a manifest entry whose
    ``missing_body_refs`` names the absent one.

    A tar that aborted would be the WRONG fix, and this test is what pins
    that: the operator asked for whatever bytes exist, and truncating the
    stream would take away the ones that do.
    """
    ctx = await _instance_with_a_partial_body(tmp_path)
    row = await _one_row(ctx)
    client = _client_for(ctx, tmp_path)

    response = client.get("/v1/admin/export.tar")
    assert response.status_code == 200

    with tarfile.open(fileobj=io.BytesIO(response.content)) as tf:
        names = tf.getnames()
        manifest_member = tf.extractfile("manifest.json")
        assert manifest_member is not None
        manifest = json.loads(manifest_member.read())

    assert f"bodies/{row.chain_id}/{_PRESENT_REF}" in names
    assert f"bodies/{row.chain_id}/{_ABSENT_REF}" not in names

    entry = next(e for e in manifest if e["chain_id"] == str(row.chain_id))
    assert entry["missing_body_refs"] == [_ABSENT_REF]
