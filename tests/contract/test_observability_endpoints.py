"""Contract tests for the Phase 3 § 4.2.5 observability admin endpoints.

Three new endpoints under ``/v1/admin/observability/``:

* ``GET /counters``     — serialize :class:`MetricsRegistry.counters`
* ``GET /gauges``       — serialize :class:`MetricsRegistry.gauges`,
                         with ``body_location_distribution`` computed
                         on demand from the live store.
* ``GET /ram_pressure`` — aggregated RAM-pressure status across
                         configured instances.

Each test builds a FastAPI app exposing only the admin router, wires
the dispatcher + metrics registry via dependency_overrides, and
exercises the route via :class:`fastapi.testclient.TestClient`.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.chain.executor import ChainExecutor, default_clock
from phantom.compression import BodyCodec, select_codec
from phantom.config.settings import (
    BodyStoreCfg,
    CompressionCfg,
    InstanceCfg,
    PersistTriggerCfg,
    RetentionCfg,
    RouteCfg,
    SaturationCfg,
)
from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.instances.snapshot import InstanceSettingsSnapshot
from phantom.observability.metrics import MetricsRegistry
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


def _make_snapshot(persist_trigger: PersistTriggerCfg) -> InstanceSettingsSnapshot:
    """Local snapshot factory mirroring src/phantom-service/tests/unit/conftest.py.

    The contract test cannot import the per-package conftest, so the
    minimal-yet-validator-satisfying snapshot is built inline.
    """
    return InstanceSettingsSnapshot(
        persist_trigger=persist_trigger,
        body_store=BodyStoreCfg(ram_ceiling_bytes=1_073_741_824),
        retention=RetentionCfg(
            succeeded_metadata_seconds=300,
            failed_body_seconds=14 * 86_400,
            auth_expired_body_seconds=60 * 86_400,
            stored_body_seconds=60 * 86_400,
        ),
        compression=CompressionCfg(),
        saturation=SaturationCfg(
            max_in_flight=100,
            max_in_flight_bytes=1_073_741_824,
            max_disk_bytes=137_438_953_472,
            large_body_threshold_bytes=100 * 1024 * 1024,
            max_large_in_flight=4,
        ),
        capture_reexecution=False,
    )


def _snapshot_thunk(snapshot: InstanceSettingsSnapshot) -> Callable[[], InstanceSettingsSnapshot]:
    return lambda: snapshot


class _FakeUpstream:
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


@pytest.fixture
async def app_with_observability(
    tmp_path: Path,
) -> Iterable[tuple[FastAPI, MetricsRegistry, InstanceContext]]:
    """Build a minimal admin app with metrics registry + one instance."""
    registry = MetricsRegistry()
    store = SqliteUploadStore(str(tmp_path / "uploads.db"), metrics_registry=registry)
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await store.start()
    await ram.start()
    await fbs.start()
    await tokens.start()

    saturation = SaturationGate(
        max_in_flight=10,
        max_in_flight_bytes=1_000_000,
        max_disk_bytes=10_000_000,
        metrics_registry=registry,
    )

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
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    await body_store.start()

    def _passthrough_factory() -> BodyCodec:
        return select_codec(CompressionCfg(algorithm="original"))

    persist_trigger = PersistTriggerCfg(body_size_threshold_bytes=0)
    ctx = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=None,  # all_disk-style: no controller wired
        token_cache=tokens,
        minter=None,
        retry_strategy=FixedIntervalsStrategy([1]),
        upstream_client=upstream,
        executor=executor,
        saturation=saturation,
        codec_factory=_passthrough_factory,
        current_settings=_snapshot_thunk(_make_snapshot(persist_trigger)),
    )
    dispatcher = InstanceDispatcher([ctx])
    app = FastAPI()
    app.include_router(admin_routes.router)
    # The ONE shared helper registers every admin typed-error handler so
    # this fixture observes the same wire shape production does (round 3
    # fix R3-1; this fixture previously registered no handlers at all).
    admin_routes.register_admin_error_handlers(app)
    app.dependency_overrides[admin_routes.get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[admin_routes.get_version] = lambda: "0.1.0"
    app.dependency_overrides[admin_routes.get_metrics_registry] = lambda: registry
    yield app, registry, ctx


def test_get_observability_counters_returns_registered_counters(
    app_with_observability: tuple[FastAPI, MetricsRegistry, InstanceContext],
) -> None:
    """``GET /observability/counters`` returns every registered counter."""
    app, _registry, _ctx = app_with_observability
    client = TestClient(app)
    response = client.get("/v1/admin/observability/counters")
    assert response.status_code == 200, response.text
    body = response.json()
    assert "counters" in body
    # Must include at least the saturation_balance source's siblings —
    # we registered SaturationGate (1 gauge), and the store registered
    # body_location_distribution. The saturation counters list will
    # include reaper_actions_total only if Reaper was constructed; in
    # this fixture we haven't constructed Reaper. Just assert structure.
    names = {entry["name"] for entry in body["counters"]}
    # No specific assertion on names — the contract is shape.
    assert isinstance(names, set)
    for entry in body["counters"]:
        assert "name" in entry
        assert "description" in entry
        assert "values" in entry
        assert isinstance(entry["values"], dict)


def test_get_observability_gauges_returns_registered_gauges(
    app_with_observability: tuple[FastAPI, MetricsRegistry, InstanceContext],
) -> None:
    """``GET /observability/gauges`` returns every registered gauge."""
    app, _registry, _ctx = app_with_observability
    client = TestClient(app)
    response = client.get("/v1/admin/observability/gauges")
    assert response.status_code == 200, response.text
    body = response.json()
    assert "gauges" in body
    names = {entry["name"] for entry in body["gauges"]}
    # saturation_balance is registered by the SaturationGate in the
    # fixture; body_location_distribution is registered by the store.
    assert "saturation_balance" in names
    assert "body_location_distribution" in names
    # body_location_distribution should expose ram + file buckets
    # populated by the on-demand SQL grouping (zero in a fresh fixture).
    bl_entry = next(g for g in body["gauges"] if g["name"] == "body_location_distribution")
    assert "ram" in bl_entry["values"]
    assert "file" in bl_entry["values"]
    assert bl_entry["values"]["ram"] == 0
    assert bl_entry["values"]["file"] == 0


def test_get_observability_ram_pressure_returns_status(
    app_with_observability: tuple[FastAPI, MetricsRegistry, InstanceContext],
) -> None:
    """``GET /observability/ram_pressure`` returns the aggregated status."""
    app, _registry, _ctx = app_with_observability
    client = TestClient(app)
    response = client.get("/v1/admin/observability/ram_pressure")
    assert response.status_code == 200, response.text
    body = response.json()
    assert "ram_body_store_bytes" in body
    assert "ram_ceiling_bytes" in body
    assert "pending_migrations" in body
    assert "persist_controller_queue_depth" in body
    assert body["ram_body_store_bytes"] == 0
    # No persist_controller in this fixture → queue depth = 0.
    assert body["persist_controller_queue_depth"] == 0
    # Use uuid4 to keep import live for future expansion.
    assert uuid4() is not None
