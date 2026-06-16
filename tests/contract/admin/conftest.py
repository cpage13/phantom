"""Shared fixture for admin contract tests.

Boots a minimal admin-only FastAPI app via the same dependency-
override pattern used in ``tests/contract/test_observability_endpoints.py``.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from pathlib import Path

import pytest
from fastapi import FastAPI
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
from phantom.routes import health as health_routes
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


def _make_snapshot() -> InstanceSettingsSnapshot:
    return InstanceSettingsSnapshot(
        persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=0),
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
async def admin_app(
    tmp_path: Path,
) -> Iterable[tuple[FastAPI, InstanceContext]]:
    """Yield a minimal admin-only FastAPI app + the wired InstanceContext.

    The InstanceContext gives tests direct write-access to the store
    so they can seed rows / tokens / etc. before exercising the
    endpoint. Lifecycle is per-test (the SQLite stores live in
    tmp_path).
    """
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
        current_settings=_snapshot_thunk(_make_snapshot()),
    )
    dispatcher = InstanceDispatcher([ctx])
    app = FastAPI()
    app.include_router(admin_routes.router)
    # Match the production app's exception-handler wiring so contract
    # tests observe the same wire shape callers do: the ONE shared
    # helper registers every admin typed-error handler (round 3
    # defender fix R3-1).
    admin_routes.register_admin_error_handlers(app)
    app.dependency_overrides[admin_routes.get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[admin_routes.get_version] = lambda: "0.1.0"
    app.dependency_overrides[admin_routes.get_metrics_registry] = lambda: registry
    yield app, ctx


@pytest.fixture
def health_app(admin_app: tuple[FastAPI, InstanceContext]) -> tuple[FastAPI, InstanceContext]:
    """Yield a minimal PUBLIC health app (R12-1) + the wired InstanceContext.

    Liveness (``GET /v1/healthz``) and readiness (``GET /v1/readyz``) moved
    off the admin router to the public health router in R12-1. This fixture
    mounts that router with its own dependency placeholders over the same
    :class:`InstanceContext` the ``admin_app`` fixture builds, so the
    health/ready contract tests exercise the real public router.

    Args:
        admin_app: The admin-app fixture (its ``InstanceContext`` is reused).

    Returns:
        ``(app, ctx)`` where ``app`` serves the public health router.
    """
    _admin, ctx = admin_app
    dispatcher = InstanceDispatcher([ctx])
    app = FastAPI()
    app.include_router(health_routes.router)
    app.dependency_overrides[health_routes.get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[health_routes.get_version] = lambda: "0.1.0"
    return app, ctx
