"""App builder shared by the admin-surface regression modules.

Not collected by pytest (the leading underscore keeps it out of
``python_files``); it exists so the three modules that pin the admin
surface against the 2026-09-08 review findings build their instances the
same way instead of carrying three drifting copies of the same 60 lines.

The builder differs from the per-module fixtures elsewhere in this suite in
exactly two ways, both needed by those findings: it can build MORE THAN ONE
instance (the aggregate status, the RAM-pressure ceiling and the
instance-scoped bulk delete are all only wrong when N > 1), and it can bind
the typed degraded set the readiness seam reads.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from fastapi import FastAPI
from phantom.chain.executor import ChainExecutor, default_clock
from phantom.compression import BodyCodec, select_codec
from phantom.config.settings import (
    BodyStoreCfg,
    CompressionCfg,
    InstanceCfg,
    PersistTriggerCfg,
    RouteCfg,
)
from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.refresh.ad_client_credentials import AdMinter
from phantom.routes import admin as admin_routes
from phantom.routes import health as health_routes
from phantom.routing import resolve_route
from phantom.runtime.startup_checks import DegradedInstance
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


class FakeUpstream:
    """Stub UpstreamClient - these tests never reach upstream."""

    async def start(self) -> None:
        """No-op."""

    async def stop(self) -> None:
        """No-op."""

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        """Answer every request with an empty 200."""
        return UpstreamResponse(status=200, body=b"{}")


def _passthrough_factory() -> BodyCodec:
    """Always-encode with the passthrough codec (``algorithm="original"``)."""
    return select_codec(CompressionCfg(algorithm="original"))


async def build_instance(
    tmp_path: Path,
    instance_id: str,
    *,
    minter: AdMinter | None = None,
    body_store_cfg: BodyStoreCfg | None = None,
) -> InstanceContext:
    """Build one started :class:`InstanceContext` rooted at ``tmp_path``.

    Args:
        tmp_path: Directory for this instance's DBs and body tree. Pass a
            distinct path per instance.
        instance_id: The instance id, which is also the ``instance_id``
            written on its rows.
        minter: Optional AD minter, which is what makes the instance report
            ``refresh_strategy="ad_client_credentials"``.
        body_store_cfg: Optional body-store block for the instance's
            settings snapshot (the RAM ceiling lives here).

    Returns:
        The started context, registered for teardown.
    """
    root = tmp_path / instance_id
    root.mkdir(parents=True, exist_ok=True)
    store = SqliteUploadStore(str(root / "uploads.db"))
    ram = RamBodyStore()
    files = FileBodyStore(root / "bodies")
    tokens = SqliteTokenCache(str(root / "tokens.db"))
    await store.start()
    await ram.start()
    await files.start()
    await tokens.start()
    cfg = InstanceCfg(
        id=instance_id,
        host_prefixes=["files.example.com"],
        data_dir=instance_id,
        routes=[RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer")],
    )
    upstream = FakeUpstream()
    body_store = HybridBodyStore(ram=ram, disk=files)
    await body_store.start()
    ctx = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=files,
        body_store=body_store,
        persist_controller=None,
        token_cache=tokens,
        minter=minter,
        retry_strategy=FixedIntervalsStrategy([1]),
        upstream_client=upstream,
        executor=ChainExecutor(
            token_cache=tokens,
            upstream_client=upstream,
            resolve_route=resolve_route,
            clock=default_clock,
            instance=cfg,
        ),
        saturation=SaturationGate(
            max_in_flight=10, max_in_flight_bytes=10_000_000, max_disk_bytes=100_000_000
        ),
        codec_factory=_passthrough_factory,
        current_settings=snapshot_thunk(
            make_snapshot(
                persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=0),
                body_store=body_store_cfg,
            )
        ),
    )
    track_instance(ctx)
    return ctx


def build_admin_app(
    instances: Sequence[InstanceContext],
    *,
    degraded: Sequence[DegradedInstance] = (),
) -> FastAPI:
    """Mount the admin router over ``instances`` with the degraded set bound.

    Args:
        instances: The live instances the dispatcher serves. May be empty,
            which is what a deployment whose every instance failed its
            storage boot looks like.
        degraded: The typed degraded set (seam 3). The admin status routes
            read the SAME seam ``/v1/readyz`` reads, so binding it here is
            binding it for both.

    Returns:
        The configured app, error handlers registered.
    """
    dispatcher = InstanceDispatcher(list(instances))
    app = FastAPI()
    app.include_router(admin_routes.router)
    app.include_router(health_routes.router)
    admin_routes.register_admin_error_handlers(app)
    app.dependency_overrides[admin_routes.get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[admin_routes.get_version] = lambda: "0.1.0"
    app.dependency_overrides[health_routes.get_dispatcher] = lambda: dispatcher
    app.dependency_overrides[health_routes.get_version] = lambda: "0.1.0"
    app.dependency_overrides[health_routes.get_degraded_instances] = lambda: tuple(degraded)
    return app
