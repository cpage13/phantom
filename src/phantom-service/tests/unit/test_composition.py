"""Production-path per-mode wiring + data-root creation (plan § 2.3.10).

These tests drive the REAL composition root — ``create_app``'s FastAPI
lifespan, entered via ``app.router.lifespan_context`` — and assert the
per-mode shape ON THE LIVE :class:`InstanceContext` that production wires
(``app.state.instances``):

* ``hybrid``  → ``InstanceContext.body_store`` is a HybridBodyStore and
  ``persist_controller`` is set;
* ``all_ram`` → ``body_store`` is the RamBodyStore half, no controller;
* ``all_disk`` → ``body_store`` is the FileBodyStore half, no controller.

It also pins that the composition root creates the per-instance storage
tree if missing, and that the mode binding is read ONCE at startup — a
later snapshot mutation does NOT re-bind the live body store (the
structural "mode is a restart-required knob" property).

Provenance: these assertions used to drive the deleted ``compose_and_run``
+ its ``Runtime`` dataclass (R-2). They now verify the path production
runs. The mode-*decision* table itself is unit-tested at its single
source (:func:`phantom.runtime.startup_checks.build_body_store`) in
``test_composition_root.py``.
"""

from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from phantom.app import create_app
from phantom.config.settings import (
    BodyStoreCfg,
    InstanceCfg,
    RouteCfg,
    Settings,
    StorageCfg,
)
from phantom.storage.file_body_store import FileBodyStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.storage.ram_body_store import RamBodyStore

if TYPE_CHECKING:
    from fastapi import FastAPI

_INSTANCE_ID = "primary"
_INSTANCE_DATA_DIR = "primary"
_LIFESPAN_TIMEOUT_SECONDS = 30.0


def _instance() -> InstanceCfg:
    """Build a minimal single-route InstanceCfg for the default instance."""
    hosts = ["files.example.com"]
    return InstanceCfg(
        id=_INSTANCE_ID,
        host_prefixes=hosts,
        data_dir=_INSTANCE_DATA_DIR,
        routes=[RouteCfg(name="files", hosts=hosts, auth_mode="phantom_bearer")],
    )


def _settings(*, mode: str, data_root: Path) -> Settings:
    """Build a production-shaped Settings tagged for the chosen mode."""
    return Settings(
        storage=StorageCfg(
            data_dir=str(data_root),
            body_store=BodyStoreCfg(mode=mode, ram_ceiling_bytes=1024 * 1024),
        ),
        instances=[_instance()],
    )


def _lifespan_of(app: FastAPI) -> AbstractAsyncContextManager[None]:
    """Return the app's production lifespan context manager."""
    return app.router.lifespan_context(app)


def _instance_ctx(app: FastAPI) -> Any:
    """Return the single wired InstanceContext (after lifespan entry)."""
    return app.state.instances[0]


async def test_hybrid_mode_wires_hybrid_store_and_controller(tmp_path: Path) -> None:
    """``hybrid`` instance: HybridBodyStore binding + PersistController."""
    app = create_app(_settings(mode="hybrid", data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        ctx = _instance_ctx(app)
        assert isinstance(ctx.body_store, HybridBodyStore)
        assert ctx.persist_controller is not None


async def test_all_ram_mode_wires_ram_store_no_controller(tmp_path: Path) -> None:
    """``all_ram`` instance: RamBodyStore binding; no PersistController."""
    app = create_app(_settings(mode="all_ram", data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        ctx = _instance_ctx(app)
        assert isinstance(ctx.body_store, RamBodyStore)
        assert ctx.body_store is ctx.ram_body_store
        assert ctx.persist_controller is None


async def test_all_disk_mode_wires_file_store_no_controller(tmp_path: Path) -> None:
    """``all_disk`` instance: FileBodyStore binding; no PersistController."""
    app = create_app(_settings(mode="all_disk", data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        ctx = _instance_ctx(app)
        assert isinstance(ctx.body_store, FileBodyStore)
        assert ctx.body_store is ctx.file_body_store
        assert ctx.persist_controller is None


async def test_composition_creates_per_instance_data_root(tmp_path: Path) -> None:
    """The composition root creates the per-instance storage tree if missing."""
    nested = tmp_path / "nested" / "data_root"
    instance_root = nested / _INSTANCE_DATA_DIR
    assert not instance_root.exists()
    app = create_app(_settings(mode="hybrid", data_root=nested))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        assert instance_root.is_dir()
        assert (instance_root / "uploads.db").exists()


async def test_mode_is_read_once_at_startup_not_re_read(tmp_path: Path) -> None:
    """A snapshot mode flip after startup does NOT re-bind the live body store.

    The composition root selects the body store ONCE in
    ``build_body_store`` (called from ``_build_instance_context`` at
    lifespan entry) and stores it on the InstanceContext; workers consult
    that pre-bound binding, never re-reading ``mode`` from a hot-reloaded
    snapshot. Mutating the mode after entry must leave the binding type
    unchanged — the structural reason mode is a "restart-required" knob.
    """
    settings = _settings(mode="hybrid", data_root=tmp_path)
    app = create_app(settings)
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        ctx = _instance_ctx(app)
        live_binding_type = type(ctx.body_store)
        # Operator-equivalent mid-run mode flip via direct mutation.
        object.__setattr__(settings.storage.body_store, "mode", "all_ram")
        # The live binding is NOT re-bound — still HybridBodyStore.
        assert type(ctx.body_store) is live_binding_type
        assert isinstance(ctx.body_store, HybridBodyStore)
