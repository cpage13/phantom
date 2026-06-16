"""Per-mode happy-path integration tests for Phase 1 (plan § 2.3.21 #6).

Each test exercises one of the three first-class production
``BodyStore`` modes via the REAL composition root — ``create_app``'s
FastAPI lifespan, entered via ``app.router.lifespan_context`` — and
asserts the per-mode wiring matrix from plan § 2.3.10 ON THE LIVE
:class:`InstanceContext` (``app.state.instances``):

================  ===========================================
Mode              Wired components
================  ===========================================
``hybrid``        HybridBodyStore + PersistController (migrates
                  RAM → file on enqueue)
``all_ram``       RamBodyStore only; no PersistController (body
                  stays in RAM, body_location never flips)
``all_disk``      FileBodyStore only; no PersistController (body
                  persists directly on put)
================  ===========================================

Provenance: these tests drove the deleted ``compose_and_run`` composition
shim (R-2). They now drive the path production runs — ``app.py``'s
lifespan wires each per-instance ``InstanceContext`` through the same
shared ``build_body_store`` decision table, so the worker-spawn site and
the wiring authority are one and the same.
"""

from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from phantom.app import create_app
from phantom.config.settings import (
    BodyStoreCfg,
    InstanceCfg,
    RouteCfg,
    Settings,
    StorageCfg,
)
from phantom.models.upload import UploadRow
from phantom.storage.file_body_store import FileBodyStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.storage.ram_body_store import RamBodyStore

if TYPE_CHECKING:
    from fastapi import FastAPI

pytestmark = pytest.mark.asyncio

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
        routes=[RouteCfg(name="upstream-files", hosts=hosts, auth_mode="phantom_bearer")],
    )


def _settings(*, mode: str, data_root: Path) -> Settings:
    """Build a production-shaped Settings for one mode under ``data_root``."""
    return Settings(
        storage=StorageCfg(
            data_dir=str(data_root),
            body_store=BodyStoreCfg(
                mode=mode,
                ram_ceiling_bytes=1024 * 1024,
                body_orphan_sweep_seconds=60,
                ram_pressure_poll_seconds=1.0,
                linger_seconds=90,
            ),
        ),
        instances=[_instance()],
    )


def _lifespan_of(app: FastAPI) -> AbstractAsyncContextManager[None]:
    """Return the app's production lifespan context manager."""
    return app.router.lifespan_context(app)


def _instance_ctx(app: FastAPI) -> Any:
    """Return the single wired InstanceContext (after lifespan entry)."""
    return app.state.instances[0]


def _row(*, body_location: str) -> UploadRow:
    """Build a minimal :class:`UploadRow` for admission-replacement insertion."""
    from datetime import UTC, datetime
    from uuid import uuid4

    now = datetime.now(tz=UTC)
    return UploadRow.model_validate(
        {
            "chain_id": uuid4(),
            "instance_id": _INSTANCE_ID,
            "group_id": uuid4(),
            "multifile_id": uuid4(),
            "send_order": 0,
            "route_name": "upstream-files",
            "state": "queued",
            "body_location": body_location,
            "received_at": now,
            "updated_at": now,
            "endpoint": "upstream.example.com",
            "uid": "user-1",
            "chain_envelope_json": "{}",
            "idempotency_key": "k",
            "capture_reexecution_active": False,
        }
    )


async def test_hybrid_mode_admits_ram_then_persist_controller_migrates_to_file(
    tmp_path: Path,
) -> None:
    """Hybrid mode: admission writes RAM; PersistController migrates to file.

    End-to-end exercise of plan § 2.3.21 #6 first row against the live
    InstanceContext. Asserts:

    * ``HybridBodyStore`` is the wired body store + the PersistController
      is present;
    * a row inserted with ``body_location='ram'`` plus a body put in RAM
      gets migrated to file after :meth:`PersistController.enqueue`;
    * post-migration, the body reads via the disk fallback.
    """
    app = create_app(_settings(mode="hybrid", data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        ctx = _instance_ctx(app)
        assert isinstance(ctx.body_store, HybridBodyStore)
        assert ctx.persist_controller is not None

        row = _row(body_location="ram")
        await ctx.store.insert(row)
        await ctx.body_store.put(row.chain_id, {"a": b"hello"})

        handle = await ctx.persist_controller.enqueue(row.chain_id)
        await asyncio.wait_for(handle, timeout=5.0)

        fetched = await ctx.store.get(row.chain_id)
        assert fetched is not None
        assert fetched.body_location == "file"
        # Body still readable post-migration (disk fallback).
        assert await ctx.body_store.get(row.chain_id, "a") == b"hello"


async def test_all_ram_mode_admits_ram_and_no_persist_controller(
    tmp_path: Path,
) -> None:
    """All-RAM mode: PersistController absent; admission writes RAM only.

    Plan § 2.3.21 #6 second row. The strategy commits to losing bodies on
    restart by design (all_ram is volatile); we assert no PersistController
    is wired so restarts cannot silently migrate to disk and
    ``body_location`` never flips off ``ram``.
    """
    app = create_app(_settings(mode="all_ram", data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        ctx = _instance_ctx(app)
        assert isinstance(ctx.body_store, RamBodyStore)
        assert ctx.persist_controller is None

        # Admission-equivalent: insert + put body in RAM.
        row = _row(body_location="ram")
        await ctx.store.insert(row)
        await ctx.body_store.put(row.chain_id, {"a": b"ephemeral"})

        # Body is readable (RAM-only path).
        assert await ctx.body_store.get(row.chain_id, "a") == b"ephemeral"

        fetched = await ctx.store.get(row.chain_id)
        assert fetched is not None
        # ``body_location`` stays 'ram' — no persist controller to flip it.
        assert fetched.body_location == "ram"


async def test_all_disk_mode_admits_file_and_no_persist_controller(
    tmp_path: Path,
) -> None:
    """All-disk mode: PersistController absent; admission writes file directly.

    Plan § 2.3.21 #6 third row. In ``all_disk`` mode the body store IS the
    file body store; there is no RAM to migrate from, so the
    PersistController is intentionally absent. Bodies persist directly on
    admission.
    """
    app = create_app(_settings(mode="all_disk", data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        ctx = _instance_ctx(app)
        assert isinstance(ctx.body_store, FileBodyStore)
        assert ctx.persist_controller is None

        row = _row(body_location="file")
        await ctx.store.insert(row)
        await ctx.body_store.put(row.chain_id, {"a": b"durable"})

        assert await ctx.body_store.has_body_ref(row.chain_id, "a")
        fetched = await ctx.store.get(row.chain_id)
        assert fetched is not None
        assert fetched.body_location == "file"
