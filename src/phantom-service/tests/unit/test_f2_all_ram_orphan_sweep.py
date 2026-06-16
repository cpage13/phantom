"""Regression test for aggressor finding F-2 / validation F-P8-B (back-up-and-run).

F-2 is the disk-leak half of the all_ram-ignores-disk-state defect (A-3
is the data-loss half). On a data dir that previously ran a disk-touching
mode, booting ``all_ram`` left the leftover body files unswept forever —
no BodyOrphanJanitor spawns in ``all_ram``.

Current decision: BACK UP AND RUN (plan § 1.2 / ADR-025, which supersedes
the prior fail-closed A-3/F-2 decision). The composition root's
:func:`phantom.runtime.startup_checks.check_body_store_mode` relocates the
live DB + body tree to a recoverable ``mode_switch`` quarantine pair before
the all_ram store opens. The F-2 leak still cannot accrue, but by a
different mechanism than the old refuse-to-boot: the leftover body files are
MOVED OUT of the live tree into the backup (not left in place, and not swept
away), so no orphan can strand under ``all_ram`` and the bytes remain
recoverable via the admin restore route.

This test therefore asserts the SAME no-leak outcome as
``test_a3_mode_flip_with_persisted_rows.py`` (one guard closes both
findings), and additionally proves the leftover disk files are preserved in
the backup, not destroyed.

Provenance: this two-phase test drove the deleted ``compose_and_run``
(R-2). It now drives the REAL composition root — ``create_app``'s FastAPI
lifespan — over a body tree a genuine hybrid run persisted, on the
PRODUCTION ``bodies/`` layout (NOT the dead path's ``body_store/``).
"""

from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from phantom.app import create_app
from phantom.config.settings import (
    BodyStoreCfg,
    InstanceCfg,
    RouteCfg,
    Settings,
    StorageCfg,
)
from phantom.models.upload import BodyHashes, CapturedValues, UploadRow
from phantom.runtime.startup_checks import MODE_SWITCH_BACKUP_COUNTER_NAME
from phantom.storage.integrity import list_quarantines

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
        routes=[RouteCfg(name="files", hosts=hosts, auth_mode="phantom_bearer")],
    )


def _settings(*, mode: str, data_root: Path, body_orphan_sweep_seconds: int = 60) -> Settings:
    """Build a production-shaped Settings for one mode under ``data_root``."""
    return Settings(
        storage=StorageCfg(
            data_dir=str(data_root),
            body_store=BodyStoreCfg(
                mode=mode,
                ram_ceiling_bytes=1024 * 1024,
                body_orphan_sweep_seconds=body_orphan_sweep_seconds,
            ),
        ),
        instances=[_instance()],
    )


def _lifespan_of(app: FastAPI) -> AbstractAsyncContextManager[None]:
    """Return the app's production lifespan context manager."""
    return app.router.lifespan_context(app)


def _persisted_row(chain_id: object) -> UploadRow:
    """A queued, RAM-located row the PersistController can migrate to disk."""
    now = datetime.now(tz=UTC)
    return UploadRow(  # type: ignore[call-arg]
        chain_id=chain_id,
        instance_id=_INSTANCE_ID,
        group_id=chain_id,
        multifile_id=chain_id,
        send_order=0,
        route_name="files",
        state="queued",
        body_location="ram",
        next_attempt_at=now,
        received_at=now,
        updated_at=now,
        endpoint="e",
        uid="u",
        chain_envelope_json="{}",
        captured_values=CapturedValues(),
        current_step_index=0,
        idempotency_key="dummy-f2-regression",
        chain_id_at_ingress=None,
        capture_reexecution_active=False,
        body_size_bytes=10,
        storage_encoding="original",
        body_hashes={
            "body": BodyHashes(  # type: ignore[call-arg]
                body_hash="0" * 64,
                storage_hash="0" * 64,
            ),
        },
    )


async def test_all_ram_on_populated_dir_backs_up_so_leak_cannot_accrue(
    tmp_path: Path,
) -> None:
    """Booting all_ram on a populated data dir backs up (leak prevented).

    Plan § 1.2 / ADR-025 (back-up-and-run, supersedes the prior fail-closed
    A-3/F-2 decision). The guard relocates the live DB + body tree to a
    recoverable ``mode_switch`` quarantine pair and the service boots fresh.
    The F-2 leak cannot accrue: the leftover disk body files are MOVED OUT of
    the live ``bodies/`` tree into the backup (where no all_ram orphan can
    strand), the bytes are preserved (not swept, not corrupted), and
    ``mode_switch_backup_total`` bumps. The short ``body_orphan_sweep_seconds``
    is irrelevant under all_ram (no janitor spawns) and the move happens at
    boot before any sweep could run regardless.
    """
    bodies_root = tmp_path / _INSTANCE_DATA_DIR / "bodies"

    # Phase 1 — hybrid: persist a body to disk through the real lifespan.
    chain_id = uuid4()
    app_hybrid = create_app(_settings(mode="hybrid", data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app_hybrid):
        ctx = app_hybrid.state.instances[0]
        await ctx.store.insert(_persisted_row(chain_id))
        await ctx.body_store.put(chain_id, {"body": b"0123456789"})
        assert ctx.persist_controller is not None
        handle = await ctx.persist_controller.enqueue(chain_id)
        await asyncio.wait_for(handle, timeout=5.0)

    files_before = sorted(p.read_bytes() for p in bodies_root.rglob("*") if p.is_file())
    assert files_before, "phase-1 didn't produce any disk body files"

    # Phase 2 — all_ram on the same populated data dir. The guard backs up
    # the live tree and boots fresh; the short sweep period would have let a
    # janitor leak-sweep, but no janitor ever spawns AND the move precedes it.
    app_allram = create_app(
        _settings(mode="all_ram", data_root=tmp_path, body_orphan_sweep_seconds=1)
    )
    instance_root = tmp_path / _INSTANCE_DATA_DIR
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app_allram):
        counter = app_allram.state.metrics_registry.counters[MODE_SWITCH_BACKUP_COUNTER_NAME]
        assert counter.snapshot()[""] == 1
        assert [inst.cfg.id for inst in app_allram.state.instances] == [_INSTANCE_ID]

    # The leftover disk bodies were MOVED OUT of the live tree into the
    # mode_switch backup, not left to leak under all_ram and not destroyed.
    live_files_after = [p for p in bodies_root.rglob("*") if p.is_file()]
    assert not live_files_after, (
        "the unsafe-switch backup must clear the live bodies/ tree (leak "
        f"prevented); leftover live files={live_files_after!r}"
    )
    backup = next(
        e
        for e in list_quarantines(instance_root)
        if e.reason == "mode_switch" and e.has_body and not e.anomaly
    )
    assert backup.body_path is not None
    preserved = sorted(p.read_bytes() for p in backup.body_path.rglob("*") if p.is_file())
    assert preserved == files_before, (
        "the pre-existing disk bodies must be preserved byte-for-byte in the "
        f"mode_switch backup; before={files_before!r} in-backup={preserved!r}"
    )
