"""Regression test for aggressor finding A-3 (back-up-and-run, plan § 1 / ADR-025).

Asserts that a hybrid → all_ram mode flip on a non-empty data dir does
NOT silently corrupt persisted (``body_location='file'``) rows. Round 1
found that booting all_ram on a populated data dir condemned every
disk-resident row to ``corrupted`` (recovery's ``has_body_ref`` asks the
RamBodyStore, which has no disk knowledge) and leaked the body files (no
janitor in all_ram) — silent data loss + disk leak on a documented-safe
config change.

Current decision: BACK UP AND RUN (plan § 1.2 / ADR-025, which supersedes
the prior fail-closed A-3/F-2 decision). The composition root runs
:func:`phantom.runtime.startup_checks.check_body_store_mode` at startup;
when ``mode=all_ram`` and the FileBodyStore root holds pre-existing chain
body directories, it relocates the live DB + body tree to a recoverable
``mode_switch`` quarantine pair (no data loss: the bytes are preserved,
not condemned to ``corrupted`` nor leaked), bumps
``mode_switch_backup_total``, and the service boots fresh over the now-empty
live tree. The operator restores the backup with the one-call admin restore
route after switching back to a disk-backed mode. One guard closes BOTH A-3
(no silent corruption: the persisted bytes survive in the backup) and F-2
(no leak: the body tree is moved OUT of the live tree, not left to leak);
see ``test_f2_all_ram_orphan_sweep.py`` for the F-2 sibling assertion.

Provenance: this two-phase test drove the deleted ``compose_and_run``
(R-2). It now drives the REAL composition root — ``create_app``'s FastAPI
lifespan — so the guard is exercised over a body tree a genuine hybrid
run persisted (not a hand-made dir), on the PRODUCTION ``bodies/`` layout
(NOT the dead path's ``body_store/``). The unit falsifier over a synthetic
populated tree lives in
``test_startup_guards_prod_path.py::test_all_ram_over_populated_bodies_backs_up_and_boots_fresh``;
this adds the realistic hybrid-persist → all_ram-flip sequence.
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


def _settings(*, mode: str, data_root: Path) -> Settings:
    """Build a production-shaped Settings for one mode under ``data_root``."""
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
        idempotency_key="dummy-A3-test",
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


async def test_hybrid_to_all_ram_with_persisted_bodies_backs_up_and_boots_fresh(
    tmp_path: Path,
) -> None:
    """A hybrid→all_ram flip with disk bodies backs up the live data and runs.

    Plan § 1.2 / ADR-025 (back-up-and-run, supersedes the prior fail-closed
    A-3/F-2 decision). The acceptance criterion is no SILENT data loss: the
    persisted ``body_location='file'`` row and its bytes must not be condemned
    to ``corrupted`` nor leaked. Booting all_ram over the populated tree moves
    the live DB + body tree to a recoverable ``mode_switch`` backup pair, bumps
    ``mode_switch_backup_total``, and the service boots fresh. The persisted
    bytes survive in the backup, retrievable via the admin restore route.
    """
    persisted_body = b"0123456789"

    # Phase 1 — hybrid: persist a body to disk through the real lifespan.
    chain_id = uuid4()
    app_hybrid = create_app(_settings(mode="hybrid", data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app_hybrid):
        ctx = app_hybrid.state.instances[0]
        await ctx.store.insert(_persisted_row(chain_id))
        await ctx.body_store.put(chain_id, {"body": persisted_body})
        assert ctx.persist_controller is not None
        handle = await ctx.persist_controller.enqueue(chain_id)
        await asyncio.wait_for(handle, timeout=5.0)

    # The body landed on disk under the PRODUCTION bodies/ layout.
    instance_root = tmp_path / _INSTANCE_DATA_DIR
    bodies_root = instance_root / "bodies"
    assert [p for p in bodies_root.rglob("*") if p.is_file()], "phase-1 didn't persist to disk"

    # Phase 2 — all_ram on the same data dir. The guard backs up the live
    # data and the service boots fresh (no silent corruption / leak).
    app_allram = create_app(_settings(mode="all_ram", data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app_allram):
        counter = app_allram.state.metrics_registry.counters[MODE_SWITCH_BACKUP_COUNTER_NAME]
        assert counter.snapshot()[""] == 1
        assert [inst.cfg.id for inst in app_allram.state.instances] == [_INSTANCE_ID]

    # The live DB + body tree were relocated to ONE manifested mode_switch
    # backup (both halves on one entry, cycle-7 seam 2), visible via the same
    # inventory GET /v1/admin/quarantine serves. A genuine hybrid-persisted
    # DB + body, not a hand-made tree.
    mode_switch_entries = list_quarantines(instance_root)
    assert len(mode_switch_entries) == 1, mode_switch_entries
    backup = mode_switch_entries[0]
    assert backup.reason == "mode_switch"
    assert backup.anomaly is False
    assert backup.backup_id is not None
    assert backup.has_db is True, "expected the DB half of the backup pair on disk"
    assert backup.has_body is True, "expected the body half of the backup pair on disk"
    # The persisted body bytes survive in the backup body tree.
    assert backup.body_path is not None
    preserved = [
        p for p in backup.body_path.rglob("*") if p.is_file() and p.read_bytes() == persisted_body
    ]
    assert preserved, "the hybrid-persisted body bytes must survive in the mode_switch backup"
    # The live bodies/ tree booted fresh (no leftover chain bodies).
    assert not [p for p in bodies_root.rglob("*") if p.is_file()]
