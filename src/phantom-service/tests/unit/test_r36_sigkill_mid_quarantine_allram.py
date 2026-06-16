"""Regression test for aggressor Round-3 finding R3-6 (adopted + adapted).

A SIGKILL between the two ``shutil.move`` steps of integrity-quarantine
used to leave a half-state that wedged an ``all_ram`` boot against the
A-3/F-2 guard, while hybrid/all_disk self-healed — an asymmetry on the
documented kill/restart path (Pi-class field boxes lose power).

Defender R4 contract decision (finding R3-6 — option (a), crash-atomic
quarantine): ``quarantine`` now moves the body store FIRST and the corrupt
DB LAST. Quarantine only ever runs
because the DB FAILED ``integrity_check``, so the corrupt DB is the
re-trigger: a kill between the two moves leaves the corrupt DB in place
(body store already gone). On restart the integrity gate finds the same
corrupt DB and re-quarantines it (moving a now-absent body store is a
tolerated no-op), then boots fresh in EVERY mode — including all_ram.
This closes the wedge WITHOUT weakening the A-3/F-2 guard (option (b),
"make the guard distinguish orphan garbage from live bodies", was
REJECTED: in all_ram the fresh DB references nothing, so it cannot tell
the A-3 "preserve my bodies" case from the R3-6 garbage case — it would
re-open the A-3 hole).

This test reproduces the realistic post-kill state under the NEW ordering
(corrupt DB present + body store already moved away) and asserts all_ram
boots and the corrupt DB is re-quarantined. The proposed seed asserted
boot after moving ONLY the DB (the OLD half-state); that state can no
longer arise from a real kill under option (a) and the unchanged guard
still (correctly) refuses it, so the test is adapted to the chosen
direction — the explicit "adapt the assertion to the recovery posture"
call the seed flagged.

Provenance: this test used the deleted ``compose_and_run`` (R-2) to drive
the hybrid persist + the all_ram reboot. It now drives the REAL
composition root — ``create_app``'s FastAPI lifespan — on the PRODUCTION
``bodies/`` layout (NOT the dead path's ``body_store/``), so the
crash-atomic recovery is exercised exactly where production runs it.
"""

from __future__ import annotations

import asyncio
import shutil
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
from phantom.storage.integrity import _BODY_QUARANTINE_INFIX, _DB_QUARANTINE_INFIX, quarantine_paths

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


def _corrupt_db_file(db_path: Path) -> None:
    """Overwrite a SQLite file with garbage so ``integrity_check`` fails."""
    db_path.write_bytes(b"this is not a valid sqlite database header at all" * 8)


@pytest.mark.asyncio
async def test_sigkill_mid_quarantine_does_not_wedge_allram(tmp_path: Path) -> None:
    """A kill mid-quarantine (body store moved, corrupt DB left) must not wedge all_ram.

    Under the crash-atomic ordering the corrupt DB is what survives a kill
    between the two moves, so the next boot re-quarantines it and recovers
    in every mode. all_ram must boot (not refuse forever).
    """
    # Per-instance production layout: <data_root>/<instance>/{uploads.db,bodies}.
    instance_root = tmp_path / _INSTANCE_DATA_DIR
    db_path = instance_root / "uploads.db"
    bodies_root = instance_root / "bodies"

    # Populate bodies/ via a hybrid run through the real lifespan so the
    # on-disk tree looks exactly like a real prior-mode deployment.
    chain_id = uuid4()
    app_hybrid = create_app(_settings(mode="hybrid", data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app_hybrid):
        ctx = app_hybrid.state.instances[0]
        now = datetime.now(tz=UTC)
        row = UploadRow(  # type: ignore[call-arg]
            chain_id=chain_id,
            instance_id=_INSTANCE_ID,
            group_id=chain_id,
            multifile_id=chain_id,
            send_order=0,
            route_name="r",
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
            idempotency_key="dummy",
            chain_id_at_ingress=None,
            capture_reexecution_active=False,
            body_size_bytes=10,
            storage_encoding="original",
            body_hashes={"body": BodyHashes(body_hash="0" * 64, storage_hash="0" * 64)},  # type: ignore[call-arg]
        )
        await ctx.store.insert(row)
        await ctx.body_store.put(chain_id, {"body": b"0123456789"})
        assert ctx.persist_controller is not None
        handle = await ctx.persist_controller.enqueue(chain_id)
        await asyncio.wait_for(handle, timeout=5.0)

    assert [p for p in bodies_root.rglob("*") if p.is_file()], "premise: body persisted"

    # The DB got corrupted (the only reason quarantine ever runs).
    _corrupt_db_file(db_path)

    # Simulate the SIGKILL under the crash-atomic ordering: the body-store
    # move (step A) completed, the corrupt-DB move (step B) had NOT. Move
    # ONLY the body store away; leave the corrupt DB in place.
    _, quarantined_body = quarantine_paths(db_path, bodies_root, backup_id=uuid4())
    shutil.move(str(bodies_root), str(quarantined_body))
    assert db_path.exists(), "corrupt DB should remain (kill landed before its move)"
    assert not bodies_root.exists(), "body store already moved (kill landed after its move)"

    # Restart in all_ram. The guard sees an absent/empty bodies/ root
    # (passes), then the integrity gate finds the still-corrupt DB and
    # re-quarantines it, booting fresh — must NOT wedge.
    app_allram = create_app(_settings(mode="all_ram", data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app_allram):
        ctx2 = app_allram.state.instances[0]
        rows, _ = await ctx2.store.list_uploads()
        assert rows == [], "fresh DB after re-quarantine should have no rows"

    # The corrupt DB was re-quarantined (a second .corrupted.<iso>.db
    # artifact now exists) and the earlier body-store quarantine survives.
    db_quarantines = [p for p in instance_root.iterdir() if _DB_QUARANTINE_INFIX in p.name]
    body_quarantines = [p for p in instance_root.iterdir() if _BODY_QUARANTINE_INFIX in p.name]
    assert db_quarantines, "the corrupt DB should have been re-quarantined on the all_ram boot"
    assert body_quarantines, "the pre-kill body-store quarantine artifact should survive"
