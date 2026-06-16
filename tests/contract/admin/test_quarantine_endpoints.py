"""Contract tests for the quarantine inventory + restore admin endpoints.

``GET /v1/admin/quarantine`` (plan § 5.2.5 / cycle-7 seam 2) lists ONE entry
per BACKUP (read from the backup manifests, keyed by ``backup_id``) under the
targeted instance's per-instance ``data_root``
(``<storage.data_dir>/<cfg.data_dir>/``, Finding F-1), plus one flagged
anomaly entry per on-disk artifact no manifest claims.
``POST /v1/admin/quarantine/restore?backup_id=...`` (plan § 1.5) moves a
chosen ``mode_switch`` backup back into the live tree by IDENTITY, backing up
any current live data first; it never string-matches filenames, so an
unmanifested artifact is not addressable at all (R5-P unrepresentable).

These tests use the shared ``admin_app`` fixture (a real dispatcher with one
``primary`` instance whose ``cfg.data_dir == "primary"``) and override
``get_data_root`` to ``tmp_path`` so the route resolves the per-instance
``data_root`` at ``tmp_path / "primary"``. Backups are staged through the REAL
:func:`phantom.storage.integrity.quarantine` mover so the manifests and
artifact names are the production article. The endpoints are loopback-only
(ADR-004) and shape responses as the strict admin response models.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from phantom.instances.context import InstanceContext
from phantom.routes import admin as admin_routes
from phantom.storage.integrity import (
    BACKUP_MOVE_MARKER_NAME,
    BackupManifest,
    quarantine,
)

# Pinned display timestamps (identity lives in backup_id, never here).
_TS = datetime(2026, 5, 27, 14, 0, 0)
_ISO = "20260527T140000Z"


def _instance_root(tmp_path: Path) -> Path:
    """Per-instance data_root the route resolves for the ``primary`` instance."""
    root = tmp_path / "primary"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _override_data_root(app: FastAPI, tmp_path: Path) -> None:
    """Point the quarantine routes at ``tmp_path`` (top-level data dir)."""
    app.dependency_overrides[admin_routes.get_data_root] = lambda: tmp_path


def _stage_backup(
    root: Path,
    *,
    reason: str,
    db_bytes: bytes = b"backup-db",
    body_bytes: bytes = b"z" * 16,
) -> BackupManifest:
    """Stage a manifested backup pair through the production mover.

    Creates a live ``uploads.db`` + ``bodies/`` tree under ``root`` and runs
    :func:`quarantine`, leaving the live tree empty and one manifested
    backup behind (manifest + artifact pair, exactly what a real boot-time
    backup produces).
    """
    db_path = root / "uploads.db"
    bodies = root / "bodies"
    db_path.write_bytes(db_bytes)
    (bodies / "chain").mkdir(parents=True)
    (bodies / "chain" / "b.bin").write_bytes(body_bytes)
    return quarantine(db_path, bodies, _TS, reason=reason)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Inventory (GET /v1/admin/quarantine)
# ---------------------------------------------------------------------------


def test_inventory_empty_on_clean_data_root(
    admin_app: tuple[FastAPI, InstanceContext], tmp_path: Path
) -> None:
    """No backups under the instance data_root → empty ``quarantines`` list."""
    app, _ctx = admin_app
    _instance_root(tmp_path)
    _override_data_root(app, tmp_path)
    client = TestClient(app)
    response = client.get("/v1/admin/quarantine")
    assert response.status_code == 200, response.text
    assert response.json() == {"quarantines": []}


def test_inventory_one_entry_per_backup_with_identity(
    admin_app: tuple[FastAPI, InstanceContext], tmp_path: Path
) -> None:
    """F-1 + seam 2: a backup PAIR surfaces as ONE entry keyed by backup_id."""
    app, _ctx = admin_app
    root = _instance_root(tmp_path)
    manifest = _stage_backup(root, reason="corrupted", db_bytes=b"corrupt-bytes")
    _override_data_root(app, tmp_path)
    client = TestClient(app)
    response = client.get("/v1/admin/quarantine")
    assert response.status_code == 200, response.text
    entries = response.json()["quarantines"]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["backup_id"] == str(manifest.backup_id)
    assert entry["reason"] == "corrupted"
    assert entry["iso_display"] == _ISO
    assert entry["db_path"] == str(manifest.db_path)
    assert entry["body_path"] == str(manifest.body_path)
    assert entry["has_db"] is True
    assert entry["has_body"] is True
    assert entry["bytes"] == len(b"corrupt-bytes") + 16
    assert entry["anomaly"] is False


def test_inventory_top_level_artifacts_are_invisible(
    admin_app: tuple[FastAPI, InstanceContext], tmp_path: Path
) -> None:
    """F-1 regression: artifacts at the TOP-LEVEL data dir are not scanned.

    Before the F-1 fix the route scanned the top level and missed the
    per-instance subdir. Now it scans the per-instance subdir; a stray
    artifact at the top level must NOT appear.
    """
    app, _ctx = admin_app
    _instance_root(tmp_path)
    # Stage a quarantine-shaped artifact at the TOP level (wrong place).
    (tmp_path / f"uploads.corrupted.{_ISO}-deadbeef.db").write_bytes(b"top-level")
    _override_data_root(app, tmp_path)
    client = TestClient(app)
    response = client.get("/v1/admin/quarantine")
    assert response.status_code == 200, response.text
    assert response.json() == {"quarantines": []}


def test_inventory_classifies_mode_switch_reason(
    admin_app: tuple[FastAPI, InstanceContext], tmp_path: Path
) -> None:
    """A mode_switch backup surfaces as one entry with ``reason='mode_switch'``."""
    app, _ctx = admin_app
    root = _instance_root(tmp_path)
    manifest = _stage_backup(root, reason="mode_switch")
    _override_data_root(app, tmp_path)
    client = TestClient(app)
    response = client.get("/v1/admin/quarantine")
    assert response.status_code == 200, response.text
    entries = response.json()["quarantines"]
    assert len(entries) == 1
    assert entries[0]["reason"] == "mode_switch"
    assert entries[0]["backup_id"] == str(manifest.backup_id)
    assert entries[0]["iso_display"] == _ISO


def test_inventory_flags_unmanifested_artifact_as_anomaly(
    admin_app: tuple[FastAPI, InstanceContext], tmp_path: Path
) -> None:
    """R5-P: a loose body-only artifact (no manifest) is a flagged anomaly.

    It carries ``anomaly=true`` and ``backup_id=null``, so the restore route
    cannot even address it (restore is keyed on backup_id).
    """
    app, _ctx = admin_app
    root = _instance_root(tmp_path)
    loose_body = root / f"bodies.mode_switch.{_ISO}-deadbeef"
    (loose_body / "chain").mkdir(parents=True)
    (loose_body / "chain" / "b.bin").write_bytes(b"o" * 8)
    _override_data_root(app, tmp_path)
    client = TestClient(app)
    response = client.get("/v1/admin/quarantine")
    assert response.status_code == 200, response.text
    entries = response.json()["quarantines"]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["anomaly"] is True
    assert entry["backup_id"] is None
    assert entry["reason"] == "mode_switch"
    assert entry["body_path"] == str(loose_body)
    assert entry["db_path"] is None
    assert entry["has_db"] is False
    assert entry["has_body"] is True


def test_inventory_unknown_instance_returns_421(
    admin_app: tuple[FastAPI, InstanceContext], tmp_path: Path
) -> None:
    """``?instance=`` naming an unknown instance → 421 ErrorEnvelope."""
    app, _ctx = admin_app
    _instance_root(tmp_path)
    _override_data_root(app, tmp_path)
    client = TestClient(app)
    response = client.get("/v1/admin/quarantine", params={"instance": "nope"})
    assert response.status_code == 421, response.text
    assert response.json()["error"]["code"] == "instance_unknown"


# ---------------------------------------------------------------------------
# Restore (POST /v1/admin/quarantine/restore?backup_id=...)
# ---------------------------------------------------------------------------


def test_restore_unknown_backup_id_returns_404(
    admin_app: tuple[FastAPI, InstanceContext], tmp_path: Path
) -> None:
    """A ``backup_id`` naming no mode_switch backup → 404 ErrorEnvelope."""
    app, _ctx = admin_app
    _instance_root(tmp_path)
    _override_data_root(app, tmp_path)
    client = TestClient(app)
    response = client.post("/v1/admin/quarantine/restore", params={"backup_id": str(uuid4())})
    assert response.status_code == 404, response.text
    assert response.json()["error"]["code"] == "not_found"


def test_restore_corrupted_backup_is_not_restorable(
    admin_app: tuple[FastAPI, InstanceContext], tmp_path: Path
) -> None:
    """Only mode_switch backups are restorable; a corrupted backup_id → 404."""
    app, _ctx = admin_app
    root = _instance_root(tmp_path)
    manifest = _stage_backup(root, reason="corrupted")
    _override_data_root(app, tmp_path)
    client = TestClient(app)
    response = client.post(
        "/v1/admin/quarantine/restore", params={"backup_id": str(manifest.backup_id)}
    )
    assert response.status_code == 404, response.text


def test_restore_db_missing_refuses_409_without_displacing_live(
    admin_app: tuple[FastAPI, InstanceContext], tmp_path: Path
) -> None:
    """A manifested backup whose DB half is gone refuses UP FRONT (409).

    R5-P inverse leg: the refusal happens before any live data is displaced,
    so the live tree is untouched and no interim backup is taken.
    """
    app, _ctx = admin_app
    root = _instance_root(tmp_path)
    manifest = _stage_backup(root, reason="mode_switch")
    # Remove the DB half out-of-band (an interrupted move / operator action).
    manifest.db_path.unlink()
    # Live data that must NOT be displaced by the doomed restore.
    (root / "uploads.db").write_bytes(b"live-db")
    _override_data_root(app, tmp_path)
    client = TestClient(app)
    response = client.post(
        "/v1/admin/quarantine/restore", params={"backup_id": str(manifest.backup_id)}
    )
    assert response.status_code == 409, response.text
    payload = response.json()
    assert payload["error"]["code"] == "restore_noop"
    assert payload["error"]["details"]["backup_id"] == str(manifest.backup_id)
    assert payload["error"]["details"]["interim_backup_db"] is None
    # The live tree was never touched.
    assert (root / "uploads.db").read_bytes() == b"live-db"


def test_restore_moves_backup_into_live_tree(
    admin_app: tuple[FastAPI, InstanceContext], tmp_path: Path
) -> None:
    """Restore moves the chosen mode_switch pair into the live db_path/bodies."""
    app, _ctx = admin_app
    root = _instance_root(tmp_path)
    manifest = _stage_backup(root, reason="mode_switch")
    _override_data_root(app, tmp_path)
    client = TestClient(app)
    response = client.post(
        "/v1/admin/quarantine/restore", params={"backup_id": str(manifest.backup_id)}
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["restored_db"] == str(root / "uploads.db")
    assert payload["restored_body"] == str(root / "bodies")
    assert payload["restart_required"] is True
    assert str(manifest.backup_id) in payload["detail"]
    # No live data existed beforehand, so nothing was backed up.
    assert payload["interim_backup_db"] is None
    assert payload["interim_backup_body"] is None
    # The chosen backup is now the live tree; the backup artifacts are gone.
    assert (root / "uploads.db").read_bytes() == b"backup-db"
    assert (root / "bodies" / "chain" / "b.bin").read_bytes() == b"z" * 16
    assert not manifest.db_path.exists()
    assert manifest.body_path is not None
    assert not manifest.body_path.exists()
    # The in-progress marker was cleared and the CONSUMED manifest deleted:
    # the inventory no longer lists the restored backup.
    assert not (root / BACKUP_MOVE_MARKER_NAME).exists()
    inv = client.get("/v1/admin/quarantine")
    assert inv.json() == {"quarantines": []}


def test_restore_backs_up_current_live_data_first(
    admin_app: tuple[FastAPI, InstanceContext], tmp_path: Path
) -> None:
    """Existing live data is backed up to a fresh manifested backup first."""
    app, _ctx = admin_app
    root = _instance_root(tmp_path)
    manifest = _stage_backup(root, reason="mode_switch")
    # Stage CURRENT live data that must be preserved, not clobbered.
    (root / "uploads.db").write_bytes(b"live-db")
    (root / "bodies").mkdir()
    (root / "bodies" / "live.bin").write_bytes(b"L" * 8)
    _override_data_root(app, tmp_path)
    client = TestClient(app)
    response = client.post(
        "/v1/admin/quarantine/restore", params={"backup_id": str(manifest.backup_id)}
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    # The interim backup captured the prior live data (clobber-safe).
    assert payload["interim_backup_db"] is not None
    assert payload["interim_backup_body"] is not None
    assert Path(payload["interim_backup_db"]).read_bytes() == b"live-db"
    assert (Path(payload["interim_backup_body"]) / "live.bin").read_bytes() == b"L" * 8
    # The chosen backup is now live.
    assert (root / "uploads.db").read_bytes() == b"backup-db"
    # The interim backup itself shows up in the inventory as ONE manifested
    # mode_switch backup (the restored one was consumed).
    inv = client.get("/v1/admin/quarantine").json()["quarantines"]
    assert len(inv) == 1
    assert inv[0]["reason"] == "mode_switch"
    assert inv[0]["anomaly"] is False
    assert inv[0]["db_path"] == payload["interim_backup_db"]


def test_restore_round_trips_via_inventory_backup_id(
    admin_app: tuple[FastAPI, InstanceContext], tmp_path: Path
) -> None:
    """The backup_id surfaced by the inventory is accepted verbatim by restore."""
    app, _ctx = admin_app
    root = _instance_root(tmp_path)
    _stage_backup(root, reason="mode_switch")
    _override_data_root(app, tmp_path)
    client = TestClient(app)
    inv = client.get("/v1/admin/quarantine").json()["quarantines"]
    assert len(inv) == 1
    backup_id = inv[0]["backup_id"]
    assert backup_id is not None
    restore = client.post("/v1/admin/quarantine/restore", params={"backup_id": backup_id})
    assert restore.status_code == 200, restore.text
    assert restore.json()["restored_db"] == str(root / "uploads.db")
