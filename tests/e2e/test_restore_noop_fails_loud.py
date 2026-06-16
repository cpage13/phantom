"""The restore fail-loud guards, re-targeted to the manifest world (R5-P / L-2 / H-1).

The prior cycle's findings: a restore that moved nothing returned a
success-shaped response (L-2), and a BODY-ONLY loose artifact passed the
filename-matching membership check, moved its body half, no-opped the DB,
and stranded the upload metadata (R5-P). Cycle-7 seam 2 makes the loose-pair
class UNREPRESENTABLE: a backup exists only as a MANIFEST; the restore route
addresses ``backup_id`` and never string-matches filenames; an unmanifested
artifact is an inventory ANOMALY with no identity at all. The residual
fail-loud guards stay for the races that remain representable:

* :func:`test_unmanifested_body_only_artifact_is_an_anomaly_not_restorable` -
  the R5-P attack itself, now impossible: a loose body-only artifact
  surfaces as a flagged anomaly (``backup_id`` null) and cannot be
  addressed by the restore route at all; the artifact is untouched and the
  live tree is never displaced.

* :func:`test_restore_of_backup_with_missing_db_refuses_up_front` - a
  MANIFESTED backup whose DB half is gone (interrupted move debris,
  operator deletion) is refused 409 ``restore_noop`` BEFORE any live data
  is displaced: no interim backup is taken, the live tree is untouched.

* :func:`test_residual_vanish_race_after_displacement_names_the_interim` -
  the artifacts vanish BETWEEN the route's up-front check and the move (the
  residual race the post-move guard exists for): 409 ``restore_noop`` whose
  ``details`` name the interim backup of the displaced live data, so
  nothing is lost.

* :func:`test_restore_of_a_db_only_backup_succeeds` - the R5-P INVERSE leg:
  a legitimately metadata-only backup (manifest declares the pair; only the
  DB half exists, ``has_body=false``) restores cleanly; the guard keys on
  the DB landing, not on the body.

Public e2e-light lane (plan § 5.0): drives the REAL routes over the
in-process stack via the SDK, no ``PHANTOM_ENABLED``. Backups are staged
through the real :func:`phantom.storage.integrity.quarantine` mover.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest
from phantom.routes import admin as admin_mod
from phantom.storage.integrity import BackupManifest, quarantine
from phantom_client import PhantomConflictError, PhantomNotFoundError

from tests.e2e.helpers.stack import E2EStack, boot_stack

# A display iso for hand-named anomaly artifacts (display material only).
_ANOMALY_STAMP = "20991231T235959Z-deadbeef"

_LIVE_TAG = "FRESH-LIVE-SENTINEL"
_BACKUP_TAG = "PRECIOUS-BACKUP-ROW"


def _instance_root(stack: E2EStack) -> Path:
    """Return the per-instance data root for the sole booted instance."""
    return stack.data_dir / stack.settings.instances[0].data_dir


def _write_min_uploads_db(path: Path, tag: str) -> None:
    """Write a minimal SQLite ``uploads`` table carrying one tagged sentinel row.

    Args:
        path: The DB file path to (re)create (stale WAL/SHM siblings removed).
        tag: The sentinel value written into the single ``uploads`` row.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    for stale in (path, path.with_name(path.name + "-wal"), path.with_name(path.name + "-shm")):
        stale.unlink(missing_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS uploads (chain_id TEXT PRIMARY KEY, tag TEXT)")
        conn.execute("INSERT INTO uploads (chain_id, tag) VALUES (?, ?)", (str(uuid4()), tag))
        conn.commit()
    finally:
        conn.close()


def _read_tag(path: Path) -> str | None:
    """Read the single sentinel ``tag`` from an on-disk ``uploads`` table.

    Args:
        path: The DB file to read.

    Returns:
        The ``tag`` value, or ``None`` when the file is absent / has no row.
    """
    if not path.exists():
        return None
    conn = sqlite3.connect(str(path))
    try:
        row = conn.execute("SELECT tag FROM uploads LIMIT 1").fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _stage_backup(inst_root: Path, *, tag: str, with_body: bool = True) -> BackupManifest:
    """Stage one manifested ``mode_switch`` backup through the production mover.

    Args:
        inst_root: The per-instance data root.
        tag: The sentinel row written into the backup DB.
        with_body: Whether a body tree exists at backup time (``False``
            stages a legitimately metadata-only backup, ``has_body=false``).

    Returns:
        The backup's manifest (its ``backup_id`` is the restore handle).
    """
    db_path = inst_root / "uploads.db"
    bodies = inst_root / "bodies"
    _write_min_uploads_db(db_path, tag)
    if with_body:
        (bodies / "shard").mkdir(parents=True, exist_ok=True)
        (bodies / "shard" / "precious.bin").write_bytes(b"backup-body")
    return quarantine(db_path, bodies, reason="mode_switch")


async def test_unmanifested_body_only_artifact_is_an_anomaly_not_restorable(
    tmp_path: Path,
) -> None:
    """R5-P, made impossible: a loose body-only artifact has no restore handle.

    Stage a REAL body-only ``mode_switch``-named directory with NO manifest
    (the artifact-tampering / un-reconciled-debris class R5-P exploited).
    The inventory must surface it as a flagged anomaly with ``backup_id``
    null, so the identity-keyed restore route cannot address it AT ALL; a
    probe with a fresh uuid 404s, the artifact is untouched, and the live
    tree is never displaced.
    """
    stack = await boot_stack(
        tmp_path=tmp_path, config_overrides={"storage": {"body_store": {"mode": "all_disk"}}}
    )
    try:
        inst_root = _instance_root(stack)
        body_only = inst_root / f"bodies.mode_switch.{_ANOMALY_STAMP}"
        body_only.mkdir(parents=True, exist_ok=True)
        (body_only / "precious.bin").write_bytes(b"body-only-backup")
        live_db = inst_root / "uploads.db"
        _write_min_uploads_db(live_db, _LIVE_TAG)

        # The inventory flags it: anomaly, no identity, not a backup.
        inv = await stack.phantom_client.get_quarantine_inventory(instance="primary")
        anomalies = [e for e in inv.quarantines if e.anomaly]
        assert len(anomalies) == 1, f"the loose artifact must be flagged; got {inv.quarantines!r}"
        anomaly = anomalies[0]
        assert anomaly.backup_id is None, "an anomaly has no identity to restore by"
        assert anomaly.body_path == str(body_only)
        assert anomaly.has_db is False
        assert anomaly.has_body is True

        # There is no handle to address it with; any uuid probe is a 404.
        with pytest.raises(PhantomNotFoundError):
            await stack.phantom_client.restore_quarantine_backup(
                backup_id=uuid4(), instance="primary"
            )

        # Nothing moved: the artifact is untouched, the live tree undisplaced.
        assert (body_only / "precious.bin").read_bytes() == b"body-only-backup"
        assert _read_tag(live_db) == _LIVE_TAG, "the live tree must never be displaced"
    finally:
        await stack.tear_down()


async def test_restore_of_backup_with_missing_db_refuses_up_front(tmp_path: Path) -> None:
    """A manifested backup whose DB half is gone refuses 409 BEFORE displacement.

    The DB is the load-bearing half (it holds the upload rows). When the
    manifest's declared DB artifact is absent on disk, the route must refuse
    up front: 409 ``restore_noop`` (SDK ``PhantomConflictError``), NO interim
    backup taken, live tree untouched. The doomed restore displaces nothing.
    """
    stack = await boot_stack(
        tmp_path=tmp_path, config_overrides={"storage": {"body_store": {"mode": "all_disk"}}}
    )
    try:
        inst_root = _instance_root(stack)
        manifest = _stage_backup(inst_root, tag=_BACKUP_TAG)
        # The DB half vanishes out-of-band (operator action / tampering).
        manifest.db_path.unlink()
        live_db = inst_root / "uploads.db"
        _write_min_uploads_db(live_db, _LIVE_TAG)

        # The inventory reports the truth: has_db false (not restorable).
        inv = await stack.phantom_client.get_quarantine_inventory(instance="primary")
        entry = next(e for e in inv.quarantines if e.backup_id == manifest.backup_id)
        assert entry.has_db is False
        assert entry.has_body is True

        with pytest.raises(PhantomConflictError) as excinfo:
            await stack.phantom_client.restore_quarantine_backup(
                backup_id=manifest.backup_id, instance="primary"
            )
        err = excinfo.value
        assert err.status_code == 409
        assert err.error_code == "restore_noop"
        details = err.details
        assert details.get("backup_id") == str(manifest.backup_id)
        # Up-front refusal: nothing was displaced, so no interim pointers.
        assert details.get("interim_backup_db") is None
        assert details.get("interim_backup_body") is None
        assert _read_tag(live_db) == _LIVE_TAG, (
            "the up-front refusal must leave the live tree untouched"
        )
        # The backup's body half is also untouched (still recoverable).
        assert manifest.body_path is not None
        assert (manifest.body_path / "shard" / "precious.bin").exists()
    finally:
        await stack.tear_down()


async def test_residual_vanish_race_after_displacement_names_the_interim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The post-move guard still fails loud and names the interim backup.

    The residual race the post-move guard exists for: the backup's DB
    artifact vanishes BETWEEN the route's up-front existence check and the
    move. The live tree IS displaced (the interim quarantine ran before the
    move), so the 409 ``restore_noop`` ``details`` must carry non-null
    ``interim_backup_db`` / ``interim_backup_body`` pointers to on-disk
    artifacts, or the displaced live data would be stranded behind the
    error. The race is collapsed into a certainty by wrapping the route's
    ``restore_mode_switch_backup`` to delete the DB artifact just before
    delegating (the only seam where the check and the move can disagree).
    """
    stack = await boot_stack(
        tmp_path=tmp_path, config_overrides={"storage": {"body_store": {"mode": "all_disk"}}}
    )
    try:
        inst_root = _instance_root(stack)
        manifest = _stage_backup(inst_root, tag=_BACKUP_TAG)
        live_db = inst_root / "uploads.db"
        _write_min_uploads_db(live_db, _LIVE_TAG)
        (inst_root / "bodies").mkdir(exist_ok=True)

        real_restore = admin_mod.restore_mode_switch_backup

        def vanish_then_restore(db_path: Path, body_store_root: Path, m: BackupManifest) -> object:
            m.db_path.unlink(missing_ok=True)
            return real_restore(db_path, body_store_root, m)

        monkeypatch.setattr(admin_mod, "restore_mode_switch_backup", vanish_then_restore)

        with pytest.raises(PhantomConflictError) as excinfo:
            await stack.phantom_client.restore_quarantine_backup(
                backup_id=manifest.backup_id, instance="primary"
            )
        details = excinfo.value.details
        assert details.get("backup_id") == str(manifest.backup_id)
        assert details.get("instance_id") == "primary"
        interim_db = details.get("interim_backup_db")
        interim_body = details.get("interim_backup_body")
        assert interim_db is not None, (
            f"the displaced live data must be recoverable; details={details!r}"
        )
        assert interim_body is not None
        assert Path(interim_db).exists()
        assert Path(interim_body).exists()
        assert _read_tag(Path(interim_db)) == _LIVE_TAG, (
            "the interim backup must hold the displaced live row"
        )
    finally:
        await stack.tear_down()


async def test_restore_of_a_db_only_backup_succeeds(tmp_path: Path) -> None:
    """The R5-P INVERSE leg: a metadata-only (DB-only) backup restore SUCCEEDS.

    A manifested backup whose body tree did not exist at backup time
    (``has_body=false``) is a legitimate metadata-only backup. The fail-loud
    guard keys on the DB landing (the load-bearing half), so this restore
    must succeed with ``restart_required=True``, not 409. Locks the guard
    against an over-tightening drift that would also require the body half.
    """
    stack = await boot_stack(
        tmp_path=tmp_path, config_overrides={"storage": {"body_store": {"mode": "all_disk"}}}
    )
    try:
        inst_root = _instance_root(stack)
        # Remove the booted store's bodies dir so the staged backup is DB-only.
        bodies = inst_root / "bodies"
        if bodies.exists():
            import shutil

            shutil.rmtree(bodies)
        manifest = _stage_backup(inst_root, tag=_BACKUP_TAG, with_body=False)
        assert manifest.has_body is False
        assert manifest.has_db is True

        resp = await stack.phantom_client.restore_quarantine_backup(
            backup_id=manifest.backup_id, instance="primary"
        )
        assert resp.restart_required is True, (
            "a DB-only restore stages on disk and needs a restart like any restore"
        )
        assert not manifest.db_path.exists(), (
            "the backup DB must have been MOVED into the live tree (db_moved=True)"
        )
        assert _read_tag(inst_root / "uploads.db") == _BACKUP_TAG, (
            "the restored DB must be present at the live path after a DB-only restore"
        )
    finally:
        await stack.tear_down()
