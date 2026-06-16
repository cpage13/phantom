"""Two same-second backups coexist and BOTH restore (re-attack of H-1 inverse legs).

Formerly ``test_restore_suffixed_iso_round_trip.py``: the prior cycle's H-1
fix bolted a monotonic ``-1``/``-2`` disambiguation suffix onto the
second-granularity iso, and these legs re-attacked that machinery (a run of
collisions, a suffixed token's inventory/restore/reconcile round trip).
Cycle-7 seam 1 DELETED that machinery: a backup's identity is a uuid
``backup_id``, artifact names carry the id's hex token after the display
iso, and the manifest (seam 2) declares the pair. The re-target proves the
phase-3 acceptance criterion directly, as make-it-impossible coverage:

* :func:`test_two_same_second_backups_coexist_and_both_restore` - two
  backups minted in the SAME pinned wall-clock second coexist with distinct
  backup_ids, the inventory reports one entry each, and BOTH restore
  correctly through the real route, in sequence, with every interim
  quarantine also landing in that same second.

* :func:`test_same_second_backup_survives_a_boot_reconcile` - the
  boot-reconcile leg: a half-finished RESTORE move marked (by backup_id)
  for a backup whose display iso is shared with ANOTHER backup in the same
  second is finished forward by ``reconcile_interrupted_backup_move``
  grounded in the manifest's declared paths; the sibling same-second backup
  is untouched. Proves reconciliation never resolves a backup through its
  timestamp.

Public e2e-light lane (plan § 5.0): generic sentinel rows, no
``PHANTOM_ENABLED``. The route leg drives the real
``POST /v1/admin/quarantine/restore`` over the in-process stack; the
reconcile leg drives the public integrity surface directly.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from phantom.storage import integrity as integrity_mod
from phantom.storage.integrity import (
    BACKUP_MOVE_MARKER_NAME,
    BackupManifest,
    BackupMoveMarker,
    load_backup_manifest,
    quarantine,
    reconcile_interrupted_backup_move,
)

from tests.e2e.helpers.stack import E2EStack, boot_stack

# This module mixes async route-driving tests (auto-detected by
# ``asyncio_mode = "auto"``) and a sync integrity-surface test, so no
# module-level asyncio mark is applied (it would wrongly tag the sync test).

# A FIXED iso every backup in the route leg derives (utc_stamp pinned), so
# the same-second case is deterministic, not raced.
_PINNED_ISO = "20260101T000000Z"
# The same instant as an aware datetime, for the explicit-timestamp leg.
_PINNED_MOMENT = datetime(2026, 1, 1, tzinfo=UTC)

_LIVE_TAG = "FRESH-LIVE-SENTINEL"
_TAG_A = "PRECIOUS-ROW-A"
_TAG_B = "PRECIOUS-ROW-B"


def _instance_root(stack: E2EStack) -> Path:
    """Return the per-instance data root (where uploads.db + flat backups live)."""
    return stack.data_dir / stack.settings.instances[0].data_dir


def _write_min_uploads_db(path: Path, tag: str) -> None:
    """Write a minimal SQLite ``uploads`` table carrying one tagged sentinel row.

    Shape-minimal on purpose: these tests assert WHICH file ends up at a given
    path (by reading back the sentinel ``tag``), not that the row is a full
    production ``UploadRow``. Any pre-existing file at ``path`` plus its
    ``-wal``/``-shm`` siblings is removed first so the sentinel schema is
    authoritative rather than colliding with a real ``uploads`` table that has
    no ``tag`` column.

    Args:
        path: The DB file path to (re)create.
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


def _stage_backup(
    inst_root: Path,
    *,
    tag: str,
    body_byte: bytes,
    moment: datetime | None = None,
) -> BackupManifest:
    """Stage one manifested ``mode_switch`` backup through the production mover.

    Args:
        inst_root: The per-instance data root.
        tag: The sentinel row written into the backup DB.
        body_byte: A marker byte written into the backup body dir.
        moment: Optional explicit display timestamp (the route legs pin
            ``utc_stamp`` instead and pass ``None``).

    Returns:
        The backup's manifest (its ``backup_id`` is the restore handle).
    """
    db_path = inst_root / "uploads.db"
    bodies = inst_root / "bodies"
    _write_min_uploads_db(db_path, tag)
    (bodies / "shard").mkdir(parents=True, exist_ok=True)
    (bodies / "shard" / "precious.bin").write_bytes(body_byte)
    return quarantine(db_path, bodies, moment, reason="mode_switch")


async def test_two_same_second_backups_coexist_and_both_restore(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two backups minted in ONE wall-clock second coexist and BOTH restore.

    The phase-3 acceptance criterion end to end: stage backup A then backup B
    in the same pinned second (identity-token names cannot collide), assert
    the inventory carries one entry per backup with distinct backup_ids and
    one shared display iso, then restore A and B in sequence through the
    real route. Every interim quarantine the route takes also lands in the
    pinned second. Each restore must land ITS backup's row at the live path.
    """
    monkeypatch.setattr(integrity_mod, "utc_stamp", lambda *a, **k: _PINNED_ISO)
    stack = await boot_stack(
        tmp_path=tmp_path, config_overrides={"storage": {"body_store": {"mode": "all_disk"}}}
    )
    try:
        inst_root = _instance_root(stack)
        db_path = inst_root / "uploads.db"

        manifest_a = _stage_backup(inst_root, tag=_TAG_A, body_byte=b"a")
        manifest_b = _stage_backup(inst_root, tag=_TAG_B, body_byte=b"b")
        assert manifest_a.backup_id != manifest_b.backup_id
        assert manifest_a.iso_display == manifest_b.iso_display == _PINNED_ISO
        # Both artifact pairs are physically on disk at distinct names.
        assert manifest_a.db_path.exists() and manifest_b.db_path.exists()

        # A fresh live tree (so each restore's interim quarantine has work).
        _write_min_uploads_db(db_path, _LIVE_TAG)
        (inst_root / "bodies").mkdir(exist_ok=True)

        inv = await stack.phantom_client.get_quarantine_inventory(instance="primary")
        entries = {e.backup_id for e in inv.quarantines if e.reason == "mode_switch"}
        assert entries == {manifest_a.backup_id, manifest_b.backup_id}, (
            f"one entry per same-second backup, keyed by identity; got {inv.quarantines!r}"
        )
        assert all(not e.anomaly for e in inv.quarantines)

        # Restore A: its row lands; its interim backup (the fresh sentinel)
        # coexists in the same second.
        restore_a = await stack.phantom_client.restore_quarantine_backup(
            backup_id=manifest_a.backup_id, instance="primary"
        )
        assert restore_a.restart_required is True
        assert _read_tag(db_path) == _TAG_A, (
            f"backup A must restore correctly; live row is {_read_tag(db_path)!r}"
        )

        # Restore B: displaces the just-restored A into ANOTHER same-second
        # interim backup, then lands B's row.
        restore_b = await stack.phantom_client.restore_quarantine_backup(
            backup_id=manifest_b.backup_id, instance="primary"
        )
        assert restore_b.restart_required is True
        assert _read_tag(db_path) == _TAG_B, (
            f"backup B must restore correctly; live row is {_read_tag(db_path)!r}"
        )
        assert (inst_root / "bodies" / "shard" / "precious.bin").read_bytes() == b"b"

        # A's row survives, recoverable: B's interim backup carries it.
        assert restore_b.interim_backup_db is not None
        assert _read_tag(Path(restore_b.interim_backup_db)) == _TAG_A, (
            "the displaced A row must survive in B's interim backup"
        )
        # No crash-recovery debt left behind.
        assert not (inst_root / BACKUP_MOVE_MARKER_NAME).exists()
    finally:
        await stack.tear_down()


def test_same_second_backup_survives_a_boot_reconcile(tmp_path: Path) -> None:
    """A half-finished RESTORE is finished forward by identity, not timestamp.

    Two manifested backups share one pinned display second. A restore of
    backup B crashed mid-move (marker present, keyed on B's backup_id; B's
    artifacts still at their backup paths). The per-instance boot step
    ``reconcile_interrupted_backup_move`` must load B's MANIFEST by the
    marker's backup_id and finish B forward into the live tree, leaving the
    same-second sibling A untouched. Under timestamp-keyed reconciliation
    this would be ambiguous; under identity it cannot be.
    """
    inst_root = tmp_path / "primary"
    inst_root.mkdir()
    db_path = inst_root / "uploads.db"
    bodies_root = inst_root / "bodies"

    manifest_a = _stage_backup(inst_root, tag=_TAG_A, body_byte=b"a", moment=_PINNED_MOMENT)
    manifest_b = _stage_backup(inst_root, tag=_TAG_B, body_byte=b"b", moment=_PINNED_MOMENT)
    assert manifest_a.iso_display == manifest_b.iso_display

    # Mid-restore crash state for B: marker written (by backup_id), live
    # targets empty, B's artifacts still at their backup paths. The marker is
    # written through the same atomic writer the route uses.
    integrity_mod._write_json_model_atomic(
        inst_root / BACKUP_MOVE_MARKER_NAME,
        BackupMoveMarker(backup_id=manifest_b.backup_id, direction="restore"),
    )

    direction = reconcile_interrupted_backup_move(db_path=db_path, body_store_root=bodies_root)

    assert direction == "restore", (
        f"the reconciler must complete the marked restore; got {direction!r}"
    )
    assert _read_tag(db_path) == _TAG_B, (
        f"backup B (the marker's identity) must be finished forward; live row is "
        f"{_read_tag(db_path)!r}"
    )
    assert (bodies_root / "shard" / "precious.bin").read_bytes() == b"b"
    # The same-second sibling A is untouched (still a restorable backup).
    assert manifest_a.db_path.exists(), "the sibling same-second backup must be untouched"
    assert _read_tag(manifest_a.db_path) == _TAG_A
    assert load_backup_manifest(inst_root, manifest_a.backup_id) is not None
    # B was consumed: marker cleared, manifest gone.
    assert not (inst_root / BACKUP_MOVE_MARKER_NAME).exists(), "the marker must be cleared"
    assert load_backup_manifest(inst_root, manifest_b.backup_id) is None
