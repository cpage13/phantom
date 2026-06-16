"""Same-second admin restore must not be silently lost (re-attack of H-1).

The prior cycle's H-1: the one-call admin restore route's interim quarantine
derived a SECOND-granularity name; when the restore fired in the same
wall-clock second as the backup it was undoing, the names collided, the
clobber-safe moves silently no-opped, and the route returned a
success-shaped response while the buffered uploads were never restored.

Cycle-7 seams 1 + 2 make that class UNREPRESENTABLE rather than guarded:

* a backup's identity is a uuid ``backup_id`` minted at creation; artifact
  names carry the display iso PLUS the id's hex token, so two same-second
  backups land on distinct names BY CONSTRUCTION (no disambiguation
  machinery exists to get wrong);
* every backup is declared by ONE manifest, and the restore route addresses
  the manifest by ``backup_id`` (never by parsing names).

This test pins the acceptance behavior over the REAL
``POST /v1/admin/quarantine/restore`` route (in-process stack, public
e2e-light lane) with ``utc_stamp`` pinned to one fixed iso so EVERYTHING
(the staged backup, the route's interim quarantine) lands in the SAME
wall-clock second deterministically. After the restore + a fresh read of
the on-disk DB (the route is ``restart_required``), the live ``uploads``
table must contain the BACKED-UP row, not the fresh sentinel row, and the
displaced fresh sentinel must be recoverable from the interim backup.

Falsifier: re-key the artifact names on the display iso alone (drop the
identity token) so the interim quarantine collides with the staged backup
-> the live DB keeps the fresh sentinel -> RED.

Public e2e-light lane (plan § 5.0): generic sentinel rows, no
``PHANTOM_ENABLED``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest
from phantom.storage import integrity as integrity_mod
from phantom.storage.integrity import BackupManifest, quarantine

from tests.e2e.helpers.stack import E2EStack, boot_stack

pytestmark = [pytest.mark.asyncio]

# A FIXED iso both the staged backup and the route's interim quarantine will
# use, so the same-second case is guaranteed rather than raced.
_PINNED_ISO = "20260101T000000Z"

_BACKUP_TAG = "PRECIOUS-BACKUP-ROW"
_FRESH_TAG = "FRESH-LIVE-SENTINEL-ROW"


def _instance_root(stack: E2EStack) -> Path:
    """Return the per-instance data root (where uploads.db + backups live)."""
    instance_cfg = stack.settings.instances[0]
    return stack.data_dir / instance_cfg.data_dir


def _live_db_path(stack: E2EStack) -> Path:
    """Return the live ``uploads.db`` path for the sole instance."""
    return _instance_root(stack) / "uploads.db"


def _write_min_uploads_db(path: Path, tag: str) -> None:
    """Write a minimal SQLite ``uploads`` table carrying one tagged sentinel row.

    Shape-minimal on purpose: this test asserts WHICH file ends up at the live
    ``uploads.db`` path (by reading back the sentinel ``tag``), not that the row
    is a full production ``UploadRow``. The route's movers operate on the file,
    not its schema.

    Any pre-existing file at ``path`` (e.g. the real-schema ``uploads.db`` the
    booted store created) plus its ``-wal``/``-shm`` siblings is removed first,
    so this minimal sentinel schema is authoritative rather than colliding with
    a real ``uploads`` table that has no ``tag`` column. The booted store keeps
    its own open descriptor; the test reads this on-disk file directly.
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
    """Read the single sentinel ``tag`` from an on-disk ``uploads`` table."""
    if not path.exists():
        return None
    conn = sqlite3.connect(str(path))
    try:
        row = conn.execute("SELECT tag FROM uploads LIMIT 1").fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _stage_manifested_backup(stack: E2EStack, *, tag: str) -> BackupManifest:
    """Stage a REAL manifested ``mode_switch`` backup through the production mover.

    Writes a tagged sentinel ``uploads.db`` + a body tree at the live paths,
    then drives :func:`phantom.storage.integrity.quarantine` exactly as a
    boot-time back-up-and-run would, leaving one manifest + artifact pair
    behind and the live tree empty.
    """
    inst_root = _instance_root(stack)
    db_path = _live_db_path(stack)
    bodies = inst_root / "bodies"
    _write_min_uploads_db(db_path, tag)
    (bodies / "shard").mkdir(parents=True, exist_ok=True)
    (bodies / "shard" / "precious.bin").write_bytes(b"precious-body-bytes")
    return quarantine(db_path, bodies, reason="mode_switch")


async def test_same_second_restore_actually_restores_the_backup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A restore landing in the boot-backup's second must still restore the backup.

    Pins ``utc_stamp`` to a fixed iso so the staged backup AND the route's
    interim quarantine share one wall-clock second (the H-1 premise,
    deterministic). The CORRECT outcome: the live ``uploads.db`` ends up
    holding the BACKED-UP row, the displaced fresh sentinel survives in the
    interim backup, and both backups' names coexist (identity-token naming,
    seam 1). This is make-it-impossible coverage: there is no disambiguation
    code left for this test to exercise.
    """
    # Pin the clock the integrity mover reads so every backup minted in this
    # test carries the SAME display iso (the same-second case, deterministic).
    monkeypatch.setattr(integrity_mod, "utc_stamp", lambda *a, **k: _PINNED_ISO)

    # Boot an all_disk stack (no RAM bodies; the row lives in the on-disk DB the
    # route moves). One instance ("primary"), so the restore needs no disambiguation.
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={"storage": {"body_store": {"mode": "all_disk"}}},
    )
    try:
        inst_root = _instance_root(stack)
        db_path = _live_db_path(stack)

        # Stage a manifested mode_switch backup carrying the precious row,
        # exactly as a boot-time back-up-and-run would have left it.
        manifest = _stage_manifested_backup(stack, tag=_BACKUP_TAG)
        assert manifest.iso_display == _PINNED_ISO

        # Overwrite the on-disk live tree with the fresh sentinel so we can
        # tell whether the restore swapped it for the backup. (The route is
        # restart_required; we read the on-disk file directly after the move,
        # never through the running store.)
        _write_min_uploads_db(db_path, _FRESH_TAG)
        (inst_root / "bodies").mkdir(exist_ok=True)
        assert _read_tag(db_path) == _FRESH_TAG, "precondition: live DB holds the fresh sentinel"

        # The backup is visible in the inventory as ONE entry, keyed by its id.
        inv = await stack.phantom_client.get_quarantine_inventory(instance="primary")
        mode_switch = [e for e in inv.quarantines if e.reason == "mode_switch"]
        assert len(mode_switch) == 1, f"one staged backup => one entry; got {inv.quarantines!r}"
        assert mode_switch[0].backup_id == manifest.backup_id
        assert mode_switch[0].anomaly is False
        assert mode_switch[0].iso_display == _PINNED_ISO

        # Fire the REAL restore route BY IDENTITY. Its interim quarantine uses
        # the pinned iso too -> the same-second case, impossible to collide.
        restore = await stack.phantom_client.restore_quarantine_backup(
            backup_id=manifest.backup_id, instance="primary"
        )
        assert restore.restart_required is True

        # CORRECT behavior: the live on-disk uploads.db now holds the BACKED-UP
        # row. (Under the prior cycle's H-1 defect it still held the FRESH
        # sentinel because both clobber-safe moves no-op'd on the name
        # collision.)
        restored_tag = _read_tag(db_path)
        assert restored_tag == _BACKUP_TAG, (
            "same-second restore was silently lost: the live uploads.db still holds "
            f"{restored_tag!r}, not the backup row {_BACKUP_TAG!r}."
        )
        # The displaced fresh sentinel is recoverable from the interim backup,
        # which coexists in the SAME wall-clock second under its own identity.
        assert restore.interim_backup_db is not None
        assert _read_tag(Path(restore.interim_backup_db)) == _FRESH_TAG, (
            "the displaced live tree must survive in the interim backup"
        )
        inv_after = await stack.phantom_client.get_quarantine_inventory(instance="primary")
        interim_entries = [e for e in inv_after.quarantines if e.reason == "mode_switch"]
        assert len(interim_entries) == 1, (
            "the restored backup was consumed; only the interim backup remains: "
            f"{inv_after.quarantines!r}"
        )
        assert interim_entries[0].backup_id is not None
        assert interim_entries[0].backup_id != manifest.backup_id
        assert interim_entries[0].iso_display == _PINNED_ISO
    finally:
        await stack.tear_down()
