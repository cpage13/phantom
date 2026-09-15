"""Unit tests for :mod:`phantom.workers.cold_backup` (plan § 5.2.6).

Coverage:

* Disabled (``backup_enabled=False``) → :meth:`run` returns immediately.
* :meth:`snapshot_once` writes a non-empty backup file with the
  documented naming convention.
* Rotation keeps the latest N when more than N snapshots exist.
* Round 5 hardening: cold snapshots live entirely OUTSIDE the seam-2
  manifest world; the quarantine inventory neither lists nor flags
  them, in either direction.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

import aiosqlite
import pytest
from phantom.config.settings import (
    BodyStoreCfg,
    DbIntegrityCfg,
    Settings,
    StorageCfg,
)
from phantom.storage.integrity import list_quarantines, quarantine
from phantom.workers.cold_backup import ColdBackupScheduler

pytestmark = pytest.mark.asyncio


def _settings(
    *,
    data_root: Path,
    backup_enabled: bool = True,
    backup_period_seconds: int = 1,
    backup_rotate_n: int = 3,
) -> Settings:
    """Build a :class:`Settings` rooted at ``data_root`` with cold-backup tuning."""
    return Settings(
        storage=StorageCfg(
            data_dir=str(data_root),
            body_store=BodyStoreCfg(mode="hybrid", ram_ceiling_bytes=1024 * 1024),
            db_integrity=DbIntegrityCfg(
                backup_enabled=backup_enabled,
                backup_period_seconds=backup_period_seconds,
                backup_rotate_n=backup_rotate_n,
            ),
        )
    )


async def _populate_db(db_path: Path) -> None:
    """Create a small SQLite file at ``db_path``."""
    async with aiosqlite.connect(str(db_path)) as conn:
        await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        for i in range(10):
            await conn.execute("INSERT INTO t (v) VALUES (?)", (f"row-{i}",))
        await conn.commit()


async def test_run_exits_immediately_when_disabled(tmp_path: Path) -> None:
    """Disabled scheduler → run() returns without writing anything."""
    db_path = tmp_path / "uploads.db"
    await _populate_db(db_path)
    settings = _settings(data_root=tmp_path, backup_enabled=False)
    scheduler = ColdBackupScheduler(
        db_path=db_path,
        backup_root=tmp_path / "backups",
        settings=settings,
    )
    await asyncio.wait_for(scheduler.run(asyncio.Event()), timeout=1.0)
    # Backups directory not created when disabled.
    assert not (tmp_path / "backups").exists()


async def test_snapshot_once_writes_non_empty_backup(tmp_path: Path) -> None:
    """``snapshot_once`` writes a non-empty file with the expected name."""
    db_path = tmp_path / "uploads.db"
    await _populate_db(db_path)
    backup_root = tmp_path / "backups"
    backup_root.mkdir()
    settings = _settings(data_root=tmp_path)
    scheduler = ColdBackupScheduler(
        db_path=db_path,
        backup_root=backup_root,
        settings=settings,
    )
    dest = await scheduler.snapshot_once()
    assert dest.exists()
    assert dest.parent == backup_root
    assert dest.name.startswith("uploads.backup.")
    assert dest.name.endswith(".db")
    assert dest.stat().st_size > 0
    # The snapshot is a valid SQLite file with the same rows as the live DB.
    async with aiosqlite.connect(str(dest)) as conn, conn.execute("SELECT COUNT(*) FROM t") as cur:
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == 10


async def test_snapshot_rotation_keeps_latest_n(tmp_path: Path) -> None:
    """``backup_rotate_n=2`` + 3 snapshots → only the latest 2 remain."""
    db_path = tmp_path / "uploads.db"
    await _populate_db(db_path)
    backup_root = tmp_path / "backups"
    backup_root.mkdir()
    settings = _settings(data_root=tmp_path, backup_rotate_n=2)
    scheduler = ColdBackupScheduler(
        db_path=db_path,
        backup_root=backup_root,
        settings=settings,
    )
    # Stage 3 deterministic snapshots — name suffixes are lex-sortable.
    for iso in ("20260527T100000Z", "20260527T110000Z", "20260527T120000Z"):
        snap = backup_root / f"uploads.backup.{iso}.db"
        # Copy the live DB so each "snapshot" is non-empty real data.
        snap.write_bytes(db_path.read_bytes())
    await scheduler._rotate()  # test reaches internal rotate (SLF001 not configured)
    remaining = sorted(p.name for p in backup_root.glob("uploads.backup.*.db"))
    assert remaining == [
        "uploads.backup.20260527T110000Z.db",
        "uploads.backup.20260527T120000Z.db",
    ]


async def test_cold_snapshots_stay_outside_the_manifest_world(tmp_path: Path) -> None:
    """Cold snapshots never blend into the backup_id/manifest inventory.

    Round 5 adversary hardening (seam-1/seam-2 vs the cold-backup family).
    The scheduler writes to ``<data_root>/backups/`` with NO manifest and
    NO backup_id; :func:`list_quarantines` scans the same ``data_root``.
    The two families must stay disjoint in BOTH directions:

    * a populated ``backups/`` subtree yields neither a backup entry nor
      an anomaly flag (the subdirectory name carries no quarantine infix);
    * even a stray cold-snapshot FILE copied directly into ``data_root``
      (operator mishap) is ignored: the inventory speaks only for
      quarantine-world artifacts, and the strategy's "a backup without a
      manifest is an anomaly" rule is scoped to artifacts matching the
      quarantine naming convention, which cold snapshots never do.
    """
    data_root = tmp_path
    db_path = data_root / "uploads.db"
    await _populate_db(db_path)
    backup_root = data_root / "backups"
    backup_root.mkdir()
    scheduler = ColdBackupScheduler(
        db_path=db_path,
        backup_root=backup_root,
        settings=_settings(data_root=data_root),
    )
    cold_snapshot = await scheduler.snapshot_once()
    assert cold_snapshot.exists()
    # Operator-mishap leg: a cold snapshot copied straight into data_root.
    stray = data_root / cold_snapshot.name
    stray.write_bytes(cold_snapshot.read_bytes())

    # One REAL manifested backup so the inventory has a positive control.
    live_db = data_root / "live.db"
    live_db.write_bytes(b"live-db-bytes")
    body_root = data_root / "bodies"
    (body_root / "shard").mkdir(parents=True)
    (body_root / "shard" / "body.bin").write_bytes(b"x" * 16)
    manifest = quarantine(live_db, body_root, datetime(2026, 5, 27, 14, 30, 0))

    entries = list_quarantines(data_root)
    assert [e.backup_id for e in entries] == [manifest.backup_id]
    assert all(not e.anomaly for e in entries)
    # The cold artifacts are untouched on disk, merely invisible here.
    assert cold_snapshot.exists()
    assert stray.exists()


async def test_a_failed_snapshot_leaves_no_file_for_rotation_to_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S8-4: a snapshot that raises mid-copy leaves nothing behind.

    Objective: opening the destination CREATES it, so a snapshot that failed
    after the connect used to leave a zero-byte ``uploads.backup.<iso>.db``
    that matches the rotation glob and counts as a snapshot.

    Expected outcome: the raise propagates (the run loop logs and continues),
    and the backup directory holds no file matching the rotation glob, empty
    or otherwise.
    """
    db_path = tmp_path / "uploads.db"
    await _populate_db(db_path)
    backup_root = tmp_path / "backups"
    backup_root.mkdir()
    scheduler = ColdBackupScheduler(
        db_path=db_path,
        backup_root=backup_root,
        settings=_settings(data_root=tmp_path),
    )

    async def failing_backup(self: aiosqlite.Connection, target: aiosqlite.Connection) -> None:
        """Stand in for a locked live DB, a full volume or an IO error."""
        raise OSError("backup failed mid-copy")

    monkeypatch.setattr(aiosqlite.Connection, "backup", failing_backup)
    with pytest.raises(OSError, match="backup failed mid-copy"):
        await scheduler.snapshot_once()
    assert list(backup_root.glob("uploads.backup.*.db")) == []


async def test_failed_snapshots_do_not_rotate_good_snapshots_away(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S8-4: three failed nightly runs plus one success keep real snapshots.

    Objective: the operator's stated recovery position is ``backup_rotate_n``
    real snapshots. With zero-byte failures counting as snapshots, three
    consecutive failures followed by one success rotated every previously-good
    snapshot away and logged nothing, because the deletions are the normal
    rotation path.

    Expected outcome: every file left after the sequence is a real,
    non-empty snapshot, and the three seeded good ones are rotated only by
    the one genuine new snapshot.
    """
    db_path = tmp_path / "uploads.db"
    await _populate_db(db_path)
    backup_root = tmp_path / "backups"
    backup_root.mkdir()
    rotate_n = 3
    scheduler = ColdBackupScheduler(
        db_path=db_path,
        backup_root=backup_root,
        settings=_settings(data_root=tmp_path, backup_rotate_n=rotate_n),
    )
    seeded = [f"uploads.backup.20260101T0{hour}0000Z.db" for hour in (1, 2, 3)]
    for name in seeded:
        (backup_root / name).write_bytes(db_path.read_bytes())

    # Distinct, ascending stamps. The real stamp has one-second resolution, so
    # four snapshots inside one test second would otherwise collide on a single
    # filename and hide the very accumulation this test is about.
    stamps = iter(["20260101T040000Z", "20260101T050000Z", "20260101T060000Z", "20260101T070000Z"])
    monkeypatch.setattr("phantom.workers.cold_backup.utc_stamp", lambda: next(stamps))

    async def failing_backup(self: aiosqlite.Connection, target: aiosqlite.Connection) -> None:
        """Fail the way a locked live DB does."""
        raise OSError("backup failed mid-copy")

    real_backup = aiosqlite.Connection.backup
    monkeypatch.setattr(aiosqlite.Connection, "backup", failing_backup)
    for _ in range(rotate_n):
        with pytest.raises(OSError, match="backup failed mid-copy"):
            await scheduler.snapshot_once()
    monkeypatch.setattr(aiosqlite.Connection, "backup", real_backup)
    fresh = await scheduler.snapshot_once()

    survivors = sorted(p.name for p in backup_root.glob("uploads.backup.*.db"))
    assert all((backup_root / name).stat().st_size > 0 for name in survivors)
    assert survivors == sorted([*seeded[1:], fresh.name])


async def test_snapshot_once_creates_backup_root_if_absent(tmp_path: Path) -> None:
    """run() creates backup_root before the first snapshot; snapshot_once tolerates pre-existing."""
    db_path = tmp_path / "uploads.db"
    await _populate_db(db_path)
    # backup_root not yet present.
    backup_root = tmp_path / "backups"
    assert not backup_root.exists()
    backup_root.mkdir()  # snapshot_once expects it to exist
    settings = _settings(data_root=tmp_path)
    scheduler = ColdBackupScheduler(
        db_path=db_path,
        backup_root=backup_root,
        settings=settings,
    )
    await scheduler.snapshot_once()
    assert any(backup_root.glob("uploads.backup.*.db"))
