"""Unit tests for :mod:`phantom.storage.integrity` (cycle-7 seams 1 + 2).

Coverage targets per the plan acceptance bullets:

* :func:`check_integrity` on a fresh path returns ``ok=True``; on a
  deliberately corrupted SQLite (overwrite the first 256 bytes with junk)
  returns ``ok=False``.
* :func:`quarantine_paths` / :func:`_artifact_destinations` produce the
  documented naming convention per reason (mode_switch uses
  ``.mode_switch.`` for both artifacts; same stamp -> same dests), with the
  cycle-7 identity token (``<iso>-<backup_id hex>``) after the display iso.
* :func:`quarantine` writes ONE :class:`BackupManifest` (atomic,
  BEFORE the artifact moves) and renames the live DB + body-store root; the
  ``mode_switch`` reason writes-then-clears the ``direction="backup"``
  marker while the corruption reason never writes one (regression). Two
  backups minted in the SAME wall-clock second coexist with DISTINCT
  backup_ids (seam-1 acceptance; no disambiguation machinery exists).
* :func:`isolate_db_file` produces a manifested body-less backup.
* :class:`BackupManifest` / :class:`BackupMoveMarker` JSON round-trip and
  are written atomically.
* :func:`restore_mode_switch_backup` is addressed by MANIFEST, moves the
  pair into empty live targets, deletes the consumed manifest, and clears
  the marker.
* :func:`reconcile_interrupted_backup_move` keys on the marker's
  ``backup_id``, completes BOTH directions forward from every half-state
  using the manifest's declared paths, is a no-op without a marker, and
  clears a marker whose manifest is missing.
* :func:`list_quarantines` returns ONE entry per backup (manifest-driven)
  plus flagged anomaly entries for unmanifested artifacts, and skips the
  marker + manifest files.
* :class:`IntegrityChecker` bundles the above behind a single facade.
"""

from __future__ import annotations

import re
import shutil
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import aiosqlite
import pytest
from phantom.storage.integrity import (
    BACKUP_ID_FILENAME_TOKEN_LENGTH,
    BACKUP_MOVE_MARKER_NAME,
    BackupManifest,
    BackupMoveMarker,
    IntegrityChecker,
    IntegrityCheckResult,
    _artifact_destinations,
    _parse_quarantine_iso,
    _read_backup_move_marker,
    _write_json_model_atomic,
    backup_manifest_path,
    check_integrity,
    isolate_db_file,
    list_quarantines,
    load_backup_manifest,
    quarantine,
    quarantine_paths,
    reconcile_interrupted_backup_move,
    restore_mode_switch_backup,
)

# Fixed timestamp used across naming-convention assertions so the
# test reads against a deterministic display half.
_FIXED_TS = datetime(2026, 5, 27, 14, 30, 0)
_FIXED_TS_ISO = "20260527T143000Z"
# A fixed backup identity for the PURE naming helpers (quarantine() mints its
# own uuid internally; pure-path tests pin one so dests are deterministic).
_FIXED_BACKUP_ID = UUID("12345678-1234-5678-1234-567812345678")
_FIXED_TOKEN = _FIXED_BACKUP_ID.hex[:BACKUP_ID_FILENAME_TOKEN_LENGTH]
_FIXED_STAMP = f"{_FIXED_TS_ISO}-{_FIXED_TOKEN}"

# Shape of a freshly minted artifact stamp: display iso + uuid hex token.
_STAMP_PATTERN = rf"\d{{8}}T\d{{6}}Z-[0-9a-f]{{{BACKUP_ID_FILENAME_TOKEN_LENGTH}}}"


# ---------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------


async def _create_real_sqlite(path: Path) -> None:
    """Create a small, valid SQLite file at ``path``."""
    async with aiosqlite.connect(str(path)) as conn:
        await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        await conn.execute("INSERT INTO t (v) VALUES (?)", ("hello",))
        await conn.commit()


def _corrupt_first_bytes(path: Path, n: int = 256) -> None:
    """Overwrite the first ``n`` bytes of ``path`` with junk."""
    with path.open("r+b") as fh:
        fh.write(b"\x00\xff" * (n // 2))


def _make_body_tree(root: Path, marker_byte: bytes = b"x") -> None:
    """Create a small populated body-store directory at ``root``."""
    (root / "shard").mkdir(parents=True)
    (root / "shard" / "body.bin").write_bytes(marker_byte * 16)


def _make_db_with_siblings(db_path: Path) -> None:
    """Create a DB file plus its ``-wal`` / ``-shm`` siblings."""
    db_path.write_bytes(b"sqlite-header")
    db_path.with_name(db_path.name + "-wal").write_bytes(b"wal-bytes")
    db_path.with_name(db_path.name + "-shm").write_bytes(b"shm-bytes")


def _stage_mode_switch_backup(
    tmp_path: Path, *, db_bytes: bytes = b"sqlite-header", body_byte: bytes = b"r"
) -> BackupManifest:
    """Stage a REAL manifested mode_switch backup pair via the production mover.

    Creates a live DB (with WAL/SHM siblings) + body tree under ``tmp_path``
    and runs :func:`quarantine` so the manifest, the marker discipline, and
    the artifact names are all the production article, not a hand-built
    imitation.
    """
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "bodies"
    db_path.write_bytes(db_bytes)
    db_path.with_name(db_path.name + "-wal").write_bytes(b"wal-bytes")
    db_path.with_name(db_path.name + "-shm").write_bytes(b"shm-bytes")
    _make_body_tree(body_root, marker_byte=body_byte)
    return quarantine(db_path, body_root, _FIXED_TS, reason="mode_switch")


def _hand_built_manifest(tmp_path: Path) -> BackupManifest:
    """Build (and persist) a ``mode_switch`` manifest for half-state tests.

    The reconcile tests construct crash half-states by placing artifacts at
    the manifest's DECLARED destinations by hand; the manifest itself is
    written through the same atomic writer production uses.
    """
    q_db, q_body = quarantine_paths(
        tmp_path / "uploads.db",
        tmp_path / "bodies",
        _FIXED_TS,
        backup_id=_FIXED_BACKUP_ID,
        reason="mode_switch",
    )
    manifest = BackupManifest(
        backup_id=_FIXED_BACKUP_ID,
        reason="mode_switch",
        iso_display=_FIXED_TS_ISO,
        db_path=q_db,
        body_path=q_body,
        has_db=True,
        has_body=True,
        created_at=_FIXED_TS.replace(tzinfo=UTC),
    )
    _write_json_model_atomic(backup_manifest_path(tmp_path, _FIXED_BACKUP_ID), manifest)
    return manifest


def _write_marker(data_root: Path, backup_id: UUID, direction: str) -> None:
    """Persist a backup-move marker into ``data_root``."""
    _write_json_model_atomic(
        data_root / BACKUP_MOVE_MARKER_NAME,
        BackupMoveMarker(backup_id=backup_id, direction=direction),  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------
# :func:`check_integrity`.
# ---------------------------------------------------------------------


async def test_check_integrity_returns_ok_on_missing_path(tmp_path: Path) -> None:
    """A missing DB path is a healthy fresh deployment."""
    result = await check_integrity(tmp_path / "uploads.db")
    assert isinstance(result, IntegrityCheckResult)
    assert result.ok is True
    assert "fresh" in result.message


async def test_check_integrity_returns_ok_on_valid_sqlite(tmp_path: Path) -> None:
    """A valid SQLite file passes the integrity probe."""
    db_path = tmp_path / "uploads.db"
    await _create_real_sqlite(db_path)
    result = await check_integrity(db_path)
    assert result.ok is True
    assert result.message == "ok"


async def test_check_integrity_returns_not_ok_on_corrupted_header(tmp_path: Path) -> None:
    """A SQLite file with its first 256 bytes overwritten fails."""
    db_path = tmp_path / "uploads.db"
    await _create_real_sqlite(db_path)
    _corrupt_first_bytes(db_path)
    result = await check_integrity(db_path)
    assert result.ok is False
    assert result.message


async def test_check_integrity_returns_not_ok_on_garbage_file(tmp_path: Path) -> None:
    """A non-SQLite file (random bytes) fails the integrity probe.

    Zero-byte files are deliberately not tested here — SQLite treats an
    empty file as a fresh deployment and ``integrity_check`` returns
    ``ok`` against it. The corruption probe targets SQLite-shaped-but-
    damaged files; that's the operational concern.
    """
    db_path = tmp_path / "uploads.db"
    db_path.write_bytes(b"not-a-sqlite-file" * 64)
    result = await check_integrity(db_path)
    assert result.ok is False


# ---------------------------------------------------------------------
# :func:`quarantine_paths` / :func:`_artifact_destinations` (pure naming).
# ---------------------------------------------------------------------


def test_quarantine_paths_uses_documented_naming_convention(tmp_path: Path) -> None:
    """Dests follow ``<stem>.corrupted.<iso>-<token>.db`` / ``<name>.quarantine.<iso>-<token>``."""
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "body_store"
    quarantined_db, quarantined_body = quarantine_paths(
        db_path, body_root, _FIXED_TS, backup_id=_FIXED_BACKUP_ID
    )
    assert quarantined_db == tmp_path / f"uploads.corrupted.{_FIXED_STAMP}.db"
    assert quarantined_body == tmp_path / f"body_store.quarantine.{_FIXED_STAMP}"


def test_quarantine_paths_default_timestamp_is_pure(tmp_path: Path) -> None:
    """The default-timestamp branch still produces shaped names."""
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "body_store"
    quarantined_db, quarantined_body = quarantine_paths(
        db_path, body_root, backup_id=_FIXED_BACKUP_ID
    )
    # Display half depends on now() but the shape (iso + identity token) holds.
    assert ".corrupted." in quarantined_db.name
    assert quarantined_db.name.endswith(f"-{_FIXED_TOKEN}.db")
    assert ".quarantine." in quarantined_body.name
    assert quarantined_body.name.endswith(f"-{_FIXED_TOKEN}")


def test_quarantine_paths_mode_switch_uses_one_infix_for_both(tmp_path: Path) -> None:
    """The mode_switch reason uses ``.mode_switch.`` for the DB and the body."""
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "bodies"
    q_db, q_body = quarantine_paths(
        db_path, body_root, _FIXED_TS, backup_id=_FIXED_BACKUP_ID, reason="mode_switch"
    )
    assert q_db == tmp_path / f"uploads.mode_switch.{_FIXED_STAMP}.db"
    assert q_body == tmp_path / f"bodies.mode_switch.{_FIXED_STAMP}"


def test_artifact_destinations_same_stamp_yields_same_dests(tmp_path: Path) -> None:
    """``_artifact_destinations`` is a pure function of (paths, reason, stamp)."""
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "bodies"
    first = _artifact_destinations(db_path, body_root, "mode_switch", _FIXED_STAMP)
    second = _artifact_destinations(db_path, body_root, "mode_switch", _FIXED_STAMP)
    assert first == second
    # And the corruption infixes differ from the mode_switch ones.
    corrupt = _artifact_destinations(db_path, body_root, "corrupted", _FIXED_STAMP)
    assert corrupt != first


# ---------------------------------------------------------------------
# :class:`BackupManifest` + :class:`BackupMoveMarker` (on-disk records).
# ---------------------------------------------------------------------


def test_backup_manifest_json_round_trip(tmp_path: Path) -> None:
    """The manifest JSON-round-trips with every field intact."""
    manifest = BackupManifest(
        backup_id=_FIXED_BACKUP_ID,
        reason="mode_switch",
        iso_display=_FIXED_TS_ISO,
        db_path=tmp_path / f"uploads.mode_switch.{_FIXED_STAMP}.db",
        body_path=tmp_path / f"bodies.mode_switch.{_FIXED_STAMP}",
        has_db=True,
        has_body=False,
        created_at=_FIXED_TS.replace(tzinfo=UTC),
    )
    restored = BackupManifest.model_validate_json(manifest.model_dump_json())
    assert restored == manifest
    assert restored.backup_id == _FIXED_BACKUP_ID
    assert restored.body_path == tmp_path / f"bodies.mode_switch.{_FIXED_STAMP}"


def test_backup_manifest_path_and_load_round_trip(tmp_path: Path) -> None:
    """``load_backup_manifest`` finds a manifest written at ``backup_manifest_path``."""
    manifest = _hand_built_manifest(tmp_path)
    path = backup_manifest_path(tmp_path, manifest.backup_id)
    assert path.exists()
    # No temp sibling left behind after the atomic replace.
    assert not path.with_name(path.name + ".tmp").exists()
    assert load_backup_manifest(tmp_path, manifest.backup_id) == manifest


def test_load_backup_manifest_absent_and_unreadable(tmp_path: Path) -> None:
    """A missing or unparseable manifest loads as ``None`` (treated as absent)."""
    assert load_backup_manifest(tmp_path, uuid4()) is None
    bad_id = uuid4()
    backup_manifest_path(tmp_path, bad_id).write_text("{not json", encoding="utf-8")
    assert load_backup_manifest(tmp_path, bad_id) is None


@pytest.mark.parametrize("direction", ["backup", "restore"])
def test_backup_move_marker_json_round_trip(direction: str) -> None:
    """The marker JSON-round-trips both directions, keyed by backup_id."""
    marker = BackupMoveMarker(backup_id=_FIXED_BACKUP_ID, direction=direction)  # type: ignore[arg-type]
    restored = BackupMoveMarker.model_validate_json(marker.model_dump_json())
    assert restored == marker
    assert restored.backup_id == _FIXED_BACKUP_ID
    assert restored.direction == direction


def test_backup_move_marker_atomic_write_then_read(tmp_path: Path) -> None:
    """``_write_json_model_atomic`` then ``_read_backup_move_marker`` round-trips on disk."""
    marker_path = tmp_path / BACKUP_MOVE_MARKER_NAME
    marker = BackupMoveMarker(backup_id=_FIXED_BACKUP_ID, direction="backup")
    _write_json_model_atomic(marker_path, marker)
    assert marker_path.exists()
    # No temp sibling is left behind after the atomic replace.
    assert not (tmp_path / (BACKUP_MOVE_MARKER_NAME + ".tmp")).exists()
    assert _read_backup_move_marker(marker_path) == marker


# ---------------------------------------------------------------------
# :func:`quarantine`.
# ---------------------------------------------------------------------


def test_quarantine_renames_and_manifests_the_pair(tmp_path: Path) -> None:
    """When both inputs exist, both are renamed and ONE manifest records the pair."""
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "body_store"
    db_path.write_bytes(b"placeholder")
    body_root.mkdir()
    (body_root / "shard1").mkdir()
    (body_root / "shard1" / "body1.bin").write_bytes(b"x" * 100)
    manifest = quarantine(db_path, body_root, _FIXED_TS)
    assert not db_path.exists()
    assert not body_root.exists()
    assert manifest.db_path.exists()
    assert manifest.body_path is not None
    assert manifest.body_path.exists()
    assert (manifest.body_path / "shard1" / "body1.bin").exists()
    # The display half is the pinned iso; the identity token is minted per
    # call, so the SHAPE (iso-dash-hex) is asserted, not an exact value.
    assert re.fullmatch(
        rf"body_store\.quarantine\.{_FIXED_TS_ISO}-[0-9a-f]{{{BACKUP_ID_FILENAME_TOKEN_LENGTH}}}",
        manifest.body_path.name,
    )
    assert re.fullmatch(
        rf"uploads\.corrupted\.{_FIXED_TS_ISO}-[0-9a-f]{{{BACKUP_ID_FILENAME_TOKEN_LENGTH}}}\.db",
        manifest.db_path.name,
    )
    # The manifest is on disk, keyed by the minted backup_id, and truthful.
    loaded = load_backup_manifest(tmp_path, manifest.backup_id)
    assert loaded == manifest
    assert loaded is not None
    assert loaded.reason == "corrupted"
    assert loaded.iso_display == _FIXED_TS_ISO
    assert loaded.has_db is True
    assert loaded.has_body is True


def test_quarantine_writes_manifest_before_moving(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The manifest DECLARES intent: it is on disk before the first artifact move.

    Seam-2 ordering is load-bearing: a crash between the manifest write and
    the moves leaves a declared-but-empty backup (honest inventory noise),
    while the inverse order would leave moved artifacts no manifest claims
    (anomalies). The test intercepts the move primitive and asserts the
    manifest file already exists at the first move.
    """
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "bodies"
    db_path.write_bytes(b"live")
    _make_body_tree(body_root)
    manifest_seen_at_first_move: list[bool] = []
    real_move = shutil.move

    def _spy_move(src: str, dst: str) -> str:
        if not manifest_seen_at_first_move:
            manifests = list(tmp_path.glob("backup.*.manifest.json"))
            manifest_seen_at_first_move.append(bool(manifests))
        result: str = real_move(src, dst)
        return result

    monkeypatch.setattr(shutil, "move", _spy_move)
    quarantine(db_path, body_root, _FIXED_TS)
    assert manifest_seen_at_first_move == [True], (
        "the manifest must be written BEFORE the first artifact move"
    )


def test_quarantine_same_second_backups_coexist_with_distinct_ids(tmp_path: Path) -> None:
    """Two backups minted in the SAME wall-clock second coexist (seams 1 + 2).

    The prior cycle's H-1 class: a second-granularity name collided with a
    backup taken in the same second and the clobber-safe move silently
    no-opped, stranding the live tree. With identity-token naming the two
    dest pairs differ BY CONSTRUCTION: both backups land, each has its own
    manifest and backup_id, nothing is stranded, and there is no
    disambiguation machinery left to exhaust (M-3-B). The timestamp is
    pinned so the same-second case is deterministic, not raced.
    """
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "bodies"
    db_path.write_bytes(b"first-live-db")
    _make_body_tree(body_root, marker_byte=b"1")
    first = quarantine(db_path, body_root, _FIXED_TS, reason="mode_switch")
    # Recreate the live tree and back up AGAIN at the SAME pinned second.
    db_path.write_bytes(b"second-live-db")
    _make_body_tree(body_root, marker_byte=b"2")
    second = quarantine(db_path, body_root, _FIXED_TS, reason="mode_switch")

    # Distinct identities, distinct destinations, both pairs present.
    assert first.backup_id != second.backup_id
    assert first.db_path != second.db_path
    assert first.body_path != second.body_path
    assert first.db_path.read_bytes() == b"first-live-db"
    assert second.db_path.read_bytes() == b"second-live-db"
    assert first.body_path is not None
    assert second.body_path is not None
    assert (first.body_path / "shard" / "body.bin").read_bytes() == b"1" * 16
    assert (second.body_path / "shard" / "body.bin").read_bytes() == b"2" * 16
    assert not db_path.exists()
    assert not body_root.exists()
    # Both manifests are on disk and the inventory reports one entry each.
    entries = list_quarantines(tmp_path)
    assert {e.backup_id for e in entries} == {first.backup_id, second.backup_id}
    assert all(not e.anomaly for e in entries)


def test_quarantine_moves_wal_and_shm_siblings(tmp_path: Path) -> None:
    """WAL/SHM siblings are moved alongside the DB so the artifact is self-contained."""
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "body_store"
    db_path.write_bytes(b"sqlite-header")
    db_path.with_name("uploads.db-wal").write_bytes(b"wal-bytes")
    db_path.with_name("uploads.db-shm").write_bytes(b"shm-bytes")
    body_root.mkdir()
    manifest = quarantine(db_path, body_root, _FIXED_TS)
    quarantined_db = manifest.db_path
    assert quarantined_db.with_name(quarantined_db.name + "-wal").exists()
    assert quarantined_db.with_name(quarantined_db.name + "-shm").exists()
    assert not db_path.with_name("uploads.db-wal").exists()
    assert not db_path.with_name("uploads.db-shm").exists()


def test_quarantine_tolerates_missing_inputs_and_writes_no_manifest(tmp_path: Path) -> None:
    """Missing DB + missing body root: no move, no exception, NO manifest file.

    An empty pair is not a backup; writing a manifest for nothing would
    pollute the inventory with declared-but-empty entries on every restore
    over an empty live tree.
    """
    db_path = tmp_path / "absent.db"
    body_root = tmp_path / "absent_body"
    manifest = quarantine(db_path, body_root, _FIXED_TS)
    assert manifest.has_db is False
    assert manifest.has_body is False
    assert not manifest.db_path.exists()
    assert manifest.body_path is not None
    assert not manifest.body_path.exists()
    assert load_backup_manifest(tmp_path, manifest.backup_id) is None
    assert list_quarantines(tmp_path) == []


def test_quarantine_corruption_reason_writes_no_marker(tmp_path: Path) -> None:
    """The corruption path leaves no backup marker (regression).

    The marker is the mode_switch-only re-trigger; corruption is its own
    re-trigger (the corrupt DB stays on disk), so it must never write one.
    The manifest IS still written (every backup is manifested).
    """
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "bodies"
    db_path.write_bytes(b"corrupt")
    _make_body_tree(body_root)
    manifest = quarantine(db_path, body_root, _FIXED_TS)
    assert not (tmp_path / BACKUP_MOVE_MARKER_NAME).exists()
    assert load_backup_manifest(tmp_path, manifest.backup_id) is not None


def test_quarantine_mode_switch_clears_marker_and_makes_one_stamp_pair(tmp_path: Path) -> None:
    """``reason=mode_switch`` produces the pair under one stamp and clears the marker."""
    manifest = _stage_mode_switch_backup(tmp_path)
    # Marker cleared on the clean path.
    assert not (tmp_path / BACKUP_MOVE_MARKER_NAME).exists()
    # Live tree fully drained.
    assert not (tmp_path / "uploads.db").exists()
    assert not (tmp_path / "bodies").exists()
    # The pair shares ONE stamp (same iso AND same identity token) and the
    # mode_switch infix; the token itself is minted per call.
    q_db, q_body = manifest.db_path, manifest.body_path
    assert q_body is not None
    assert re.fullmatch(rf"uploads\.mode_switch\.{_STAMP_PATTERN}\.db", q_db.name)
    assert re.fullmatch(rf"bodies\.mode_switch\.{_STAMP_PATTERN}", q_body.name)
    db_stamp = q_db.name.removeprefix("uploads.mode_switch.").removesuffix(".db")
    body_stamp = q_body.name.removeprefix("bodies.mode_switch.")
    assert db_stamp == body_stamp
    assert db_stamp.startswith(f"{_FIXED_TS_ISO}-")
    assert q_db.exists()
    assert q_body.exists()
    # WAL/SHM siblings travelled with the DB.
    assert q_db.with_name(q_db.name + "-wal").exists()
    assert q_db.with_name(q_db.name + "-shm").exists()


# ---------------------------------------------------------------------
# :func:`isolate_db_file` (the body-less backup).
# ---------------------------------------------------------------------


def test_isolate_db_file_is_a_manifested_body_less_backup(tmp_path: Path) -> None:
    """The token-cache isolate writes a body-less manifest and moves the DB."""
    db_path = tmp_path / "token_cache.db"
    _make_db_with_siblings(db_path)
    manifest = isolate_db_file(db_path, timestamp=_FIXED_TS)
    assert manifest.reason == "corrupted"
    assert manifest.body_path is None
    assert manifest.has_db is True
    assert manifest.has_body is False
    assert manifest.db_path.exists()
    assert manifest.db_path.with_name(manifest.db_path.name + "-wal").exists()
    assert not db_path.exists()
    assert load_backup_manifest(tmp_path, manifest.backup_id) == manifest
    # It surfaces as ONE backup entry, never an anomaly.
    entries = list_quarantines(tmp_path)
    assert len(entries) == 1
    assert entries[0].backup_id == manifest.backup_id
    assert entries[0].anomaly is False
    assert entries[0].body_path is None


def test_isolate_db_file_missing_source_writes_nothing(tmp_path: Path) -> None:
    """A missing source is a tolerated no-op with no manifest file."""
    manifest = isolate_db_file(tmp_path / "absent.db", timestamp=_FIXED_TS)
    assert manifest.has_db is False
    assert load_backup_manifest(tmp_path, manifest.backup_id) is None
    assert list_quarantines(tmp_path) == []


# ---------------------------------------------------------------------
# :func:`restore_mode_switch_backup`.
# ---------------------------------------------------------------------


def test_restore_mode_switch_backup_moves_pair_into_empty_live(tmp_path: Path) -> None:
    """Restore moves the manifested pair into empty live targets, consuming it."""
    manifest = _stage_mode_switch_backup(tmp_path, body_byte=b"r")
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "bodies"
    outcome = restore_mode_switch_backup(db_path, body_root, manifest)
    assert outcome.db_path == db_path
    assert outcome.body_path == body_root
    # Both halves actually moved into the empty live tree (the H-1 / L-2
    # no-op signal: db_moved/body_moved are True on a real restore).
    assert outcome.db_moved is True
    assert outcome.body_moved is True
    # The backup pair moved into the live tree.
    assert db_path.exists()
    assert (body_root / "shard" / "body.bin").read_bytes() == b"r" * 16
    assert not manifest.db_path.exists()
    assert manifest.body_path is not None
    assert not manifest.body_path.exists()
    # Siblings travelled; marker cleared; the CONSUMED manifest is deleted.
    assert db_path.with_name(db_path.name + "-wal").exists()
    assert not (tmp_path / BACKUP_MOVE_MARKER_NAME).exists()
    assert load_backup_manifest(tmp_path, manifest.backup_id) is None
    assert list_quarantines(tmp_path) == []


def test_restore_noop_keeps_manifest_when_artifacts_remain(tmp_path: Path) -> None:
    """A clobber-blocked restore keeps the manifest (the backup still exists).

    The live targets are occupied, so the clobber-safe moves no-op
    (``db_moved=False``, the route's fail-loud signal) and the backup's
    artifacts stay at their backup locations: deleting the manifest then
    would decay a real backup into anomalies.
    """
    manifest = _stage_mode_switch_backup(tmp_path)
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "bodies"
    # Re-occupy the live tree so the restore moves are clobber-blocked.
    db_path.write_bytes(b"fresh-live")
    _make_body_tree(body_root, marker_byte=b"f")
    outcome = restore_mode_switch_backup(db_path, body_root, manifest)
    assert outcome.db_moved is False
    assert outcome.body_moved is False
    assert manifest.db_path.exists()
    assert load_backup_manifest(tmp_path, manifest.backup_id) == manifest
    assert not (tmp_path / BACKUP_MOVE_MARKER_NAME).exists()


# ---------------------------------------------------------------------
# :func:`reconcile_interrupted_backup_move`.
# ---------------------------------------------------------------------


def test_reconcile_no_marker_is_noop_returning_none(tmp_path: Path) -> None:
    """Without a marker, reconciliation is a no-op returning ``None``."""
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "bodies"
    db_path.write_bytes(b"live")
    _make_body_tree(body_root)
    assert reconcile_interrupted_backup_move(db_path, body_root) is None
    # Live tree untouched.
    assert db_path.exists()
    assert body_root.exists()


@pytest.mark.parametrize("half", ["body_moved_db_not", "db_moved_body_not", "neither", "both"])
def test_reconcile_backup_direction_completes_from_every_half_state(
    tmp_path: Path, half: str
) -> None:
    """A ``direction="backup"`` marker completes to a whole backup + clean live tree.

    The four crash half-states (body moved / DB moved / neither / both) all
    finish forward idempotently, keyed on the marker's backup_id and the
    manifest's DECLARED paths: live tree empty, the pair whole, marker
    cleared, return value ``"backup"``.
    """
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "bodies"
    manifest = _hand_built_manifest(tmp_path)
    q_db, q_body = manifest.db_path, manifest.body_path
    assert q_body is not None
    # Set up the requested half-state (backup = live -> quarantine).
    if half in ("body_moved_db_not", "both"):
        _make_body_tree(q_body)
    else:
        _make_body_tree(body_root)
    if half in ("db_moved_body_not", "both"):
        _make_db_with_siblings(q_db)
    else:
        _make_db_with_siblings(db_path)
    _write_marker(tmp_path, manifest.backup_id, "backup")

    result = reconcile_interrupted_backup_move(db_path, body_root)

    assert result == "backup"
    # Live tree fully drained, backup pair whole, marker cleared, manifest kept.
    assert not db_path.exists()
    assert not body_root.exists()
    assert q_db.exists()
    assert q_body.exists()
    assert q_db.with_name(q_db.name + "-wal").exists()
    assert q_db.with_name(q_db.name + "-shm").exists()
    assert not (tmp_path / BACKUP_MOVE_MARKER_NAME).exists()
    assert load_backup_manifest(tmp_path, manifest.backup_id) == manifest


@pytest.mark.parametrize("half", ["body_moved_db_not", "db_moved_body_not", "neither", "both"])
def test_reconcile_restore_direction_completes_from_every_half_state(
    tmp_path: Path, half: str
) -> None:
    """A ``direction="restore"`` marker completes the restore into the live tree.

    The consumed backup's manifest is deleted at the end, exactly as the
    uninterrupted restore does.
    """
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "bodies"
    manifest = _hand_built_manifest(tmp_path)
    q_db, q_body = manifest.db_path, manifest.body_path
    assert q_body is not None
    # Set up the requested half-state (restore = quarantine -> live).
    if half in ("body_moved_db_not", "both"):
        _make_body_tree(body_root)
    else:
        _make_body_tree(q_body)
    if half in ("db_moved_body_not", "both"):
        _make_db_with_siblings(db_path)
    else:
        _make_db_with_siblings(q_db)
    _write_marker(tmp_path, manifest.backup_id, "restore")

    result = reconcile_interrupted_backup_move(db_path, body_root)

    assert result == "restore"
    # The backup pair is now in the live tree; the quarantine slots are empty.
    assert db_path.exists()
    assert body_root.exists()
    assert not q_db.exists()
    assert not q_body.exists()
    assert db_path.with_name(db_path.name + "-wal").exists()
    assert not (tmp_path / BACKUP_MOVE_MARKER_NAME).exists()
    # Consumed: the manifest is gone, so the inventory shows no phantom backup.
    assert load_backup_manifest(tmp_path, manifest.backup_id) is None


def test_reconcile_sweeps_stray_wal_after_main_db_already_moved(tmp_path: Path) -> None:
    """A crash after the main-DB rename but mid-sibling rename still sweeps the WAL (m-1).

    Backup direction, DB main file already at its dest but a ``-wal`` sibling
    still at the live source: reconciliation must move the straggler.
    """
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "bodies"
    manifest = _hand_built_manifest(tmp_path)
    q_db, q_body = manifest.db_path, manifest.body_path
    assert q_body is not None
    # Main DB + body already moved to the backup; a -wal straggler remains live.
    q_db.write_bytes(b"sqlite-header")
    _make_body_tree(q_body)
    db_path.with_name(db_path.name + "-wal").write_bytes(b"stray-wal")
    _write_marker(tmp_path, manifest.backup_id, "backup")

    result = reconcile_interrupted_backup_move(db_path, body_root)

    assert result == "backup"
    # The straggling WAL was swept to the backup dest; live source is gone.
    assert q_db.with_name(q_db.name + "-wal").read_bytes() == b"stray-wal"
    assert not db_path.with_name(db_path.name + "-wal").exists()
    assert not (tmp_path / BACKUP_MOVE_MARKER_NAME).exists()


def test_reconcile_completed_move_just_clears_marker(tmp_path: Path) -> None:
    """A completed-but-marker-not-cleared crash moves nothing and clears the marker."""
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "bodies"
    manifest = _hand_built_manifest(tmp_path)
    q_db, q_body = manifest.db_path, manifest.body_path
    assert q_body is not None
    q_db.write_bytes(b"done")
    _make_body_tree(q_body)
    body_before = (q_body / "shard" / "body.bin").read_bytes()
    _write_marker(tmp_path, manifest.backup_id, "backup")

    result = reconcile_interrupted_backup_move(db_path, body_root)

    assert result == "backup"
    assert q_db.read_bytes() == b"done"
    assert (q_body / "shard" / "body.bin").read_bytes() == body_before
    assert not db_path.exists()
    assert not (tmp_path / BACKUP_MOVE_MARKER_NAME).exists()


def test_reconcile_marker_without_manifest_clears_and_moves_nothing(tmp_path: Path) -> None:
    """A marker naming a manifest-less backup_id is cleared without any move.

    The leg exists for a restore that finished through the manifest delete
    but crashed before the marker clear (and for operator tampering): the
    next boot must not wedge on it, and must not invent moves it cannot
    ground in a declared-path manifest.
    """
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "bodies"
    db_path.write_bytes(b"live")
    _make_body_tree(body_root)
    _write_marker(tmp_path, uuid4(), "restore")

    result = reconcile_interrupted_backup_move(db_path, body_root)

    assert result is None
    assert not (tmp_path / BACKUP_MOVE_MARKER_NAME).exists()
    # Live tree untouched.
    assert db_path.read_bytes() == b"live"
    assert body_root.exists()


# ---------------------------------------------------------------------
# :func:`list_quarantines` (manifest-driven inventory + anomalies).
# ---------------------------------------------------------------------


def test_list_quarantines_returns_empty_on_missing_dir(tmp_path: Path) -> None:
    """Missing data_root yields an empty list."""
    assert list_quarantines(tmp_path / "absent") == []


def test_list_quarantines_returns_empty_on_clean_dir(tmp_path: Path) -> None:
    """data_root with no backups or quarantine-shaped artifacts yields an empty list."""
    (tmp_path / "uploads.db").write_bytes(b"live")
    (tmp_path / "body_store").mkdir()
    assert list_quarantines(tmp_path) == []


def test_list_quarantines_one_entry_per_backup(tmp_path: Path) -> None:
    """A backup PAIR (db + body) surfaces as ONE entry keyed by backup_id."""
    manifest = _stage_mode_switch_backup(tmp_path)
    entries = list_quarantines(tmp_path)
    assert len(entries) == 1
    entry = entries[0]
    assert entry.backup_id == manifest.backup_id
    assert entry.anomaly is False
    assert entry.reason == "mode_switch"
    assert entry.iso_display == _FIXED_TS_ISO
    assert entry.db_path == manifest.db_path
    assert entry.body_path == manifest.body_path
    assert entry.has_db is True
    assert entry.has_body is True
    # Bytes = db file size + recursive body sum (the staged body is 16 bytes).
    assert entry.bytes == len(b"sqlite-header") + 16


def test_list_quarantines_reports_disk_truth_for_missing_halves(tmp_path: Path) -> None:
    """``has_db`` / ``has_body`` report CURRENT disk presence, not stale intent."""
    manifest = _stage_mode_switch_backup(tmp_path)
    assert manifest.body_path is not None
    # Operator removed the body half out-of-band.
    shutil.rmtree(manifest.body_path)
    entries = list_quarantines(tmp_path)
    assert len(entries) == 1
    assert entries[0].has_db is True
    assert entries[0].has_body is False
    assert entries[0].bytes == len(b"sqlite-header")


def test_list_quarantines_flags_unmanifested_artifacts_as_anomalies(tmp_path: Path) -> None:
    """Quarantine-shaped artifacts no manifest claims surface as flagged anomalies.

    R5-P unrepresentable: a loose body-only (or db-only) artifact is never a
    restorable backup; it has no backup_id and carries ``anomaly=True``.
    """
    # A manifested backup AND two loose artifacts (no manifest).
    manifest = _stage_mode_switch_backup(tmp_path)
    loose_body = tmp_path / f"bodies.mode_switch.{_FIXED_TS_ISO}-deadbeef"
    loose_body.mkdir()
    (loose_body / "b.bin").write_bytes(b"y" * 20)
    loose_db = tmp_path / f"uploads.corrupted.{_FIXED_TS_ISO}-cafef00d.db"
    loose_db.write_bytes(b"c-db")

    entries = list_quarantines(tmp_path)
    backups = [e for e in entries if not e.anomaly]
    anomalies = [e for e in entries if e.anomaly]
    assert len(backups) == 1
    assert backups[0].backup_id == manifest.backup_id
    assert len(anomalies) == 2
    assert all(a.backup_id is None for a in anomalies)
    db_anomaly = next(a for a in anomalies if a.db_path is not None)
    body_anomaly = next(a for a in anomalies if a.body_path is not None)
    assert db_anomaly.reason == "corrupted"
    assert db_anomaly.has_db is True
    assert db_anomaly.has_body is False
    assert db_anomaly.bytes == len(b"c-db")
    assert db_anomaly.iso_display == f"{_FIXED_TS_ISO}-cafef00d"
    assert body_anomaly.reason == "mode_switch"
    assert body_anomaly.has_body is True
    assert body_anomaly.has_db is False
    assert body_anomaly.bytes == 20


def test_list_quarantines_skips_marker_and_manifest_files(tmp_path: Path) -> None:
    """The in-progress marker and the manifest files are never artifacts."""
    manifest = _stage_mode_switch_backup(tmp_path)
    _write_marker(tmp_path, manifest.backup_id, "backup")
    entries = list_quarantines(tmp_path)
    assert len(entries) == 1
    assert entries[0].backup_id == manifest.backup_id


def test_parse_quarantine_iso_is_display_only_inverse() -> None:
    """``_parse_quarantine_iso`` recovers the display stamp for anomaly entries."""
    assert (
        _parse_quarantine_iso(f"uploads.corrupted.{_FIXED_STAMP}.db", "db", "corrupted")
        == _FIXED_STAMP
    )
    assert (
        _parse_quarantine_iso(f"bodies.quarantine.{_FIXED_STAMP}", "body_store", "corrupted")
        == _FIXED_STAMP
    )
    assert (
        _parse_quarantine_iso(f"uploads.mode_switch.{_FIXED_STAMP}.db", "db", "mode_switch")
        == _FIXED_STAMP
    )
    # A name lacking the expected infix yields the empty string (defensive).
    assert _parse_quarantine_iso("not-a-quarantine.db", "db", "corrupted") == ""


# ---------------------------------------------------------------------
# :class:`IntegrityChecker` facade.
# ---------------------------------------------------------------------


async def test_integrity_checker_check_delegates_to_check_integrity(tmp_path: Path) -> None:
    """:meth:`IntegrityChecker.check` runs the same probe as the function."""
    db_path = tmp_path / "uploads.db"
    await _create_real_sqlite(db_path)
    body_root = tmp_path / "body_store"
    body_root.mkdir()
    checker = IntegrityChecker(
        db_path=db_path,
        body_store_root=body_root,
        data_root=tmp_path,
    )
    result = await checker.check()
    assert result.ok is True
    assert result.message == "ok"


async def test_integrity_checker_check_flags_corruption(tmp_path: Path) -> None:
    """Corruption surfaces through the facade."""
    db_path = tmp_path / "uploads.db"
    await _create_real_sqlite(db_path)
    _corrupt_first_bytes(db_path)
    checker = IntegrityChecker(
        db_path=db_path,
        body_store_root=tmp_path / "body_store",
        data_root=tmp_path,
    )
    result = await checker.check()
    assert result.ok is False


def test_integrity_checker_quarantine_now_and_list(tmp_path: Path) -> None:
    """:meth:`quarantine_now` returns the manifest; :meth:`list_quarantines` reports it."""
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "body_store"
    db_path.write_bytes(b"placeholder")
    body_root.mkdir()
    (body_root / "x.bin").write_bytes(b"y" * 8)
    checker = IntegrityChecker(
        db_path=db_path,
        body_store_root=body_root,
        data_root=tmp_path,
    )
    manifest = checker.quarantine_now(_FIXED_TS)
    entries = checker.list_quarantines()
    assert [e.backup_id for e in entries] == [manifest.backup_id]
    assert entries[0].db_path == manifest.db_path
    assert entries[0].body_path == manifest.body_path


@pytest.mark.parametrize("missing", ["db", "body"])
def test_integrity_checker_quarantine_now_tolerates_missing(tmp_path: Path, missing: str) -> None:
    """quarantine_now() is a no-op for whichever artifact is absent."""
    db_path = tmp_path / "uploads.db"
    body_root = tmp_path / "body_store"
    if missing != "db":
        db_path.write_bytes(b"x")
    if missing != "body":
        body_root.mkdir()
    checker = IntegrityChecker(
        db_path=db_path,
        body_store_root=body_root,
        data_root=tmp_path,
    )
    manifest = checker.quarantine_now(_FIXED_TS)
    assert manifest.has_db == (missing != "db")
    assert manifest.has_body == (missing != "body")
