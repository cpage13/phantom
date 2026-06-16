"""Backup identity round-trips end to end; a same-second flood coexists (re-attack of M-3-B).

Formerly ``test_restore_widened_uuid_iso_round_trip.py``: the prior cycle's
M-3-B fix made the disambiguation search widen its namespace with a uuid
token when the monotonic suffix range was exhausted, and these legs proved
the widened token round-tripped the inventory and the restore route.
Cycle-7 seam 1 made the uuid THE identity (``backup_id``) and deleted the
search outright, so there is no bound left to exhaust and no token to
widen. The re-target proves the same properties as make-it-impossible
coverage:

* :func:`test_same_second_backup_flood_all_coexist_and_a_chosen_one_restores` -
  a FLOOD of backups minted in ONE pinned wall-clock second (the old
  exhaustion premise, well past the old monotonic comfort zone) all coexist
  on disk with distinct identities and one manifest each; a chosen one
  restores correctly through the real route. Nothing is ever stranded
  because nothing can collide.

* :func:`test_inventory_backup_id_round_trips_the_restore_route` - the
  identity surfaced by ``GET /v1/admin/quarantine`` is the EXACT handle
  ``POST /v1/admin/quarantine/restore?backup_id=...`` accepts: no caller
  ever parses a filename or reconstructs a token.

Public e2e-light lane (plan § 5.0): generic sentinel rows, no
``PHANTOM_ENABLED``. Drives the real routes over the in-process stack via
the SDK.
"""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from uuid import uuid4

import pytest
from phantom.storage import integrity as integrity_mod
from phantom.storage.integrity import BackupManifest, quarantine
from phantom_client import PhantomConflictError, PhantomNotFoundError
from phantom_client.models.admin import QuarantineRestoreResponse

from tests.e2e.helpers.stack import E2EStack, boot_stack

pytestmark = [pytest.mark.asyncio]

# A FIXED iso every backup in these tests derives (utc_stamp pinned), so the
# same-second flood is deterministic, not raced.
_PINNED_ISO = "20260101T000000Z"

# Flood size for the coexistence leg. The OLD machinery special-cased the
# first few same-second collisions (readable monotonic suffixes) and only
# widened past a bound, so a single-digit flood already crosses where its
# behavior used to CHANGE; eight keeps the leg cheap while being well past
# any "first collision" special case (there are none left).
_SAME_SECOND_FLOOD_COUNT = 8

_LIVE_TAG = "FRESH-LIVE-SENTINEL"


def _instance_root(stack: E2EStack) -> Path:
    """Return the per-instance data root (where uploads.db + flat backups live)."""
    return stack.data_dir / stack.settings.instances[0].data_dir


def _write_min_uploads_db(path: Path, tag: str) -> None:
    """Write a minimal SQLite ``uploads`` table carrying one tagged sentinel row.

    Shape-minimal on purpose: these tests assert WHICH file ends up at a
    given path (by reading back the sentinel ``tag``). Any pre-existing file
    at ``path`` plus its ``-wal``/``-shm`` siblings is removed first.

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


def _stage_backup(inst_root: Path, *, tag: str) -> BackupManifest:
    """Stage one manifested ``mode_switch`` backup through the production mover.

    Args:
        inst_root: The per-instance data root.
        tag: The sentinel row written into the backup DB.

    Returns:
        The backup's manifest (its ``backup_id`` is the restore handle).
    """
    db_path = inst_root / "uploads.db"
    bodies = inst_root / "bodies"
    _write_min_uploads_db(db_path, tag)
    (bodies / "shard").mkdir(parents=True, exist_ok=True)
    (bodies / "shard" / "precious.bin").write_bytes(tag.encode())
    return quarantine(db_path, bodies, reason="mode_switch")


async def test_same_second_backup_flood_all_coexist_and_a_chosen_one_restores(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A flood of same-second backups all coexist; a chosen one restores.

    The old M-3-B attack exhausted the monotonic disambiguation range so the
    next same-second backup silently stranded the live tree. With identity
    naming there is no range: every backup in the flood lands (its DB row is
    intact at its own artifact path), the inventory lists one entry per
    backup, and restoring a middle one lands exactly its row.
    """
    monkeypatch.setattr(integrity_mod, "utc_stamp", lambda *a, **k: _PINNED_ISO)
    stack = await boot_stack(
        tmp_path=tmp_path, config_overrides={"storage": {"body_store": {"mode": "all_disk"}}}
    )
    try:
        inst_root = _instance_root(stack)
        db_path = inst_root / "uploads.db"

        manifests = [
            _stage_backup(inst_root, tag=f"FLOOD-ROW-{i}") for i in range(_SAME_SECOND_FLOOD_COUNT)
        ]
        # Every backup landed: distinct ids, distinct paths, intact rows.
        assert len({m.backup_id for m in manifests}) == _SAME_SECOND_FLOOD_COUNT
        assert len({m.db_path for m in manifests}) == _SAME_SECOND_FLOOD_COUNT
        for i, manifest in enumerate(manifests):
            assert _read_tag(manifest.db_path) == f"FLOOD-ROW-{i}", (
                f"backup {i} must be intact at its own artifact path (nothing stranded)"
            )
            assert manifest.iso_display == _PINNED_ISO

        # The inventory reports one entry per backup, none anomalous.
        inv = await stack.phantom_client.get_quarantine_inventory(instance="primary")
        ids = {e.backup_id for e in inv.quarantines if e.reason == "mode_switch"}
        assert ids == {m.backup_id for m in manifests}
        assert all(not e.anomaly for e in inv.quarantines)

        # A fresh live tree, then restore a MIDDLE flood member by identity.
        _write_min_uploads_db(db_path, _LIVE_TAG)
        (inst_root / "bodies").mkdir(exist_ok=True)
        chosen = manifests[_SAME_SECOND_FLOOD_COUNT // 2]
        restore = await stack.phantom_client.restore_quarantine_backup(
            backup_id=chosen.backup_id, instance="primary"
        )
        assert restore.restart_required is True
        expected_tag = f"FLOOD-ROW-{_SAME_SECOND_FLOOD_COUNT // 2}"
        assert _read_tag(db_path) == expected_tag, (
            f"the chosen flood member must restore; live row is {_read_tag(db_path)!r}"
        )
        # The other flood members are untouched.
        for i, manifest in enumerate(manifests):
            if manifest.backup_id == chosen.backup_id:
                continue
            assert _read_tag(manifest.db_path) == f"FLOOD-ROW-{i}"
    finally:
        await stack.tear_down()


async def test_inventory_backup_id_round_trips_the_restore_route(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The inventory's ``backup_id`` is the exact restore handle (no parsing).

    Stage one manifested backup, read its identity OFF THE INVENTORY (never
    off a filename), restore by that identity, and assert the backup's row +
    body land at the live paths and the consumed backup leaves the
    inventory.
    """
    monkeypatch.setattr(integrity_mod, "utc_stamp", lambda *a, **k: _PINNED_ISO)
    stack = await boot_stack(
        tmp_path=tmp_path, config_overrides={"storage": {"body_store": {"mode": "all_disk"}}}
    )
    try:
        inst_root = _instance_root(stack)
        db_path = inst_root / "uploads.db"
        staged = _stage_backup(inst_root, tag="ROUND-TRIP-ROW")
        _write_min_uploads_db(db_path, _LIVE_TAG)
        (inst_root / "bodies").mkdir(exist_ok=True)

        inv = await stack.phantom_client.get_quarantine_inventory(instance="primary")
        mode_switch = [e for e in inv.quarantines if e.reason == "mode_switch"]
        assert len(mode_switch) == 1
        backup_id = mode_switch[0].backup_id
        assert backup_id == staged.backup_id
        assert backup_id is not None

        restore = await stack.phantom_client.restore_quarantine_backup(
            backup_id=backup_id, instance="primary"
        )
        assert restore.restart_required is True
        assert _read_tag(db_path) == "ROUND-TRIP-ROW"
        assert (inst_root / "bodies" / "shard" / "precious.bin").read_bytes() == b"ROUND-TRIP-ROW"
        # Consumed: the restored backup no longer appears (only the interim
        # backup of the displaced fresh sentinel remains).
        inv_after = await stack.phantom_client.get_quarantine_inventory(instance="primary")
        remaining = {e.backup_id for e in inv_after.quarantines}
        assert backup_id not in remaining
    finally:
        await stack.tear_down()


async def test_concurrent_restores_of_the_same_backup_id_refuse_the_loser(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two simultaneous restores of ONE backup_id: one wins, one refuses cleanly.

    Round 2 adversary seed. The restore route's critical section runs
    synchronously on the event loop (manifest load, up-front DB check,
    interim quarantine, move), so two coroutines racing the same handle
    serialize; the loser must surface a CLEAN typed refusal (the 404 of
    a consumed manifest or the 409 ``restore_noop``), never a stack
    trace or a success-shaped lie, and the winner's restored tree plus
    the inventory must be exactly the single-restore outcome.
    """
    monkeypatch.setattr(integrity_mod, "utc_stamp", lambda *a, **k: _PINNED_ISO)
    stack = await boot_stack(
        tmp_path=tmp_path, config_overrides={"storage": {"body_store": {"mode": "all_disk"}}}
    )
    try:
        inst_root = _instance_root(stack)
        db_path = inst_root / "uploads.db"
        staged = _stage_backup(inst_root, tag="RACE-ROW")
        _write_min_uploads_db(db_path, _LIVE_TAG)
        (inst_root / "bodies").mkdir(exist_ok=True)
        assert staged.backup_id is not None

        first, second = await asyncio.gather(
            stack.phantom_client.restore_quarantine_backup(
                backup_id=staged.backup_id, instance="primary"
            ),
            stack.phantom_client.restore_quarantine_backup(
                backup_id=staged.backup_id, instance="primary"
            ),
            return_exceptions=True,
        )
        outcomes = [first, second]
        winners = [o for o in outcomes if isinstance(o, QuarantineRestoreResponse)]
        losers = [o for o in outcomes if not isinstance(o, QuarantineRestoreResponse)]
        assert len(winners) == 1, f"exactly one restore must win; outcomes={outcomes!r}"
        assert winners[0].restart_required is True
        assert len(losers) == 1
        assert isinstance(losers[0], (PhantomNotFoundError, PhantomConflictError)), (
            "the losing restore must refuse with the typed 404 (consumed "
            f"manifest) or 409 (restore_noop); got {losers[0]!r}"
        )

        # The winner's outcome is exactly the single-restore outcome.
        assert _read_tag(db_path) == "RACE-ROW"
        inv_after = await stack.phantom_client.get_quarantine_inventory(instance="primary")
        remaining = {e.backup_id for e in inv_after.quarantines}
        assert staged.backup_id not in remaining, "the consumed backup must leave the inventory"
        assert all(not e.anomaly for e in inv_after.quarantines), (
            "the losing restore must not strand a half-moved artifact as an anomaly"
        )
    finally:
        await stack.tear_down()
