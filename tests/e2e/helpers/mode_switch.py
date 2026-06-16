"""E2E helpers for the back-up-and-run mode-switch path (plan § 5 Part 5.A).

Two reusable capabilities the mode-switch coverage (and the § 5.C chaos /
§ 5.D admin parts that build on it) need:

* :func:`restart_phantom_on_data_dir` - the "restart-with-overrides on the
  same ``data_dir``" convenience. The subprocess harness already exposes
  :func:`write_phantom_config` + :class:`PhantomSubprocess`; this folds the
  config-write + make + start dance into one call so a mode swap is a single
  line. A mode switch is exactly "stop the process, restart it on the SAME
  ``data_dir`` with a different ``body_store.mode``", which the production
  boot path then reacts to (safe modes do nothing; an ``all_ram`` switch over
  a populated tree backs up and runs).

* :func:`halt_mode_switch_backup_after` - the halt-by-omission technique
  for the marked backup mover. It reproduces the EXACT ordered move sequence
  :func:`phantom.storage.integrity.quarantine` performs for a
  ``mode_switch`` backup (write manifest -> write marker -> move body ->
  move db -> clear marker; cycle-7 seam 2) against a real on-disk
  per-instance tree, and STOPS after a chosen
  step, leaving a genuinely half-finished backup on disk. The next real boot
  runs :func:`phantom.storage.integrity.reconcile_interrupted_backup_move`,
  which must finish the move forward (keyed on the marker's ``backup_id``,
  grounded in the manifest's declared paths) and let the service boot. Built
  on the PUBLIC integrity surface only (``quarantine_paths`` for the exact
  dest names, the ``BackupManifest`` + ``BackupMoveMarker`` models,
  ``backup_manifest_path``, and ``BACKUP_MOVE_MARKER_NAME``), so it never
  couples to private movers.
"""

from __future__ import annotations

import enum
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

from phantom.storage.integrity import (
    BACKUP_MOVE_MARKER_NAME,
    BackupManifest,
    BackupMoveMarker,
    backup_manifest_path,
    quarantine_paths,
)
from phantom.storage.timestamps import utc_stamp

from tests.e2e._harness.subprocess_harness import (
    DEFAULT_INSTANCE,
    PhantomSubprocess,
    allocate_port,
    db_path_for,
    instance_dir,
    write_phantom_config,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


async def restart_phantom_on_data_dir(
    data_dir: Path,
    *,
    config_overrides: Mapping[str, Any] | None = None,
) -> PhantomSubprocess:
    """Boot a fresh Phantom subprocess on ``data_dir`` with ``config_overrides``.

    The restart-with-overrides primitive (plan § 5.1): allocate a new port,
    rewrite the pinned e2e config onto ``data_dir`` with the overlay (e.g. a
    new ``body_store.mode``), and start the process, returning the running
    handle once ``/v1/healthz`` answers. Used for every mode swap: the
    caller stops the prior process, then calls this to bring the service back
    on the SAME ``data_dir`` under the new mode so the production boot path
    reacts to the switch.

    The caller owns the returned handle's lifecycle (``terminate()`` in
    teardown).

    Args:
        data_dir: The shared on-disk ``storage.data_dir`` to boot over.
        config_overrides: Deep-merged into the pinned config before boot
            (typically ``{"storage": {"body_store": {"mode": ...}}}``).

    Returns:
        A started :class:`PhantomSubprocess` answering on a fresh port.
    """
    port = allocate_port()
    cfg = write_phantom_config(
        data_dir=data_dir,
        bind_port=port,
        config_overrides=dict(config_overrides) if config_overrides is not None else None,
    )
    proc = PhantomSubprocess.make(cfg, port)
    await proc.start()
    return proc


class BackupHaltStep(enum.Enum):
    """Where to halt the hand-driven ``mode_switch`` backup move sequence.

    The steps mirror :func:`phantom.storage.integrity.quarantine`'s ordered
    move for a ``mode_switch`` backup (cycle-7 seam 2: manifest first, then
    marker, then the moves). Halting after each leaves a distinct partial
    state the reconciler must finish forward:

    * :attr:`MARKER_ONLY` - manifest + marker written, nothing moved yet.
    * :attr:`BODY_MOVED` - manifest + marker + body tree moved, DB still live.
    * :attr:`DB_MOVED` - manifest + marker + both halves moved, marker not
      yet cleared.
    """

    MARKER_ONLY = "marker_only"
    BODY_MOVED = "body_moved"
    DB_MOVED = "db_moved"


@dataclass(frozen=True)
class BackupHaltState:
    """The on-disk state left by a halted ``mode_switch`` backup move.

    Attributes:
        backup_id: The pinned backup identity (keys the manifest, the
            marker, and the admin restore route).
        manifest: The backup's :class:`BackupManifest` (written to disk
            before the halt, exactly as production declares intent).
        live_db: The live DB path (present iff the halt preceded the DB move).
        live_bodies: The live body-store root (present iff the halt preceded
            the body move).
        quarantined_db: The DB backup destination
            (``uploads.mode_switch.<stamp>.db``).
        quarantined_bodies: The body backup destination
            (``bodies.mode_switch.<stamp>``).
        marker_path: The in-progress marker file path (present while halted).
    """

    backup_id: UUID
    manifest: BackupManifest
    live_db: Path
    live_bodies: Path
    quarantined_db: Path
    quarantined_bodies: Path
    marker_path: Path


# A pinned, clearly-historical backup timestamp so the dest names are
# deterministic across runs (no clock read in the halt driver).
_PINNED_BACKUP_MOMENT = datetime(2020, 1, 1, tzinfo=UTC)

# A pinned backup identity so the halt driver's dest names (which carry the
# backup_id-derived uniqueness token, cycle-7 seam 1) are deterministic too.
_PINNED_BACKUP_ID = UUID("0a1b2c3d-0000-4000-8000-00000000c0de")


def halt_mode_switch_backup_after(
    data_dir: Path,
    *,
    stop_after: BackupHaltStep,
    instance_id: str = DEFAULT_INSTANCE,
    moment: datetime = _PINNED_BACKUP_MOMENT,
) -> BackupHaltState:
    """Drive a ``mode_switch`` backup by hand and stop after ``stop_after``.

    Reproduces the EXACT ordered sequence
    :func:`phantom.storage.integrity.quarantine` performs for a
    ``mode_switch`` backup over the per-instance tree under ``data_dir``
    (cycle-7 seam 2):

    1. write the :class:`BackupManifest` (declares intent: the backup's
       identity + both declared artifact destinations);
    2. write the :class:`BackupMoveMarker` (``direction="backup"``, keyed
       on ``backup_id``);
    3. move the body store root aside;
    4. move the DB (the caller is responsible for any ``-wal``/``-shm``
       siblings - this helper drives a clean on-disk seed that has none);
    5. (NOT performed - the halt leaves the marker for the reconciler).

    Halting leaves a genuinely half-finished backup the next boot's
    :func:`reconcile_interrupted_backup_move` must finish forward, keyed on
    the marker's ``backup_id`` and grounded in the manifest's declared
    paths. The dest names come from the PUBLIC :func:`quarantine_paths`, so
    they are byte-identical to what production would have produced for the
    pinned identity + moment.

    Args:
        data_dir: The ``storage.data_dir`` whose per-instance tree to act on.
        stop_after: The step to halt after (see :class:`BackupHaltStep`).
        instance_id: The per-instance subdir (default ``primary``).
        moment: The backup timestamp; pinned (not clock-read) so the dest
            names are deterministic across runs.

    Returns:
        The :class:`BackupHaltState` describing the partial on-disk layout.
    """
    inst_root = instance_dir(data_dir, instance_id)
    live_db = db_path_for(data_dir, instance_id)
    live_bodies = inst_root / "bodies"
    quarantined_db, quarantined_bodies = quarantine_paths(
        live_db, live_bodies, moment, backup_id=_PINNED_BACKUP_ID, reason="mode_switch"
    )
    marker_path = inst_root / BACKUP_MOVE_MARKER_NAME
    manifest = BackupManifest(
        backup_id=_PINNED_BACKUP_ID,
        reason="mode_switch",
        iso_display=utc_stamp(moment),
        db_path=quarantined_db,
        body_path=quarantined_bodies,
        has_db=live_db.exists(),
        has_body=live_bodies.exists(),
        created_at=moment,
    )

    # Step 1 - write the manifest FIRST (production declares intent before
    # anything moves).
    backup_manifest_path(inst_root, _PINNED_BACKUP_ID).write_text(
        manifest.model_dump_json(), encoding="utf-8"
    )
    # Step 2 - write the in-progress marker BEFORE moving anything.
    marker_path.write_text(
        BackupMoveMarker(backup_id=_PINNED_BACKUP_ID, direction="backup").model_dump_json(),
        encoding="utf-8",
    )
    if stop_after is BackupHaltStep.MARKER_ONLY:
        return _halt_state(
            manifest, live_db, live_bodies, quarantined_db, quarantined_bodies, marker_path
        )

    # Step 3 - body store FIRST (production's crash-safe ordering).
    if live_bodies.exists():
        shutil.move(str(live_bodies), str(quarantined_bodies))
    if stop_after is BackupHaltStep.BODY_MOVED:
        return _halt_state(
            manifest, live_db, live_bodies, quarantined_db, quarantined_bodies, marker_path
        )

    # Step 4 - DB LAST. (The seed has no WAL/SHM siblings; production sweeps
    # each independently, exercised by the integrity unit tests.)
    if live_db.exists():
        shutil.move(str(live_db), str(quarantined_db))
    # stop_after is DB_MOVED: marker deliberately left for the reconciler.
    return _halt_state(
        manifest, live_db, live_bodies, quarantined_db, quarantined_bodies, marker_path
    )


def _halt_state(
    manifest: BackupManifest,
    live_db: Path,
    live_bodies: Path,
    quarantined_db: Path,
    quarantined_bodies: Path,
    marker_path: Path,
) -> BackupHaltState:
    """Build a :class:`BackupHaltState` (keeps the public function flat)."""
    return BackupHaltState(
        backup_id=manifest.backup_id,
        manifest=manifest,
        live_db=live_db,
        live_bodies=live_bodies,
        quarantined_db=quarantined_db,
        quarantined_bodies=quarantined_bodies,
        marker_path=marker_path,
    )
