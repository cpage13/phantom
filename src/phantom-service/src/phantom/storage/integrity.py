"""SQLite integrity-check + quarantine path (plan § 5.2.1, § 1.1).

Strategy §3 commits: under no circumstance does the service stop
accepting traffic because the SQLite file is unreadable. On corruption:
preserve the corrupted file (timestamped); preserve the body-store root
(timestamped + renamed); start fresh; service resumes with an
ERROR-level log + ``db_quarantine_total`` counter bump.

The 2026-06 hardening cycle (plan § 1) generalizes the ONE mover with a
``reason`` discriminator: ``corrupted`` (the integrity gate above) and
``mode_switch`` (an unsafe ``all_ram``-over-populated-disk switch backs up
and runs rather than refusing to boot). A corruption backup is its own
re-trigger (the corrupt DB stays on disk), but a mode-switch backup runs
over a HEALTHY DB, which cannot be its own re-trigger; so the mode-switch
backup (and the symmetric one-call restore) write a :class:`BackupMoveMarker`
and are completed forward by :func:`reconcile_interrupted_backup_move` on the
next boot.

Backup identity (cycle-7 seam 1): every backup mints a ``backup_id``
(``uuid4``) at creation. The UUID is the backup's IDENTITY; the human-readable
``utc_stamp`` timestamp in artifact names is DISPLAY AND SORT ONLY. Artifact
filenames carry the timestamp followed by a short uniqueness token derived
from ``backup_id`` (its first
:data:`BACKUP_ID_FILENAME_TOKEN_LENGTH` hex chars), so two backups created in
the same wall-clock second can never collide on the filesystem and no
same-second disambiguation logic exists (the prior cycle's H-1 / M-3-B bug
class is unrepresentable, not guarded).

The manifest (cycle-7 seam 2): every backup writes ONE
:class:`BackupManifest` JSON (temp-then-rename, atomic) into the instance
data_root, named by ``backup_id``, BEFORE any artifact moves, so it declares
intent. The manifest names both artifacts (db path, body path), which
artifact the pair carries, the reason discriminator, and the display
timestamp. The inventory (:func:`list_quarantines`) reads manifests and
returns ONE entry per backup; an artifact on disk with NO manifest is
surfaced as an anomaly entry (flagged, never restorable) - convention-paired
loose files are no longer a representable backup (R5-P unrepresentable). The
in-progress marker and :func:`reconcile_interrupted_backup_move` key on
``backup_id`` and complete a half-finished move forward using the manifest's
DECLARED paths, never by re-deriving names.

Public surface (plan § 5.2.1, § 1.1, cycle-7 § 4):

* :func:`check_integrity` — async ``PRAGMA integrity_check`` probe.
* :func:`quarantine_paths` - pure helper returning the destinations for a
  known backup identity.
* :func:`quarantine` — side-effecting rename of DB + body store root
  (live -> quarantine), reason-parameterized; returns the written
  :class:`BackupManifest`.
* :func:`isolate_db_file` - body-less DB isolate (the token cache),
  manifested like every other backup.
* :func:`restore_mode_switch_backup` — side-effecting rename of a
  ``mode_switch`` backup pair back into the live tree (quarantine -> live),
  addressed by its manifest.
* :func:`reconcile_interrupted_backup_move` — finish-forward boot
  reconciliation for an interrupted backup OR restore move, keyed on
  ``backup_id``.
* :func:`list_quarantines` - manifest-driven inventory (one entry per
  backup plus anomaly entries).
* :func:`load_backup_manifest` / :func:`backup_manifest_path` - manifest
  addressing for the admin restore route.
* :class:`IntegrityChecker` — thin orchestrator that bundles the
  helpers into a single ``check(...)`` entry point usable from the
  composition root. The class form gives callers a single dependency
  to inject; the underlying free functions remain importable for unit
  tests that exercise pieces independently.
"""

from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, TypeAlias
from uuid import UUID, uuid4

import aiosqlite
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from phantom.storage.timestamps import utc_stamp

logger = logging.getLogger(__name__)

# The two backup reasons. ``corrupted`` is the existing integrity-gate
# path; ``mode_switch`` is the unsafe ``all_ram``-over-populated-disk
# switch (plan § 1.2). These are the ONLY two reasons: the schema case
# (plan § 4S) DELETES a pre-version DB rather than backing it up, so it
# adds no quarantine reason and reuses none of this machinery.
#
# NOTE: ``TypeAlias`` form (not ``type X = ...``) is intentional, matching
# the repo convention in ``phantom.models.chain`` / ``.token``: a 3.12+
# ``type X = Literal[...]`` alias returns an empty tuple from
# ``typing.get_args``, so the runtime-visible ``TypeAlias`` form is kept for
# any introspection.
QuarantineReason: TypeAlias = Literal["corrupted", "mode_switch"]  # noqa: UP040 — see note above

# Sentinel naming pieces. For ``reason="corrupted"`` the DB form is
# ``<stem>.corrupted.<stamp>.db`` and the body-store form is
# ``<basename>.quarantine.<stamp>/``. For ``reason="mode_switch"`` ONE infix
# (``.mode_switch.``) serves both the DB file and the body directory: the
# ``kind`` is recovered from file-plus-``.db`` vs directory, so one token
# suffices and the two reasons never collide. The ``<stamp>`` token is
# ``<utc iso>-<backup_id first hex>`` (see ``_backup_name_token``); the iso
# half is display/sort only, the uuid half guarantees filesystem uniqueness.
_DB_QUARANTINE_INFIX = ".corrupted."
_BODY_QUARANTINE_INFIX = ".quarantine."
_MODE_SWITCH_INFIX = ".mode_switch."

# How many leading hex characters of ``backup_id`` are appended to artifact
# filenames after the display iso. Eight hex chars (32 bits) keep the names
# short and readable while making a same-second filename collision require a
# 1-in-4-billion uuid prefix match WITHIN one wall-clock second; the full
# uuid remains the backup's identity everywhere that matters (the marker, the
# inventory, restore addressing), so the token is uniqueness-of-naming only.
BACKUP_ID_FILENAME_TOKEN_LENGTH = 8

# In-progress marker for the two-artifact (DB file + body directory)
# mode-switch backup and the symmetric one-call restore. Neither is atomic
# as a PAIR, so they share ONE marker and ONE finish-forward reconciliation
# (:func:`reconcile_interrupted_backup_move`). Only mode-switch backups
# write a marker (corruption never does), so the reason is implicit and no
# ``reason`` field is stored.
BACKUP_MOVE_MARKER_NAME = ".backup_move.in_progress"

# Manifest filename pieces (cycle-7 seam 2): one
# ``backup.<backup_id>.manifest.json`` per backup, flat in the instance
# data_root beside the artifacts it names. The pieces contain none of the
# artifact infixes above, so the anomaly scan never classifies a manifest
# as an artifact.
_MANIFEST_PREFIX = "backup."
_MANIFEST_SUFFIX = ".manifest.json"


# ---------------------------------------------------------------------
# Result types.
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class IntegrityCheckResult:
    """Outcome of :func:`check_integrity`.

    Attributes:
        ok: ``True`` when the SQLite file parses cleanly and
            ``PRAGMA integrity_check`` returned ``ok``. ``True`` also
            on a missing file (fresh deployment).
        message: Human-readable diagnostic; ``"ok"`` on success,
            ``"fresh — no file"`` on missing input, or a descriptive
            error string from SQLite / OS errors otherwise.
    """

    ok: bool
    message: str


@dataclass(frozen=True)
class RestoreMoveOutcome:
    """Outcome of :func:`restore_mode_switch_backup`.

    Surfaces whether each half of the backup pair actually moved into the live
    tree, so the one-call restore route can distinguish a real restore from a
    silent no-op (finding H-1 / L-2). The clobber-safe movers skip an
    already-occupied dest; if the live DB was never cleared first, the restore
    DB move no-ops and the operator would otherwise be told it succeeded.
    ``db_moved=False`` is the route's fail-loud signal.

    Attributes:
        db_path: The live DB destination the backup DB was (or would be)
            restored to. Returned whether or not a move occurred, mirroring
            :func:`quarantine`'s best-effort path contract.
        body_path: The live body-store root the backup body tree was (or would
            be) restored to.
        db_moved: ``True`` iff the backup DB file itself was moved into
            ``db_path`` (the destination was free). The load-bearing no-op
            signal: a restore that moved no DB stranded the chosen backup.
        body_moved: ``True`` iff the backup body tree was moved into
            ``body_path``.
    """

    db_path: Path
    body_path: Path
    db_moved: bool
    body_moved: bool


class BackupManifest(BaseModel):
    """The single on-disk record naming one backup's artifact pair (seam 2).

    Written atomically (temp sibling then ``os.replace``) into the instance
    data_root, named by ``backup_id``
    (``backup.<backup_id>.manifest.json``), BEFORE any artifact moves, so it
    DECLARES the backup's intent: which artifacts belong to this backup and
    exactly where they land. Everything downstream (the inventory, the admin
    restore route, crash reconciliation) reads declared paths off the
    manifest; nothing string-matches or pairs artifacts by filename
    convention (R5-P unrepresentable). An artifact with no manifest is an
    inventory ANOMALY, never a restorable unit.

    Serialization-boundary model (on-disk JSON), so it is a Pydantic
    ``BaseModel`` with ``extra="forbid"``.
    """

    model_config = ConfigDict(extra="forbid")

    backup_id: UUID = Field(
        description=(
            "The backup's uuid4 identity, minted at creation (seam 1). Keys "
            "the manifest filename, the in-progress marker, the inventory "
            "entry, and the admin restore route."
        ),
    )
    reason: QuarantineReason = Field(
        description=(
            "Why the backup was taken: 'corrupted' (integrity-gate "
            "quarantine or the body-less token-cache isolate) or "
            "'mode_switch' (an unsafe all_ram-over-populated-disk switch "
            "backed the live tree up). Only 'mode_switch' backups are "
            "restorable via the admin route."
        ),
    )
    iso_display: str = Field(
        description=(
            "The human-readable utc_stamp half of the artifact names. "
            "DISPLAY AND SORT ONLY; never identity (seam 1)."
        ),
    )
    db_path: Path = Field(
        description=(
            "Declared absolute destination of the backup's DB artifact "
            "(present on disk only when the artifact actually moved)."
        ),
    )
    body_path: Path | None = Field(
        description=(
            "Declared absolute destination of the backup's body-tree "
            "artifact, or null for a body-less backup (the token-cache "
            "isolate has no coupled body tree)."
        ),
    )
    has_db: bool = Field(
        description=(
            "Whether the DB artifact existed at its live source when the "
            "backup was created (the pair's intended DB content)."
        ),
    )
    has_body: bool = Field(
        description=(
            "Whether the body tree existed at its live source when the "
            "backup was created (the pair's intended body content)."
        ),
    )
    created_at: datetime = Field(
        description="ISO-8601 UTC instant the backup was created.",
    )


class BackupMoveMarker(BaseModel):
    """On-disk marker for an in-progress mode-switch backup or restore move.

    A mode-switch backup (live -> quarantine) and a one-call restore
    (quarantine -> live) are each a two-artifact move (a DB file plus a body
    directory) that is NOT atomic as a pair. Each individual ``shutil.move``
    IS atomic (the source and destination are siblings on one filesystem, so
    the move is a rename), so any crash leaves each artifact wholly at its
    source or wholly at its destination, never torn. This marker records
    WHICH BACKUP's move was in flight so
    :func:`reconcile_interrupted_backup_move` can load that backup's
    manifest and complete whichever half remains on the next boot, using the
    manifest's DECLARED paths (seam 2; nothing is re-derived from names).

    Serialization-boundary model (on-disk JSON), so it is a Pydantic
    ``BaseModel`` with ``extra="forbid"``.
    """

    model_config = ConfigDict(extra="forbid")

    backup_id: UUID = Field(
        description=(
            "Identity of the backup whose two-artifact move is in flight; "
            "reconciliation loads this backup's manifest for the declared "
            "artifact paths."
        ),
    )
    direction: Literal["backup", "restore"] = Field(
        description=(
            "'backup' for a live->quarantine move, 'restore' for a "
            "quarantine->live move; selects the source/dest direction "
            "reconciliation finishes forward."
        ),
    )


def _write_json_model_atomic(path: Path, model: BaseModel) -> None:
    """Atomically write ``model`` as JSON to ``path``.

    Writes a temp sibling then ``os.replace`` so a reader never sees a
    partial file (the rename is atomic on one filesystem). Shared by the
    manifest write and the marker write - the two on-disk records whose
    torn-write would corrupt crash recovery.

    Args:
        path: Destination path (inside the instance ``data_root``).
        model: The Pydantic model to persist.
    """
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(model.model_dump_json(), encoding="utf-8")
    os.replace(tmp_path, path)


def _read_backup_move_marker(marker_path: Path) -> BackupMoveMarker:
    """Read and validate the backup-move marker at ``marker_path``.

    Args:
        marker_path: The marker path to read.

    Returns:
        The parsed :class:`BackupMoveMarker`.
    """
    return BackupMoveMarker.model_validate_json(marker_path.read_text(encoding="utf-8"))


def backup_manifest_path(data_root: Path, backup_id: UUID) -> Path:
    """Return the manifest path for ``backup_id`` under ``data_root``.

    Args:
        data_root: The instance data_root holding the backup artifacts.
        backup_id: The backup's uuid identity.

    Returns:
        ``<data_root>/backup.<backup_id>.manifest.json``.
    """
    return data_root / f"{_MANIFEST_PREFIX}{backup_id}{_MANIFEST_SUFFIX}"


def load_backup_manifest(data_root: Path, backup_id: UUID) -> BackupManifest | None:
    """Load one backup's manifest by identity, or ``None``.

    The admin restore route and boot reconciliation address a backup through
    this function; there is no filename-matching fallback (seam 2).

    Args:
        data_root: The instance data_root holding the manifests.
        backup_id: The backup identity to load.

    Returns:
        The parsed :class:`BackupManifest`, or ``None`` when no manifest
        exists for ``backup_id`` or the file does not parse (an unreadable
        manifest is logged and treated as absent; its artifacts surface as
        inventory anomalies rather than as a restorable backup).
    """
    path = backup_manifest_path(data_root, backup_id)
    if not path.exists():
        return None
    try:
        return BackupManifest.model_validate_json(path.read_text(encoding="utf-8"))
    except OSError, ValidationError:
        logger.warning("Unreadable backup manifest at %s; treating as absent", path, exc_info=True)
        return None


def _iter_backup_manifests(data_root: Path) -> list[BackupManifest]:
    """Parse every backup manifest under ``data_root``.

    Unreadable manifest files are logged and skipped (their artifacts then
    surface as inventory anomalies, which is the honest report).

    Args:
        data_root: The instance data_root to scan.

    Returns:
        Parsed manifests in no particular order (callers sort for display).
    """
    manifests: list[BackupManifest] = []
    for path in data_root.glob(f"{_MANIFEST_PREFIX}*{_MANIFEST_SUFFIX}"):
        try:
            manifests.append(BackupManifest.model_validate_json(path.read_text(encoding="utf-8")))
        except OSError, ValidationError:
            logger.warning("Skipping unreadable backup manifest %s", path, exc_info=True)
    return manifests


def _move_if_pending(source: Path, dest: Path) -> bool:
    """Move ``source`` to ``dest`` iff the source exists and the dest does not.

    The idempotent primitive both the movers and reconciliation use: a
    finished half (source gone, dest present) is a no-op. Returns whether a
    move actually happened so callers can log only real moves.

    Args:
        source: The artifact's current path.
        dest: The artifact's target path.

    Returns:
        ``True`` when a move was performed, ``False`` when nothing was due.
    """
    if source.exists() and not dest.exists():
        shutil.move(str(source), str(dest))
        return True
    return False


def _move_db_with_siblings(db_source: Path, db_dest: Path) -> bool:
    """Move a DB file plus its ``-wal``/``-shm`` siblings, each idempotently.

    The main DB and each WAL/SHM sibling are moved INDEPENDENTLY (a sibling
    iff it exists at its source and is absent at its dest), so a crash after
    the main-DB rename but mid-sibling rename still sweeps the stragglers on
    the next boot (review m-1).

    Args:
        db_source: The DB's current path.
        db_dest: The DB's target path.

    Returns:
        ``True`` when the main DB file itself was moved, ``False`` otherwise.
        (Sibling-only sweeps return ``False``: the main artifact was already
        in place, so reconciliation has no new direction event to report.)
    """
    moved_db = _move_if_pending(db_source, db_dest)
    for sibling_suffix in ("-wal", "-shm"):
        _move_if_pending(
            db_source.with_name(db_source.name + sibling_suffix),
            db_dest.with_name(db_dest.name + sibling_suffix),
        )
    return moved_db


# ---------------------------------------------------------------------
# Probe.
# ---------------------------------------------------------------------


async def check_integrity(db_path: Path) -> IntegrityCheckResult:
    """Run ``PRAGMA integrity_check`` against the SQLite file at ``db_path``.

    Args:
        db_path: Path to the persistent SQLite file. Missing files are
            treated as a healthy fresh deployment.

    Returns:
        An :class:`IntegrityCheckResult` with ``ok=True`` when the file
        parses + integrity check passes (or the file doesn't exist),
        ``ok=False`` with a diagnostic ``message`` otherwise.
    """
    if not db_path.exists():
        return IntegrityCheckResult(ok=True, message="fresh — no file")
    try:
        async with (
            aiosqlite.connect(str(db_path)) as conn,
            conn.execute("PRAGMA integrity_check") as cur,
        ):
            row = await cur.fetchone()
        if row is None or row[0] != "ok":
            return IntegrityCheckResult(
                ok=False,
                message=f"integrity_check returned: {row!r}",
            )
        return IntegrityCheckResult(ok=True, message="ok")
    except (aiosqlite.Error, OSError) as exc:
        return IntegrityCheckResult(ok=False, message=f"open failed: {exc}")


# ---------------------------------------------------------------------
# Path computation + rename.
# ---------------------------------------------------------------------


def _resolve_utc_moment(timestamp: datetime | None) -> datetime:
    """Resolve an optional caller timestamp to a tz-aware UTC instant.

    Mirrors :func:`phantom.storage.timestamps.utc_stamp`'s handling so the
    manifest's ``created_at`` and the artifact names' display iso always
    agree: ``None`` reads the clock, a naive datetime is taken as UTC
    wall-clock, and an aware one is converted to UTC.

    Args:
        timestamp: The caller-supplied moment, or ``None`` for now.

    Returns:
        A timezone-aware UTC ``datetime``.
    """
    if timestamp is None:
        return datetime.now(tz=UTC)
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC)


def _backup_name_token(iso: str, backup_id: UUID) -> str:
    """Build the filename stamp for one backup: display iso + identity token.

    The iso half is human-readable display/sort material; the appended
    ``backup_id`` prefix (:data:`BACKUP_ID_FILENAME_TOKEN_LENGTH` hex chars)
    is what makes the name unique, so two backups minted in the same
    wall-clock second never collide on the filesystem and no disambiguation
    search exists anywhere (cycle-7 seam 1; H-1 / M-3-B unrepresentable).

    Args:
        iso: The display timestamp (``utc_stamp`` output).
        backup_id: The backup's UUID identity.

    Returns:
        ``"<iso>-<first hex of backup_id>"`` (dot-free, filename-safe).
    """
    return f"{iso}-{backup_id.hex[:BACKUP_ID_FILENAME_TOKEN_LENGTH]}"


def _artifact_destinations(
    db_path: Path,
    body_store_root: Path,
    reason: QuarantineReason,
    name_token: str,
) -> tuple[Path, Path]:
    """Build the (db_dest, body_dest) pair from an explicit name token.

    Pure function - no side effects, no clock read, no uuid mint. Factoring
    the name construction by a GIVEN token (rather than re-deriving it) is
    what keeps the naming a single implementation across the movers and the
    pure :func:`quarantine_paths` helper.

    Naming by reason (``<stamp>`` = ``_backup_name_token`` output):

    * ``corrupted``  -> ``<stem>.corrupted.<stamp>.db`` and
      ``<name>.quarantine.<stamp>/``.
    * ``mode_switch`` -> ``<stem>.mode_switch.<stamp>.db`` and
      ``<name>.mode_switch.<stamp>/`` (one infix for both; ``kind`` is
      recovered from file-plus-``.db`` vs directory).

    Args:
        db_path: Live DB path.
        body_store_root: Live body-store root directory.
        reason: ``corrupted`` or ``mode_switch`` — selects the infix(es).
        name_token: The shared dest stamp (``_backup_name_token`` output).

    Returns:
        A 2-tuple ``(quarantined_db, quarantined_body_store_root)``.
    """
    db_infix = _DB_QUARANTINE_INFIX if reason == "corrupted" else _MODE_SWITCH_INFIX
    body_infix = _BODY_QUARANTINE_INFIX if reason == "corrupted" else _MODE_SWITCH_INFIX
    quarantined_db = db_path.with_name(f"{db_path.stem}{db_infix}{name_token}.db")
    quarantined_body = body_store_root.parent / (f"{body_store_root.name}{body_infix}{name_token}")
    return quarantined_db, quarantined_body


def quarantine_paths(
    db_path: Path,
    body_store_root: Path,
    timestamp: datetime | None = None,
    *,
    backup_id: UUID,
    reason: QuarantineReason = "corrupted",
) -> tuple[Path, Path]:
    """Compute (db_destination, body_store_destination) for a given backup identity.

    Pure function - no side effects, no uuid mint. Useful for tests and
    harnesses that want the exact destination names a backup with a KNOWN
    ``backup_id`` would produce, without performing the rename.

    ``backup_id`` and ``reason`` are KEYWORD-ONLY and placed AFTER
    ``timestamp`` so historical positional calls
    (``quarantine_paths(db_path, body_store_root, timestamp)``) can never
    silently bind ``timestamp`` into another slot (B-1).

    Args:
        db_path: Live DB path that would be quarantined.
        body_store_root: Live body-store root directory.
        timestamp: Optional override for the display-timestamp half of the
            name. Defaults to ``datetime.now(tz=UTC)`` (computed by
            :func:`phantom.storage.timestamps.utc_stamp`). Tests pin
            this to a known value; a supplied tz-aware datetime is
            converted to UTC before formatting so the ``Z`` suffix is
            always truthful.
        backup_id: The backup's UUID identity; its leading hex token is the
            uniqueness half of the artifact names.
        reason: ``corrupted`` (default) or ``mode_switch`` (the
            ``.mode_switch.`` infix for both artifacts).

    Returns:
        A 2-tuple ``(quarantined_db, quarantined_body_store_root)``.
    """
    # The display half carries a trailing ``Z`` (UTC). Finding A-2: this site
    # previously stamped naive local time, contradicting this docstring and
    # producing a UTC-labelled name holding local-wall-clock time. The shared
    # ``utc_stamp`` helper computes the stamp in UTC unconditionally;
    # resolving it ONCE here (not per artifact) keeps the DB + body stamp
    # identical.
    name_token = _backup_name_token(utc_stamp(timestamp), backup_id)
    return _artifact_destinations(db_path, body_store_root, reason, name_token)


def quarantine(
    db_path: Path,
    body_store_root: Path,
    timestamp: datetime | None = None,
    *,
    reason: QuarantineReason = "corrupted",
) -> BackupManifest:
    """Rename the live DB + body-store root into one manifested backup.

    Side effects: the backup's :class:`BackupManifest` is written FIRST
    (atomic temp-then-rename, declaring intent), then
    ``shutil.move(body_store_root, <name>.<infix>.<stamp>/)``
    if the body-store root exists, THEN
    ``shutil.move(db_path, <stem>.<infix>.<stamp>.db)`` if the DB file
    exists. Missing inputs are tolerated (the rename is a best-effort
    preserve); when NEITHER input exists nothing is written or moved at all
    (no empty-pair manifest pollutes the inventory) and the returned
    manifest simply describes the no-op. For ``reason="mode_switch"`` a
    :class:`BackupMoveMarker` is written BEFORE any move and cleared AFTER
    both moves (see below).

    ``reason`` is KEYWORD-ONLY and placed AFTER ``timestamp`` (BLOCKING fix
    B-1). :meth:`IntegrityChecker.quarantine_now` passes ``timestamp``
    POSITIONALLY (``quarantine(self._db_path, self._body_store_root,
    timestamp)``), so inserting ``reason`` as a third POSITIONAL parameter
    would bind that ``timestamp`` to ``reason`` (a ``datetime`` matched
    against ``"corrupted"``) and break the corruption path. Keyword-only
    ``reason`` keeps every existing positional caller byte-identical: no
    marker, same names, same order.

    Crash-safe ordering (finding R3-6). For ``reason="corrupted"`` the
    corrupt DB is the re-trigger: as long as it is on disk, the next startup
    re-runs the integrity gate. We move the body store FIRST and the corrupt
    DB LAST so a SIGKILL between the two moves leaves the corrupt DB in
    place; on restart the gate finds the same corrupt DB and re-quarantines
    (moving a now-absent body store is a tolerated no-op), then boots fresh
    in EVERY mode. The old DB-first order left the inverse half-state (DB
    gone, body store present), which a fresh-empty DB passed integrity over,
    so quarantine did NOT re-run and the leftover body dirs wedged an
    ``all_ram`` boot against the A-3/F-2 guard
    (:func:`phantom.runtime.startup_checks.check_body_store_mode`).
    hybrid/all_disk self-healed via the orphan janitor; only all_ram, which
    has neither janitor nor disk-aware recovery, stayed stuck.

    For ``reason="mode_switch"`` the backup runs over a HEALTHY DB, which
    cannot be its own re-trigger. The :class:`BackupMoveMarker`
    (``direction="backup"``) IS that re-trigger: written before the body
    moves so a crash with ``bodies_root`` already emptied is finished
    forward by :func:`reconcile_interrupted_backup_move` (which moves the DB
    too) before :func:`check_body_store_mode` runs, leaving a fully-clean
    live tree. Without the marker, a crash after the body move but before
    the DB move would let a marker-less mode guard see an empty
    ``bodies_root``, judge "safe", and boot ``all_ram`` over a healthy DB
    whose ``body_location='file'`` rows now point only into the backup —
    the exact A-3 data loss the guard exists to prevent.

    The strategy commits to "renames only, never deletes" — the operator
    decides when to remove quarantine artifacts. WAL/SHM siblings of the
    SQLite are also moved when present so the quarantined DB is
    self-contained.

    Backup identity (cycle-7 seam 1). Every call mints a fresh ``backup_id``
    (``uuid4``); the artifact names carry the display iso PLUS the
    ``backup_id``'s leading hex token, so two backups in the same wall-clock
    second land on distinct names BY CONSTRUCTION. There is no collision
    probe and no disambiguation search (the prior cycle's H-1 / M-3-B class
    is unrepresentable, not guarded).

    Args:
        db_path: Live DB path to rename.
        body_store_root: Live body-store root to rename.
        timestamp: Optional display-timestamp override; defaults to UTC now.
        reason: ``corrupted`` (default; no marker) or ``mode_switch``
            (writes-then-clears a backup marker).

    Returns:
        The backup's :class:`BackupManifest` (written to disk unless neither
        source existed). ``db_path`` / ``body_path`` on it are the artifact
        destinations; ``has_db`` / ``has_body`` say which sources existed.
    """
    backup_id = uuid4()
    moment = _resolve_utc_moment(timestamp)
    iso = utc_stamp(moment)
    name_token = _backup_name_token(iso, backup_id)
    quarantined_db, quarantined_body = _artifact_destinations(
        db_path, body_store_root, reason, name_token
    )
    manifest = BackupManifest(
        backup_id=backup_id,
        reason=reason,
        iso_display=iso,
        db_path=quarantined_db,
        body_path=quarantined_body,
        has_db=db_path.exists(),
        has_body=body_store_root.exists(),
        created_at=moment,
    )
    if not manifest.has_db and not manifest.has_body:
        # Nothing to back up: write no manifest, move nothing. The returned
        # manifest still describes the would-be backup (best-effort path
        # contract for callers that only inspect the destinations).
        return manifest
    data_root = db_path.parent
    # Manifest FIRST (declares intent before anything moves, seam 2).
    _write_json_model_atomic(backup_manifest_path(data_root, backup_id), manifest)
    marker_path = data_root / BACKUP_MOVE_MARKER_NAME
    # A mode_switch backup runs over a healthy DB and needs the re-trigger:
    # write the marker BEFORE moving anything.
    if reason != "corrupted":
        _write_json_model_atomic(
            marker_path, BackupMoveMarker(backup_id=backup_id, direction="backup")
        )
    # Body store FIRST (see crash-safe ordering above).
    if _move_if_pending(body_store_root, quarantined_body):
        logger.error(
            "Body store quarantined (%s, backup_id=%s): %s -> %s",
            reason,
            backup_id,
            body_store_root,
            quarantined_body,
        )
    # DB LAST — for corruption its continued presence is the re-quarantine
    # gate; for mode_switch the marker is the re-trigger.
    if _move_db_with_siblings(db_path, quarantined_db):
        logger.error(
            "DB quarantined (%s, backup_id=%s): %s -> %s",
            reason,
            backup_id,
            db_path,
            quarantined_db,
        )
    if reason != "corrupted":
        marker_path.unlink(missing_ok=True)
    return manifest


def isolate_db_file(db_path: Path, *, timestamp: datetime | None = None) -> BackupManifest:
    """Isolate a body-LESS SQLite file to a ``.corrupted.<stamp>.db`` sibling.

    A DB-only counterpart to :func:`quarantine` for a database that has NO
    coupled body-store tree — the per-instance token cache
    (``token_cache.db``). :func:`quarantine` moves the body-store root FIRST and
    couples a DB path with a body ROOT, so handing it the instance's
    ``bodies_root`` to isolate the token cache would relocate the UPLOAD body
    tree — wrong (review m-4 / finding F-18: a body-less isolate fits the
    coupled corruption mover least well). This helper moves only ``db_path`` plus
    its ``-wal`` / ``-shm`` siblings to a timestamped sibling, leaving any body
    tree untouched.

    The destination reuses the same ``.corrupted.`` infix and the same
    iso-plus-identity-token stamp as :func:`quarantine`'s corruption path
    (a fresh ``backup_id`` is minted here too, cycle-7 seam 1), so an
    isolated body-less DB lands in the SAME flat-sibling
    naming scheme :func:`list_quarantines` already classifies as
    ``(reason=corrupted, kind=db)`` and surfaces on ``GET /v1/admin/quarantine``.
    No in-progress marker is written: an un-openable DB on disk is its own
    re-trigger (identical to the corruption path's body-less analogue), and a
    DB-only move has no second artifact to leave half-done — the
    :func:`_move_db_with_siblings` primitive already sweeps each WAL/SHM sibling
    independently so a crash mid-move is finished by the same idempotent move on
    the next boot.

    We ISOLATE rather than delete because an un-openable DB may be a real buffer
    we merely cannot open RIGHT NOW (a transient permission glitch, an external
    tool, a lock mis-classification); the operator decides when to remove the
    artifact (the strategy's "renames only, never deletes").

    Like every backup, the isolate is MANIFESTED (seam 2): a
    :class:`BackupManifest` with ``body_path=None`` / ``has_body=False`` is
    written before the move, so an isolated token cache appears in the
    inventory as one backup entry, never as an anomaly.

    Args:
        db_path: Live SQLite file path to isolate (e.g. ``token_cache.db``).
        timestamp: Optional timestamp override; defaults to UTC now. Pinned by
            tests for a deterministic dest name.

    Returns:
        The backup's :class:`BackupManifest` (written to disk unless the
        source was absent); ``db_path`` on it is the destination
        ``<stem>.corrupted.<stamp>.db`` beside the live file, returned
        whether or not the source existed, mirroring :func:`quarantine`'s
        best-effort contract (a missing source is a tolerated no-op).
    """
    backup_id = uuid4()
    moment = _resolve_utc_moment(timestamp)
    iso = utc_stamp(moment)
    name_token = _backup_name_token(iso, backup_id)
    isolated_db = db_path.with_name(f"{db_path.stem}{_DB_QUARANTINE_INFIX}{name_token}.db")
    manifest = BackupManifest(
        backup_id=backup_id,
        reason="corrupted",
        iso_display=iso,
        db_path=isolated_db,
        body_path=None,
        has_db=db_path.exists(),
        has_body=False,
        created_at=moment,
    )
    if not manifest.has_db:
        return manifest
    _write_json_model_atomic(backup_manifest_path(db_path.parent, backup_id), manifest)
    if _move_db_with_siblings(db_path, isolated_db):
        logger.error(
            "DB isolated (corrupted, body-less, backup_id=%s): %s -> %s",
            backup_id,
            db_path,
            isolated_db,
        )
    return manifest


def restore_mode_switch_backup(
    db_path: Path,
    body_store_root: Path,
    manifest: BackupManifest,
) -> RestoreMoveOutcome:
    """Move a ``mode_switch`` backup pair back into the live tree.

    The quarantine -> live direction of the marked mover, used by the
    one-call admin restore route (plan § 1.5). The backup is addressed by
    its MANIFEST (seam 2): the sources are the manifest's declared artifact
    paths, never names re-derived from a token. Writes a
    :class:`BackupMoveMarker` (``direction="restore"``, keyed on
    ``backup_id``) BEFORE moving and clears it AFTER both moves, so a crash
    mid-restore is finished forward by
    :func:`reconcile_interrupted_backup_move` on the next boot and the chosen
    backup is never half-consumed and lost (review M-1).

    When the restore fully consumes the backup (neither declared artifact
    remains at its backup location), the manifest file is deleted: the
    backup ceased to exist, and a manifest left behind would advertise a
    restorable backup whose artifacts are gone. A no-op or partial restore
    KEEPS the manifest so the still-present artifacts remain a first-class
    backup rather than decaying into anomalies.

    Precondition: the caller (the § 1.5 route) has already moved any current
    live db/bodies out of the way via :func:`quarantine`, so the live
    targets are empty when this runs. When that precondition holds the DB move
    lands and :attr:`RestoreMoveOutcome.db_moved` is ``True``; when the live
    targets are NOT empty (a clobber-safe move into an occupied dest), the move
    no-ops and ``db_moved`` is ``False``, the route's fail-loud signal that the
    backup was not actually restored (finding H-1 / L-2).

    Args:
        db_path: The live DB destination.
        body_store_root: The live body-store root destination.
        manifest: The manifest of the ``mode_switch`` backup to restore.

    Returns:
        A :class:`RestoreMoveOutcome` carrying the live targets and whether each
        half actually moved.
    """
    data_root = db_path.parent
    marker_path = data_root / BACKUP_MOVE_MARKER_NAME
    _write_json_model_atomic(
        marker_path, BackupMoveMarker(backup_id=manifest.backup_id, direction="restore")
    )
    body_moved = False
    if manifest.body_path is not None:
        body_moved = _move_if_pending(manifest.body_path, body_store_root)
    if body_moved:
        logger.warning(
            "Mode-switch backup restored (body, backup_id=%s): %s -> %s",
            manifest.backup_id,
            manifest.body_path,
            body_store_root,
        )
    db_moved = _move_db_with_siblings(manifest.db_path, db_path)
    if db_moved:
        logger.warning(
            "Mode-switch backup restored (db, backup_id=%s): %s -> %s",
            manifest.backup_id,
            manifest.db_path,
            db_path,
        )
    _unlink_manifest_if_consumed(data_root, manifest)
    marker_path.unlink(missing_ok=True)
    return RestoreMoveOutcome(
        db_path=db_path,
        body_path=body_store_root,
        db_moved=db_moved,
        body_moved=body_moved,
    )


def _unlink_manifest_if_consumed(data_root: Path, manifest: BackupManifest) -> None:
    """Delete ``manifest``'s file iff the backup's artifacts are all gone.

    A backup is CONSUMED when neither declared artifact remains at its
    backup location (a completed restore moved them into the live tree).
    Deleting the manifest then keeps the inventory truthful; keeping it on a
    partial/no-op restore keeps the still-present artifacts first-class.

    Args:
        data_root: The instance data_root holding the manifest.
        manifest: The backup's manifest.
    """
    body_remains = manifest.body_path is not None and manifest.body_path.exists()
    if manifest.db_path.exists() or body_remains:
        return
    backup_manifest_path(data_root, manifest.backup_id).unlink(missing_ok=True)


def reconcile_interrupted_backup_move(
    db_path: Path,
    body_store_root: Path,
) -> Literal["backup", "restore"] | None:
    """Finish forward an interrupted mode-switch backup OR restore move.

    Reads the :class:`BackupMoveMarker` at
    ``db_path.parent / BACKUP_MOVE_MARKER_NAME``. A no-marker call is a no-op
    returning ``None``. With a marker, it loads the named backup's
    :class:`BackupManifest` (seam 2: the marker carries ``backup_id``; the
    manifest carries the DECLARED artifact paths - nothing is re-derived from
    filenames) and completes whichever half remains:

    * ``direction="backup"``  - source/dest = ``(db_path -> declared db)``
      and ``(body_store_root -> declared body)``.
    * ``direction="restore"`` - source/dest = ``(declared db -> db_path)``
      and ``(declared body -> body_store_root)``; a fully-consumed backup's
      manifest is then deleted, exactly as the uninterrupted restore does.

    Each artifact is moved idempotently (source exists and dest absent), and
    each DB ``-wal``/``-shm`` sibling is swept INDEPENDENTLY, so a crash
    after the main-DB rename but mid-sibling rename still completes (m-1).
    A completed-but-marker-not-cleared crash finds every dest present, moves
    nothing, and just clears the marker. The marker is always unlinked; a
    WARNING naming the direction and backup is logged whenever a marker was
    found. A marker whose manifest is MISSING (a restore that finished
    through the manifest delete but crashed before the marker clear, or
    operator tampering) moves nothing, logs, and clears the marker - the
    artifacts, if any remain, surface as inventory anomalies.

    Must run per-instance AFTER ``run_integrity_gate`` and BEFORE
    :func:`check_body_store_mode`, so an interrupted backup is fully
    completed (live tree clean) before the mode guard inspects
    ``bodies_root``.

    Args:
        db_path: This instance's live DB path.
        body_store_root: This instance's live body-store root.

    Returns:
        The completed ``direction`` when a marker was found, its manifest
        loaded, and the move finished; ``None`` when there was no marker or
        the marker's manifest was missing.
    """
    data_root = db_path.parent
    marker_path = data_root / BACKUP_MOVE_MARKER_NAME
    if not marker_path.exists():
        return None
    marker = _read_backup_move_marker(marker_path)
    manifest = load_backup_manifest(data_root, marker.backup_id)
    if manifest is None:
        logger.error(
            "Backup-move marker (direction=%s) names backup_id=%s but no manifest "
            "exists; clearing the marker. Any remaining artifacts surface as "
            "inventory anomalies.",
            marker.direction,
            marker.backup_id,
        )
        marker_path.unlink(missing_ok=True)
        return None
    q_db = manifest.db_path
    q_body = manifest.body_path
    if marker.direction == "backup":
        db_source, db_dest = db_path, q_db
        body_pair = (body_store_root, q_body) if q_body is not None else None
    else:
        db_source, db_dest = q_db, db_path
        body_pair = (q_body, body_store_root) if q_body is not None else None
    # Body half then DB half (with siblings), each idempotent.
    if body_pair is not None:
        _move_if_pending(*body_pair)
    _move_db_with_siblings(db_source, db_dest)
    if marker.direction == "restore":
        _unlink_manifest_if_consumed(data_root, manifest)
    marker_path.unlink(missing_ok=True)
    logger.warning(
        "Reconciled interrupted %s move (backup_id=%s): db %s -> %s, body %s -> %s",
        marker.direction,
        marker.backup_id,
        db_source,
        db_dest,
        body_pair[0] if body_pair is not None else "(none)",
        body_pair[1] if body_pair is not None else "(none)",
    )
    return marker.direction


# ---------------------------------------------------------------------
# Inventory.
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class QuarantineInventoryEntry:
    """Internal per-BACKUP entry shape returned by :func:`list_quarantines`.

    One entry per backup (seam 2: the manifest is the unit, not the loose
    artifact), plus one ANOMALY entry per on-disk artifact no manifest
    claims. The admin endpoint converts these into the public
    :class:`phantom.models.admin.QuarantineEntry` response model.

    Attributes:
        backup_id: The backup's uuid identity from its manifest; ``None``
            for an anomaly (an unmanifested artifact has no identity and is
            never restorable).
        reason: ``corrupted`` (integrity-gate quarantine / token-cache
            isolate) or ``mode_switch`` (unsafe-mode-switch backup). From
            the manifest for a backup; recovered from the artifact's infix
            for an anomaly (display classification only).
        iso_display: The human-readable timestamp half of the artifact
            names. Display and sort material ONLY, never identity.
        db_path: The backup's declared DB artifact path (``None`` for a
            body-only anomaly).
        body_path: The backup's declared body-tree artifact path (``None``
            for a body-less backup or a db-only anomaly).
        has_db: Whether the DB artifact is PRESENT on disk right now (the
            restorability signal; a manifest may declare an artifact that
            an interrupted move or an operator removed).
        has_body: Whether the body-tree artifact is present on disk now.
        bytes: Total size of the present artifacts (file size for the DB;
            recursive sum for the body tree).
        anomaly: ``True`` for an artifact no manifest claims. Anomalies are
            surfaced for the operator but are not restorable.
    """

    backup_id: UUID | None
    reason: QuarantineReason
    iso_display: str
    db_path: Path | None
    body_path: Path | None
    has_db: bool
    has_body: bool
    bytes: int
    anomaly: bool


def _dir_byte_sum(directory: Path) -> int:
    """Recursive sum of file sizes under ``directory`` (vanished files skipped).

    Args:
        directory: The body-store directory to size.

    Returns:
        The total size in bytes of all regular files found.
    """
    size = 0
    for p in directory.rglob("*"):
        if p.is_file():
            try:
                size += p.stat().st_size
            except OSError:
                # File vanished mid-walk — operator removed it.
                continue
    return size


def _parse_quarantine_iso(
    name: str, kind: Literal["db", "body_store"], reason: QuarantineReason
) -> str:
    """Recover the name stamp from a quarantine artifact name (DISPLAY ONLY).

    Inverse of :func:`_artifact_destinations`. The name is
    ``<stem><infix><stamp>`` for a body-store directory and
    ``<stem><infix><stamp>.db`` for a DB file, where ``<infix>`` is the
    reason-specific token and ``<stamp>`` is ``<utc iso>-<hex token>``
    (dot-free), so splitting once on the infix and dropping any ``.db``
    suffix yields the stamp unambiguously. The stamp is never identity:
    parsing it out of a filename is permitted for display and sorting only
    (cycle-7 forbidden-pattern rule).

    Args:
        name: The artifact's basename.
        kind: ``db`` for a SQLite file; ``body_store`` for a directory.
        reason: The artifact's quarantine reason (selects the infix).

    Returns:
        The iso token, or the empty string if the expected infix is absent
        (a defensive fallback; :func:`list_quarantines` only calls this for
        names it has already matched by infix).
    """
    if reason == "mode_switch":
        infix = _MODE_SWITCH_INFIX
    elif kind == "db":
        infix = _DB_QUARANTINE_INFIX
    else:
        infix = _BODY_QUARANTINE_INFIX
    _, _, after = name.partition(infix)
    if not after:
        return ""
    return after[: -len(".db")] if kind == "db" and after.endswith(".db") else after


def _file_size_or_zero(path: Path) -> int:
    """Return ``path``'s size, or 0 when it vanished mid-scan.

    Args:
        path: The file to stat.

    Returns:
        The size in bytes, or 0 on a stat failure (operator removed it).
    """
    try:
        return path.stat().st_size
    except OSError:
        return 0


def list_quarantines(data_root: Path) -> list[QuarantineInventoryEntry]:
    """Enumerate backups (one entry each) plus unmanifested anomalies.

    Manifest-driven (seam 2): every ``backup.<backup_id>.manifest.json``
    under ``data_root`` yields ONE entry per BACKUP, with ``has_db`` /
    ``has_body`` reporting whether each declared artifact is present on disk
    RIGHT NOW (the restorability truth) and ``bytes`` summing the present
    artifacts.

    A second flat scan (``iterdir``, not recursive) surfaces ANOMALIES:
    on-disk artifacts matching the quarantine naming convention
    (``.corrupted.`` / ``.quarantine.`` / ``.mode_switch.`` infixes) that no
    manifest claims. Each yields one flagged, non-restorable entry; the
    reason and display stamp are recovered from the name for OPERATOR
    DISPLAY only (never identity; cycle-7 forbidden-pattern rule). The
    in-progress marker file (:data:`BACKUP_MOVE_MARKER_NAME`), the manifest
    files themselves, and DB ``-wal``/``-shm`` stragglers are never reported
    as artifacts.

    Entries are sorted backups-first by ``(iso_display, backup_id)``, then
    anomalies by path name, so the listing is deterministic for operators
    and tests.

    Args:
        data_root: The directory holding the quarantine artifacts (the
            per-instance ``data_root``).

    Returns:
        A list of :class:`QuarantineInventoryEntry` — empty when
        ``data_root`` is missing or contains no backups or anomalies.
    """
    if not data_root.exists():
        return []
    backups: list[QuarantineInventoryEntry] = []
    claimed: set[Path] = set()
    for manifest in _iter_backup_manifests(data_root):
        has_db = manifest.db_path.exists()
        has_body = manifest.body_path is not None and manifest.body_path.exists()
        total_bytes = 0
        if has_db:
            total_bytes += _file_size_or_zero(manifest.db_path)
        if has_body and manifest.body_path is not None:
            total_bytes += _dir_byte_sum(manifest.body_path)
        claimed.add(manifest.db_path)
        if manifest.body_path is not None:
            claimed.add(manifest.body_path)
        backups.append(
            QuarantineInventoryEntry(
                backup_id=manifest.backup_id,
                reason=manifest.reason,
                iso_display=manifest.iso_display,
                db_path=manifest.db_path,
                body_path=manifest.body_path,
                has_db=has_db,
                has_body=has_body,
                bytes=total_bytes,
                anomaly=False,
            )
        )

    anomalies: list[QuarantineInventoryEntry] = []
    for entry in data_root.iterdir():
        name = entry.name
        if name == BACKUP_MOVE_MARKER_NAME:
            continue
        if name.startswith(_MANIFEST_PREFIX) and name.endswith(_MANIFEST_SUFFIX):
            continue
        if entry in claimed:
            continue
        is_db_file = name.endswith(".db") and entry.is_file()
        is_dir = entry.is_dir()
        # mode_switch uses one infix for both kinds; corruption uses two.
        reason: QuarantineReason
        kind: Literal["db", "body_store"]
        if _MODE_SWITCH_INFIX in name and (is_db_file or is_dir):
            reason = "mode_switch"
            kind = "db" if is_db_file else "body_store"
        elif _DB_QUARANTINE_INFIX in name and is_db_file:
            reason = "corrupted"
            kind = "db"
        elif _BODY_QUARANTINE_INFIX in name and is_dir:
            reason = "corrupted"
            kind = "body_store"
        else:
            continue
        anomalies.append(
            QuarantineInventoryEntry(
                backup_id=None,
                reason=reason,
                iso_display=_parse_quarantine_iso(name, kind, reason),
                db_path=entry if kind == "db" else None,
                body_path=entry if kind == "body_store" else None,
                has_db=kind == "db",
                has_body=kind == "body_store",
                bytes=_file_size_or_zero(entry) if kind == "db" else _dir_byte_sum(entry),
                anomaly=True,
            )
        )

    backups.sort(key=lambda e: (e.iso_display, str(e.backup_id)))
    anomalies.sort(key=lambda e: str(e.db_path or e.body_path))
    return backups + anomalies


# ---------------------------------------------------------------------
# Class facade (plan § 5.2.2 — composition-root injection point).
# ---------------------------------------------------------------------


class IntegrityChecker:
    """Composition-root-facing integrity-check orchestrator.

    ``app.py``'s lifespan constructs one :class:`IntegrityChecker`
    per instance at startup and calls :meth:`check` before opening
    the SqliteUploadStore. On corruption, :meth:`quarantine_now`
    moves the DB + body-store root aside and increments the
    ``db_quarantine_total`` counter; the caller decides whether to
    proceed (``fail_open``) or abort startup.

    Attributes:
        db_path: Persistent SQLite path.
        body_store_root: Body-store root directory.
        data_root: The Phantom ``storage.data_dir`` directory. Used
            for :meth:`list_quarantines` inventory queries.
    """

    def __init__(
        self,
        *,
        db_path: Path,
        body_store_root: Path,
        data_root: Path,
    ) -> None:
        """Store paths; no side effects at construction.

        Args:
            db_path: Persistent SQLite path.
            body_store_root: Body-store root directory.
            data_root: The Phantom ``storage.data_dir`` directory.
        """
        self._db_path = db_path
        self._body_store_root = body_store_root
        self._data_root = data_root

    async def check(self) -> IntegrityCheckResult:
        """Run :func:`check_integrity` against the configured DB path."""
        return await check_integrity(self._db_path)

    def quarantine_now(self, timestamp: datetime | None = None) -> BackupManifest:
        """Quarantine the configured DB + body-store root.

        Wrapper around :func:`quarantine` keyed off the constructor
        paths. Returns the written :class:`BackupManifest`.

        Args:
            timestamp: Optional UTC timestamp override; defaults to now.
        """
        return quarantine(self._db_path, self._body_store_root, timestamp)

    def list_quarantines(self) -> list[QuarantineInventoryEntry]:
        """Enumerate quarantine artifacts under the data root."""
        return list_quarantines(self._data_root)


__all__ = [
    "BACKUP_ID_FILENAME_TOKEN_LENGTH",
    "BACKUP_MOVE_MARKER_NAME",
    "BackupManifest",
    "BackupMoveMarker",
    "IntegrityCheckResult",
    "IntegrityChecker",
    "QuarantineInventoryEntry",
    "QuarantineReason",
    "RestoreMoveOutcome",
    "backup_manifest_path",
    "check_integrity",
    "isolate_db_file",
    "list_quarantines",
    "load_backup_manifest",
    "quarantine",
    "quarantine_paths",
    "reconcile_interrupted_backup_move",
    "restore_mode_switch_backup",
]
