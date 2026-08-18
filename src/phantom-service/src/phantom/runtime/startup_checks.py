"""Startup hardening seam — the single typed home for the boot-time guards.

This module is the one place the production composition root
(:func:`phantom.app.create_app`'s ``lifespan``) reaches for the
five startup behaviors that defend a Pi-class deployment:

* :func:`apply_umask` — process-wide bare-metal file-perm hardening
  (WS-4 F6): every file the service creates after this call is
  owner-only (``0o600`` / ``0o700``).
* :func:`check_retention_floor` — process-wide config invariant
  (plan § 4.2.4): bodies must not be retained longer than their
  metadata row, else a reaped row orphans its body file. It is the one
  guard here with a SECOND caller: ``apply_reload`` re-runs it between
  the YAML load and the swap (F14), where a violation rejects the
  reload rather than crashing the process.
* :func:`check_instance_isolation` — process-wide config invariant
  (plan § 9.10.1): every configured instance must be *completely
  isolated* (unique id, its own storage partition, unambiguous
  routing). Fail-closed on a duplicate ``id``, a colliding/nested
  ``data_dir``, or a duplicate ``host_prefix``.
* :func:`check_body_store_mode` — per-instance back-up-and-run guard
  (findings A-3 + F-2, plan § 1.2 / ADR-025): when ``all_ram`` is
  selected over a populated on-disk body tree it relocates the live DB +
  body tree to a recoverable ``mode_switch`` quarantine pair and boots
  fresh, rather than refusing to boot or silently corrupting the data.
* :func:`run_integrity_gate` — per-instance DB-corruption gate
  (plan § 5.2.2): runs ``PRAGMA integrity_check`` before the store
  opens, quarantines a corrupt DB + body tree, bumps
  ``db_quarantine_total``, and either proceeds fresh (``fail_open``)
  or aborts startup.

It also owns the single mode-wiring decision table
(:func:`build_body_store`) so the ``hybrid`` / ``all_ram`` / ``all_disk``
composition lives in exactly one place — no second copy to drift.

Reused, never duplicated: :class:`phantom.storage.integrity.IntegrityChecker`,
:class:`phantom.workers.cold_backup.ColdBackupScheduler`, and the
:class:`ConfigInvariantError` exception defined here.

Provenance: relocated from ``runtime/composition.py`` during the
2026-05-29 refinement (Option B, ADR pending) — these guards previously
ran only in the dead-path ``compose_and_run``, which R-2 then deleted.
``app.py``'s lifespan is now the one production composition root.
"""

from __future__ import annotations

import dataclasses
import enum
import logging
import os
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Literal, assert_never

import aiosqlite

from phantom.config.settings import Settings
from phantom.storage.file_body_store import FileBodyStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.storage.integrity import BackupManifest, IntegrityChecker, quarantine
from phantom.storage.interface import BodyStore
from phantom.storage.ram_body_store import RamBodyStore
from phantom.storage.sqlite_store import _SCHEMA_PATH, SCHEMA_VERSION
from phantom.workers.persist_controller import PersistController

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from phantom.config.settings import InstanceCfg
    from phantom.observability.metrics import MetricsRegistry
    from phantom.storage.sqlite_store import SqliteUploadStore

logger = logging.getLogger(__name__)

# Owner-only umask for bare-metal deploys (WS-4 F6): strip group + world
# bits so buffered bodies + the SQLite DB are created ``0o600``/``0o700``.
UMASK_OWNER_ONLY: int = 0o077

# Canonical ``db_quarantine_total`` metric identity. Registered on the
# process-wide MetricsRegistry by the composition root AND (idempotently)
# by :func:`run_integrity_gate`, so the gate can bump it without the
# caller having pre-registered it.
DB_QUARANTINE_COUNTER_NAME: str = "db_quarantine_total"
DB_QUARANTINE_COUNTER_DESCRIPTION: str = (
    "Startup quarantine events on DB corruption (plan § 5.2.2)."
)

# Canonical ``mode_switch_backup_total`` metric identity (plan § 1.3),
# mirroring the ``db_quarantine_total`` precedent. Registered canonically on
# the process-wide MetricsRegistry by the composition root AND bumped from
# two startup paths: :func:`check_body_store_mode` (a backup completed in one
# boot) and ``app.py``'s ``reconcile_interrupted_backup_move`` wiring (a
# backup interrupted by a crash and finished on the next boot). Like all
# counters in ``observability/metrics.py`` it is in-process and resets on
# restart, so cross-restart exactness is not a goal.
MODE_SWITCH_BACKUP_COUNTER_NAME: str = "mode_switch_backup_total"
MODE_SWITCH_BACKUP_COUNTER_DESCRIPTION: str = (
    "Mode-switch back-up-and-run events (an unsafe all_ram-over-populated-disk "
    "switch backed up the live data and booted fresh; plan § 1.2 / ADR-025)."
)

# Canonical ``schema_discard_total`` metric identity (plan § 4S.2), mirroring
# the ``db_quarantine_total`` / ``mode_switch_backup_total`` precedent.
# Registered canonically on the process-wide MetricsRegistry by the
# composition root and bumped at the ``run_schema_gate`` call site when a
# pre-version / wrong-schema DB is deleted. In-process like every counter in
# ``observability/metrics.py`` (resets on restart): an interrupted-then-
# finished discard simply re-fires on the next boot, and there is no
# reconciliation path (nothing is preserved), so there is no cross-counting
# concern.
SCHEMA_DISCARD_COUNTER_NAME: str = "schema_discard_total"
SCHEMA_DISCARD_COUNTER_DESCRIPTION: str = (
    "Schema-discard boot events (a pre-version or wrong-schema database was "
    "deleted and the instance booted fresh; plan § 4S.2 / ADR-025's scoped "
    "population-of-zero exception)."
)

# The :class:`phantom.config.settings.BodyStoreCfg.mode` Literal — kept
# inline so callers can narrow on a match without importing the model.
BodyStoreMode = Literal["hybrid", "all_ram", "all_disk"]


def _derive_expected_uploads_columns() -> frozenset[str]:
    """Derive the ``uploads`` column-name set from ``schema.sql`` at import.

    Builds a throwaway in-memory SQLite, applies ``storage/schema.sql``'s DDL,
    and reads ``frozenset(name for PRAGMA table_info(uploads))``. This makes
    ``schema.sql`` the SOLE source of the expected column set — there is no
    hand-maintained name list to drift out of sync (the deletion test: a
    duplicated frozenset would need a mirror test to guard it; deriving removes
    the duplicate entirely). Synchronous :mod:`sqlite3` (not ``aiosqlite``) is
    used so the derivation runs at module import without an event loop.

    Returns:
        The frozenset of column names declared on the ``uploads`` table.
    """
    conn = sqlite3.connect(":memory:")
    try:
        conn.row_factory = sqlite3.Row
        conn.executescript(_SCHEMA_PATH.read_text(encoding="utf-8"))
        return frozenset(row["name"] for row in conn.execute("PRAGMA table_info(uploads)"))
    finally:
        conn.close()


# The columns the code reads off the ``uploads`` table, DERIVED from
# ``schema.sql`` ONCE at import (single source of truth, no duplicate to
# drift; plan § 4S.2 / § 4S.6). The schema gate's check is a SUBSET test
# (``EXPECTED_UPLOADS_COLUMNS <= observed_columns``): the on-disk table must
# CONTAIN every column the code reads. A missing required column -> discard; an
# EXTRA/future column with all required present -> tolerated (it never crashes
# a name-keyed read in ``_row_to_upload``). Comparison is column-NAME only;
# type/constraint changes ride the ``SCHEMA_VERSION`` bump (a future
# migration), not this never-crash gate.
EXPECTED_UPLOADS_COLUMNS: frozenset[str] = _derive_expected_uploads_columns()


class IntegrityFailClosedError(RuntimeError):
    """The DB failed ``PRAGMA integrity_check`` and ``fail_open=False`` forbids running.

    Raised by :func:`run_integrity_gate` AFTER quarantining the corrupt
    artifacts, when the operator's ``db_integrity.fail_open=false`` selects
    the strict posture. This is a DELIBERATE abort, not a fault to degrade:
    ADR-025 explicitly preserves the corruption fail-closed escape hatch
    (the operator chose "do not run without my data"), so the cycle-7 typed
    boot outcome (seam 3) lets this exception propagate and stop the process
    rather than mapping it to a :class:`DegradeReason`. Subclasses
    ``RuntimeError`` so historical handlers and tests keep matching.
    """


# ---------------------------------------------------------------------
# Cycle-7 seam 3 - the typed per-instance boot outcome.
# ---------------------------------------------------------------------


class DegradeReason(enum.Enum):
    """Why one instance booted DEGRADED (terminal until restart; seam 3).

    Each member corresponds to a classified fault at one stage of the
    per-instance boot pipeline in ``phantom.app._build_instance_context``
    (directory prep, integrity gate, backup reconcile, mode guard, schema
    gate, upload-store open, token-cache open). The boot loop folds the
    typed ``BootOutcome`` union exhaustively, so an UNenumerated fault
    cannot silently join the degrade family: a new member without a match
    arm in :func:`degrade_action_hint` fails mypy strict.

    Two per-instance boot refusals are DELIBERATELY not degrade reasons:

    * :class:`IntegrityFailClosedError` - the operator's
      ``fail_open=false`` strict abort (ADR-025's preserved hatch).
    * An unclassified error over a probe-confirmed WRITABLE substrate
      (e.g. a transient ``mkdir`` failure on a healthy directory) - a real,
      unexplained fault is re-raised loudly, never silently degraded.

    Members:
        SUBSTRATE_UNWRITABLE: The instance's ``data_root`` is unwritable
            (probe-confirmed: read-only, full, or a non-directory in the
            way) at directory prep or at either DB open. No writable disk
            means no durable buffering; the one physics boundary.
        QUARANTINE_BACKUP_FAILED: The integrity gate found corruption but
            the quarantine backup move itself failed, so the corrupt data
            could not be preserved aside; booting fresh anyway would risk
            the very data the gate protects.
        BACKUP_RECONCILE_FAILED: An in-progress backup/restore marker was
            found but finishing the move forward failed (unreadable marker
            or manifest, or a move fault); booting over a half-moved tree
            would be the A-3 data loss the marker exists to prevent.
        MODE_SWITCH_BACKUP_FAILED: The all_ram-over-populated-disk guard
            decided to back up and the backup move failed; booting all_ram
            anyway would condemn the disk-resident rows (A-3).
        SCHEMA_GATE_FAILED: Probing or discarding a pre-version DB failed,
            so the schema gate could not guarantee a safe shape to open.
        DB_UNRECOVERABLE: The uploads DB open failed past EVERY recovery
            (transient-lock ride-out, isolate aside, fresh recreate) on a
            writable substrate; the instance's primary database storage is
            unrecoverable at boot.
        STORE_OPEN_FAILED: The instance's auxiliary store (the token
            cache) failed open past the same recovery ladder on a writable
            substrate; the detail names the store.
    """

    SUBSTRATE_UNWRITABLE = "substrate_unwritable"
    QUARANTINE_BACKUP_FAILED = "quarantine_backup_failed"
    BACKUP_RECONCILE_FAILED = "backup_reconcile_failed"
    MODE_SWITCH_BACKUP_FAILED = "mode_switch_backup_failed"
    SCHEMA_GATE_FAILED = "schema_gate_failed"
    DB_UNRECOVERABLE = "db_unrecoverable"
    STORE_OPEN_FAILED = "store_open_failed"


@dataclasses.dataclass(frozen=True)
class DegradedInstance:
    """One instance's typed DEGRADED boot outcome (seam 3).

    Returned by ``phantom.app._build_instance_context`` instead of a context
    when a classified per-instance fault means the instance cannot serve.
    The boot loop folds these into the degraded set the ready/status
    surfaces and the ``POST /v1/send`` guard read; there is no side-channel
    dict. Degrade is terminal until restart.

    Attributes:
        instance_id: The configured instance id that booted degraded.
        reason: The classified :class:`DegradeReason`.
        detail: Operator-facing fault description (the failing stage plus
            the original error).
    """

    instance_id: str
    reason: DegradeReason
    detail: str


def degrade_action_hint(reason: DegradeReason) -> str:
    """Return the operator's next action for one :class:`DegradeReason`.

    The exhaustive ``match`` (closed by ``assert_never``) is the seam-3
    enforcement point: adding a :class:`DegradeReason` member without
    deciding its operator guidance fails mypy strict, so the next fault
    class surfaces at type-check time, not as an unexplained runtime state.

    Args:
        reason: The degrade reason to describe.

    Returns:
        A one-line operator action, embedded in degrade logs and the
        ready/health detail strings.
    """
    match reason:
        case DegradeReason.SUBSTRATE_UNWRITABLE:
            return (
                "make the instance data_dir writable (mount, permissions, or "
                "free space) and restart"
            )
        case DegradeReason.QUARANTINE_BACKUP_FAILED:
            return (
                "inspect the instance data_dir (the corrupt DB could not be "
                "moved aside), resolve the filesystem fault, and restart"
            )
        case DegradeReason.BACKUP_RECONCILE_FAILED:
            return (
                "inspect the in-progress backup marker and manifest in the "
                "instance data_dir, resolve the filesystem fault, and restart"
            )
        case DegradeReason.MODE_SWITCH_BACKUP_FAILED:
            return (
                "the all_ram switch could not back up the populated disk "
                "tree; revert body_store.mode to a disk-backed mode or "
                "resolve the filesystem fault, then restart"
            )
        case DegradeReason.SCHEMA_GATE_FAILED:
            return (
                "the on-disk database could not be probed or discarded for "
                "the schema gate; inspect the DB file, then restart"
            )
        case DegradeReason.DB_UNRECOVERABLE:
            return (
                "the uploads database could not be opened, isolated, or "
                "recreated; inspect the data_dir and the quarantine "
                "inventory, then restart"
            )
        case DegradeReason.STORE_OPEN_FAILED:
            return (
                "the token cache database could not be opened, isolated, or "
                "recreated; inspect the data_dir and the quarantine "
                "inventory, then restart"
            )
        case _:
            assert_never(reason)


class ConfigInvariantError(ValueError):
    """Raised when a load-bearing config invariant fails at startup.

    Plan § 4.2.4: composition-time assertions catch validator-permitted
    but runtime-incompatible YAML configurations. The asserted
    invariants are:

    * **Retention floor** (:func:`check_retention_floor`):
      ``retention.body_seconds`` must not exceed
      ``retention.metadata_seconds`` for any state — bodies cannot
      outlive their metadata row.
    * **Instance isolation** (:func:`check_instance_isolation`):
      every instance must have a unique ``id``, a non-colliding /
      non-nested ``data_dir``, and a unique ``host_prefix``.

    NOTE: the ``all_ram``-over-populated-disk case (findings A-3 + F-2) is
    NO LONGER a :class:`ConfigInvariantError`. Plan § 1.2 / ADR-025 changed
    :func:`check_body_store_mode` from refuse-to-boot to back-up-and-run: it
    relocates the live data to a recoverable ``mode_switch`` quarantine pair
    and boots fresh instead of raising.

    Other invariants are deliberately NOT runtime-asserted because the
    structural enforcement (pre-commit greps, single-writer manifest)
    is the primary defense:

    * TaskGroup membership: pre-commit grep
      ``forbid-asyncio-create-task-outside-composition`` (plan § 0.3).
    * Single-writer body_location='file' transition: pre-commit grep
      against ``mark_persisted(`` calls outside persist_controller.py
      (plan § 4.2.6).
    * ``auto_vacuum != FULL``: SQLite pragma is hardcoded NONE in
      :class:`phantom.storage.sqlite_store.SqliteUploadStore` + the
      defensive pragma readback at open time.
    """


# ---------------------------------------------------------------------
# Process-wide guards (run ONCE at the top of the lifespan).
# ---------------------------------------------------------------------


def apply_umask() -> None:
    """Set the process umask so startup-created files are owner-only.

    Bare-metal trust boundary (WS-4 F6). Called once, process-wide,
    before any instance directory or store is created — every file the
    service writes (the SQLite DB, its WAL/SHM, buffered body files,
    the token cache) is then created ``0o600`` / ``0o700`` rather than
    inheriting a potentially group/world-readable process default.
    """
    os.umask(UMASK_OWNER_ONLY)


def check_retention_floor(settings: Settings) -> None:
    """Validate the retention-floor invariant before workers start.

    For every terminal state, ``body_seconds`` must not exceed
    ``metadata_seconds``: the reaper drops the body file at
    ``body_seconds`` then the metadata row at ``metadata_seconds``, so
    a body outliving its row would orphan the body file after the row
    vanished. Raises :class:`ConfigInvariantError` on any violation.

    TWO callers, with two different consequences (F14). At BOOT
    (``phantom.app``'s lifespan) the exception propagates out and
    crashes startup before any worker spawns. At RELOAD
    (:func:`phantom.runtime.reload.apply_reload`, between the YAML load
    and any swap) it is a member of ``RELOAD_FAILURE_ERRORS``, so the
    reload is rejected whole and the previous config keeps running: 422
    ``envelope_invalid`` on the admin route, log-and-keep-previous on
    SIGHUP. The second door exists because the reaper live-reads
    retention on every sweep, so an inverted window installed by a
    reload bites immediately, and the RAM bodies it strands are
    unreclaimable.

    The ``-1`` sentinel means "forever": a ``-1`` metadata window is a
    forever-row, so any finite body window is fine; a ``-1`` body window
    over a finite metadata window is rejected (bodies would outlive the
    reaped row).

    Args:
        settings: The validated :class:`Settings` to check. Must have
            been resolved (probe-fillable knobs populated).

    Raises:
        ConfigInvariantError: When ``retention.<state>_body_seconds``
            exceeds ``retention.<state>_metadata_seconds`` for any
            terminal state.
    """
    retention = settings.retention
    # Per-state pairs from RetentionCfg.
    retention_pairs: list[tuple[str, int | None, int | None]] = [
        ("succeeded", retention.succeeded_body_seconds, retention.succeeded_metadata_seconds),
        ("failed", retention.failed_body_seconds, retention.failed_metadata_seconds),
        ("cancelled", retention.cancelled_body_seconds, retention.cancelled_metadata_seconds),
        ("stored", retention.stored_body_seconds, retention.stored_metadata_seconds),
        (
            "auth_expired",
            retention.auth_expired_body_seconds,
            retention.auth_expired_metadata_seconds,
        ),
        ("corrupted", retention.corrupted_body_seconds, retention.corrupted_metadata_seconds),
        # NB: this list is (state, body, metadata) — OPPOSITE of the reaper's
        # (state, metadata, body). ADR-032's ``expired`` pair, in THIS order:
        ("expired", retention.expired_body_seconds, retention.expired_metadata_seconds),
    ]
    for state, body_seconds, metadata_seconds in retention_pairs:
        # Validator guarantees every retention field is non-None
        # post-resolution. Skip if either is still unresolved (the
        # surfaceable assertion is the body > metadata violation).
        if body_seconds is None or metadata_seconds is None:
            continue
        # A -1 metadata implies a forever-row, so any finite body is fine.
        if metadata_seconds < 0:
            continue
        if body_seconds < 0:
            # Body kept forever but metadata expires — bodies outlive
            # rows. Reject.
            msg = (
                f"retention.{state}_body_seconds={body_seconds} (forever) "
                f"exceeds retention.{state}_metadata_seconds={metadata_seconds} "
                f"(rows would be reaped while bodies persist)"
            )
            raise ConfigInvariantError(msg)
        if body_seconds > metadata_seconds:
            msg = (
                f"retention.{state}_body_seconds={body_seconds} must be "
                f"<= retention.{state}_metadata_seconds={metadata_seconds}"
            )
            raise ConfigInvariantError(msg)


def check_instance_isolation(instances: Sequence[InstanceCfg]) -> None:
    """Fail closed unless every instance is completely isolated.

    Plan § 9.10.1 (human-approved 2026-05-29). Each instance must have a
    unique identity, its own storage partition, and unambiguous routing.
    Without this guard a duplicate ``id`` silently collapses in the
    dispatcher's ``_by_id`` map (an instance becomes unreachable) and a
    shared ``data_dir`` puts two stores on one ``uploads.db`` (a
    single-writer violation that corrupts the DB). This runs once,
    process-wide, at the top of the lifespan — before any instance
    context is built.

    Three collisions are rejected:

    * **Duplicate ``id``** — exact string match.
    * **Colliding ``data_dir``** — compared after :meth:`Path.resolve`
      normalization (so ``foo`` == ``./foo`` == ``foo/``). Rejected on
      an exact match AND on true path-*component* nesting (one resolved
      path is an ancestor of another via :attr:`Path.parents`), so
      siblings ``a/b`` and ``a/bc`` stay distinct while ``a`` nested
      inside ``a/b`` is caught. Raw-string prefix comparison is
      deliberately NOT used (it both missed ``./foo`` dups and falsely
      rejected sibling prefixes).
    * **Duplicate ``host_prefix``** — lower-cased before comparing
      (matching the dispatcher's ``.lower()``), exact-match only. Glob
      *overlap* (e.g. ``*.example.com`` vs ``api.example.com``) stays
      permitted by declaration order — first-match-wins is intentional,
      not a collision.

    Args:
        instances: The configured instances, in declaration order.

    Raises:
        ConfigInvariantError: On the first collision found, naming the
            offending ``id`` / ``data_dir`` / ``host_prefix``.
    """
    seen_ids: set[str] = set()
    # resolved data_dir -> the instance id that first claimed it.
    resolved_dirs: dict[Path, str] = {}
    # lower-cased host_prefix -> the instance id that first claimed it.
    seen_prefixes: dict[str, str] = {}

    for cfg in instances:
        # 1. Duplicate id.
        if cfg.id in seen_ids:
            msg = (
                f"Duplicate instance id {cfg.id!r}: instance ids must be unique "
                f"(a duplicate would silently collapse in the dispatcher's id map, "
                f"making one instance unreachable). Rename one instance."
            )
            raise ConfigInvariantError(msg)
        seen_ids.add(cfg.id)

        # 2. Colliding / nested data_dir (resolve-normalized).
        resolved = Path(cfg.data_dir).resolve()
        for other_dir, other_id in resolved_dirs.items():
            if resolved == other_dir:
                msg = (
                    f"Instances {other_id!r} and {cfg.id!r} share data_dir "
                    f"{resolved} (two stores on one uploads.db is a single-writer "
                    f"violation that corrupts the DB). Give each instance its own "
                    f"data_dir."
                )
                raise ConfigInvariantError(msg)
            # True component nesting in either direction — an instance's
            # data_dir must not live inside another's storage partition.
            if other_dir in resolved.parents:
                msg = (
                    f"Instance {cfg.id!r} data_dir {resolved} is nested inside "
                    f"instance {other_id!r} data_dir {other_dir} (overlapping "
                    f"storage partitions). Give each instance an independent "
                    f"data_dir."
                )
                raise ConfigInvariantError(msg)
            if resolved in other_dir.parents:
                msg = (
                    f"Instance {other_id!r} data_dir {other_dir} is nested inside "
                    f"instance {cfg.id!r} data_dir {resolved} (overlapping storage "
                    f"partitions). Give each instance an independent data_dir."
                )
                raise ConfigInvariantError(msg)
        resolved_dirs[resolved] = cfg.id

        # 3. Duplicate host_prefix (lower-cased, exact match).
        for prefix in cfg.host_prefixes:
            key = prefix.lower()
            if key in seen_prefixes:
                msg = (
                    f"Instances {seen_prefixes[key]!r} and {cfg.id!r} both declare "
                    f"host_prefix {prefix!r} (case-insensitive): an exact-duplicate "
                    f"prefix makes routing ambiguous. Glob overlap is permitted by "
                    f"declaration order, but identical prefixes are not. Remove the "
                    f"duplicate."
                )
                raise ConfigInvariantError(msg)
            seen_prefixes[key] = cfg.id


# ---------------------------------------------------------------------
# Per-instance guards (run for EACH instance, after data_root.mkdir,
# before the store opens).
# ---------------------------------------------------------------------


def _disk_body_chain_dir_count(bodies_root: Path) -> int:
    """Count chain_id body directories under a FileBodyStore root.

    Mirrors :meth:`FileBodyStore.list_chain_ids`'s layout walk
    (``<root>/<shard>/<chain_id>/...``) but only needs a presence count,
    so it counts every chain dir found. Dot-prefixed shards (the
    ``.tmp`` staging dir) and non-directory entries are skipped; a chain
    dir is any subdirectory of a shard. The chain dir's name is NOT
    required to be a valid UUID here — any leftover upload tree counts as
    "populated" for the fail-closed decision.

    Args:
        bodies_root: The per-instance ``<data_root>/bodies`` path.

    Returns:
        The number of chain directories found (the caller only checks
        ``> 0``).
    """
    if not bodies_root.is_dir():
        return 0
    count = 0
    for shard in bodies_root.iterdir():
        if not shard.is_dir() or shard.name.startswith("."):
            continue
        for upload_dir in shard.iterdir():
            if upload_dir.is_dir():
                count += 1
    return count


def check_body_store_mode(
    *, mode: BodyStoreMode, bodies_root: Path, db_path: Path
) -> BackupManifest | None:
    """Back up and run on ``all_ram`` over a populated FileBodyStore root.

    Findings A-3 + F-2 (shared root cause). ``all_ram`` wires a
    :class:`RamBodyStore` with no disk half. If a prior ``hybrid`` /
    ``all_disk`` deployment left body files under ``bodies_root``,
    booting ``all_ram`` directly would:

    * condemn every ``body_location='file'`` row to ``corrupted`` —
      recovery's ``has_body_ref`` walk asks the RamBodyStore (zero disk
      knowledge), so physically-intact bytes are declared missing
      (A-3 silent data loss); AND
    * leak those bytes forever — no :class:`BodyOrphanJanitor` spawns in
      ``all_ram`` (F-2 disk leak).

    Plan § 1.2 / ADR-025: rather than refusing to boot (the prior
    behavior) OR silently corrupting + leaking, this guard now BACKS UP AND
    RUNS. When ``all_ram`` is selected over a populated tree it relocates the
    live DB + body tree to a recoverable ``mode_switch`` quarantine pair via
    :func:`phantom.storage.integrity.quarantine` (reason=``mode_switch``,
    which writes an in-progress marker so a crash mid-backup is finished
    forward on the next boot) and returns the destinations, leaving a clean
    empty live tree the service boots fresh over. The operator restores the
    backup with the one-call admin restore route after switching back to a
    disk-backed mode. Other modes (``hybrid`` / ``all_disk``) carry a
    FileBodyStore that handles the pre-existing tree correctly, so the guard
    is ``all_ram`` only.

    This function is a pure decision-plus-move with no logging coupling: the
    WARNING log and the ``mode_switch_backup_total`` counter bump live in the
    composition root (``app.py``, plan § 1.3), driven by a non-``None``
    return.

    Args:
        mode: The configured ``storage.body_store.mode``.
        bodies_root: The per-instance ``<data_root>/bodies`` path
            (the production layout — NOT ``body_store/``).
        db_path: The per-instance ``<data_root>/uploads.db`` path — backed
            up alongside ``bodies_root`` so the healthy DB and its body tree
            are preserved as one recoverable pair.

    Returns:
        ``None`` when no backup was needed (a non-``all_ram`` mode, or an
        ``all_ram`` mode over an empty/absent tree); otherwise the
        :class:`phantom.storage.integrity.BackupManifest` of the
        ``mode_switch`` backup (cycle-7 seam 2: the manifest names both
        artifact destinations and carries the backup's uuid identity).
    """
    if mode != "all_ram":
        return None
    if _disk_body_chain_dir_count(bodies_root) == 0:
        return None
    return quarantine(db_path, bodies_root, reason="mode_switch")


async def run_integrity_gate(
    *,
    db_path: Path,
    bodies_root: Path,
    data_root: Path,
    fail_open: bool,
    metrics_registry: MetricsRegistry,
) -> None:
    """Run the DB integrity gate before the persistent store opens.

    Plan § 5.2.2 / strategy §3 ("service never fails on bad SQLite").
    Constructs an :class:`IntegrityChecker` for this instance's paths,
    runs ``PRAGMA integrity_check``, and on failure quarantines the live
    DB + body tree into timestamped artifacts, bumps the
    ``db_quarantine_total`` counter, and then either proceeds (the
    caller opens a fresh empty SQLite + body tree when ``fail_open``) or
    raises :class:`RuntimeError` to abort startup. The corrupted
    artifacts survive on disk for operator retrieval via the
    ``/admin/quarantine`` endpoint (plan § 5.2.5).

    Must run AFTER ``data_root.mkdir(...)`` but BEFORE the
    :class:`SqliteUploadStore` opens and before body-store construction.

    Args:
        db_path: This instance's ``<data_root>/uploads.db``.
        bodies_root: This instance's ``<data_root>/bodies`` directory
            (the production layout).
        data_root: This instance's storage root (used by the checker's
            quarantine inventory).
        fail_open: When ``True`` (strategy §3 default) the service
            proceeds fresh after quarantining; when ``False`` it raises
            after quarantining so the process exits.
        metrics_registry: Process-wide registry; the gate idempotently
            registers + bumps ``db_quarantine_total`` on it.

    Raises:
        IntegrityFailClosedError: When the integrity check failed and
            ``fail_open`` is ``False`` (the quarantine still fired). The
            deliberate operator abort ADR-025 preserves; NOT a degrade.
    """
    db_quarantine_total = metrics_registry.register_counter(
        DB_QUARANTINE_COUNTER_NAME,
        DB_QUARANTINE_COUNTER_DESCRIPTION,
    )
    integrity_checker = IntegrityChecker(
        db_path=db_path,
        body_store_root=bodies_root,
        data_root=data_root,
    )
    integrity_result = await integrity_checker.check()
    if integrity_result.ok:
        return
    logger.error(
        "DB integrity check failed (%s) — quarantining live artifacts",
        integrity_result.message,
    )
    integrity_checker.quarantine_now()
    await db_quarantine_total.inc()
    if not fail_open:
        msg = (
            "DB integrity check failed and db_integrity.fail_open=False; "
            f"refusing to start. Diagnostic: {integrity_result.message}"
        )
        raise IntegrityFailClosedError(msg)


# ---------------------------------------------------------------------
# Schema gate (per-instance, runs after the mode guard, before the store
# opens) + the future-migration seam. Plan § 4S.2 / § 4S.3 / ADR-025.
# ---------------------------------------------------------------------


class SchemaAction(enum.Enum):
    """What the boot-time schema gate decides to do with the on-disk DB.

    Plan § 4S.2. An enum (not a raw string) per the coding standard — a
    categorical value with a known, closed set.

    Members:
        BOOT_AS_IS: The on-disk schema matches ``SCHEMA_VERSION`` and carries
            every column the code reads, OR the DB is fresh (no file / no
            ``uploads`` table). Open it normally.
        DISCARD_AND_BOOT_FRESH: A mismatch (pre-version stamp, or a missing
            required column). Delete the database and boot empty. Justified by
            the population-of-zero fact (no field DB can hold real undelivered
            uploads yet); see :func:`decide_schema_action` and § 4S.0.
    """

    BOOT_AS_IS = "boot_as_is"
    DISCARD_AND_BOOT_FRESH = "discard_and_boot_fresh"


@dataclasses.dataclass(frozen=True)
class SchemaGateResult:
    """The outcome of :func:`run_schema_gate` for one instance.

    Returned for testability and for the WARNING log payload at the ``app.py``
    call site (the gate itself does not log or count).

    Attributes:
        action: The decided :class:`SchemaAction`.
        observed_version: The ``PRAGMA user_version`` read off the DB (``0``
            for a fresh / non-existent file or an unstamped DB).
        observed_columns: The ``uploads`` column-name set read off the DB
            (empty for a fresh / non-existent file or a present-file-without-
            table).
        discarded_path: The deleted DB path when a discard fired, else
            ``None``.
    """

    action: SchemaAction
    observed_version: int
    observed_columns: frozenset[str]
    discarded_path: Path | None


@dataclasses.dataclass(frozen=True)
class SchemaMigration:
    """A forward, in-place schema migration step (the future-migration seam).

    Plan § 4S.3. This is the seam for forward schema migrations. Until a
    migration is registered in :data:`SCHEMA_MIGRATIONS`, a non-current
    database is DELETED (population of zero) rather than migrated — see
    :func:`decide_schema_action`. Defined now so the seam is concrete and a
    future migration is a localized addition, not a redesign.

    This shape is intentional dead code today (nothing constructs it yet); it is
    a RESERVED seam, owner-ruled KEEP. See the :data:`SCHEMA_MIGRATIONS` comment.

    Attributes:
        from_version: The ``user_version`` this step migrates FROM.
        to_version: The ``user_version`` this step migrates TO.
        description: Human-readable summary of the schema change.
        apply: The forward migration coroutine a future author writes; given
            an open connection, it performs the in-place DDL/data migration.
    """

    from_version: int
    to_version: int
    description: str
    apply: Callable[[aiosqlite.Connection], Awaitable[None]]


# Forward schema migrations, ordered by ``from_version``. EMPTY today: no
# migration logic exists yet; § 4S deliberately versions the schema first and
# defers migration. When ``schema.sql`` changes, bump ``SCHEMA_VERSION``
# (§ 4S.1) and add a :class:`SchemaMigration` here. Until then,
# :func:`decide_schema_action` routes every non-current schema to
# ``DISCARD_AND_BOOT_FRESH``.
#
# INTENTIONAL DEAD CODE (the one place in the codebase that is dead on purpose).
# There is genuinely nothing to migrate right now, so this registry is empty and
# the :class:`SchemaMigration` shape + the ``if SCHEMA_MIGRATIONS:`` branch in
# :func:`decide_schema_action` are unexercised today. They are RESERVED, not an
# oversight: the seam is defined now so the FIRST real schema migration is a
# local addition here (register one ``SchemaMigration``), not new scaffolding
# threaded through the boot path under time pressure. Owner-ruled KEEP (do not
# cut as dead code); known and intentional.
SCHEMA_MIGRATIONS: tuple[SchemaMigration, ...] = ()


def decide_schema_action(
    *, observed_version: int, observed_columns: frozenset[str]
) -> SchemaAction:
    """Decide whether to boot the on-disk DB as-is or discard it (pure).

    Plan § 4S.3. No I/O — the gate (:func:`run_schema_gate`) does the probing
    and the deleting; this function only decides, so it is trivially
    unit-testable and the future-migration change stays local.

    Logic, in order:

    1. FRESH DB (``observed_columns`` empty — no ``uploads`` table): there is
       nothing on disk to discard; ``start()`` creates + stamps it. The gate
       passes an empty set for both a missing file (handled earlier) and a
       present-file-without-table. -> ``BOOT_AS_IS``.
    2. STAMPED-CURRENT AND COLUMN-COMPLETE (``observed_version ==
       SCHEMA_VERSION`` AND ``EXPECTED_UPLOADS_COLUMNS <= observed_columns``;
       extras tolerated): the code can read this DB safely. -> ``BOOT_AS_IS``.
    3. Otherwise the DB is pre-version (``user_version != SCHEMA_VERSION``,
       including the unstamped ``0`` case) OR a required column is missing. The
       code cannot safely keep it. -> ``DISCARD_AND_BOOT_FRESH`` (today).

    BOTH the version stamp and the column subset must hold for the step-2
    ``BOOT_AS_IS``: an unstamped-but-column-compatible DB (``user_version = 0``
    with the current columns) is PRE-VERSION and discards (population of zero —
    it cannot be a live field buffer); a stamped-but-column-deficient DB
    discards (the never-crash net).

    Args:
        observed_version: The DB's ``PRAGMA user_version``.
        observed_columns: The DB's ``uploads`` column-name set (empty when the
            table is absent).

    Returns:
        ``BOOT_AS_IS`` when the DB is fresh or stamped-current-and-complete;
        ``DISCARD_AND_BOOT_FRESH`` otherwise.
    """
    # 1. Fresh DB — no uploads table to discard.
    if not observed_columns:
        return SchemaAction.BOOT_AS_IS
    # 2. Stamped current AND every required column present (extras tolerated).
    #    ``observed_columns >= EXPECTED_UPLOADS_COLUMNS`` is the subset
    #    predicate "observed is a superset of expected" — every column the
    #    code reads is present (the gate's never-crash column check).
    if observed_version == SCHEMA_VERSION and observed_columns >= EXPECTED_UPLOADS_COLUMNS:
        return SchemaAction.BOOT_AS_IS
    # 3. Pre-version OR column-deficient — cannot safely keep it.
    #
    # FUTURE-MIGRATION HOOK: when SCHEMA_MIGRATIONS is non-empty and a path
    # observed_version -> SCHEMA_VERSION exists, run it and return BOOT_AS_IS
    # (a failed migration falls back to DISCARD_AND_BOOT_FRESH); today the
    # registry is empty so every non-current schema deletes and boots fresh.
    # Keeping the seam inside this pure decision function (no I/O) keeps it
    # unit-testable and makes the future change local — the migration runner
    # would slot in right here, consulting SCHEMA_MIGRATIONS.
    if (
        SCHEMA_MIGRATIONS
    ):  # pragma: no cover: intentional dead code, reserved seam (see SCHEMA_MIGRATIONS).
        # INTENTIONALLY UNEXERCISED today: SCHEMA_MIGRATIONS is empty, so this
        # branch never runs (hence the coverage pragma). It is the reserved seam,
        # not dead-by-accident: a future author resolves + runs the migration
        # chain here, returning BOOT_AS_IS on success and falling through to
        # discard on failure. Owner-ruled KEEP; see the SCHEMA_MIGRATIONS comment.
        pass
    return SchemaAction.DISCARD_AND_BOOT_FRESH


async def run_schema_gate(*, db_path: Path, bodies_root: Path) -> SchemaGateResult:
    """Gate the on-disk schema before the persistent store opens (never crash).

    Plan § 4S.2 / ADR-025. A schema mismatch is a DISTINCT failure from
    corruption: structurally sound bytes, wrong column set. The corruption
    gate (:func:`run_integrity_gate`) runs ``PRAGMA integrity_check`` and
    passes an old-but-valid schema, so a SEPARATE gate is needed. This gate
    probes ``PRAGMA user_version`` + ``PRAGMA table_info(uploads)``, hands the
    observations to :func:`decide_schema_action`, and on a mismatch DELETES the
    database file plus its ``-wal`` / ``-shm`` siblings (idempotent unlink),
    then the instance boots fresh.

    Must run AFTER the mode guard (:func:`check_body_store_mode`) and BEFORE
    the :class:`SqliteUploadStore` opens — a mismatched DB is deleted before
    ``executescript`` can crash on it (an old DB missing an indexed column dies
    inside ``start()``). The version STAMP, by contrast, runs inside
    ``start()`` on the connection it opens (§ 4S.1).

    This gate takes NO ``metrics_registry``: it is a pure decision-plus-delete
    that RETURNS its result; the WARNING + ``schema_discard_total`` bump live
    at the ``app.py`` call site (mirroring :func:`check_body_store_mode`'s
    split). It uses NONE of § 1's mover/marker/reconciliation — nothing is
    preserved, so no crash-atomicity marker is needed; an interrupted unlink
    re-evaluates to ``DISCARD_AND_BOOT_FRESH`` on the next boot and finishes
    (a half-deleted set is just another non-current DB).

    The discarded DB's now-orphaned body tree under ``bodies_root`` is
    reclaimed by the EXISTING :class:`phantom.workers.body_orphan_janitor`
    in ``hybrid`` / ``all_disk`` modes (a persistent body tree the janitor
    sweeps for directories with no live row); ``all_ram`` has no persistent
    body tree. This gate does NOT touch the body tree.

    Args:
        db_path: This instance's ``<data_root>/uploads.db``.
        bodies_root: This instance's ``<data_root>/bodies`` directory (accepted
            for signature symmetry with the other boot guards and to document
            that the orphaned body tree is the janitor's concern, NOT this
            gate's; the gate never touches it).

    Returns:
        A :class:`SchemaGateResult` carrying the action, the observations, and
        ``discarded_path`` (the deleted path on a discard, else ``None``).
    """
    # A non-existent file is a fresh instance — no connection is opened; the
    # store will create + stamp the DB.
    if not db_path.exists():
        return SchemaGateResult(
            action=SchemaAction.BOOT_AS_IS,
            observed_version=0,
            observed_columns=frozenset(),
            discarded_path=None,
        )

    # Open-probe-close discipline (mirrors check_integrity): a short-lived READ
    # connection, NOT the long-lived store connection (which is not open yet).
    # The connection is CLOSED before any unlink so the delete is not blocked
    # by an open handle (matching the corruption mover's discipline).
    async with aiosqlite.connect(str(db_path)) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("PRAGMA user_version") as cur:
            version_row = await cur.fetchone()
        observed_version = int(version_row[0]) if version_row is not None else 0
        async with conn.execute("PRAGMA table_info(uploads)") as cur:
            column_rows = await cur.fetchall()
        # An EMPTY result means the uploads table does not exist (a DB file
        # present but without the table, e.g. a half-initialized or unrelated
        # SQLite file): decide_schema_action treats this as fresh-enough.
        observed_columns = frozenset(row["name"] for row in column_rows)

    action = decide_schema_action(
        observed_version=observed_version,
        observed_columns=observed_columns,
    )
    if action is SchemaAction.BOOT_AS_IS:
        return SchemaGateResult(
            action=action,
            observed_version=observed_version,
            observed_columns=observed_columns,
            discarded_path=None,
        )

    # DISCARD_AND_BOOT_FRESH. Delete the DB file + its WAL/SHM siblings.
    #
    # WHY DELETE, NOT BACK UP (the population-of-zero justification, ADR-025's
    # scoped exception): this is a PRE-VERSION (user_version != SCHEMA_VERSION)
    # or WRONG-SCHEMA (a required column missing) database. Phantom has ZERO
    # field deployments, so such a database CANNOT occur in the field and holds
    # NO field data to preserve — no field DB can carry real undelivered
    # uploads yet. Deleting it discards nothing. This is the deliberate, scoped
    # exception to the cycle's "never discard without a recoverable backup"
    # headline: corruption (the integrity gate) and a mode switch (§ 1) KEEP
    # back-up-and-run because both hit a LIVE, readable DB that may hold
    # undelivered uploads; only the pre-version / wrong-schema case deletes.
    # Idempotent: each sibling is unlinked only if present, so a half-deleted
    # set on a crashed prior boot simply finishes here.
    for path in (db_path, _wal_sibling(db_path), _shm_sibling(db_path)):
        path.unlink(missing_ok=True)
    return SchemaGateResult(
        action=action,
        observed_version=observed_version,
        observed_columns=observed_columns,
        discarded_path=db_path,
    )


def _wal_sibling(db_path: Path) -> Path:
    """Return the ``-wal`` sibling path for a SQLite DB file."""
    return db_path.with_name(db_path.name + "-wal")


def _shm_sibling(db_path: Path) -> Path:
    """Return the ``-shm`` sibling path for a SQLite DB file."""
    return db_path.with_name(db_path.name + "-shm")


# ---------------------------------------------------------------------
# Mode-wiring decision table (the ONE shared copy).
# ---------------------------------------------------------------------


async def build_body_store(
    *,
    mode: BodyStoreMode,
    ram_body_store: RamBodyStore,
    file_body_store: FileBodyStore,
    store: SqliteUploadStore,
    metrics_registry: MetricsRegistry,
) -> tuple[BodyStore, PersistController | None]:
    """Compose the mode-selected body store + optional PersistController.

    The single decision table for the three deployment modes
    (plan § 2.3.10) — app.py's lifespan (the composition root) calls this
    so the wiring lives in exactly one place:

    =========== ===================== =====================
    Mode        Public BodyStore      PersistController
    =========== ===================== =====================
    ``hybrid``  HybridBodyStore       ✓ (RAM source + disk target)
    ``all_ram`` the RamBodyStore      ✗ (no disk target)
    ``all_disk`` the FileBodyStore    ✗ (no RAM source)
    =========== ===================== =====================

    The caller constructs AND starts both halves (they are stored on the
    InstanceContext regardless of mode); this helper only selects the
    public binding, starts the :class:`HybridBodyStore` wrapper in
    ``hybrid`` mode, and wires the :class:`PersistController` when there
    is a RAM source + disk target. The two halves are passed in (not
    created here) so the InstanceContext can hold them for the
    workers that need each half directly.

    Args:
        mode: The configured ``storage.body_store.mode``.
        ram_body_store: The already-started RAM half.
        file_body_store: The already-started file half.
        store: The persistent SqliteUploadStore (the PersistController's
            metadata writer).
        metrics_registry: Process-wide registry threaded to the
            PersistController.

    Returns:
        ``(body_store, persist_controller)`` — ``persist_controller`` is
        set only in ``hybrid`` mode, ``None`` otherwise.

    Raises:
        ValueError: When ``mode`` is not one of the three Literal values
            (defensive — Pydantic forbids other values at construction).
    """
    if mode == "hybrid":
        body_store: BodyStore = HybridBodyStore(ram=ram_body_store, disk=file_body_store)
        await body_store.start()
        persist_controller = PersistController(
            store=store,
            ram_body_store=ram_body_store,
            file_body_store=file_body_store,
            metrics_registry=metrics_registry,
        )
        return body_store, persist_controller
    if mode == "all_ram":
        return ram_body_store, None
    if mode == "all_disk":
        return file_body_store, None
    msg = f"Unknown body_store.mode: {mode!r}"
    raise ValueError(msg)


__all__ = [
    "DB_QUARANTINE_COUNTER_DESCRIPTION",
    "DB_QUARANTINE_COUNTER_NAME",
    "EXPECTED_UPLOADS_COLUMNS",
    "MODE_SWITCH_BACKUP_COUNTER_DESCRIPTION",
    "MODE_SWITCH_BACKUP_COUNTER_NAME",
    "SCHEMA_DISCARD_COUNTER_DESCRIPTION",
    "SCHEMA_DISCARD_COUNTER_NAME",
    "SCHEMA_MIGRATIONS",
    "UMASK_OWNER_ONLY",
    "BodyStoreMode",
    "ConfigInvariantError",
    "DegradeReason",
    "DegradedInstance",
    "IntegrityFailClosedError",
    "SchemaAction",
    "SchemaGateResult",
    "SchemaMigration",
    "apply_umask",
    "build_body_store",
    "check_body_store_mode",
    "check_instance_isolation",
    "check_retention_floor",
    "decide_schema_action",
    "degrade_action_hint",
    "run_integrity_gate",
    "run_schema_gate",
]
