"""ColdBackupScheduler - periodic SQLite online-backup snapshots.

Plan § 5.2.6 / strategy §3 "optional cold backup snapshots."

Off by default (``Settings.storage.db_integrity.backup_enabled = False``).
When enabled, the composition root spawns the scheduler under its
TaskGroup; the scheduler runs SQLite's online-backup API at
``backup_period_seconds`` cadence and rotates ``backup_rotate_n``
snapshot files. Backups land in ``<data_dir>/backups/`` and are named
``uploads.backup.<iso>.db``. A snapshot in progress carries a further
``.part`` suffix and takes its final name only once the copy has
completed, so a file under the final name is always a whole snapshot
and rotation can never count a failed one (finding S8-4).

The online-backup API is non-blocking on the live database - readers
and writers continue to operate during the snapshot. The scheduler
never writes to the live DB; its writes target the snapshot directory
only (single-writer-per-purpose, plan § 0.5).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from pathlib import Path

import aiosqlite

from phantom.config.settings import Settings
from phantom.storage.timestamps import utc_stamp

logger = logging.getLogger(__name__)

# Glob pattern used by :meth:`_rotate` to find rotation candidates.
# The timestamp suffix is produced by ``phantom.storage.timestamps``
# (ISO 8601 basic, trailing ``Z`` = UTC) so the suffix is lex-sortable
# and matches the integrity-quarantine naming convention.
_BACKUP_GLOB = "uploads.backup.*.db"

# Suffix for the in-progress snapshot. A snapshot is written under
# ``<final name>.part`` and renamed onto its final name only after
# ``backup()`` returns, because ``aiosqlite.connect`` CREATES the destination
# file on connect: a snapshot that failed after the connect used to leave a
# zero-byte file that :meth:`_rotate` counted as a snapshot, so three failed
# nightly runs plus one success evicted every previously-good snapshot and
# logged nothing, the deletions being the normal rotation path (finding S8-4).
# The suffix must NOT end in ``.db``, or :data:`_BACKUP_GLOB` would match the
# staging file and put the hole straight back.
_STAGING_SUFFIX = ".part"

# SQLite sidecar files the destination connection can leave beside a staging
# file. Removed with it so a failed snapshot leaves the backup directory
# exactly as it found it.
_SQLITE_SIDECAR_SUFFIXES = ("-journal", "-wal", "-shm")


class ColdBackupScheduler:
    """Periodic SQLite online-backup snapshots.

    Plan § 5.2.6. Spawned only when
    ``Settings.storage.db_integrity.backup_enabled`` is True. The
    scheduler self-checks the flag on entry - toggling it off via hot
    reload while running has no effect; mode-flip requires a restart
    (consistent with the body-store mode contract, plan § 2.3.10).

    Attributes:
        db_path: Live SQLite path (read-only from this worker's view).
        backup_root: Snapshot directory. Created at startup if absent.
    """

    def __init__(
        self,
        *,
        db_path: Path,
        backup_root: Path,
        settings: Settings,
    ) -> None:
        """Store paths + settings; no side effects at construction.

        Args:
            db_path: Path to the live SQLite file.
            backup_root: Directory to write snapshot files into.
            settings: Top-level :class:`Settings`. Read at run-loop
                entry for the cadence + rotation knobs.
        """
        self._db_path = db_path
        self._backup_root = backup_root
        self._settings = settings

    async def run(self, stop_event: asyncio.Event) -> None:
        """Run the snapshot loop until stopped.

        Composition-root spawn point - :func:`phantom.app.create_app`'s
        lifespan :class:`asyncio.TaskGroup`. Exits immediately when
        ``backup_enabled`` is False; otherwise snapshots every
        ``backup_period_seconds``. Per-snapshot exceptions are logged
        and the loop continues - a failed snapshot does not kill the
        process (strategy §3 commit: "service comes up serving empty
        state on any persistence failure").

        Args:
            stop_event: Set by the lifespan on shutdown; the loop exits
                cleanly as soon as it fires - the same ``stop_event``-drain
                idiom every other lifespan worker uses (sender / janitor /
                …) so the supervising :class:`asyncio.TaskGroup` is not
                blocked waiting on a never-returning task at shutdown.
        """
        cfg = self._settings.storage.db_integrity
        if not cfg.backup_enabled:
            return
        self._backup_root.mkdir(parents=True, exist_ok=True)
        # stop_event-driven loop (mirrors BodyOrphanJanitor.run) so the
        # lifespan TaskGroup drains cleanly at shutdown - the snapshot
        # cadence is the timeout on the stop wait, so a set event both
        # exits the loop promptly AND cuts the inter-snapshot sleep short.
        while not stop_event.is_set():
            try:
                await self.snapshot_once()
            except Exception:
                logger.exception("cold backup snapshot failed; continuing")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=cfg.backup_period_seconds)

    async def snapshot_once(self) -> Path:
        """Take one snapshot now and run rotation.

        The copy lands on a ``.part`` staging name and is renamed onto its
        final name only once ``backup()`` has returned, so a file matching
        the rotation glob exists ONLY when it is a complete snapshot
        (finding S8-4). A failure removes the staging file and re-raises;
        :meth:`run` logs it and continues, and the operator's existing
        snapshots are untouched.

        Returns the path of the new snapshot. Exposed publicly (no
        leading underscore) so unit tests can exercise the snapshot
        path without driving the run loop. Production callers go
        through :meth:`run`.

        Raises:
            Exception: Whatever the connect or the online-backup raised,
                re-raised after the staging file is discarded.
        """
        # UTC stamp via the shared helper - the ``Z`` suffix declares
        # UTC, so naive local time here would lie (finding A-2) and a
        # backwards clock step could break the lex-sort rotation
        # invariant in :meth:`_rotate`.
        iso = utc_stamp()
        dest = self._backup_root / f"uploads.backup.{iso}.db"
        staging = dest.with_name(dest.name + _STAGING_SUFFIX)
        try:
            async with (
                aiosqlite.connect(str(self._db_path)) as src,
                aiosqlite.connect(str(staging)) as dst,
            ):
                await src.backup(dst)  # SQLite online-backup
        except BaseException:
            # BaseException, not Exception: a cancelled snapshot leaves the
            # same half-written staging file a failed one does.
            self._discard_staging(staging)
            raise
        # Rename onto the final name. ``replace`` rather than ``rename`` so a
        # re-run within the same stamp second overwrites its own earlier
        # attempt instead of raising on Windows.
        staging.replace(dest)
        await self._rotate()
        logger.info("cold backup snapshot written: %s", dest)
        return dest

    @staticmethod
    def _discard_staging(staging: Path) -> None:
        """Remove a failed snapshot's staging file and any SQLite sidecars.

        Args:
            staging: The ``.part`` path the failed snapshot was writing to.
                Its sidecars (:data:`_SQLITE_SIDECAR_SUFFIXES`) go with it;
                the destination connection creates them while copying and a
                failure can leave one behind.
        """
        for suffix in ("", *_SQLITE_SIDECAR_SUFFIXES):
            path = staging.with_name(staging.name + suffix)
            try:
                path.unlink(missing_ok=True)
            except OSError:
                # Logged, not raised: the caller is already unwinding a
                # snapshot failure and that error is the one worth surfacing.
                logger.exception("failed to remove cold-backup staging file: %s", path)

    async def _rotate(self) -> None:
        """Keep ``backup_rotate_n`` most-recent snapshots; delete older.

        Sorted alphabetically - the ISO timestamp suffix is
        lex-sortable so the lex order matches chronological order. Every
        file the glob returns is a COMPLETE snapshot, because an in-progress
        one carries :data:`_STAGING_SUFFIX` until it is finished (S8-4).
        """
        keep = self._settings.storage.db_integrity.backup_rotate_n
        files = sorted(self._backup_root.glob(_BACKUP_GLOB))
        for old in files[:-keep]:
            try:
                old.unlink()
            except OSError:
                logger.exception("failed to rotate snapshot: %s", old)


__all__ = ["ColdBackupScheduler"]
