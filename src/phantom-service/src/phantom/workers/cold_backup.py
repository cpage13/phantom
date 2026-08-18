"""ColdBackupScheduler - periodic SQLite online-backup snapshots.

Plan § 5.2.6 / strategy §3 "optional cold backup snapshots."

Off by default (``Settings.storage.db_integrity.backup_enabled = False``).
When enabled, the composition root spawns the scheduler under its
TaskGroup; the scheduler runs SQLite's online-backup API at
``backup_period_seconds`` cadence and rotates ``backup_rotate_n``
snapshot files. Backups land in ``<data_dir>/backups/`` and are named
``uploads.backup.<iso>.db``.

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

        Returns the path of the new snapshot. Exposed publicly (no
        leading underscore) so unit tests can exercise the snapshot
        path without driving the run loop. Production callers go
        through :meth:`run`.
        """
        # UTC stamp via the shared helper - the ``Z`` suffix declares
        # UTC, so naive local time here would lie (finding A-2) and a
        # backwards clock step could break the lex-sort rotation
        # invariant in :meth:`_rotate`.
        iso = utc_stamp()
        dest = self._backup_root / f"uploads.backup.{iso}.db"
        async with (
            aiosqlite.connect(str(self._db_path)) as src,
            aiosqlite.connect(str(dest)) as dst,
        ):
            await src.backup(dst)  # SQLite online-backup
        await self._rotate()
        logger.info("cold backup snapshot written: %s", dest)
        return dest

    async def _rotate(self) -> None:
        """Keep ``backup_rotate_n`` most-recent snapshots; delete older.

        Sorted alphabetically - the ISO timestamp suffix is
        lex-sortable so the lex order matches chronological order.
        """
        keep = self._settings.storage.db_integrity.backup_rotate_n
        files = sorted(self._backup_root.glob(_BACKUP_GLOB))
        for old in files[:-keep]:
            try:
                old.unlink()
            except OSError:
                logger.exception("failed to rotate snapshot: %s", old)


__all__ = ["ColdBackupScheduler"]
