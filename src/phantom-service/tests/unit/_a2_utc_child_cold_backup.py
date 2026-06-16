"""Child-interpreter body for the A-2 cold-backup UTC regression test.

Run by ``test_a2_utc_timestamps_regression.py`` in a fresh interpreter
whose environment carries ``TZ=America/Los_Angeles``: a new process reads
``TZ`` at first localtime use, so the non-UTC zone takes effect without
``time.tzset`` (absent from some CPython builds, notably
python-build-standalone on macOS). All assertions live here in the child;
a non-zero exit fails the parent test with this script's output.

Usage: ``python _a2_utc_child_cold_backup.py <tmp_dir>``
"""

from __future__ import annotations

import asyncio
import re
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from phantom.config.settings import DbIntegrityCfg, Settings, StorageCfg
from phantom.workers.cold_backup import ColdBackupScheduler

# Stamp format shared with the parent test module (kept in lockstep).
_FMT = "%Y%m%dT%H%M%S"

# Max tolerated gap between the filename stamp and current UTC. Generous
# against slow CI, far below the hours-scale skew a local-time regression
# would produce under the forced non-UTC zone.
_MAX_SKEW = timedelta(seconds=10)

# America/Los_Angeles sits at UTC-8 (PST) or UTC-7 (PDT); the guard below
# proves the forced zone took effect in this interpreter.
_LA_OFFSETS = (timedelta(hours=-8), timedelta(hours=-7))


def _assert_la_tz_active() -> None:
    """Fail loudly if the forced TZ did not take effect in this process."""
    offset = datetime.now().astimezone().utcoffset()
    assert offset in _LA_OFFSETS, (
        f"TZ=America/Los_Angeles did not take effect in the child "
        f"interpreter (local offset {offset}); the regression would be "
        f"unobservable, so fail rather than pass vacuously."
    )


async def _exercise(tmp_dir: Path) -> None:
    """Snapshot once and assert the filename stamp encodes UTC."""
    db_path = tmp_dir / "uploads.db"
    db_path.touch()
    backup_root = tmp_dir / "backups"
    backup_root.mkdir()
    settings = Settings(
        storage=StorageCfg(
            data_dir=str(tmp_dir),
            db_integrity=DbIntegrityCfg(backup_enabled=True),
        ),
    )
    scheduler = ColdBackupScheduler(
        db_path=db_path,
        backup_root=backup_root,
        settings=settings,
    )
    dest = await scheduler.snapshot_once()

    m = re.search(r"uploads\.backup\.(\d{8}T\d{6})Z\.db", dest.name)
    assert m is not None, f"unexpected backup name: {dest.name}"
    parsed_naive = datetime.strptime(m.group(1), _FMT)
    now_utc = datetime.now(tz=UTC).replace(tzinfo=None)
    delta = abs(now_utc - parsed_naive)
    assert delta < _MAX_SKEW, (
        f"backup filename {dest.name!r} carries 'Z' (UTC) but encodes a "
        f"timestamp {delta} off from current UTC."
    )


def main() -> None:
    """Entry point: ``argv[1]`` is the writable scratch directory."""
    _assert_la_tz_active()
    asyncio.run(_exercise(Path(sys.argv[1])))


if __name__ == "__main__":
    main()
