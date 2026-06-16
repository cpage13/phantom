"""UTC filesystem-artifact timestamp helper.

Phantom names two kinds of on-disk artifact with a timestamp:
cold-backup snapshots (``<data_dir>/backups/uploads.backup.<ts>.db``)
and quarantine/mode-switch backup artifacts
(``<data_dir>/<stem>.corrupted.<ts>-<token>.db`` +
``<name>.quarantine.<ts>-<token>/``, where ``<token>`` is a uniqueness
token derived from the backup's uuid identity; the timestamp half is
display and sort material only).

Both use ISO 8601 *basic* format (no separators) with a literal ``Z``
suffix. The ``Z`` declares UTC — so the timestamp MUST be computed from
``datetime.now(tz=UTC)``, never naive local time. Two sites historically
diverged here (aggressor finding A-2): ``cold_backup.py`` and
``integrity.py`` each stamped ``datetime.now()`` (naive local) while
labelling the result ``Z``. On a non-UTC host the filename then lied
about the wall clock, and a backwards clock step (DST fall-back / NTP
correction) could make a newer snapshot lex-sort *before* an older one —
breaking the rotation invariant that assumes lex order matches
chronological order.

This module is the single source of truth for that timestamp. Both
call sites import :func:`utc_stamp` so a third instance cannot
reintroduce the naive-local bug.
"""

from __future__ import annotations

from datetime import UTC, datetime

# ISO 8601 basic timestamp format — no separators, trailing literal
# ``Z`` (UTC). Safe across POSIX/NTFS filenames and lex-sortable so
# lex order matches chronological order (REQUIRED by cold-backup
# rotation, which keeps the lexically-highest N files). Seconds
# precision is enough because the timestamp is never an identity:
# quarantine/mode-switch backup names append a uniqueness token derived
# from the backup's uuid ``backup_id`` (cycle-7 seam 1), so same-second
# artifacts cannot collide, and cold-backup cadence is many seconds.
FS_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%SZ"


def utc_stamp(moment: datetime | None = None) -> str:
    """Return a UTC filesystem-artifact timestamp string.

    The result is formatted with :data:`FS_TIMESTAMP_FORMAT` — ISO 8601
    basic with a trailing ``Z``. Because the ``Z`` declares UTC, the
    timestamp is always computed in UTC regardless of the host timezone.

    Args:
        moment: Optional timezone-aware datetime to format. When ``None``
            (the production path) the current UTC instant is used. When
            supplied (test path), it is converted to UTC before
            formatting so a caller pinning a known instant gets the
            canonical UTC rendering even if they pass a non-UTC tzinfo.
            A naive datetime is assumed to already be UTC wall-clock.

    Returns:
        A ``%Y%m%dT%H%M%SZ`` string in UTC.
    """
    if moment is None:
        moment = datetime.now(tz=UTC)
    elif moment.tzinfo is not None:
        moment = moment.astimezone(UTC)
    return moment.strftime(FS_TIMESTAMP_FORMAT)


__all__ = ["FS_TIMESTAMP_FORMAT", "utc_stamp"]
