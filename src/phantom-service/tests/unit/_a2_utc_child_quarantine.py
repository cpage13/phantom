"""Child-interpreter body for the A-2 quarantine-name UTC regression test.

Run by ``test_a2_utc_timestamps_regression.py`` in a fresh interpreter
whose environment carries ``TZ=America/Los_Angeles``: a new process reads
``TZ`` at first localtime use, so the non-UTC zone takes effect without
``time.tzset`` (absent from some CPython builds, notably
python-build-standalone on macOS). All assertions live here in the child;
a non-zero exit fails the parent test with this script's output.

Usage: ``python _a2_utc_child_quarantine.py <tmp_dir>``
"""

from __future__ import annotations

import re
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

from phantom.storage.integrity import quarantine_paths

# Stamp format shared with the parent test module (kept in lockstep).
_FMT = "%Y%m%dT%H%M%S"

# Max tolerated gap between the artifact stamp and current UTC. Generous
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


def _exercise(tmp_dir: Path) -> None:
    """Derive quarantine names and assert their stamps encode UTC."""
    db_path = tmp_dir / "uploads.db"
    body_root = tmp_dir / "body_store"
    quarantined_db, quarantined_body = quarantine_paths(db_path, body_root, backup_id=uuid4())

    for artifact in (quarantined_db.name, quarantined_body.name):
        # Display iso, then the cycle-7 backup-identity hex token.
        m = re.search(r"\.(\d{8}T\d{6})Z-[0-9a-f]+(?:\.db)?$", artifact)
        assert m is not None, f"unexpected quarantine name: {artifact}"
        parsed_naive = datetime.strptime(m.group(1), _FMT)
        now_utc = datetime.now(tz=UTC).replace(tzinfo=None)
        delta = abs(now_utc - parsed_naive)
        assert delta < _MAX_SKEW, (
            f"quarantine name {artifact!r} carries 'Z' (UTC) but encodes a "
            f"timestamp {delta} off from current UTC."
        )


def main() -> None:
    """Entry point: ``argv[1]`` is the writable scratch directory."""
    _assert_la_tz_active()
    _exercise(Path(sys.argv[1]))


if __name__ == "__main__":
    main()
