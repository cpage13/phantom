"""Regression test for aggressor finding A-2 (adopted Round 2).

Both filesystem-artifact timestamp sites, cold-backup snapshots and
integrity-quarantine names, carry a literal ``Z`` (UTC) suffix and so
MUST be stamped from UTC, never naive local time. Round 1 found both
stamping ``datetime.now()`` (naive local) while labelling the output
``Z``; on a non-UTC host the filename lied and a backwards clock step
could break the lex-sort rotation invariant.

The fix routes both sites through
:func:`phantom.storage.timestamps.utc_stamp`. These tests force a
non-UTC zone so the bug would be observable if reintroduced, and pin
the post-fix UTC behavior so a regression can't slip back in.

Round 6 pre-round revision (2026-06-11): the zone flip moved from an
in-process ``time.tzset()`` call to a CHILD interpreter started with
``TZ=America/Los_Angeles`` in its environment. A fresh process reads
``TZ`` at first localtime use, so the flip needs no ``time.tzset``,
which is absent from some CPython builds (python-build-standalone on
macOS among them). The child bodies live in ``_a2_utc_child_*.py``
beside this module, are real linted modules, assert internally
(including that the forced zone actually took effect, so the test can
never pass vacuously), and exit non-zero on failure; the parent
asserts on the exit status with the child's output in the message.
"""

from __future__ import annotations

import importlib
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from phantom.storage import timestamps

# The non-UTC zone forced into each child interpreter's environment.
_CHILD_TZ = "America/Los_Angeles"

# Backstop for a hung child; generous against cold-start import cost.
_CHILD_TIMEOUT_SECONDS = 120

_CHILD_DIR = Path(__file__).parent


def _run_child_with_la_tz(child_module_filename: str, tmp_path: Path) -> None:
    """Run a child body in a fresh interpreter with a forced non-UTC TZ.

    Args:
        child_module_filename: Filename of the ``_a2_utc_child_*.py``
            script beside this module that performs the assertions.
        tmp_path: Writable scratch directory handed to the child as argv.
    """
    env = {**os.environ, "TZ": _CHILD_TZ}
    proc = subprocess.run(
        [sys.executable, str(_CHILD_DIR / child_module_filename), str(tmp_path)],
        env=env,
        capture_output=True,
        text=True,
        timeout=_CHILD_TIMEOUT_SECONDS,
        check=False,
    )
    assert proc.returncode == 0, (
        f"{child_module_filename} failed under TZ={_CHILD_TZ} "
        f"(exit {proc.returncode}).\n--- child stdout ---\n{proc.stdout}"
        f"\n--- child stderr ---\n{proc.stderr}"
    )


def test_cold_backup_snapshot_filename_encodes_utc(tmp_path: Path) -> None:
    """The snapshot's filename timestamp matches UTC, not local time."""
    _run_child_with_la_tz("_a2_utc_child_cold_backup.py", tmp_path)


def test_quarantine_dir_name_encodes_utc(tmp_path: Path) -> None:
    """The quarantine artifact names match UTC, not local time (second A-2 site)."""
    _run_child_with_la_tz("_a2_utc_child_quarantine.py", tmp_path)


def test_utc_stamp_converts_supplied_tzaware_to_utc() -> None:
    """A supplied tz-aware datetime is rendered in UTC, so the 'Z' is truthful."""
    importlib.reload(timestamps)
    # 2026-01-02 12:00:00 in a +05:00 zone is 07:00:00 UTC.
    plus5 = timezone(timedelta(hours=5))
    moment = datetime(2026, 1, 2, 12, 0, 0, tzinfo=plus5)
    assert timestamps.utc_stamp(moment) == "20260102T070000Z"
