"""Regression test: ``X-Phantom-Next-Attempt-At`` is UTC on any host.

``routes/envelope.build_response_headers`` exists to centralise the
``X-Phantom-*`` response-header contract, and its own comment says
"Trailing Z per plan 5.3" while phantom-client's ``models/envelope.py``
documents the field as an "ISO-8601 UTC timestamp". It rendered the value
with ``next_attempt_at.astimezone()``, whose no-argument form converts to
the HOST's local zone, so on a non-UTC host the header carried an offset
like ``-04:00`` instead. Both guard lines under it were dead:
``datetime.isoformat`` never emits a ``Z`` in CPython, so the
``endswith("Z")`` test always passed, and a local-offset string holds no
``+00:00`` for the replacement to find. The SDK survived only because
``datetime.fromisoformat`` accepts both forms.

The zone is forced by starting a CHILD interpreter with
``TZ=America/Los_Angeles`` in its environment, the pattern
``test_a2_utc_timestamps_regression.py`` established: a fresh process
reads ``TZ`` at first localtime use, so no ``time.tzset`` is needed, which
matters because this repo's uv-provided CPython does not have it. The
child asserts internally (including that the forced zone actually took
effect, so the test can never pass vacuously) and exits non-zero on
failure; the parent asserts on the exit status with the child's output in
the message.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from phantom.routes.envelope import build_response_headers

# The non-UTC zone forced into the child interpreter's environment.
_CHILD_TZ = "America/Los_Angeles"

# Backstop for a hung child; generous against cold-start import cost.
_CHILD_TIMEOUT_SECONDS = 120

_CHILD_DIR = Path(__file__).parent
_CHILD_MODULE = "_next_attempt_header_utc_child.py"

_HEADER = "X-Phantom-Next-Attempt-At"

# Fixed response-header inputs for the in-process assertions; only the
# timestamp is under test.
_ATTEMPTS = 1
_POLL_AFTER_SECONDS = 5


def _render(moment: datetime) -> str:
    """Build the headers for ``moment`` and return the timestamp header."""
    headers = build_response_headers(
        upload_id=uuid4(),
        group_id=uuid4(),
        state="queued",
        attempts=_ATTEMPTS,
        next_attempt_at=moment,
        suggested_poll_after_seconds=_POLL_AFTER_SECONDS,
    )
    return headers[_HEADER]


def test_header_is_utc_with_z_on_a_non_utc_host(tmp_path: Path) -> None:
    """The header stays UTC-with-Z when the host's local zone is not UTC.

    Objective: reproduce the defect the only way it is observable, by
    running the builder in an interpreter whose local zone is UTC-7/8.
    Success is the child exiting 0, meaning every one of its renderings
    (a UTC value, the same instant in a +05:00 zone, and a naive value)
    came back as the pinned ``...Z`` string.

    Pre-fix the child fails on its first assertion, having rendered a
    ``-07:00`` / ``-08:00`` offset where the contract requires ``Z``.
    """
    env = {**os.environ, "TZ": _CHILD_TZ}
    proc = subprocess.run(
        [sys.executable, str(_CHILD_DIR / _CHILD_MODULE), str(tmp_path)],
        env=env,
        capture_output=True,
        text=True,
        timeout=_CHILD_TIMEOUT_SECONDS,
        check=False,
    )
    assert proc.returncode == 0, (
        f"{_CHILD_MODULE} failed under TZ={_CHILD_TZ} (exit {proc.returncode})."
        f"\n--- child stdout ---\n{proc.stdout}\n--- child stderr ---\n{proc.stderr}"
    )


def test_a_non_utc_tzinfo_is_converted_not_relabelled() -> None:
    """A tz-aware value in another zone renders as the SAME UTC instant.

    Objective: the ``Z`` must be truthful, not appended to whatever the
    caller happened to carry. Success is 12:00 in a +05:00 zone coming
    back as 07:00Z, which is a conversion rather than a relabel.
    """
    plus_five = timezone(timedelta(hours=5))
    moment = datetime(2026, 1, 2, 12, 0, 0, tzinfo=plus_five)

    assert _render(moment) == "2026-01-02T07:00:00Z"


def test_utc_value_renders_with_z_not_an_offset() -> None:
    """A UTC value never leaks the ``+00:00`` spelling onto the wire.

    Objective: pin the substitution that produces the documented suffix,
    since CPython's ``isoformat`` never emits a ``Z`` of its own. Success
    is a trailing ``Z`` and no ``+00:00`` anywhere in the value.
    """
    rendered = _render(datetime(2026, 9, 8, 1, 8, 45, 400547, tzinfo=UTC))

    assert rendered == "2026-09-08T01:08:45.400547Z"
    assert "+00:00" not in rendered
