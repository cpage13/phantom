"""Child-interpreter body for the ``X-Phantom-Next-Attempt-At`` UTC test.

Run by ``test_next_attempt_at_header_utc.py`` in a fresh interpreter whose
environment carries ``TZ=America/Los_Angeles``: a new process reads ``TZ``
at first localtime use, so the non-UTC zone takes effect without
``time.tzset`` (absent from this repo's uv-provided CPython). All
assertions live here in the child; a non-zero exit fails the parent test
with this script's output.

The regression: ``build_response_headers`` rendered the timestamp with
``next_attempt_at.astimezone()``, whose no-argument form converts to LOCAL
time. Under this zone the same UTC instant came out as
``2026-09-07T21:08:45.400547-04:00`` while the header contract, the
function's own comment ("Trailing Z per plan 5.3") and phantom-client's
``models/envelope.py`` all say ISO-8601 UTC with a trailing ``Z``. The two
guard lines below the conversion were both dead: ``datetime.isoformat``
never emits a ``Z``, and a local-offset string holds no ``+00:00`` to
replace.

Usage: ``python _next_attempt_header_utc_child.py <tmp_dir>``
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

from phantom.routes.envelope import build_response_headers

_HEADER = "X-Phantom-Next-Attempt-At"

# A pinned instant, so the expected rendering is a literal rather than
# something recomputed with the logic under test.
_MOMENT_UTC = datetime(2026, 9, 8, 1, 8, 45, 400547, tzinfo=UTC)
_EXPECTED = "2026-09-08T01:08:45.400547Z"

# The same instant expressed in a +05:00 zone: a tz-aware value that is
# neither UTC nor the host's local zone must still render as _EXPECTED.
_PLUS_FIVE = timezone(timedelta(hours=5))

# America/Los_Angeles sits at UTC-8 (PST) or UTC-7 (PDT); the guard below
# proves the forced zone took effect in this interpreter.
_LA_OFFSETS = (timedelta(hours=-8), timedelta(hours=-7))

# Arbitrary but fixed response-header inputs; only the timestamp is under
# test, so the rest just have to be well-typed.
_ATTEMPTS = 1
_POLL_AFTER_SECONDS = 5


def _assert_la_tz_active() -> None:
    """Fail loudly if the forced TZ did not take effect in this process."""
    offset = datetime.now().astimezone().utcoffset()
    assert offset in _LA_OFFSETS, (
        f"TZ=America/Los_Angeles did not take effect in the child "
        f"interpreter (local offset {offset}); the regression would be "
        f"unobservable, so fail rather than pass vacuously."
    )


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


def main() -> None:
    """Entry point: assert the header is UTC-with-Z under a non-UTC host."""
    _assert_la_tz_active()

    from_utc = _render(_MOMENT_UTC)
    assert from_utc == _EXPECTED, (
        f"{_HEADER} rendered {from_utc!r} on a UTC-7/8 host; the contract is "
        f"ISO-8601 UTC with a trailing Z ({_EXPECTED!r})."
    )

    from_other_zone = _render(_MOMENT_UTC.astimezone(_PLUS_FIVE))
    assert from_other_zone == _EXPECTED, (
        f"{_HEADER} rendered {from_other_zone!r} for the SAME instant carried "
        f"in a +05:00 zone; every tzinfo must normalise to {_EXPECTED!r}."
    )

    # A naive value is read as UTC wall clock, matching storage/timestamps.
    from_naive = _render(_MOMENT_UTC.replace(tzinfo=None))
    assert from_naive == _EXPECTED, (
        f"{_HEADER} rendered {from_naive!r} for a naive value; a naive "
        f"timestamp is UTC wall clock, not local time."
    )


if __name__ == "__main__":
    main()
