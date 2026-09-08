"""Pure builder for the ``X-Phantom-*`` response header set (plan §5.3).

TWO production callers, and they are the complete set: the envelope ingress
(``routes/send.py``) and the raw-intake catch-all (``routes/catch_all.py``).
The raw-intake arm used to hand-build two of the six headers, which the SDK's
strict ``ResponseHeaders`` model could not parse, so a successful upload
raised at the client. Anything that acks an admission builds its headers HERE;
a third hand-built ack is the defect this note exists to prevent.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from phantom.models.upload import UploadState

# The UTC offset ``datetime.isoformat`` renders, and the ``Z`` the wire
# contract requires in its place. CPython never emits a ``Z`` of its own,
# so this substitution is the ONLY thing that produces the documented
# suffix; it can only be correct because the value is converted to UTC
# first.
_UTC_OFFSET_SUFFIX = "+00:00"
_UTC_ZULU_SUFFIX = "Z"


def build_response_headers(
    *,
    upload_id: UUID,
    group_id: UUID,
    state: UploadState,
    attempts: int,
    next_attempt_at: datetime | None,
    suggested_poll_after_seconds: int,
) -> dict[str, str]:
    """Construct the six canonical ``X-Phantom-*`` response headers.

    Args:
        upload_id: The envelope's ``chain_id`` (=  ``UploadRow.chain_id``).
        group_id: The row's query-grouping handle, echoed as
            ``X-Phantom-Group-Id``. ALWAYS present: the column is NOT
            NULL (the header value when the submission supplied
            ``X-Phantom-Group-Id``, else ``chain_id``).
        state: Current row state.
        attempts: Attempts so far.
        next_attempt_at: Optional next-attempt timestamp. Rendered as an
            ISO-8601 UTC instant with a trailing ``Z`` whatever the host's
            local zone is, and whatever tzinfo the value carries; a naive
            value is read as UTC wall clock.
        suggested_poll_after_seconds: Polling hint for clients.

    Returns:
        A dict suitable for handing to FastAPI's ``Response(headers=...)``.
    """
    headers = {
        "X-Phantom-Upload-Id": str(upload_id),
        "X-Phantom-Group-Id": str(group_id),
        "X-Phantom-Status": state,
        "X-Phantom-Attempts": str(attempts),
        "X-Phantom-Suggested-Poll-After": str(suggested_poll_after_seconds),
    }
    if next_attempt_at is not None:
        # Trailing Z per plan §5.3, and the Z has to be TRUE. A bare
        # ``astimezone()`` converts to the HOST's local zone, so on a
        # non-UTC host the header carried an offset like ``-04:00``; the
        # two guards that followed it were both dead, because
        # ``datetime.isoformat`` never emits a ``Z`` (so the endswith
        # test always passed) and the string held no ``+00:00`` to
        # replace. Converting to UTC explicitly makes the suffix
        # substitution the whole contract. A naive value is taken as UTC
        # wall clock, matching ``storage/timestamps.utc_stamp``.
        moment = (
            next_attempt_at.replace(tzinfo=UTC)
            if next_attempt_at.tzinfo is None
            else next_attempt_at.astimezone(UTC)
        )
        headers["X-Phantom-Next-Attempt-At"] = moment.isoformat().replace(
            _UTC_OFFSET_SUFFIX, _UTC_ZULU_SUFFIX
        )
    return headers
