"""Pure builder for the ``X-Phantom-*`` response header set (plan §5.3).

TWO production callers, and they are the complete set: the envelope ingress
(``routes/send.py``) and the raw-intake catch-all (``routes/catch_all.py``).
The raw-intake arm used to hand-build two of the six headers, which the SDK's
strict ``ResponseHeaders`` model could not parse, so a successful upload
raised at the client. Anything that acks an admission builds its headers HERE;
a third hand-built ack is the defect this note exists to prevent.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from phantom.models.upload import UploadState


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
        next_attempt_at: Optional next-attempt timestamp.
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
        # Trailing Z per plan §5.3.
        iso = next_attempt_at.astimezone().isoformat()
        if not iso.endswith("Z"):
            iso = iso.replace("+00:00", "Z") if "+00:00" in iso else iso
        headers["X-Phantom-Next-Attempt-At"] = iso
    return headers
