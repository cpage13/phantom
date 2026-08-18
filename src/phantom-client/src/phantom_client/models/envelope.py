"""Parsed view of Phantom's ``X-Phantom-*`` response-header set.

Phantom always emits the same header set on a ``POST /v1/send``
response. The headers carry the upload-row id, the query-grouping id,
the current state, attempt count, the next-attempt time, and the
suggested poll cadence. This module provides the typed view
:class:`ResponseHeaders` and a stripper that parses the headers off an
httpx response.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from phantom_client.errors import PhantomEnvelopeError

# The six response-header names, imported from the module that declares them
# and aliased to the short local names the stripper below already reads (U4).
# These used to be re-declared literals here, under a comment claiming an
# import would create a back-edge; ``phantom_client.headers`` imports only
# ``phantom_client.config``, which imports nothing from the package, so no
# such edge exists. The aliases are kept so the stripper's body does not move.
from phantom_client.headers import (
    X_PHANTOM_ATTEMPTS as _H_ATTEMPTS,
)
from phantom_client.headers import (
    X_PHANTOM_GROUP_ID as _H_GROUP_ID,
)
from phantom_client.headers import (
    X_PHANTOM_NEXT_ATTEMPT_AT as _H_NEXT_ATTEMPT_AT,
)
from phantom_client.headers import (
    X_PHANTOM_STATUS as _H_STATUS,
)
from phantom_client.headers import (
    X_PHANTOM_SUGGESTED_POLL_AFTER as _H_SUGGESTED_POLL_AFTER,
)
from phantom_client.headers import (
    X_PHANTOM_UPLOAD_ID as _H_UPLOAD_ID,
)
from phantom_client.models.chain import ChainState


class ResponseHeaders(BaseModel):
    """Parsed view of the ``X-Phantom-*`` response headers Phantom always emits.

    The shape mirrors what the headers carry:

    - ``upload_id`` — equals ``envelope.chain_id`` for chain submissions.
    - ``group_id`` — the row's query-grouping handle; equals
      ``chain_id`` unless the submission supplied
      ``X-Phantom-Group-Id``. Always present (cycle-7: replaces the
      retired batch-named echo).
    - ``status`` — the current upload state as a snake_case literal.
    - ``attempts`` — count of attempts completed so far.
    - ``next_attempt_at`` — ISO-8601 UTC timestamp; ``None`` for terminal
      states.
    - ``suggested_poll_after_seconds`` — Phantom's hint for when to poll
      next. Integer seconds.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    upload_id: UUID = Field(
        ...,
        description="From X-Phantom-Upload-Id; equals envelope.chain_id for chain submissions.",
    )
    group_id: UUID = Field(
        ...,
        description=(
            "From X-Phantom-Group-Id; the row's query-grouping handle. "
            "Equals chain_id unless the submission supplied X-Phantom-Group-Id."
        ),
    )
    status: ChainState = Field(..., description="From X-Phantom-Status (snake_case literal).")
    attempts: int = Field(..., ge=0, description="From X-Phantom-Attempts.")
    next_attempt_at: datetime | None = Field(
        None,
        description="From X-Phantom-Next-Attempt-At; None on terminal states.",
    )
    suggested_poll_after_seconds: int = Field(
        ...,
        ge=0,
        description="From X-Phantom-Suggested-Poll-After; integer seconds.",
    )


def _require(headers: Mapping[str, str], name: str) -> str:
    """Look up ``name`` (case-insensitive); raise if absent."""
    value = headers.get(name)
    if value is None:
        raise PhantomEnvelopeError(
            f"required response header {name!r} missing from {dict(headers)!r}"
        )
    return value


def parse_response_headers(headers: Mapping[str, str]) -> ResponseHeaders:
    """Build a :class:`ResponseHeaders` from a response's headers.

    Headers are always strings on the wire; this stripper converts them
    to their typed model fields explicitly (the model itself is strict
    and does not coerce, so the wire→type coercion is centralized here).

    Args:
        headers: Case-insensitive header mapping (httpx's
            ``response.headers`` works). Required headers must all be
            present.

    Returns:
        The parsed model.

    Raises:
        PhantomEnvelopeError: When a required header is missing or any
            field fails validation.
    """
    upload_id_raw = _require(headers, _H_UPLOAD_ID)
    group_id_raw = _require(headers, _H_GROUP_ID)
    status_raw = _require(headers, _H_STATUS)
    attempts_raw = _require(headers, _H_ATTEMPTS)
    suggested_raw = _require(headers, _H_SUGGESTED_POLL_AFTER)
    next_attempt_raw = headers.get(_H_NEXT_ATTEMPT_AT) or None

    try:
        upload_id = UUID(upload_id_raw)
        group_id = UUID(group_id_raw)
        attempts = int(attempts_raw)
        suggested = int(suggested_raw)
        next_attempt_at: datetime | None = (
            datetime.fromisoformat(next_attempt_raw) if next_attempt_raw else None
        )
    except (ValueError, TypeError) as exc:
        raise PhantomEnvelopeError(
            f"response header value failed conversion: {dict(headers)!r}: {exc}"
        ) from exc

    try:
        return ResponseHeaders(
            upload_id=upload_id,
            group_id=group_id,
            # `status` is a ChainState Literal; the runtime str is
            # validated by Pydantic on construction (the `except` below
            # catches mismatches), but mypy without the Pydantic plugin
            # only sees the bare `str` and refuses the Literal narrowing.
            status=status_raw,  # type: ignore[arg-type]
            attempts=attempts,
            next_attempt_at=next_attempt_at,
            suggested_poll_after_seconds=suggested,
        )
    except Exception as exc:  # pydantic ValidationError on Literal/ge=0
        raise PhantomEnvelopeError(
            f"response headers failed validation: {dict(headers)!r}: {exc}"
        ) from exc


__all__ = [
    "ResponseHeaders",
    "parse_response_headers",
]
