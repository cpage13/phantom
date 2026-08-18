"""Typed error vocabulary for Phantom's HTTP surface.

Every error Phantom returns over HTTP carries a stable ``ErrorCode``
plus an :class:`ErrorBody`. The ``STATUS_FOR_CODE`` mapping defines
the canonical HTTP status for each code. ``ErrorEnvelope`` wraps an
``ErrorBody`` in the ``{"error": {...}}`` shape Phantom emits.

See plan §5.6 for the canonical table.
"""

from __future__ import annotations

from typing import Any, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field

ErrorCode: TypeAlias = Literal[  # noqa: UP040 - see chain.py rationale
    "envelope_invalid",
    "envelope_duplicate",
    "header_invalid",
    "body_ref_missing",
    "body_ref_orphan",
    "body_ref_duplicate",
    "body_too_large",
    "template_unresolved",
    "invalid_target",
    "instance_unknown",
    "lookup_not_configured",
    "idempotency_replay",
    "idempotency_key_conflict",
    "chain_id_in_use",
    "restore_noop",
    "replay_body_discarded",
    "replay_refused_attempting",
    "multifile_cursor_conflict",
    "key_value_match_invalid",
    "bulk_delete_filter_empty",
    "request_invalid",
    "saturation_cap",
    "disk_pressure",
    "storage_unavailable",
    "auth_token_missing",
    "upstream_unreachable",
    "internal_error",
    "not_found",
    "storage_corruption",
    "codec_round_trip_drift",
]
"""Stable string vocabulary for every error Phantom returns. New codes
require updating ``STATUS_FOR_CODE`` and the ADR-010 error table.

``storage_corruption`` and ``codec_round_trip_drift`` are terminal
chain-level codes surfaced on admin responses for rows that failed
body verification at send time; they are never returned over HTTP at
ingress.

Since N2 ``storage_corruption`` is ALSO an admin error BODY, returned by
the two single-chain body reads when the store holds fewer body_refs than
the row declares, so its ``500`` is a real response status on that path
rather than only a defensive default. ``codec_round_trip_drift`` remains a
``last_error`` value alone.
"""


STATUS_FOR_CODE: dict[ErrorCode, int] = {
    "envelope_invalid": 422,
    # A multipart submission carried two ``envelope`` parts - ambiguous
    # which chain the producer intended (the envelope holds the chain_id /
    # destination / step chain). Rejected for parity with body_ref_duplicate
    # and the duplicate-step-name check (finding R3-9 - the E-1 sibling);
    # silently last-wins dropped one envelope with no signal.
    "envelope_duplicate": 422,
    # An OPTIONAL grouping/ordering request header on POST /v1/send was
    # present but failed to parse (X-Phantom-Group-Id /
    # X-Phantom-Multifile-Id must be UUIDs; X-Phantom-Order a
    # non-negative integer). 400 Bad Request: the request HEADER itself
    # is malformed, distinct from the 422 family where a well-formed
    # request carries an unprocessable BODY. Rejecting loudly (instead
    # of filing the upload under the chain_id defaults) tells the
    # producer its grouping intent was dropped (cycle-7 plan section 3,
    # task 2.2).
    "header_invalid": 400,
    "body_ref_missing": 422,
    "body_ref_orphan": 422,
    # A multipart submission carried two parts with the same
    # ``body_refs[<name>]`` - ambiguous which body the producer intended.
    # Rejected for parity with body_ref_missing / body_ref_orphan
    # (finding E-1); silently last-wins dropped one body with no signal.
    "body_ref_duplicate": 422,
    # 413 Payload Too Large per RFC 9110 §15.5.14. Emitted by the
    # Content-Length precheck on POST /v1/send when the declared length
    # exceeds ``Settings.storage.max_buffered_bytes``, and by the
    # streaming size cap when a chunked body exceeds the same limit
    # mid-stream (H2 audit closure).
    "body_too_large": 413,
    "template_unresolved": 422,
    "invalid_target": 421,
    "instance_unknown": 421,
    # GET /v1/admin/uploads/by-captured-id was asked of an instance whose
    # configuration carries no admin_lookup binding (cycle-7 task 4.3).
    # Where the upstream identifier lives inside the captured values is
    # deployment knowledge; without the binding Phantom refuses to guess.
    # 400 Bad Request: the request cannot be served as posed against this
    # instance's configuration (not a 404; nothing was looked up at all).
    "lookup_not_configured": 400,
    "idempotency_replay": 200,
    # An X-Phantom-Idempotency-Key was reused with a DIFFERENT body than
    # the claim it collides with. An idempotency key MUST be a function
    # of the body; reusing it with different bytes is a client contract
    # violation that would otherwise silently drop the second body
    # behind a success-shaped 200 replay (finding G-1). 422 - the
    # request is well-formed but semantically unprocessable.
    "idempotency_key_conflict": 422,
    # The envelope's chain_id (the row primary key) is already in use by
    # a live row. A re-POST of the same chain_id under a fresh
    # idempotency key would otherwise escape as a naked HTTP 500
    # (finding D-1). 409 Conflict - the request conflicts with current
    # server state; the producer must mint a fresh chain_id (UUID4 collisions
    # are astronomically rare, so this almost always means a client bug).
    "chain_id_in_use": 409,
    # The one-call admin restore moved NOTHING into the live tree (finding
    # H-1 / L-2). The chosen mode_switch backup's DB never reached the live
    # path, typically because the live DB could not be set aside first
    # (a same-second collision before the disambiguation fix, or the backup
    # artifacts vanished between the membership check and the move). 409
    # Conflict: the restore conflicts with current on-disk state; the operator
    # retries (a moment later, or after checking the inventory). Returning a
    # success-shaped response here would silently strand the buffered uploads.
    "restore_noop": 409,
    # POST /v1/admin/chains/{chain_id}/replay named a row whose body was
    # already discarded per the row's own accounting (body_discarded_at
    # stamped by the ONE discard owner, discard_body_and_zero_accounting,
    # on any of its three triggers: the sender's immediate leg at
    # succeeded_body_seconds == 0, the reaper's scheduled leg, or the
    # shared expire_row send-deadline give-up per ADR-032). A replay
    # re-queue would hand the sender a
    # row with no bytes to send, laundering the operator action into a
    # scary 'corrupted' terminal on the next claim (cycle-7 phase 7
    # pre-round defender fix). 409 Conflict: the request conflicts with
    # the row's current on-disk state; the row is left exactly as it was.
    "replay_body_discarded": 409,
    # POST /v1/admin/chains/{chain_id}/replay named a row currently in
    # 'attempting': a sender is actively driving it, and a re-queue would
    # clobber the in-flight attempt (M-W4-F7 audit closure). Pre-fix this
    # refusal escaped as FastAPI's raw {"detail": ...} body; round 1
    # defender fix R1-1 promotes it to the canonical envelope. 409
    # Conflict: the request conflicts with the row's current state; the
    # operator waits for the attempt to settle (or cancels first), then
    # retries. The row is left exactly as it was.
    "replay_refused_attempting": 409,
    # GET /v1/admin/chains combined ?multifile_id= with ?cursor=. The
    # multifile listing is one-shot by design (ordered by send_order, a
    # multi-file set is producer-scale small, next_cursor always null),
    # so a cursor cannot apply to it. 422: each parameter is well-formed
    # but the combination is unprocessable as posed. Pre-fix this refusal
    # escaped as FastAPI's raw {"detail": ...} body via bare
    # HTTPException; round 2 defender fix R2-2 promotes it onto the
    # canonical envelope (ADR-017).
    "multifile_cursor_conflict": 422,
    # GET /v1/admin/chains carried a ?key_value_match= value that does
    # not parse as 'key:value' with non-empty key and value. 422: the
    # parameter value itself is malformed. Promoted onto the canonical
    # envelope by round 2 defender fix R2-2.
    "key_value_match_invalid": 422,
    # DELETE /v1/admin/chains carried an all-None filter body. An empty
    # filter would mean "delete every row", which the bulk surface
    # refuses by design (ADR-004); the caller must name at least one of
    # state/route/since/instance. 422: the body is well-formed but
    # unprocessable as posed. Promoted onto the canonical envelope by
    # round 2 defender fix R2-2.
    "bulk_delete_filter_empty": 422,
    # A typed path or query parameter failed FastAPI request validation
    # (a malformed UUID in /groups/{group_id}, a missing required
    # backup_id on the restore route, ...). FastAPI's default reply is a
    # raw {"detail": [...]} body; the shared RequestValidationError
    # handler promotes the whole class onto the canonical envelope
    # (round 6 defender fix R6-4; R1-1/R2-2 closed only the
    # bare-HTTPException escapes). 422 matches FastAPI's own status for
    # the condition: each parameter is syntactically present but
    # unprocessable as posed.
    "request_invalid": 422,
    "saturation_cap": 503,
    # disk_pressure shares the saturation-class 503 - both "server cannot
    # accept right now, retry later" - distinguished from saturation_cap
    # by the cause: disk_pressure means storage is the constraint, not
    # in-flight row/byte counts. Both emit Retry-After.
    "disk_pressure": 503,
    # A storage-layer write FAULT (an ``OSError`` - fsync EIO or ENOSPC)
    # struck while admission was durably buffering the upload body
    # (``body_store.put``). Distinct from ``disk_pressure``: that is the
    # PROACTIVE ``max_disk_bytes`` gate (a polled ceiling), whereas this is
    # a REACTIVE per-write failure that the proactive gate cannot foresee (a
    # burst that fills the real disk between probe ticks, or a flaky SD card
    # returning EIO). 503 + Retry-After - the fault is transient (the reaper/
    # operator frees space; a transient EIO may not recur), so the producer should
    # retry rather than fall back direct-to-upstream (findings R7-1-A/B,
    # R7-2-A). Durability holds regardless: the failed put commits no row, so
    # a retry-or-not loses nothing (R7-1/R7-2 durability result).
    "storage_unavailable": 503,
    "auth_token_missing": 401,
    "upstream_unreachable": 502,
    "internal_error": 500,
    "not_found": 404,
    # Body-verification corruption codes are chain-row terminal causes,
    # not ingress responses. Mapped here for completeness so the
    # ``STATUS_FOR_CODE`` lookup is total on ``ErrorCode``.
    "storage_corruption": 500,
    "codec_round_trip_drift": 500,
}
"""Canonical HTTP status per ``ErrorCode``."""


class ErrorBody(BaseModel):
    """The error body shape Phantom emits for every error response."""

    model_config = ConfigDict(strict=True, extra="forbid")

    code: ErrorCode = Field(
        ...,
        description="Stable enum value identifying the error.",
    )
    message: str = Field(
        ...,
        description="Human-readable description.",
    )
    instance_id: str = Field(
        ...,
        description=("The InstanceCfg.id that produced the error, or 'unrouted'."),
    )
    request_id: str = Field(
        ...,
        description="Per-request correlation id from logging middleware.",
    )
    details: dict[str, Any] = Field(
        default_factory=dict,
        description="Code-specific context.",
    )


class ErrorEnvelope(BaseModel):
    """Outer wrapper for every error response body: ``{"error": ErrorBody}``."""

    model_config = ConfigDict(strict=True, extra="forbid")

    error: ErrorBody = Field(..., description="The error body.")


def error_response(
    code: ErrorCode,
    message: str,
    *,
    instance_id: str,
    request_id: str,
    details: dict[str, Any] | None = None,
) -> ErrorEnvelope:
    """Build an :class:`ErrorEnvelope` with the given fields.

    Args:
        code: The stable error code.
        message: Human-readable explanation.
        instance_id: The instance that produced the error, or ``"unrouted"``.
        request_id: Per-request correlation id.
        details: Optional code-specific context.

    Returns:
        An :class:`ErrorEnvelope` wrapping a populated :class:`ErrorBody`.
    """
    return ErrorEnvelope(
        error=ErrorBody(
            code=code,
            message=message,
            instance_id=instance_id,
            request_id=request_id,
            details=details or {},
        )
    )
