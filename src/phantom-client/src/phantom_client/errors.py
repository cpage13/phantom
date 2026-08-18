"""Exception hierarchy and ADR-010 error-code mapping.

The SDK distinguishes three kinds of failure:

- :class:`PhantomTransportError` and its subclasses - connect refused,
  read timeout, network drop. **Retry-eligible**: the SDK retries these
  per :class:`phantom_client.config.RetryPolicy`. Phantom never saw the
  request, so the SDK is safe to repeat.
- :class:`PhantomHttpError` and its subclasses - Phantom returned a
  non-2xx with a structured ``ErrorEnvelope`` body. **Not** retry-
  eligible: Phantom is the retry engine, so doubling up muddies
  idempotency. The SDK surfaces the typed exception and the caller
  decides.
- SDK-side validation errors (envelope didn't parse, pre-flight check
  failed) - :class:`PhantomEnvelopeError`, :class:`PollDeadlineExceeded`,
  :class:`EmptyFilterError`.

Every ADR-010 ``error.code`` string maps to a typed exception class via
:data:`EXCEPTION_FOR_CODE`. Unknown codes raise the generic
:class:`PhantomHttpError` with the raw ``error_code`` preserved.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, NoReturn

# ---------------------------------------------------------------------------
# Root.
# ---------------------------------------------------------------------------


class PhantomClientError(Exception):
    """Base for every exception this SDK raises."""


# ---------------------------------------------------------------------------
# Transport-class (retry-eligible).
# ---------------------------------------------------------------------------


class PhantomTransportError(PhantomClientError):
    """Network-class failure, which may or may not have reached Phantom.

    The base of both classes, so it asserts neither. A failure that
    PROVABLY never landed (connect refused, connect timeout, pool
    timeout) was retried by :class:`phantom_client.transport.Transport`
    per :class:`phantom_client.config.RetryPolicy` before this surfaced.
    A failure that MAY HAVE LANDED (a read or write timeout, a reset, a
    server disconnect mid-response) surfaces immediately for any call
    that did not opt into re-sending it, which is every mutating admin
    call: the request may already have executed. Treat it as an unknown
    outcome and read the resource's state rather than repeating the
    request.
    """


class PhantomConnectError(PhantomTransportError):
    """TCP connect refused / DNS failure / unable to reach Phantom."""


class PhantomTimeoutError(PhantomTransportError):
    """Read, write, pool, or connect timeout."""


class PhantomNetworkError(PhantomTransportError):
    """Other transport-layer failure (e.g., remote reset mid-stream)."""


# ---------------------------------------------------------------------------
# HTTP-class - Phantom returned a non-2xx with an ErrorEnvelope.
# ---------------------------------------------------------------------------


class PhantomHttpError(PhantomClientError):
    """Phantom returned a non-2xx with a structured error body.

    All attributes are required keyword arguments; the SDK constructs
    these from :func:`raise_for_error_body` after parsing the
    ``ErrorEnvelope`` payload.

    Attributes:
        status_code: HTTP status code.
        error_code: ADR-010 ``error.code`` string (stable identifier).
        error_message: Human-readable message from ``error.message``.
        request_id: Per-request correlation id from ``error.request_id``.
        instance_id: Instance id from ``error.instance_id`` (``"unrouted"``
            when no instance handled the request).
        response_headers: Response headers, suitable for downstream tooling.
        details: Free-form details dict from ``error.details``.
    """

    def __init__(
        self,
        *,
        status_code: int,
        error_code: str,
        error_message: str,
        request_id: str,
        instance_id: str,
        response_headers: Mapping[str, str],
        details: dict[str, Any],
    ) -> None:
        super().__init__(f"{status_code} {error_code}: {error_message}")
        self.status_code = status_code
        self.error_code = error_code
        self.error_message = error_message
        self.request_id = request_id
        self.instance_id = instance_id
        self.response_headers: Mapping[str, str] = response_headers
        self.details: dict[str, Any] = details


class PhantomBadRequestError(PhantomHttpError):
    """400 - malformed request envelope-level shape, invalid target."""


class PhantomUnauthorizedError(PhantomHttpError):
    """401 - auth_token_missing or otherwise unauthorized."""


class PhantomNotFoundError(PhantomHttpError):
    """404 - referenced resource (upload, instance, token slot) missing."""


class PhantomConflictError(PhantomHttpError):
    """409 - state conflict (e.g., replay a row that isn't replayable)."""


class PhantomPayloadTooLargeError(PhantomHttpError):
    """413 - declared or streamed body exceeded the server's max_buffered_bytes cap (H2)."""


class PhantomUnprocessableError(PhantomHttpError):
    """422 - semantic envelope problem (generic)."""


class PhantomValidationError(PhantomUnprocessableError):
    """422 - envelope_invalid / body_ref_missing / body_ref_orphan / template_unresolved."""


class PhantomRateLimitedError(PhantomHttpError):
    """429 - caller rate-limited."""


class PhantomServerError(PhantomHttpError):
    """5xx - server-side failure not classified as a saturation cap."""


class PhantomUnavailableError(PhantomServerError):
    """503 - saturation_cap or other temporarily-unavailable."""


# ---------------------------------------------------------------------------
# SDK-side validation errors.
# ---------------------------------------------------------------------------


class PhantomEnvelopeError(PhantomClientError):
    """Server response did not parse as ``ErrorEnvelope`` or the expected model.

    Raised by :meth:`phantom_client.transport.Transport` when a non-2xx
    arrives with an unexpected body shape and by
    :func:`phantom_client.models.envelope.parse_response_headers` when
    required headers are absent from a 2xx response.
    """


class PollDeadlineExceeded(PhantomClientError):  # noqa: N818 - name fixed by plan public surface
    """:func:`phantom_client.poller.poll_until` exceeded its deadline."""


class EmptyFilterError(PhantomClientError):
    """:meth:`PhantomClient.bulk_delete` was given an empty filter."""


# ---------------------------------------------------------------------------
# Code → exception class mapping (per ADR-010).
# ---------------------------------------------------------------------------

EXCEPTION_FOR_CODE: dict[str, type[PhantomHttpError]] = {
    "envelope_invalid": PhantomValidationError,
    # Duplicate multipart ``envelope`` part (finding R3-9, the E-1 sibling)
    # - a malformed-input 422 in the same envelope/validation family.
    "envelope_duplicate": PhantomValidationError,
    # A grouping/ordering request header (X-Phantom-Group-Id /
    # X-Phantom-Multifile-Id / X-Phantom-Order) was present on
    # POST /v1/send but failed to parse. 400 Bad Request: the header
    # itself is malformed (cycle-7 task 2.2).
    "header_invalid": PhantomBadRequestError,
    "body_ref_missing": PhantomValidationError,
    "body_ref_orphan": PhantomValidationError,
    # Duplicate ``body_refs[<name>]`` multipart part (finding E-1) - a
    # malformed-input 422 in the same family as the other body_ref codes.
    "body_ref_duplicate": PhantomValidationError,
    # 413 closure: the H2 audit fix makes the service reject oversized
    # bodies before reading them. SDK callers see this typed exception
    # instead of an unexpected 5xx OOM crash on the wire.
    "body_too_large": PhantomPayloadTooLargeError,
    "template_unresolved": PhantomValidationError,
    "invalid_target": PhantomBadRequestError,
    "instance_unknown": PhantomBadRequestError,
    # The by-captured-id admin lookup was asked of an instance whose
    # configuration carries no admin_lookup binding (cycle-7 task 4.3).
    # 400: the instance cannot serve the lookup as posed; the operator
    # supplies the per-instance capture_name/json_path binding.
    "lookup_not_configured": PhantomBadRequestError,
    # Idempotency key reused with a different body (finding G-1). 422 -
    # the request is well-formed but conflicts with the prior claim under
    # the same key. A generic unprocessable (not a validation/envelope
    # problem); the caller must not retry without changing its key.
    "idempotency_key_conflict": PhantomUnprocessableError,
    # Envelope chain_id (row PK) already in use by a live row (finding
    # D-1). 409 Conflict - distinct from the saturation/disk back-pressure
    # 503s; the producer must mint a fresh chain_id.
    "chain_id_in_use": PhantomConflictError,
    # The one-call admin restore moved nothing into the live tree (finding
    # H-1 / L-2): a 409 Conflict so the operator retries rather than being
    # handed a success-shaped response that stranded the buffered uploads.
    "restore_noop": PhantomConflictError,
    # Replay of a row whose body was already discarded per the row's own
    # accounting (body_discarded_at stamped on either discard leg). A
    # re-queue could only land the row in 'corrupted' on the sender's next
    # claim, so the service refuses up front with a 409 Conflict; the
    # operator re-submits through POST /v1/send if the upload must run
    # again (cycle-7 phase 7 pre-round defender fix).
    "replay_body_discarded": PhantomConflictError,
    # Replay of a row a sender is actively driving (state 'attempting').
    # A re-queue would clobber the in-flight attempt, so the service
    # refuses up front with a 409 Conflict; the operator waits for the
    # attempt to settle (or cancels the chain first), then retries the
    # replay (round 1 defender fix, R1-1).
    "replay_refused_attempting": PhantomConflictError,
    # GET /v1/admin/chains combined multifile_id with cursor: the
    # multifile listing is one-shot (never paginated), so the
    # combination is unprocessable as posed. 422, semantic not
    # shape-malformed, so the generic unprocessable class (the
    # idempotency_key_conflict idiom). SDK-reachable via
    # list_uploads(multifile_id=..., cursor=...); pre-fix the raw
    # {"detail": ...} body raised PhantomEnvelopeError instead (round 2
    # defender fix R2-2).
    "multifile_cursor_conflict": PhantomUnprocessableError,
    # A ?key_value_match= value that does not parse as 'key:value' with
    # non-empty key and value. 422 in the malformed-input validation
    # family (the envelope_invalid idiom); the SDK always encodes the
    # colon, so this surfaces to raw-wire callers (round 2 defender fix
    # R2-2).
    "key_value_match_invalid": PhantomValidationError,
    # DELETE /v1/admin/chains with an all-None filter body: an empty
    # filter would mean "delete every row", refused by design
    # (ADR-004). 422, semantic not shape-malformed. The SDK pre-flights
    # empty filters with EmptyFilterError, so this surfaces to raw-wire
    # callers (round 2 defender fix R2-2).
    "bulk_delete_filter_empty": PhantomUnprocessableError,
    # A typed path/query parameter failed FastAPI request validation (a
    # malformed UUID, a missing required backup_id). 422 in the
    # malformed-input validation family (the envelope_invalid idiom).
    # The typed SDK coerces UUIDs before sending, so this surfaces to
    # raw-wire callers (round 6 defender fix R6-4).
    "request_invalid": PhantomValidationError,
    "saturation_cap": PhantomUnavailableError,
    # Phantom emits ``disk_pressure`` (HTTP 503 + ``Retry-After``) when
    # the DiskPressureProbe observes the configured ``max_disk_bytes``
    # cap exceeded. Semantically the same posture as saturation_cap:
    # caller should back off and retry per ``Retry-After``. The typed
    # exception lets callers tell "I'm pushing too much disk" apart from
    # "I'm pushing too many concurrent requests".
    "disk_pressure": PhantomUnavailableError,
    # A storage-layer write fault (fsync EIO / ENOSPC) struck while Phantom
    # was durably buffering the body (findings R7-1-A/B, R7-2-A). Same
    # retryable 503 + Retry-After posture as disk_pressure / saturation_cap:
    # the fault is transient, so the caller backs off and retries rather than
    # falling back direct-to-upstream. A distinct code (not disk_pressure) so
    # a caller can tell the REACTIVE per-write fault apart from the PROACTIVE
    # max_disk_bytes gate.
    "storage_unavailable": PhantomUnavailableError,
    "auth_token_missing": PhantomUnauthorizedError,
    "upstream_unreachable": PhantomServerError,
    "internal_error": PhantomServerError,
    "not_found": PhantomNotFoundError,
    # Body-verification corruption codes are chain-row terminal causes,
    # not ingress responses. Listed here for completeness so callers can
    # dispatch on the code surfaced via admin lookups; the generic
    # ``PhantomServerError`` class captures the "server-side problem"
    # posture even though the actual emission path is row-level state,
    # not HTTP response.
    "storage_corruption": PhantomServerError,
    "codec_round_trip_drift": PhantomServerError,
}
"""Mapping from ADR-010 ``error.code`` to the typed exception class.

Unknown codes fall through to the generic :class:`PhantomHttpError`
with ``error_code`` preserved so callers can dispatch on it directly.
"""


def raise_for_error_body(
    body: dict[str, Any],
    *,
    status_code: int,
    response_headers: Mapping[str, str],
) -> NoReturn:
    """Raise the typed exception for an ADR-010 error envelope.

    Reads ``body["error"]`` (shape per ADR-010), looks up the
    appropriate exception class in :data:`EXCEPTION_FOR_CODE`, and
    raises it with full context. Unknown codes raise the generic
    :class:`PhantomHttpError`.

    Args:
        body: The parsed JSON response body. Must contain ``error``.
        status_code: HTTP status code from the response.
        response_headers: Response headers (for downstream debugging).

    Raises:
        PhantomEnvelopeError: When ``body`` doesn't carry the expected
            ``error`` field.
        PhantomHttpError (or a subclass): For the parsed error envelope.
    """
    error = body.get("error")
    if not isinstance(error, dict):
        raise PhantomEnvelopeError(
            f"response body missing 'error' object: {body!r}",
        )
    code = error.get("code")
    if not isinstance(code, str):
        raise PhantomEnvelopeError(
            f"response body 'error.code' is not a string: {error!r}",
        )
    message = str(error.get("message", ""))
    request_id = str(error.get("request_id", ""))
    instance_id = str(error.get("instance_id", "unrouted"))
    details_obj = error.get("details", {})
    details: dict[str, Any] = details_obj if isinstance(details_obj, dict) else {}
    exc_class = EXCEPTION_FOR_CODE.get(code, PhantomHttpError)
    raise exc_class(
        status_code=status_code,
        error_code=code,
        error_message=message,
        request_id=request_id,
        instance_id=instance_id,
        response_headers=response_headers,
        details=details,
    )


__all__ = [
    "EXCEPTION_FOR_CODE",
    "EmptyFilterError",
    "PhantomBadRequestError",
    "PhantomClientError",
    "PhantomConflictError",
    "PhantomConnectError",
    "PhantomEnvelopeError",
    "PhantomHttpError",
    "PhantomNetworkError",
    "PhantomNotFoundError",
    "PhantomRateLimitedError",
    "PhantomServerError",
    "PhantomTimeoutError",
    "PhantomTransportError",
    "PhantomUnauthorizedError",
    "PhantomUnavailableError",
    "PhantomUnprocessableError",
    "PhantomValidationError",
    "PollDeadlineExceeded",
    "raise_for_error_body",
]
