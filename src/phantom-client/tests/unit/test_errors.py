"""Unit tests for ``phantom_client.errors``."""

from __future__ import annotations

import pytest
from phantom_client.errors import (
    EXCEPTION_FOR_CODE,
    EmptyFilterError,
    PhantomBadRequestError,
    PhantomClientError,
    PhantomConflictError,
    PhantomConnectError,
    PhantomEnvelopeError,
    PhantomHttpError,
    PhantomNetworkError,
    PhantomNotFoundError,
    PhantomPayloadTooLargeError,
    PhantomRateLimitedError,
    PhantomServerError,
    PhantomTimeoutError,
    PhantomTransportError,
    PhantomUnauthorizedError,
    PhantomUnavailableError,
    PhantomUnprocessableError,
    PhantomValidationError,
    PollDeadlineExceeded,
    raise_for_error_body,
)


def test_hierarchy_is_subtree() -> None:
    """Every SDK exception is a PhantomClientError; transport/HTTP form disjoint subtrees."""
    for cls in (
        PhantomTransportError,
        PhantomConnectError,
        PhantomTimeoutError,
        PhantomNetworkError,
    ):
        assert issubclass(cls, PhantomTransportError)
        assert issubclass(cls, PhantomClientError)
    for cls in (
        PhantomHttpError,
        PhantomBadRequestError,
        PhantomUnauthorizedError,
        PhantomNotFoundError,
        PhantomConflictError,
        PhantomPayloadTooLargeError,
        PhantomUnprocessableError,
        PhantomValidationError,
        PhantomRateLimitedError,
        PhantomServerError,
        PhantomUnavailableError,
    ):
        assert issubclass(cls, PhantomHttpError)
        assert issubclass(cls, PhantomClientError)
    assert issubclass(PhantomValidationError, PhantomUnprocessableError)
    assert issubclass(PhantomUnavailableError, PhantomServerError)
    # Transport and HTTP subtrees are disjoint.
    assert not issubclass(PhantomHttpError, PhantomTransportError)
    assert not issubclass(PhantomTransportError, PhantomHttpError)
    # SDK-side validation errors
    for cls in (PhantomEnvelopeError, PollDeadlineExceeded, EmptyFilterError):
        assert issubclass(cls, PhantomClientError)


def test_mapping_complete() -> None:
    """EXCEPTION_FOR_CODE covers every ADR-010 code the SDK knows."""
    assert set(EXCEPTION_FOR_CODE) == {
        "envelope_invalid",
        # Aggressor R3 finding R3-9: duplicate multipart envelope part.
        "envelope_duplicate",
        # Cycle-7 task 2.2: malformed grouping/ordering request header (400).
        "header_invalid",
        "body_ref_missing",
        "body_ref_orphan",
        # Aggressor R1 finding E-1: duplicate multipart body_ref part.
        "body_ref_duplicate",
        # H2 audit closure: 413 typed exception when Content-Length precheck
        # or streaming cap rejects.
        "body_too_large",
        "template_unresolved",
        "invalid_target",
        "instance_unknown",
        # Cycle-7 task 4.3: by-captured-id lookup on an instance without the
        # deployment-supplied admin_lookup binding (400; never guesses).
        "lookup_not_configured",
        # Aggressor R1 finding G-1: idempotency key reused with a different body.
        "idempotency_key_conflict",
        # Aggressor R1 finding D-1: envelope chain_id (row PK) already in use.
        "chain_id_in_use",
        # Extreme-hardening H-1 / L-2: the one-call admin restore moved nothing
        # into the live tree (409 Conflict).
        "restore_noop",
        # Cycle-7 phase 7 pre-round defender fix: replay refused because the
        # row's body was already discarded (409 Conflict).
        "replay_body_discarded",
        # Round 1 defender fix R1-1: replay refused because a sender is
        # actively driving the row (409 Conflict, canonical envelope).
        "replay_refused_attempting",
        # Round 2 defender fix R2-2: the three admin query-shape refusals
        # (multifile+cursor, malformed key_value_match, empty bulk-delete
        # filter) ride the canonical envelope as 422s.
        "multifile_cursor_conflict",
        "key_value_match_invalid",
        "bulk_delete_filter_empty",
        # Round 6 defender fix R6-4: FastAPI request-parameter validation
        # (malformed/missing typed path or query params) promoted onto the
        # canonical envelope as 422 request_invalid.
        "request_invalid",
        "saturation_cap",
        "disk_pressure",
        # Aggressor R7 findings R7-1-A/B / R7-2-A: storage-layer write fault
        # during admission body buffering (503, retryable).
        "storage_unavailable",
        "auth_token_missing",
        "upstream_unreachable",
        "internal_error",
        "not_found",
        # F3 body-verification corruption codes (chain-row terminal).
        "storage_corruption",
        "codec_round_trip_drift",
    }
    assert EXCEPTION_FOR_CODE["body_too_large"] is PhantomPayloadTooLargeError
    # Validation-class codes map to PhantomValidationError (body_ref_duplicate joins them).
    for code in (
        "envelope_invalid",
        "envelope_duplicate",
        "body_ref_missing",
        "body_ref_orphan",
        "body_ref_duplicate",
        "template_unresolved",
    ):
        assert EXCEPTION_FOR_CODE[code] is PhantomValidationError
    assert EXCEPTION_FOR_CODE["invalid_target"] is PhantomBadRequestError
    assert EXCEPTION_FOR_CODE["instance_unknown"] is PhantomBadRequestError
    # Cycle-7 task 4.3: the unconfigured-lookup refusal is a 400 the
    # operator fixes by supplying the per-instance binding.
    assert EXCEPTION_FOR_CODE["lookup_not_configured"] is PhantomBadRequestError
    # D-1 / G-1 typed dispatch.
    assert EXCEPTION_FOR_CODE["chain_id_in_use"] is PhantomConflictError
    assert EXCEPTION_FOR_CODE["idempotency_key_conflict"] is PhantomUnprocessableError
    # Cycle-7 phase 7 defender fix: body-discarded replay refusal is a
    # 409 Conflict like the other state-conflict codes.
    assert EXCEPTION_FOR_CODE["replay_body_discarded"] is PhantomConflictError
    # Round 1 defender fix R1-1: the attempting replay refusal joins the
    # same conflict class (wait for the sender, then retry).
    assert EXCEPTION_FOR_CODE["replay_refused_attempting"] is PhantomConflictError
    # Round 2 defender fix R2-2: the multifile+cursor combination and the
    # empty bulk-delete filter are well-formed-but-unprocessable 422s
    # (the idempotency_key_conflict idiom); the malformed key_value_match
    # is a 422 in the validation family.
    assert EXCEPTION_FOR_CODE["multifile_cursor_conflict"] is PhantomUnprocessableError
    assert EXCEPTION_FOR_CODE["bulk_delete_filter_empty"] is PhantomUnprocessableError
    assert EXCEPTION_FOR_CODE["key_value_match_invalid"] is PhantomValidationError
    assert EXCEPTION_FOR_CODE["saturation_cap"] is PhantomUnavailableError
    # disk_pressure shares the saturation-class 503 posture; both
    # require operator back-off per Retry-After.
    assert EXCEPTION_FOR_CODE["disk_pressure"] is PhantomUnavailableError
    # storage_unavailable (R7-1/R7-2 write fault) shares that retryable
    # 503 posture too.
    assert EXCEPTION_FOR_CODE["storage_unavailable"] is PhantomUnavailableError
    assert EXCEPTION_FOR_CODE["auth_token_missing"] is PhantomUnauthorizedError
    assert EXCEPTION_FOR_CODE["upstream_unreachable"] is PhantomServerError
    assert EXCEPTION_FOR_CODE["internal_error"] is PhantomServerError
    # 404 admin lookups map to PhantomNotFoundError.
    assert EXCEPTION_FOR_CODE["not_found"] is PhantomNotFoundError


def test_raise_for_disk_pressure_503() -> None:
    """A 503 with ``error.code == 'disk_pressure'`` raises PhantomUnavailableError.

    Phantom emits this when ``DiskPressureProbe`` detects the configured
    ``max_disk_bytes`` cap has been exceeded; the SDK surfaces the typed
    503 exception so callers can dispatch separately from ``saturation_cap``.
    Both share ``PhantomUnavailableError`` because the operator's
    correct response (back off, retry per Retry-After) is identical.
    """
    body = {
        "error": {
            "code": "disk_pressure",
            "message": "disk usage exceeds max_disk_bytes",
            "request_id": "req-d1",
            "instance_id": "primary",
            "details": {"max_disk_bytes": 1024},
        }
    }
    with pytest.raises(PhantomUnavailableError) as exc:
        raise_for_error_body(
            body,
            status_code=503,
            response_headers={"Retry-After": "60"},
        )
    assert exc.value.status_code == 503
    assert exc.value.error_code == "disk_pressure"
    assert exc.value.response_headers["Retry-After"] == "60"


def test_raise_for_replay_body_discarded_409() -> None:
    """A 409 with ``error.code == 'replay_body_discarded'`` raises PhantomConflictError.

    Phantom emits this when an admin replay names a row whose body was
    already discarded per the retention policy (cycle-7 phase 7 defender
    fix); the details name the chain and the discard instant so the
    caller can report exactly why the replay cannot run.
    """
    body = {
        "error": {
            "code": "replay_body_discarded",
            "message": "Replay refused: the body was discarded",
            "request_id": "",
            "instance_id": "primary",
            "details": {
                "chain_id": "00000000-0000-0000-0000-000000000001",
                "body_discarded_at": "2026-06-10T00:00:00+00:00",
            },
        }
    }
    with pytest.raises(PhantomConflictError) as exc:
        raise_for_error_body(body, status_code=409, response_headers={})
    assert exc.value.status_code == 409
    assert exc.value.error_code == "replay_body_discarded"
    assert exc.value.details["chain_id"] == "00000000-0000-0000-0000-000000000001"
    assert exc.value.details["body_discarded_at"] == "2026-06-10T00:00:00+00:00"


def test_raise_for_replay_refused_attempting_409() -> None:
    """A 409 with ``error.code == 'replay_refused_attempting'`` raises PhantomConflictError.

    Phantom emits this when an admin replay names a row a sender is
    actively driving (round 1 defender fix R1-1; previously the raw
    FastAPI detail body made ``raise_for_error_body`` raise
    ``PhantomEnvelopeError`` instead). The details name the chain so
    the caller can report exactly which replay must wait.
    """
    body = {
        "error": {
            "code": "replay_refused_attempting",
            "message": "Replay refused: a sender is actively driving the row",
            "request_id": "",
            "instance_id": "primary",
            "details": {"chain_id": "00000000-0000-0000-0000-000000000002"},
        }
    }
    with pytest.raises(PhantomConflictError) as exc:
        raise_for_error_body(body, status_code=409, response_headers={})
    assert exc.value.status_code == 409
    assert exc.value.error_code == "replay_refused_attempting"
    assert exc.value.details["chain_id"] == "00000000-0000-0000-0000-000000000002"


def test_raise_for_multifile_cursor_conflict_422() -> None:
    """A 422 ``multifile_cursor_conflict`` raises PhantomUnprocessableError.

    Phantom emits this when ``GET /v1/admin/chains`` combines
    ``multifile_id`` with ``cursor`` (round 2 defender fix R2-2;
    previously the raw FastAPI detail body made ``raise_for_error_body``
    raise ``PhantomEnvelopeError`` instead). SDK-reachable through
    ``list_uploads(multifile_id=..., cursor=...)``; the details echo
    both offending parameters.
    """
    body = {
        "error": {
            "code": "multifile_cursor_conflict",
            "message": "cursor cannot be combined with multifile_id",
            "request_id": "",
            "instance_id": "unrouted",
            "details": {
                "multifile_id": "00000000-0000-0000-0000-000000000003",
                "cursor": "opaque-token",
            },
        }
    }
    with pytest.raises(PhantomUnprocessableError) as exc:
        raise_for_error_body(body, status_code=422, response_headers={})
    assert exc.value.status_code == 422
    assert exc.value.error_code == "multifile_cursor_conflict"
    assert exc.value.details["multifile_id"] == "00000000-0000-0000-0000-000000000003"
    assert exc.value.details["cursor"] == "opaque-token"


def test_raise_for_known_code() -> None:
    """raise_for_error_body raises the mapped exception for a known code."""
    body = {
        "error": {
            "code": "saturation_cap",
            "message": "too busy",
            "request_id": "req-1",
            "instance_id": "primary",
            "details": {"max_in_flight": 100},
        }
    }
    with pytest.raises(PhantomUnavailableError) as exc:
        raise_for_error_body(body, status_code=503, response_headers={"x": "y"})
    assert exc.value.status_code == 503
    assert exc.value.error_code == "saturation_cap"
    assert exc.value.error_message == "too busy"
    assert exc.value.request_id == "req-1"
    assert exc.value.instance_id == "primary"
    assert exc.value.details == {"max_in_flight": 100}
    assert exc.value.response_headers == {"x": "y"}


def test_raise_for_unknown_code() -> None:
    """Unknown codes raise generic PhantomHttpError with code preserved."""
    body = {
        "error": {
            "code": "made_up_future_code",
            "message": "?",
            "request_id": "r",
            "instance_id": "unrouted",
            "details": {},
        }
    }
    with pytest.raises(PhantomHttpError) as exc:
        raise_for_error_body(body, status_code=500, response_headers={})
    assert exc.value.error_code == "made_up_future_code"
    # Must not be one of the subclasses.
    assert type(exc.value) is PhantomHttpError


def test_raise_for_missing_error_field() -> None:
    """Body without an 'error' object raises PhantomEnvelopeError."""
    with pytest.raises(PhantomEnvelopeError):
        raise_for_error_body({"not_an_error": True}, status_code=500, response_headers={})


def test_raise_for_non_string_code() -> None:
    """error.code that isn't a string raises PhantomEnvelopeError."""
    with pytest.raises(PhantomEnvelopeError):
        raise_for_error_body(
            {"error": {"code": 123, "message": "x", "request_id": "r"}},
            status_code=500,
            response_headers={},
        )


def test_http_error_repr_includes_code_and_message() -> None:
    """PhantomHttpError str() carries the status, code, and message."""
    err = PhantomHttpError(
        status_code=400,
        error_code="invalid_target",
        error_message="bad host",
        request_id="r",
        instance_id="i",
        response_headers={},
        details={},
    )
    assert "400" in str(err)
    assert "invalid_target" in str(err)
    assert "bad host" in str(err)
