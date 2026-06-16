"""Unit tests for phantom.models.errors."""

from __future__ import annotations

from typing import get_args

import pytest
from phantom.models.errors import (
    STATUS_FOR_CODE,
    ErrorBody,
    ErrorCode,
    ErrorEnvelope,
    error_response,
)
from pydantic import ValidationError


def test_error_code_literal() -> None:
    """All canonical error codes are present."""
    assert set(get_args(ErrorCode)) == {
        "envelope_invalid",
        # Aggressor R3 finding R3-9: duplicate multipart envelope part (422).
        "envelope_duplicate",
        # Cycle-7 task 2.2: malformed grouping/ordering request header (400).
        "header_invalid",
        "body_ref_missing",
        "body_ref_orphan",
        # Aggressor R1 finding E-1: duplicate multipart body_ref part (422).
        "body_ref_duplicate",
        # H2 audit closure: 413 on Content-Length precheck or streaming-cap hit.
        "body_too_large",
        "template_unresolved",
        "invalid_target",
        "instance_unknown",
        # Cycle-7 task 4.3: by-captured-id lookup on an instance without the
        # deployment-supplied admin_lookup binding (400; never guesses).
        "lookup_not_configured",
        "idempotency_replay",
        # Aggressor R1 finding G-1: idempotency key reused with a different body (422).
        "idempotency_key_conflict",
        # Aggressor R1 finding D-1: envelope chain_id (row PK) already in use (409).
        "chain_id_in_use",
        # Extreme-hardening H-1 / L-2: the one-call admin restore moved nothing
        # into the live tree (409; the operator retries rather than being told a
        # no-op succeeded).
        "restore_noop",
        # Cycle-7 phase 7 pre-round defender fix: replay of a row whose body
        # was already discarded (409; refused up front instead of laundering
        # the operator action into a corrupted terminal).
        "replay_body_discarded",
        # Round 1 defender fix R1-1: replay of a row a sender is actively
        # driving (409; promoted from FastAPI's raw detail body to the
        # canonical envelope).
        "replay_refused_attempting",
        # Round 2 defender fix R2-2: the three admin query-shape refusals
        # promoted from FastAPI's raw detail bodies to the canonical
        # envelope (all 422).
        "multifile_cursor_conflict",
        "key_value_match_invalid",
        "bulk_delete_filter_empty",
        # Round 6 defender fix R6-4: FastAPI request-parameter validation
        # promoted onto the canonical envelope (422) via one shared
        # RequestValidationError handler.
        "request_invalid",
        "saturation_cap",
        # §2.3 disk-pressure back-pressure (also 503; distinct cause).
        "disk_pressure",
        # Aggressor R7 findings R7-1-A/B / R7-2-A: a storage-layer write
        # fault (fsync EIO / ENOSPC) during admission body buffering (503,
        # retryable; distinct from the proactive disk_pressure gate).
        "storage_unavailable",
        "auth_token_missing",
        "upstream_unreachable",
        "internal_error",
        "not_found",
        # F3 body-verification corruption codes (chain-row terminal).
        "storage_corruption",
        "codec_round_trip_drift",
    }


def test_body_too_large_status_is_413() -> None:
    """body_too_large maps to 413 (RFC 9110 §15.5.14 — H2 audit closure)."""
    assert STATUS_FOR_CODE["body_too_large"] == 413


def test_disk_pressure_status_is_503() -> None:
    """disk_pressure maps to 503 (§2.3); paired with Retry-After by ingress."""
    assert STATUS_FOR_CODE["disk_pressure"] == 503


def test_storage_unavailable_status_is_503() -> None:
    """storage_unavailable maps to 503 (R7-1/R7-2 storage-write fault, retryable)."""
    assert STATUS_FOR_CODE["storage_unavailable"] == 503


def test_status_map() -> None:
    """Each error code has a defined HTTP status."""
    for code in get_args(ErrorCode):
        assert code in STATUS_FOR_CODE
        assert isinstance(STATUS_FOR_CODE[code], int)


def test_status_specific_values() -> None:
    """Status mapping matches plan §5.6 table."""
    assert STATUS_FOR_CODE["envelope_invalid"] == 422
    assert STATUS_FOR_CODE["invalid_target"] == 421
    assert STATUS_FOR_CODE["idempotency_replay"] == 200
    assert STATUS_FOR_CODE["saturation_cap"] == 503
    assert STATUS_FOR_CODE["upstream_unreachable"] == 502
    # Cycle-7 task 2.2: malformed grouping/ordering request header.
    assert STATUS_FOR_CODE["header_invalid"] == 400


def test_aggressor_round1_code_statuses() -> None:
    """ADR-017 rows for the Round-1-introduced codes (D-1, E-1, G-1)."""
    # E-1: duplicate multipart body_ref part — 422, body_ref family.
    assert STATUS_FOR_CODE["body_ref_duplicate"] == 422
    # G-1: idempotency key reused with a different body — 422.
    assert STATUS_FOR_CODE["idempotency_key_conflict"] == 422
    # D-1: chain_id (row PK) already in use — 409 Conflict.
    assert STATUS_FOR_CODE["chain_id_in_use"] == 409


def test_replay_body_discarded_status_is_409() -> None:
    """replay_body_discarded maps to 409 Conflict (cycle-7 phase 7 defender fix).

    Replaying a row whose body was already discarded conflicts with the
    row's on-disk state: there are no bytes left to send, so the replay
    is refused up front rather than re-queued into a corrupted terminal.
    """
    assert STATUS_FOR_CODE["replay_body_discarded"] == 409


def test_replay_refused_attempting_status_is_409() -> None:
    """replay_refused_attempting maps to 409 Conflict (round 1 defender fix R1-1).

    Replaying a row a sender is actively driving conflicts with the
    row's current state: a re-queue would clobber the in-flight
    attempt, so the replay is refused up front and the operator waits
    (or cancels first) before retrying.
    """
    assert STATUS_FOR_CODE["replay_refused_attempting"] == 409


def test_admin_query_shape_refusals_are_422() -> None:
    """The three R2-2 admin query-shape refusals map to 422.

    Round 2 defender fix R2-2: each request is well-formed HTTP but
    unprocessable as posed (a cursor on the un-paginated multifile
    listing, a key_value_match that does not parse as 'key:value', an
    all-None bulk-delete filter). All three ride the canonical envelope
    instead of FastAPI's raw detail body.
    """
    assert STATUS_FOR_CODE["multifile_cursor_conflict"] == 422
    assert STATUS_FOR_CODE["key_value_match_invalid"] == 422
    assert STATUS_FOR_CODE["bulk_delete_filter_empty"] == 422


def test_error_body_envelope() -> None:
    """``error_response()`` returns a well-shaped envelope."""
    env = error_response(
        "envelope_invalid",
        "bad envelope",
        instance_id="primary",
        request_id="req-1",
        details={"reason": "missing field"},
    )
    assert isinstance(env, ErrorEnvelope)
    assert env.error.code == "envelope_invalid"
    assert env.error.message == "bad envelope"
    assert env.error.instance_id == "primary"
    assert env.error.request_id == "req-1"
    assert env.error.details == {"reason": "missing field"}


def test_error_response_default_details_empty() -> None:
    """Omitting ``details`` yields an empty dict, never ``None`` on the wire."""
    env = error_response(
        "internal_error",
        "boom",
        instance_id="unrouted",
        request_id="r",
    )
    assert env.error.details == {}


def test_error_body_strict() -> None:
    """ErrorBody rejects extra fields."""
    with pytest.raises(ValidationError):
        ErrorBody.model_validate(
            {
                "code": "internal_error",
                "message": "x",
                "instance_id": "primary",
                "request_id": "r",
                "extra": "nope",
            },
        )
