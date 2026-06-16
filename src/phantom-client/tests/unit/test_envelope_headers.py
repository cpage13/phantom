"""Unit tests for ``phantom_client.models.envelope``."""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

import pytest
from phantom_client.errors import PhantomEnvelopeError
from phantom_client.models.envelope import ResponseHeaders, parse_response_headers


def _make_headers(**overrides: str | None) -> dict[str, str]:
    """Build a full set of valid X-Phantom-* headers, optionally overridden."""
    chain_id = str(uuid4())
    base = {
        "X-Phantom-Upload-Id": chain_id,
        "X-Phantom-Group-Id": chain_id,
        "X-Phantom-Status": "queued",
        "X-Phantom-Attempts": "0",
        "X-Phantom-Next-Attempt-At": "2026-05-13T12:00:00+00:00",
        "X-Phantom-Suggested-Poll-After": "1",
    }
    for k, v in overrides.items():
        if v is None:
            base.pop(k, None)
        else:
            base[k] = v
    return base


def test_parse_response_headers() -> None:
    """The happy path: every required header present."""
    headers = _make_headers()
    rh = parse_response_headers(headers)
    assert isinstance(rh, ResponseHeaders)
    assert str(rh.upload_id) == headers["X-Phantom-Upload-Id"]
    assert rh.status == "queued"
    assert rh.attempts == 0
    assert rh.suggested_poll_after_seconds == 1
    assert isinstance(rh.next_attempt_at, datetime)


def test_parse_response_headers_terminal_no_next_attempt() -> None:
    """Terminal-state responses may omit X-Phantom-Next-Attempt-At."""
    headers = _make_headers(**{"X-Phantom-Status": "succeeded", "X-Phantom-Next-Attempt-At": None})
    rh = parse_response_headers(headers)
    assert rh.status == "succeeded"
    assert rh.next_attempt_at is None


def test_missing_required_header_raises() -> None:
    """Missing any required header raises PhantomEnvelopeError."""
    for required in (
        "X-Phantom-Upload-Id",
        "X-Phantom-Group-Id",
        "X-Phantom-Status",
        "X-Phantom-Attempts",
        "X-Phantom-Suggested-Poll-After",
    ):
        headers = _make_headers(**{required: None})
        with pytest.raises(PhantomEnvelopeError):
            parse_response_headers(headers)


def test_bad_status_raises() -> None:
    """An unknown ChainState value raises PhantomEnvelopeError."""
    headers = _make_headers(**{"X-Phantom-Status": "totally_made_up"})
    with pytest.raises(PhantomEnvelopeError):
        parse_response_headers(headers)


def test_negative_attempts_raises() -> None:
    """attempts < 0 is rejected (ge=0)."""
    headers = _make_headers(**{"X-Phantom-Attempts": "-1"})
    with pytest.raises(PhantomEnvelopeError):
        parse_response_headers(headers)


def test_extra_forbidden_on_model() -> None:
    """ResponseHeaders rejects unknown fields."""
    from pydantic import ValidationError

    chain_id = uuid4()
    with pytest.raises(ValidationError):
        ResponseHeaders.model_validate(
            {
                "upload_id": chain_id,
                "group_id": chain_id,
                "status": "queued",
                "attempts": 0,
                "next_attempt_at": None,
                "suggested_poll_after_seconds": 1,
                "extra_field": "x",
            }
        )
