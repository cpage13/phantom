"""Unit tests for phantom.routes.envelope."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from phantom.routes.envelope import build_response_headers


def test_all_headers_present() -> None:
    """Every documented X-Phantom-* header appears."""
    headers = build_response_headers(
        upload_id=uuid4(),
        group_id=uuid4(),
        state="queued",
        attempts=0,
        next_attempt_at=datetime.now(tz=UTC),
        suggested_poll_after_seconds=5,
    )
    # Exact-set equality: the six documented headers and NOTHING else,
    # which structurally pins the cycle-7 retirement of the legacy
    # batch-named echo (no legacy name can reappear unnoticed).
    assert set(headers) == {
        "X-Phantom-Upload-Id",
        "X-Phantom-Group-Id",
        "X-Phantom-Status",
        "X-Phantom-Attempts",
        "X-Phantom-Next-Attempt-At",
        "X-Phantom-Suggested-Poll-After",
    }


def test_group_id_echo_always_present_and_correct() -> None:
    """X-Phantom-Group-Id carries the supplied group_id on every response."""
    group_id = uuid4()
    headers = build_response_headers(
        upload_id=uuid4(),
        group_id=group_id,
        state="queued",
        attempts=0,
        next_attempt_at=None,
        suggested_poll_after_seconds=5,
    )
    assert headers["X-Phantom-Group-Id"] == str(group_id)


def test_no_next_attempt_when_none() -> None:
    """``next_attempt_at=None`` omits the header."""
    headers = build_response_headers(
        upload_id=uuid4(),
        group_id=uuid4(),
        state="succeeded",
        attempts=1,
        next_attempt_at=None,
        suggested_poll_after_seconds=5,
    )
    assert "X-Phantom-Next-Attempt-At" not in headers


def test_status_is_snake_case() -> None:
    """``X-Phantom-Status`` carries the snake_case state."""
    headers = build_response_headers(
        upload_id=uuid4(),
        group_id=uuid4(),
        state="auth_expired",
        attempts=3,
        next_attempt_at=None,
        suggested_poll_after_seconds=5,
    )
    assert headers["X-Phantom-Status"] == "auth_expired"
