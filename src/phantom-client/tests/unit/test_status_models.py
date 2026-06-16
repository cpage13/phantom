"""Unit tests for ``phantom_client.models.status``."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from phantom_client.models.chain import ChainState
from phantom_client.models.status import (
    TERMINAL_STATES,
    HealthResponse,
    ReadyResponse,
    SortKey,
    StatsResponse,
    TokenSlot,
    UploadRow,
    UploadState,
)
from pydantic import ValidationError


def test_state_alias() -> None:
    """UploadState aliases ChainState — both are ``TypeAlias`` to the same Literal."""
    # ChainState and UploadState are both ``TypeAlias`` forms; assignment
    # makes ``UploadState`` the same object as ``ChainState``. The literal
    # members must be equal as sets regardless.
    from typing import get_args

    assert UploadState is ChainState
    assert set(get_args(ChainState)) == set(get_args(UploadState))


def test_terminal_states_set() -> None:
    """TERMINAL_STATES is the documented frozenset of every terminal state.

    ``corrupted`` joined in R6-5: it is a terminal ``ChainState`` the
    service never retries, so the default poll stop-set must include it.
    """
    assert (
        frozenset({"succeeded", "failed", "cancelled", "stored", "corrupted", "auth_expired"})
        == TERMINAL_STATES
    )


def test_sort_key_values() -> None:
    """SortKey values match the documented wire strings."""
    assert SortKey.NEXT_ATTEMPT_AT_ASC.value == "next_attempt_at_asc"
    assert SortKey.NEXT_ATTEMPT_AT_DESC.value == "next_attempt_at_desc"
    assert SortKey.RECEIVED_AT_DESC.value == "received_at_desc"


def test_upload_row_ignores_unknown_fields() -> None:
    """UploadRow tolerates unknown fields via ``extra='ignore'``."""
    chain_id = uuid4()
    group_id = uuid4()
    now = datetime.now(tz=UTC)
    row = UploadRow.model_validate(
        {
            "chain_id": str(chain_id),
            "instance_id": "primary",
            "group_id": str(group_id),
            "route_name": "upstream_two_step",
            "state": "queued",
            "body_location": "ram",
            "received_at": now.isoformat(),
            "updated_at": now.isoformat(),
            "endpoint": "upstream.example.com",
            "uid": "abc",
            "idempotency_key": "k",
            "capture_reexecution_active": False,
            # Unknown field — must NOT raise.
            "field_not_in_the_sdk_view": True,
        }
    )
    assert row.chain_id == chain_id
    assert row.state == "queued"
    assert row.body_location == "ram"
    assert row.captured_values.steps == {}
    assert row.attempts == 0


def test_upload_row_missing_required_fails() -> None:
    """UploadRow rejects missing required fields (state, chain_id, etc.)."""
    with pytest.raises(ValidationError):
        UploadRow.model_validate({"instance_id": "primary"})


def test_upload_row_parses_cycle7_fields() -> None:
    """The mirror parses group_id / multifile_id / send_order / sent_at.

    ADR-012: the SDK mirror moved in the same phase as the service
    model. Wire values land typed; the omitted-field defaults match the
    service side (multifile_id / sent_at None, send_order 0).
    """
    chain_id = uuid4()
    group_id = uuid4()
    multifile_id = uuid4()
    now = datetime.now(tz=UTC)
    payload = {
        "chain_id": str(chain_id),
        "instance_id": "primary",
        "group_id": str(group_id),
        "multifile_id": str(multifile_id),
        "send_order": 2,
        "route_name": "upstream_two_step",
        "state": "succeeded",
        "body_location": "ram",
        "received_at": now.isoformat(),
        "sent_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "endpoint": "upstream.example.com",
        "uid": "abc",
        "idempotency_key": "k",
        "capture_reexecution_active": False,
    }
    row = UploadRow.model_validate(payload)
    assert row.group_id == group_id
    assert row.multifile_id == multifile_id
    assert row.send_order == 2
    assert row.sent_at == now

    payload.pop("multifile_id")
    payload.pop("sent_at")
    payload["send_order"] = 0
    defaulted = UploadRow.model_validate(payload)
    assert defaulted.multifile_id is None
    assert defaulted.sent_at is None
    assert defaulted.send_order == 0


def test_token_slot_no_bearer_field() -> None:
    """TokenSlot has no bearer field (ADR-004 invariant)."""
    fields = set(TokenSlot.model_fields)
    assert fields == {"endpoint", "uid", "last_updated", "status"}
    assert "bearer" not in fields
    assert "token" not in fields


def test_token_slot_extra_forbidden() -> None:
    """TokenSlot rejects extra fields — bearer must not slip in."""
    with pytest.raises(ValidationError):
        TokenSlot.model_validate(
            {
                "endpoint": "x",
                "uid": "y",
                "last_updated": datetime.now(tz=UTC).isoformat(),
                "status": "fresh",
                "bearer": "secret",
            }
        )


def test_token_slot_status_literal() -> None:
    """TokenSlot.status accepts only fresh/bad/unknown."""
    with pytest.raises(ValidationError):
        TokenSlot.model_validate(
            {
                "endpoint": "x",
                "uid": "y",
                "last_updated": datetime.now(tz=UTC).isoformat(),
                "status": "stale",
            }
        )


def test_stats_response_basic() -> None:
    """StatsResponse parses the nested shape Phantom emits."""
    s = StatsResponse.model_validate(
        {
            "in_flight": {"count": 3, "bytes": 1024},
            "by_state": {
                "queued": {"count": 2, "bytes": 512},
                "attempting": {"count": 1, "bytes": 512},
                "auth_expired": {"count": 0, "bytes": 0},
                "stored": {"count": 0, "bytes": 0},
                "succeeded_recent": {"count": 0, "bytes": 0},
                "failed_recent": {"count": 0, "bytes": 0},
            },
            "body_location": {
                "ram": {"count": 2, "bytes": 512},
                "file": {"count": 1, "bytes": 512},
            },
            "saturation": {
                "max_in_flight": 100,
                "max_in_flight_bytes": 0,
                "saturated": False,
            },
            "auth": {
                "phantom_token_expires_at": None,
                "auth_expired_count": 0,
            },
            "parked_total": 0,
        }
    )
    assert s.in_flight.count == 3
    assert s.in_flight.bytes == 1024
    assert s.by_state.queued.count == 2
    assert s.body_location["ram"].count == 2
    assert s.saturation.max_in_flight == 100
    assert s.saturation.saturated is False
    assert s.auth.phantom_token_expires_at is None
    assert s.auth.auth_expired_count == 0
    assert s.parked_total == 0


def test_health_and_ready() -> None:
    """Health and ready shapes parse minimal payloads."""
    h = HealthResponse.model_validate({"status": "ok", "version": "0.1.0"})
    assert h.status == "ok"
    r = ReadyResponse.model_validate({"ready": True, "detail": None})
    assert r.ready is True
    assert r.detail is None
    r2 = ReadyResponse.model_validate({"ready": False, "detail": "no instances"})
    assert r2.ready is False
    assert r2.detail == "no instances"
