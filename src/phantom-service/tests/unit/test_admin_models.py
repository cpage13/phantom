"""Unit tests for phantom.models.admin."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from phantom.models.admin import (
    AdminStatusResponse,
    AuthStatus,
    BulkDeleteResponse,
    DeleteFilter,
    ExtractFilter,
    HealthResponse,
    InstanceStatusResponse,
    InstanceSummary,
    KeyValueMatchFilter,
    ListUploadsResponse,
    ReadyResponse,
    SaturationStatus,
    StateBreakdown,
    StatsResponse,
    TierBreakdown,
    TokenListResponse,
    TokenPushRequest,
    TokenSlot,
)
from pydantic import ValidationError


def _tier(count: int = 0, size_bytes: int = 0) -> TierBreakdown:
    return TierBreakdown(count=count, bytes=size_bytes)


def _state_breakdown() -> StateBreakdown:
    return StateBreakdown(
        queued=_tier(),
        attempting=_tier(),
        auth_expired=_tier(),
        stored=_tier(),
        succeeded_recent=_tier(),
        failed_recent=_tier(),
    )


def test_health_shape() -> None:
    """HealthResponse round-trips with status='ok'."""
    h = HealthResponse(status="ok", version="0.1.0")
    blob = h.model_dump_json()
    HealthResponse.model_validate_json(blob)


def test_health_status_literal() -> None:
    """HealthResponse rejects status values other than 'ok'."""
    with pytest.raises(ValidationError):
        HealthResponse(status="degraded", version="0.1.0")  # type: ignore[arg-type]


def test_ready_shape() -> None:
    """ReadyResponse round-trips."""
    r = ReadyResponse(ready=True)
    assert r.detail is None


def test_stats_shape() -> None:
    """StatsResponse round-trips."""
    s = StatsResponse(
        in_flight=_tier(),
        by_state=_state_breakdown(),
        body_location={"ram": _tier(), "file": _tier()},
        saturation=SaturationStatus(
            max_in_flight=1000,
            max_in_flight_bytes=10_737_418_240,
            saturated=False,
        ),
        auth=AuthStatus(phantom_token_expires_at=None, auth_expired_count=0),
        parked_total=0,
    )
    blob = s.model_dump_json()
    StatsResponse.model_validate_json(blob)


def test_admin_status_response() -> None:
    """AdminStatusResponse round-trips with default ad_reachability."""
    r = AdminStatusResponse(
        ready=True,
        disk_usage_bytes=0,
        total_backlog=0,
        instances=[],
    )
    assert r.ad_reachability == "not_configured"


def test_instance_status_response() -> None:
    """InstanceStatusResponse round-trips with default degraded_durability."""
    r = InstanceStatusResponse(
        id="primary",
        ready=True,
        in_flight=_tier(),
        by_state=_state_breakdown(),
        auth=AuthStatus(phantom_token_expires_at=None, auth_expired_count=0),
        disk_usage_bytes=0,
    )
    assert r.degraded_durability is False


def test_instance_summary() -> None:
    """InstanceSummary round-trips."""
    s = InstanceSummary(
        id="primary",
        host_prefixes=["upstream.example.com"],
        refresh_strategy="wait",
        in_flight=0,
    )
    assert s.refresh_strategy == "wait"


def test_extract_filter_optional_fields() -> None:
    """ExtractFilter accepts an all-None body (used for full export)."""
    f = ExtractFilter()
    assert f.state is None


def test_delete_filter_optional_fields() -> None:
    """DeleteFilter accepts all-None for its model shape; reject is at handler."""
    f = DeleteFilter()
    assert f.state is None


def test_list_uploads_response_pagination() -> None:
    """ListUploadsResponse exposes next_cursor."""
    r = ListUploadsResponse(uploads=[], next_cursor="abc")
    assert r.next_cursor == "abc"


def test_bulk_delete_response_non_negative() -> None:
    """BulkDeleteResponse.deleted rejects negative values."""
    with pytest.raises(ValidationError):
        BulkDeleteResponse(deleted=-1)


def test_token_slot_no_bearer() -> None:
    """TokenSlot has no ``bearer`` field — ADR-004 invariant."""
    assert "bearer" not in TokenSlot.model_fields
    slot = TokenSlot(
        endpoint="upstream.example.com",
        uid="user-1",
        last_updated=datetime.now(tz=UTC),
        status="fresh",
    )
    blob = slot.model_dump_json()
    assert "bearer" not in blob.lower()


def test_token_slot_strict() -> None:
    """TokenSlot rejects unknown keys, including ``bearer``."""
    with pytest.raises(ValidationError):
        TokenSlot.model_validate(
            {
                "endpoint": "x",
                "uid": "y",
                "last_updated": datetime.now(tz=UTC).isoformat(),
                "status": "fresh",
                "bearer": "BAD",
            },
        )


def test_token_push_request_non_empty() -> None:
    """TokenPushRequest requires a non-empty token."""
    with pytest.raises(ValidationError):
        TokenPushRequest(token="")
    TokenPushRequest(token="abc")


def test_token_list_response() -> None:
    """TokenListResponse round-trips."""
    r = TokenListResponse(tokens=[])
    assert r.tokens == []


def test_key_value_match_filter() -> None:
    """KeyValueMatchFilter requires both fields, each non-empty.

    The min_length=1 constraint mirrors the SDK side byte-for-byte
    (ADR-012; round 2 defender fix R2-1 adopted the stricter contract
    on the service: an empty key or value is not a meaningful match).
    """
    f = KeyValueMatchFilter(key="phantom_local_uuid", value="abc")
    assert f.key == "phantom_local_uuid"
    with pytest.raises(ValidationError):
        KeyValueMatchFilter(key="", value="abc")
    with pytest.raises(ValidationError):
        KeyValueMatchFilter(key="phantom_local_uuid", value="")
