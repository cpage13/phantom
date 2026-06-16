"""Unit tests for ``phantom_client.models.admin``."""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

import pytest
from phantom_client.models.admin import (
    AdminStatusResponse,
    BulkDeleteResponse,
    DeleteFilter,
    ExtractFilter,
    InstanceStatusResponse,
    InstanceSummary,
    KeyValueMatchFilter,
    UploadBundle,
)
from pydantic import ValidationError


def test_extract_filter_all_optional() -> None:
    """ExtractFilter accepts an empty body."""
    f = ExtractFilter()
    assert f.state is None
    assert f.route is None
    assert f.since is None
    assert f.chain_ids is None
    assert f.instance is None


def test_extract_filter_chain_ids_list() -> None:
    """ExtractFilter parses a list of UUIDs."""
    a, b = uuid4(), uuid4()
    f = ExtractFilter.model_validate_json(f'{{"chain_ids": ["{a}", "{b}"]}}')
    assert f.chain_ids is not None
    assert {*f.chain_ids} == {a, b}


def test_extract_filter_extra_forbidden() -> None:
    """ExtractFilter rejects unknown fields."""
    with pytest.raises(ValidationError):
        ExtractFilter.model_validate({"surprise": 1})


def test_delete_filter_is_empty() -> None:
    """DeleteFilter.is_empty distinguishes empty from non-empty."""
    assert DeleteFilter().is_empty() is True
    assert not DeleteFilter(state="failed").is_empty()
    assert not DeleteFilter(route="x").is_empty()
    assert not DeleteFilter(instance="primary").is_empty()
    since = datetime.fromisoformat("2026-01-01T00:00:00+00:00")
    assert not DeleteFilter(since=since).is_empty()


def test_key_value_match_filter_min_length() -> None:
    """KeyValueMatchFilter rejects empty strings."""
    KeyValueMatchFilter(key="phantom_local_uuid", value="x")  # ok
    with pytest.raises(ValidationError):
        KeyValueMatchFilter(key="", value="x")
    with pytest.raises(ValidationError):
        KeyValueMatchFilter(key="x", value="")


def test_bulk_delete_response() -> None:
    """BulkDeleteResponse parses with non-negative deleted count."""
    r = BulkDeleteResponse(deleted=5)
    assert r.deleted == 5
    with pytest.raises(ValidationError):
        BulkDeleteResponse(deleted=-1)


def test_instance_summary_refresh_strategy_literal() -> None:
    """InstanceSummary.refresh_strategy is wait or ad_client_credentials."""
    s = InstanceSummary(id="primary", refresh_strategy="wait", in_flight=0)
    assert s.refresh_strategy == "wait"
    with pytest.raises(ValidationError):
        InstanceSummary(id="x", refresh_strategy="nope", in_flight=0)  # type: ignore[arg-type]


def test_admin_status_response_basic() -> None:
    """AdminStatusResponse parses the documented shape."""
    r = AdminStatusResponse.model_validate(
        {
            "ready": True,
            "disk_usage_bytes": 1024,
            "total_backlog": 0,
            "instances": [{"id": "primary", "refresh_strategy": "wait", "in_flight": 0}],
        }
    )
    assert r.ready is True
    assert r.ad_reachability == "not_configured"
    assert len(r.instances) == 1


def test_instance_status_response_nested_shape() -> None:
    """InstanceStatusResponse parses the nested shape Phantom emits."""
    r = InstanceStatusResponse.model_validate(
        {
            "id": "primary",
            "ready": True,
            "in_flight": {"count": 2, "bytes": 1024},
            "by_state": {
                "queued": {"count": 2, "bytes": 1024},
                "attempting": {"count": 0, "bytes": 0},
                "auth_expired": {"count": 0, "bytes": 0},
                "stored": {"count": 0, "bytes": 0},
                "succeeded_recent": {"count": 0, "bytes": 0},
                "failed_recent": {"count": 0, "bytes": 0},
            },
            "auth": {"phantom_token_expires_at": None, "auth_expired_count": 0},
            "disk_usage_bytes": 0,
        }
    )
    assert r.id == "primary"
    assert r.ready is True
    assert r.in_flight.count == 2
    assert r.by_state.queued.count == 2
    assert r.auth.auth_expired_count == 0
    assert r.degraded_durability is False


def test_upload_bundle_carries_body_refs() -> None:
    """UploadBundle decodes the wire body_refs hex map to bytes (R-EX2).

    The server emits each ref's bytes as a hex string under ``body_refs``;
    the SDK model decodes that to a name -> bytes map, the richer shape
    that keeps every declared ref distinct.
    """
    chain_id = uuid4()
    group_id = uuid4()
    now = datetime.fromisoformat("2026-01-01T00:00:00+00:00")
    bundle = UploadBundle.model_validate(
        {
            "metadata": {
                "chain_id": str(chain_id),
                "instance_id": "primary",
                "group_id": str(group_id),
                "route_name": "primary",
                "state": "succeeded",
                "body_location": "ram",
                "received_at": now.isoformat(),
                "updated_at": now.isoformat(),
                "endpoint": "x",
                "uid": "y",
                "idempotency_key": "k",
                "capture_reexecution_active": False,
            },
            "body_refs": {"body": b"hello".hex()},
        }
    )
    assert bundle.body_refs == {"body": b"hello"}
    assert bundle.metadata.chain_id == chain_id
