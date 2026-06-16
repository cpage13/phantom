"""Unit tests for :class:`ChainAdminDetail` and its envelope projection.

These tests lock in the round-2 contract extension: ``ChainAdminDetail``
exposes ``metadata`` (top-level KVS) and ``steps`` (per-step request
envelope projection) so admin GET satisfies the user's "retrievable
with the files and the body" promise without fetching the body.

Round-3 refactor moved the projection helpers from
``phantom.routes.admin`` onto ``ChainAdminDetail`` itself as a
classmethod (``project_from_envelope``). The projection is the model's
own contract — locating it on the model is the right deepening per the
codebase review.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from phantom.models.admin import ChainAdminDetail, ChainAdminStepDetail
from pydantic import ValidationError


def test_chain_admin_step_detail_round_trips() -> None:
    """Round-trip with all fields populated."""
    step = ChainAdminStepDetail(
        name="create_file",
        method="POST",
        url="https://upstream.example.com/v2/files",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        has_body=True,
    )
    blob = step.model_dump_json()
    restored = ChainAdminStepDetail.model_validate_json(blob)
    assert restored == step


def test_chain_admin_step_detail_rejects_unknown_method() -> None:
    """Non-Literal methods (e.g. OPTIONS) are rejected."""
    with pytest.raises(ValidationError):
        ChainAdminStepDetail(
            name="weird",
            method="OPTIONS",  # type: ignore[arg-type]
            url="https://example.com",
            headers={},
            has_body=False,
        )


def test_chain_admin_detail_with_metadata_and_steps() -> None:
    """ChainAdminDetail surfaces metadata + steps as required."""
    cid = uuid4()
    now = datetime.now(tz=UTC)
    detail = ChainAdminDetail(
        chain_id=cid,
        state="succeeded",
        received_at=now,
        updated_at=now,
        next_attempt_at=None,
        sent_at=now,
        group_id=cid,
        multifile_id=None,
        send_order=0,
        body_location="file",
        last_step_completed="put_s3",
        captured=[],
        attempts=1,
        last_error=None,
        metadata={"ref_id": "abc-123", "label": "alpha"},
        steps=[
            ChainAdminStepDetail(
                name="create_file",
                method="POST",
                url="https://upstream.example.com/v2/files",
                headers={"Content-Type": "application/json"},
                has_body=True,
            ),
            ChainAdminStepDetail(
                name="put_s3",
                method="PUT",
                url="{{create_file.upload_url}}",
                headers={"x-amz-meta-ref-id": "abc-123"},
                has_body=True,
            ),
        ],
    )
    blob = detail.model_dump_json()
    restored = ChainAdminDetail.model_validate_json(blob)
    assert restored == detail
    assert restored.metadata == {"ref_id": "abc-123", "label": "alpha"}
    assert len(restored.steps) == 2
    assert restored.steps[0].name == "create_file"
    assert restored.steps[1].method == "PUT"


def test_chain_admin_detail_defaults_metadata_and_steps_empty() -> None:
    """Missing metadata + steps default to empty collections."""
    cid = uuid4()
    now = datetime.now(tz=UTC)
    detail = ChainAdminDetail(
        chain_id=cid,
        state="queued",
        received_at=now,
        updated_at=now,
        group_id=cid,
        body_location="ram",
        attempts=0,
    )
    assert detail.metadata == {}
    assert detail.steps == []
    # The cycle-7 row-sourced optionals default to their NULL semantics.
    assert detail.next_attempt_at is None
    assert detail.sent_at is None
    assert detail.multifile_id is None
    assert detail.send_order == 0


def _upstream_envelope_json(
    *,
    chain_id: str | None = None,
    kvs_camel: bool = True,
    metadata: dict[str, str] | None = None,
) -> str:
    """Build a minimal two-step envelope JSON string for projection tests.

    When ``kvs_camel`` is True, the KVS key is ``keyValueStore`` (the
    upstream camelCase convention via Pydantic alias). When False, the
    snake-case fallback is exercised.
    """
    cid = chain_id or str(uuid4())
    kvs_key = "keyValueStore" if kvs_camel else "key_value_store"
    metadata_dict = metadata or {"ref_id": "h-1", "label": "alpha"}
    envelope = {
        "chain_id": cid,
        "idempotency_key": cid,
        "steps": [
            {
                "name": "create_file",
                "method": "POST",
                "url": "https://upstream.example.com/v2/files",
                "headers": {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                "body": {
                    "kind": "json",
                    "value": {
                        "file_name": "demo",
                        "metadata": {kvs_key: metadata_dict},
                    },
                },
                "capture": [],
                "idempotency_header": "Idempotency-Key",
            },
            {
                "name": "put_s3",
                "method": "PUT",
                "url": "{{create_file.upload_url}}",
                "headers": {
                    "x-amz-meta-ref-id": metadata_dict.get("ref_id", ""),
                    "x-amz-meta-label": metadata_dict.get("label", ""),
                },
                "body": {
                    "kind": "body_ref",
                    "name": "body",
                    "content_type": "application/octet-stream",
                },
                "capture": [],
                "idempotency_header": None,
            },
        ],
        "default_target": None,
    }
    return json.dumps(envelope)


def test_project_envelope_handles_upstream_camelcase_kvs() -> None:
    """Camel-case ``keyValueStore`` (upstream wire convention) is recognized."""
    envelope_json = _upstream_envelope_json(
        kvs_camel=True,
        metadata={"ref_id": "h-7", "label": "alpha"},
    )
    metadata, steps = ChainAdminDetail.project_from_envelope(envelope_json)
    assert metadata == {"ref_id": "h-7", "label": "alpha"}
    assert len(steps) == 2
    assert steps[0].name == "create_file"
    assert steps[0].method == "POST"
    assert steps[0].has_body is True
    assert steps[1].name == "put_s3"
    assert steps[1].method == "PUT"
    assert steps[1].has_body is True
    assert steps[1].headers["x-amz-meta-ref-id"] == "h-7"


def test_project_envelope_handles_snake_case_kvs() -> None:
    """Snake-case ``key_value_store`` fallback (non-upstream-shaped chains)."""
    envelope_json = _upstream_envelope_json(
        kvs_camel=False,
        metadata={"label": "v1"},
    )
    metadata, _steps = ChainAdminDetail.project_from_envelope(envelope_json)
    assert metadata == {"label": "v1"}


def test_project_envelope_returns_empty_on_malformed_json() -> None:
    """Malformed envelope JSON degrades gracefully (defender principle)."""
    metadata, steps = ChainAdminDetail.project_from_envelope("not valid json {")
    assert metadata == {}
    assert steps == []


def test_project_envelope_handles_body_ref_only_chain() -> None:
    """A chain with only body_ref steps surfaces empty metadata + steps."""
    envelope = {
        "chain_id": str(uuid4()),
        "idempotency_key": "k",
        "steps": [
            {
                "name": "raw_put",
                "method": "PUT",
                "url": "https://upstream/raw",
                "headers": {"Content-Encoding": "gzip"},
                "body": {
                    "kind": "body_ref",
                    "name": "body",
                    "content_type": "application/octet-stream",
                },
                "capture": [],
                "idempotency_header": None,
            },
        ],
        "default_target": None,
    }
    metadata, steps = ChainAdminDetail.project_from_envelope(json.dumps(envelope))
    # No JSON body — no metadata to extract.
    assert metadata == {}
    # But the step shape still surfaces.
    assert len(steps) == 1
    assert steps[0].name == "raw_put"
    assert steps[0].has_body is True
    assert steps[0].headers == {"Content-Encoding": "gzip"}


def test_project_envelope_filters_unknown_method() -> None:
    """Step with non-Literal method is silently skipped."""
    envelope = {
        "steps": [
            {
                "name": "weird",
                "method": "TRACE",  # not in _ALLOWED_METHODS
                "url": "/x",
                "headers": {},
                "body": None,
            },
            {
                "name": "ok",
                "method": "GET",
                "url": "/y",
                "headers": {},
                "body": None,
            },
        ],
    }
    metadata, steps = ChainAdminDetail.project_from_envelope(json.dumps(envelope))
    assert metadata == {}
    assert len(steps) == 1
    assert steps[0].name == "ok"


def test_project_envelope_step_has_body_false_when_body_none() -> None:
    """A bodyless step (e.g., GET-only) reports has_body=False."""
    envelope = {
        "steps": [
            {
                "name": "ping",
                "method": "GET",
                "url": "/health",
                "headers": {},
                "body": None,
            },
        ],
    }
    _metadata, steps = ChainAdminDetail.project_from_envelope(json.dumps(envelope))
    assert steps[0].has_body is False
