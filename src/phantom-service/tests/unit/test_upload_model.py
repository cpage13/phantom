"""Unit tests for phantom.models.upload."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import get_args
from uuid import uuid4

import pytest
from phantom.models.upload import (
    BodyLocation,
    CapturedStepValues,
    CapturedValues,
    StorageEncoding,
    UploadRow,
    UploadState,
)
from pydantic import ValidationError


def test_state_alias() -> None:
    """UploadState shares the eight canonical ChainState literals."""
    assert set(get_args(UploadState)) == {
        "queued",
        "attempting",
        "succeeded",
        "failed",
        "auth_expired",
        "stored",
        "cancelled",
        "corrupted",
    }


def test_body_location_values() -> None:
    """BodyLocation covers exactly ram and file post-Slice-1.B.

    Slice 1.B (plan § 2.3.8): the pre-Phase-1 ``Tier`` alias
    (``memory`` / ``persisted``) is gone; the body-files-location
    semantic now lives on ``BodyLocation`` (``ram`` / ``file``).
    """
    assert set(get_args(BodyLocation)) == {"ram", "file"}


def test_storage_encoding_values() -> None:
    """StorageEncoding covers the three allowed values."""
    assert set(get_args(StorageEncoding)) == {"original", "zstd", "gzip"}


def _example_row(**overrides: object) -> UploadRow:
    """Build a minimal valid UploadRow for tests."""
    base: dict[str, object] = {
        "chain_id": uuid4(),
        "instance_id": "primary",
        "group_id": uuid4(),
        "multifile_id": uuid4(),
        "send_order": 0,
        "route_name": "upstream-files",
        "state": "queued",
        "body_location": "ram",
        "received_at": datetime.now(tz=UTC),
        "updated_at": datetime.now(tz=UTC),
        "endpoint": "upstream.example.com",
        "uid": "user-1",
        "chain_envelope_json": "{}",
        "idempotency_key": "k",
        "capture_reexecution_active": False,
    }
    base.update(overrides)
    return UploadRow.model_validate(base)


def test_upload_row_serialization() -> None:
    """UploadRow round-trips through model_dump / model_validate_json."""
    row = _example_row()
    blob = row.model_dump_json()
    rebuilt = UploadRow.model_validate_json(blob)
    assert rebuilt.chain_id == row.chain_id
    assert rebuilt.state == "queued"
    assert rebuilt.body_location == "ram"


def test_upload_row_strict_no_extras() -> None:
    """UploadRow rejects unknown fields (extra='forbid')."""
    with pytest.raises(ValidationError):
        UploadRow.model_validate(
            {
                "chain_id": str(uuid4()),
                "instance_id": "primary",
                "group_id": str(uuid4()),
                "multifile_id": str(uuid4()),
                "send_order": 0,
                "route_name": "upstream-files",
                "state": "queued",
                "body_location": "ram",
                "received_at": datetime.now(tz=UTC).isoformat(),
                "updated_at": datetime.now(tz=UTC).isoformat(),
                "endpoint": "upstream.example.com",
                "uid": "user-1",
                "chain_envelope_json": "{}",
                "idempotency_key": "k",
                "capture_reexecution_active": False,
                "unknown_field": "nope",
            },
        )


def test_captured_values_expiry_field() -> None:
    """CapturedStepValues exposes the per-capture expiry map."""
    step_values = CapturedStepValues(
        values={"upload_url": "https://x"},
        captured_at=datetime.now(tz=UTC),
        expires_at={"upload_url": datetime.now(tz=UTC)},
    )
    values = CapturedValues(steps={"create_file": step_values})
    assert "create_file" in values.steps
    assert "upload_url" in values.steps["create_file"].expires_at


def test_captured_values_default_empty() -> None:
    """CapturedValues defaults to an empty steps dict."""
    values = CapturedValues()
    assert values.steps == {}


def test_body_location_required() -> None:
    """``body_location`` has no default — UploadRow must specify ram or file.

    Slice 1.B replaces the pre-Slice ``test_committed_defaults_false``
    test. ``body_location`` is required because admission (Slice 1.E)
    is mode-aware: ``all_disk`` writes 'file', everything else writes
    'ram'.
    """
    with pytest.raises(ValidationError):
        UploadRow.model_validate(
            {
                "chain_id": str(uuid4()),
                "instance_id": "primary",
                "group_id": str(uuid4()),
                "multifile_id": str(uuid4()),
                "send_order": 0,
                "route_name": "upstream-files",
                "state": "queued",
                # body_location intentionally omitted
                "received_at": datetime.now(tz=UTC).isoformat(),
                "updated_at": datetime.now(tz=UTC).isoformat(),
                "endpoint": "upstream.example.com",
                "uid": "user-1",
                "chain_envelope_json": "{}",
                "idempotency_key": "k",
                "capture_reexecution_active": False,
            },
        )


def test_attempts_non_negative() -> None:
    """``attempts`` rejects negative values."""
    with pytest.raises(ValidationError):
        _example_row(attempts=-1)


def _minimal_payload_without(*omitted: str) -> dict[str, object]:
    """The minimal STRICT-typed UploadRow payload, minus ``omitted`` keys.

    Typed values (UUID / datetime objects), because the model is
    ``strict=True`` and rejects string coercion; omitting a key is then
    the ONLY reason a validation can fail.
    """
    payload: dict[str, object] = {
        "chain_id": uuid4(),
        "instance_id": "primary",
        "group_id": uuid4(),
        "route_name": "upstream-files",
        "state": "queued",
        "body_location": "ram",
        "received_at": datetime.now(tz=UTC),
        "updated_at": datetime.now(tz=UTC),
        "endpoint": "upstream.example.com",
        "uid": "user-1",
        "chain_envelope_json": "{}",
        "idempotency_key": "k",
        "capture_reexecution_active": False,
    }
    for key in omitted:
        payload.pop(key)
    return payload


def test_group_id_required() -> None:
    """``group_id`` has no default: admission always supplies a value.

    Cycle-7 phase 1: the query-grouping handle is the header value when
    present, else chain_id; the model refuses a row without one. The
    same payload WITH group_id validates, so the omission is the only
    failure cause.
    """
    UploadRow.model_validate(_minimal_payload_without())  # control: valid
    with pytest.raises(ValidationError):
        UploadRow.model_validate(_minimal_payload_without("group_id"))


def test_multifile_id_and_sent_at_default_none() -> None:
    """Omitted multifile_id / sent_at / send_order take their declared defaults."""
    row = UploadRow.model_validate(_minimal_payload_without())
    assert row.multifile_id is None
    assert row.sent_at is None
    assert row.send_order == 0


def test_send_order_non_negative() -> None:
    """``send_order`` rejects negative values (ge=0)."""
    with pytest.raises(ValidationError):
        _example_row(send_order=-1)
