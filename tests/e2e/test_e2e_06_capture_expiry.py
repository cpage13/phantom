"""Strict ADR-011 capture-expiry reexecution matrix.

The three scenarios share one two-step chain and differ only in whether the
metadata step declares ``Idempotency-Key`` and whether the instance enables
capture reexecution. The emulator's append-only successful-event oracle is the
authority for exact successful-response/accepted-side-effect cardinality and
capability identity; a separate ``error_rate_5xx`` oracle proves PUTs rejected
by that configured branch.
"""

from __future__ import annotations

import hashlib
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from phantom_client import (
    ChainBodyJson,
    ChainBodyRef,
    ChainCapture,
    ChainEnvelope,
    ChainStep,
)
from phantom_client.models.admin import ChainAdminDetail
from phantom_emulator.failure.injection import FailurePolicy, FailureScope
from phantom_emulator.state import BodyPutEvent, MetadataCreateEvent, UpstreamEvent

from .helpers.assertions import assert_chain_reaches_state
from .helpers.stack import DEFAULT_FAKE_SUB, E2EStack, boot_stack
from .helpers.timing import await_until

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]

TEST_BODY = b"phantom-e2e-capture-expiry-body-bytes"
CAPTURE_TTL_SECONDS = 2
CAPABILITY_TTL_SECONDS = 60
WAIT_BUDGET_SECONDS = 20.0
SERIAL_WAIT_COUNT = 2
CAPABILITY_LIFETIME_MARGIN_SECONDS = 10.0
WHOLE_SCENARIO_WORST_CASE_SECONDS = WAIT_BUDGET_SECONDS * SERIAL_WAIT_COUNT
EVENT_POLL_SECONDS = 0.05
FORCE_5XX_RATE = 1.0
EXPECTED_BODY_HASH = hashlib.sha256(TEST_BODY).hexdigest()

_FILE_NAME = "capture-expiry-matrix"
_DOMAIN = "generic"
_LANE_BASE = "metadata_table"
_UPLOADER_ID = "12345"
_LABEL = "capture-matrix"


def _config(*, capture_reexecution: bool | None) -> dict[str, object]:
    """Return a deterministic one-worker overlay for one matrix case."""
    instance: dict[str, object] = {
        "id": "primary",
        "host_prefixes": ["emulator", "127.0.0.1", "localhost"],
        "data_dir": "primary",
        "routes": [
            {
                "name": "emulator",
                "hosts": ["emulator", "127.0.0.1", "localhost"],
                "auth_mode": "phantom_bearer",
            }
        ],
    }
    if capture_reexecution is not None:
        instance["capture_reexecution"] = capture_reexecution
    return {
        "instances": [instance],
        "retry": {
            "worker_count": 1,
            "poll_interval_ms": 50,
            "default_strategy": {
                "type": "fixed_intervals",
                "intervals_seconds": [0, 1, 2, 5, 10],
            },
        },
        "retention": {"reaper_interval_seconds": 3600},
    }


def _build_envelope(
    *,
    chain_id: UUID,
    emulator_url: str,
    keyed: bool,
) -> ChainEnvelope:
    """Build the two-step chain with a deliberately short Phantom capture TTL."""
    create_step = ChainStep(
        name="create_file",
        method="POST",
        url=f"{emulator_url.rstrip('/')}/v2/files",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        body=ChainBodyJson(
            kind="json",
            value={
                "fileName": _FILE_NAME,
                "domain": _DOMAIN,
                "laneBaseName": _LANE_BASE,
                "metadata": {
                    "keyValueStore": {
                        "uploader_id": _UPLOADER_ID,
                        "label": _LABEL,
                        "phantom_local_uuid": str(chain_id),
                    }
                },
            },
        ),
        capture=[
            ChainCapture.model_validate(
                {
                    "name": "upload_url",
                    "from": "$.uploadUrl",
                    "ttl_seconds": CAPTURE_TTL_SECONDS,
                    "sensitive": True,
                }
            ),
            ChainCapture.model_validate(
                {
                    "name": "file_information",
                    "from": "$.fileInformation",
                    "ttl_seconds": CAPTURE_TTL_SECONDS,
                }
            ),
        ],
        idempotency_header="Idempotency-Key" if keyed else None,
    )
    put_step = ChainStep(
        name="put_s3",
        method="PUT",
        url="{{create_file.upload_url}}",
        headers={
            "x-amz-meta-uploader-id": _UPLOADER_ID,
            "x-amz-meta-label": _LABEL,
            "x-amz-meta-phantom-local-uuid": str(chain_id),
        },
        body=ChainBodyRef(kind="body_ref", name="body", content_type="application/octet-stream"),
        capture=[],
        idempotency_header=None,
    )
    return ChainEnvelope(
        chain_id=chain_id,
        idempotency_key=str(chain_id),
        steps=[create_step, put_step],
        default_target=None,
    )


def _events(stack: E2EStack, chain_id: UUID) -> list[UpstreamEvent]:
    """Return successful upstream events for the matrix chain only."""
    return [event for event in stack.emulator.upstream_events() if event.chain_id == chain_id]


def _create_events(stack: E2EStack, chain_id: UUID) -> list[MetadataCreateEvent]:
    """Return successful metadata-create events for ``chain_id``."""
    return [event for event in _events(stack, chain_id) if isinstance(event, MetadataCreateEvent)]


async def _boot_case(*, capture_reexecution: bool | None) -> E2EStack:
    """Boot with upstream lifetimes beyond the whole two-wait scenario."""
    assert CAPABILITY_TTL_SECONDS >= (
        WHOLE_SCENARIO_WORST_CASE_SECONDS + CAPABILITY_LIFETIME_MARGIN_SECONDS
    )
    stack = await boot_stack(config_overrides=_config(capture_reexecution=capture_reexecution))
    stack.emulator.clear_received()
    stack.emulator.clear_failures()
    stack.emulator.set_presigned_ttl(CAPABILITY_TTL_SECONDS)
    stack.emulator.set_idempotency_dedup_window(CAPABILITY_TTL_SECONDS)
    stack.emulator.inject_failure(
        FailurePolicy(  # type: ignore[call-arg]  # pydantic defaults; plugin unavailable
            scope=FailureScope.UPSTREAM_FILES_UPLOAD,
            error_rate_5xx=FORCE_5XX_RATE,
        )
    )
    return stack


async def _submit(stack: E2EStack, envelope: ChainEnvelope) -> None:
    """Submit one matrix chain through the public SDK."""
    await stack.phantom_client.submit_chain(
        envelope,
        body_refs={"body": TEST_BODY},
        uid=DEFAULT_FAKE_SUB,
        auth_token=f"Bearer {stack.fake_security_token()}",
    )


async def _release_after_rejected_put_and_second_create(
    stack: E2EStack,
    chain_id: UUID,
) -> None:
    """Clear the PUT fault after one rejection and create number two are observed."""

    async def _failure_then_second_create_observed() -> bool:
        rejected_puts = stack.emulator.error_rate_5xx_count(FailureScope.UPSTREAM_FILES_UPLOAD)
        return rejected_puts >= 1 and len(_create_events(stack, chain_id)) >= 2

    await await_until(
        _failure_then_second_create_observed,
        timeout_seconds=WAIT_BUDGET_SECONDS,
        poll_interval_seconds=EVENT_POLL_SECONDS,
        message="oracles never observed a rejected PUT followed by the second metadata create",
    )
    second_create = _create_events(stack, chain_id)[1]
    rejected_events = stack.emulator.error_rate_5xx_events(FailureScope.UPSTREAM_FILES_UPLOAD)
    assert stack.emulator.error_rate_5xx_count(FailureScope.UPSTREAM_FILES_UPLOAD) >= 1
    assert rejected_events
    assert rejected_events[0].occurred_at <= second_create.occurred_at
    stack.emulator.clear_failures()


def _captured_create(detail: ChainAdminDetail) -> dict[str, object]:
    """Return the final create-step captured values from admin detail."""
    by_step = {entry.step_name: entry.values for entry in detail.captured}
    value = by_step["create_file"]
    assert isinstance(value, dict)
    return value


async def test_capture_expiry_keyed_reexecutes_with_cached_identity() -> None:
    """Enabled + keyed: two creates share identity/URL, then one PUT succeeds."""
    stack = await _boot_case(capture_reexecution=True)
    chain_id = uuid4()
    try:
        await _submit(
            stack,
            _build_envelope(chain_id=chain_id, emulator_url=stack.emulator_url, keyed=True),
        )
        await _release_after_rejected_put_and_second_create(stack, chain_id)
        detail = await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="succeeded",
            timeout_seconds=WAIT_BUDGET_SECONDS,
        )

        events = _events(stack, chain_id)
        assert len(events) == 3
        first, second, put = events
        assert isinstance(first, MetadataCreateEvent)
        assert isinstance(second, MetadataCreateEvent)
        assert isinstance(put, BodyPutEvent)
        assert [first.cache_hit, second.cache_hit] == [False, True]
        assert first.idempotency_key == second.idempotency_key == str(chain_id)
        assert first.file_id == second.file_id == put.file_id
        assert first.upload_token == second.upload_token == put.upload_token
        assert first.upload_url == second.upload_url == put.upload_url
        assert second.occurred_at - first.occurred_at >= timedelta(seconds=CAPTURE_TTL_SECONDS)
        assert second.occurred_at <= put.occurred_at
        assert put.occurred_at < second.occurred_at + timedelta(seconds=CAPTURE_TTL_SECONDS)
        assert put.body_size == len(TEST_BODY)
        assert put.body_hash == EXPECTED_BODY_HASH
        captured = _captured_create(detail)
        assert captured["upload_url"] == second.upload_url
        file_information = captured["file_information"]
        assert isinstance(file_information, dict)
        assert file_information["id"] == str(second.file_id)
    finally:
        stack.emulator.clear_failures()
        await stack.tear_down()


async def test_capture_expiry_unkeyed_reexecutes_with_distinct_identity() -> None:
    """Enabled + unkeyed: two distinct creates, then one PUT uses the second."""
    stack = await _boot_case(capture_reexecution=True)
    chain_id = uuid4()
    try:
        await _submit(
            stack,
            _build_envelope(chain_id=chain_id, emulator_url=stack.emulator_url, keyed=False),
        )
        await _release_after_rejected_put_and_second_create(stack, chain_id)
        detail = await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="succeeded",
            timeout_seconds=WAIT_BUDGET_SECONDS,
        )

        events = _events(stack, chain_id)
        assert len(events) == 3
        first, second, put = events
        assert isinstance(first, MetadataCreateEvent)
        assert isinstance(second, MetadataCreateEvent)
        assert isinstance(put, BodyPutEvent)
        assert first.idempotency_key is None and second.idempotency_key is None
        assert [first.cache_hit, second.cache_hit] == [False, False]
        assert first.file_id != second.file_id
        assert first.upload_token != second.upload_token
        assert first.upload_url != second.upload_url
        assert put.file_id == second.file_id
        assert put.upload_token == second.upload_token
        assert put.upload_url == second.upload_url
        assert second.occurred_at - first.occurred_at >= timedelta(seconds=CAPTURE_TTL_SECONDS)
        assert second.occurred_at <= put.occurred_at
        assert put.occurred_at < second.occurred_at + timedelta(seconds=CAPTURE_TTL_SECONDS)
        assert put.body_size == len(TEST_BODY)
        assert put.body_hash == EXPECTED_BODY_HASH
        captured = _captured_create(detail)
        assert captured["upload_url"] == second.upload_url
        file_information = captured["file_information"]
        assert isinstance(file_information, dict)
        assert file_information["id"] == str(second.file_id)
    finally:
        stack.emulator.clear_failures()
        await stack.tear_down()


async def test_capture_expiry_disabled_stores_without_reexecution() -> None:
    """Default-disabled control: one create, zero accepted PUTs, stored."""
    stack = await _boot_case(capture_reexecution=None)
    chain_id = uuid4()
    try:
        await _submit(
            stack,
            _build_envelope(chain_id=chain_id, emulator_url=stack.emulator_url, keyed=True),
        )
        detail = await assert_chain_reaches_state(
            stack.phantom_client,
            chain_id,
            state="stored",
            timeout_seconds=WAIT_BUDGET_SECONDS,
        )

        events = _events(stack, chain_id)
        assert len(events) == 1
        create = events[0]
        assert isinstance(create, MetadataCreateEvent)
        assert create.cache_hit is False
        assert create.idempotency_key == str(chain_id)
        assert detail.last_step_completed == "create_file"
        assert detail.updated_at >= create.occurred_at + timedelta(seconds=CAPTURE_TTL_SECONDS)
        captured = _captured_create(detail)
        assert captured["upload_url"] == create.upload_url
    finally:
        stack.emulator.clear_failures()
        await stack.tear_down()
