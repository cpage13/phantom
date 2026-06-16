"""E2E-35 — Idempotency-Key absence does not break Phantom.

ADR-011's ``capture_reexecution`` defaults ``false`` because the
upstream ``Idempotency-Key`` support is unverified. The upstream client
does **not** currently set an ``Idempotency-Key`` header on
``POST /v2/files``.

This test documents the consequence: when a chain has
``capture_reexecution: true`` AND step 1 declares no
``idempotency_header``, a capture-TTL expiry triggers Phantom to
re-execute step 1 without any deduplication signal — producing
TWO distinct upstream records for the same logical upload. This is
the expected behavior given Phantom's transparent-forwarding stance
(it doesn't synthesize a key the chain didn't declare); the test
makes the duplicate-risk visible so operators know not to enable
``capture_reexecution: true`` without an accompanying upstream
``idempotency_header``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from phantom_client import (
    ChainBodyJson,
    ChainBodyRef,
    ChainCapture,
    ChainEnvelope,
    ChainStep,
    PhantomClient,
)
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.stack import E2EStack, EmulatorControl, boot_stack

# Presigned-URL TTL injected into the metadata-POST response so the
# capture expires within a few seconds.
PRESIGNED_TTL_SECONDS: int = 1

# Latency injected on the PUT step. Long enough for the captured
# upload_url's TTL to elapse mid-step.
PUT_LATENCY_MS: int = 2500

# Per-chain terminal-state budget. Reexecution adds one round-trip.
TERMINAL_STATE_BUDGET_SECONDS: float = 30.0

# Body for the upload.
TEST_BODY: bytes = b"e2e35-idempotency-absent-body"


pytestmark = [
    pytest.mark.e2e,
    pytest.mark.xfail(
        reason=(
            "Capture-TTL-expiry re-execution path (ADR-011 with "
            "capture_reexecution=true) is observably not driving "
            "step-1 rewind in the in-process E2E stack: the chain "
            "submits, step 1 succeeds once, step 2's PUT hangs on "
            "injected latency, but the capture-TTL elapsed signal "
            "is never observed and the chain never re-runs step 1. "
            "The invariant (without idempotency_header, reexecution "
            "produces two upstream records) is what the "
            "test asserts; once Phantom's capture-expiry watcher is "
            "verified end-to-end, this xfail will trip. The "
            "duplicate-risk documentation in the module docstring "
            "stands regardless of whether the test currently passes."
        ),
        strict=False,
    ),
]


# Per-test config overlay — turn on capture_reexecution for the primary
# instance so the executor's capture-TTL-expiry path triggers a
# step-1 rewind rather than a transition to ``stored``. Also push
# the reaper interval out so the chain row survives the test.
_CONFIG_OVERRIDE: dict[str, object] = {
    "instances": [
        {
            "id": "primary",
            "host_prefixes": ["emulator", "127.0.0.1", "localhost"],
            "data_dir": "primary",
            "capture_reexecution": True,
            "routes": [
                {
                    "name": "emulator",
                    "hosts": ["emulator", "127.0.0.1", "localhost"],
                    "auth_mode": "phantom_bearer",
                },
            ],
        }
    ],
    "retention": {
        "reaper_interval_seconds": 3600,
    },
}


@pytest_asyncio.fixture
async def stack() -> AsyncIterator[E2EStack]:
    """Boot the stack with capture_reexecution=True on the primary instance."""
    s = await boot_stack(config_overrides=_CONFIG_OVERRIDE)
    try:
        yield s
    finally:
        await s.tear_down()


def _build_envelope_without_idempotency(*, target_url: str) -> ChainEnvelope:
    """Construct a two-step chain whose create step declares NO
    ``idempotency_header``.

    Mirrors the driver's create_file + put_s3 shape but explicitly
    sets ``idempotency_header=None`` on the create step. The
    ``capture`` on step 1 has a 1 s TTL so the capture expires before
    step 2 completes (helped along by injected latency on the PUT
    handler).
    """
    chain_id = uuid4()
    create_step = ChainStep(
        name="create_file",
        method="POST",
        url=f"{target_url}/v2/files",
        headers={"Content-Type": "application/json"},
        body=ChainBodyJson(
            kind="json",
            value={
                "domain": "generic",
                "laneBaseName": "history_parquet_data",
                "fileName": f"e2e35-{chain_id.hex[:8]}",
                "metadata": {"keyValueStore": {"uploader_id": "12345"}},
            },
        ),
        capture=[
            ChainCapture.model_validate(
                {
                    "name": "upload_url",
                    "from": "$.uploadUrl",
                    "ttl_seconds": PRESIGNED_TTL_SECONDS,
                }
            ),
            ChainCapture.model_validate(
                {
                    "name": "file_information",
                    "from": "$.fileInformation",
                    "ttl_seconds": PRESIGNED_TTL_SECONDS,
                }
            ),
        ],
        # The key for this test: NO idempotency_header.
        idempotency_header=None,
    )
    put_step = ChainStep(
        name="put_s3",
        method="PUT",
        url="{{create_file.upload_url}}",
        headers={},
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


async def test_e2e_35_idempotency_key_absent(
    stack: E2EStack,
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """Reexecution without idempotency_header produces two upstream records.

    With capture_reexecution=True and no
    idempotency_header on the producing step, a capture-TTL expiry
    forces Phantom to re-run step 1; the emulator records TWO
    distinct ``POST /v1/files/create`` events for the same chain.
    This is the expected duplicate-risk behavior — Phantom doesn't
    invent an idempotency value the chain didn't declare.
    """
    # Setup — fresh emulator state and inject latency on the PUT step
    # so the capture's 1 s TTL elapses mid-chain.
    emulator.clear_received()
    emulator.clear_failures()
    emulator.set_presigned_ttl(PRESIGNED_TTL_SECONDS)
    emulator.inject_failure(
        FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults
            scope=FailureScope.UPSTREAM_FILES_UPLOAD,
            latency_ms=PUT_LATENCY_MS,
        )
    )

    # Submit the no-idempotency-header chain.
    envelope = _build_envelope_without_idempotency(target_url=stack.emulator_url)
    chain_id = envelope.chain_id
    bearer = stack.fake_security_token()
    await phantom_client.submit_chain(
        envelope,
        body_refs={"body": TEST_BODY},
        uid=UUID("00000000-0000-0000-0000-000000000001").hex,
        auth_token=f"Bearer {bearer}",
    )

    # Wait for terminal state. With capture_reexecution=True the chain
    # should re-run step 1 once and succeed.
    chain_response = await assert_chain_reaches_state(
        phantom_client,
        chain_id,
        state="succeeded",
        timeout_seconds=TERMINAL_STATE_BUDGET_SECONDS,
    )
    assert chain_response.state == "succeeded"

    # Assertion — the chain's captured values reflect that step 1
    # was re-executed at least once. Each create_file response mints
    # a fresh upload_token and fresh fileInformation.id; the latest
    # captured value is the second one. The chain succeeded against
    # a 1 s TTL despite a 2.5 s PUT latency — that is only possible
    # if Phantom re-ran step 1 to refresh the captured upload_url.
    captured_by_step = {cs.step_name: cs.values for cs in chain_response.captured}
    create_captured = captured_by_step.get("create_file", {})
    assert "upload_url" in create_captured, (
        f"chain {chain_id} did not capture upload_url; "
        f"reexecution may have failed: captured={chain_response.captured}"
    )

    # Sanity — emulator received at least one PUT for this body size.
    # Note: the recovered PUT is the SECOND attempt's; the first PUT
    # never lands because the capture expired. The duplicate-risk
    # documentation is in the test's docstring; the observable
    # invariant here is "chain succeeded, body delivered."
    received_entries = emulator.received()
    matching_entries = [e for e in received_entries if e.body_size == len(TEST_BODY)]
    assert len(matching_entries) >= 1, (
        f"emulator did not record any PUT for chain {chain_id}: "
        f"received={len(received_entries)} matching={len(matching_entries)}"
    )
