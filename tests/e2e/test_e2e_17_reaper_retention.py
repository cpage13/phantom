"""E2E-17 — Reaper retention windows.

Submits envelopes that land in three different terminal states
(``succeeded``, ``failed``, ``cancelled``) under tight per-state
retention; asserts the reaper sweeps each row at the configured
boundary.

Split into three test functions so each has tightly controlled per-row
timing — sharing one stack across all three terminal states made the
shared wall-clock reference and the per-state windows interact in
flaky ways. Each function gets its own stack with the same retention
config; the test budget stays well under a minute total.

The ``failed`` state is reached by routing the chain at a non-existent
upstream path (the emulator returns FastAPI's default 404 for unmatched
routes). The executor classifies 4xx as :class:`Failed4xx`, which the
sender's :func:`_on_terminal_failure` transitions to ``state="failed"``.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_client.errors import PhantomNotFoundError
from phantom_client.models.chain import ChainBodyJson, ChainEnvelope, ChainStep
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import boot_stack
from .helpers.timing import sleep_until_monotonic

# Body bytes per envelope. Small — this test is about retention timing.
BODY_BYTES: bytes = b"phantom-e2e-reaper-retention"

# Retention windows (seconds). Tight so the test runs in well under a
# minute while still exercising the per-state metadata-vs-body split.
SUCCEEDED_METADATA_S: int = 5
SUCCEEDED_BODY_S: int = 0
FAILED_METADATA_S: int = 5
FAILED_BODY_S: int = 5
CANCELLED_METADATA_S: int = 5
CANCELLED_BODY_S: int = 5
REAPER_INTERVAL_S: int = 1

# Margin added to retention boundaries before probing — the reaper
# wakes every REAPER_INTERVAL_S, so 2 * interval is a safe margin.
REAPER_SETTLE_MARGIN_S: float = 2.5

# Shared sub claim — every row derives the same uid header.
SHARED_SUB: str = "00000000-0000-0000-0000-000000000017"

# Wait budgets.
TERMINAL_WAIT_SECONDS: float = 10.0


def _retention_config() -> Mapping[str, Any]:
    """Phantom config_overrides applying the tight retention windows."""
    return {
        "retention": {
            "succeeded_metadata_seconds": SUCCEEDED_METADATA_S,
            "succeeded_body_seconds": SUCCEEDED_BODY_S,
            "failed_metadata_seconds": FAILED_METADATA_S,
            "failed_body_seconds": FAILED_BODY_S,
            "cancelled_metadata_seconds": CANCELLED_METADATA_S,
            "cancelled_body_seconds": CANCELLED_BODY_S,
            "reaper_interval_seconds": REAPER_INTERVAL_S,
        },
        "instances": [
            {
                "id": "primary",
                "host_prefixes": ["emulator", "127.0.0.1", "localhost"],
                "data_dir": "primary",
                "capture_reexecution": False,
                "routes": [
                    {
                        "name": "emulator",
                        "hosts": ["emulator", "127.0.0.1", "localhost"],
                        "auth_mode": "phantom_bearer",
                    },
                ],
            },
        ],
    }


@pytest.mark.e2e
async def test_e2e_17_reaper_succeeded() -> None:
    """A succeeded row disappears after succeeded_metadata_seconds."""
    stack = await boot_stack(config_overrides=_retention_config())
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()

        chain_id = uuid4()
        await _submit_happy(
            pc,
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            bearer=stack.fake_security_token(),
        )
        await assert_chain_reaches_state(
            pc,
            chain_id,
            state="succeeded",
            timeout_seconds=TERMINAL_WAIT_SECONDS,
        )
        terminal_at = time.monotonic()

        # Row present immediately after success.
        await _assert_present(pc, chain_id)

        # Wait past the succeeded_metadata window + reaper margin.
        await sleep_until_monotonic(
            terminal_at + SUCCEEDED_METADATA_S + REAPER_SETTLE_MARGIN_S,
        )
        await _assert_absent(pc, chain_id)
    finally:
        await stack.tear_down()


@pytest.mark.e2e
async def test_e2e_17_reaper_failed() -> None:
    """A failed row disappears after failed_metadata_seconds."""
    stack = await boot_stack(config_overrides=_retention_config())
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()

        chain_id = uuid4()
        await _submit_bad_path_chain(
            pc,
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            bearer=stack.fake_security_token(),
        )
        await assert_chain_reaches_state(
            pc,
            chain_id,
            state="failed",
            timeout_seconds=TERMINAL_WAIT_SECONDS,
        )
        terminal_at = time.monotonic()

        # Row present after failure.
        await _assert_present(pc, chain_id)

        await sleep_until_monotonic(
            terminal_at + FAILED_METADATA_S + REAPER_SETTLE_MARGIN_S,
        )
        await _assert_absent(pc, chain_id)
    finally:
        await stack.tear_down()


@pytest.mark.e2e
async def test_e2e_17_reaper_cancelled() -> None:
    """A cancelled row disappears after cancelled_metadata_seconds."""
    stack = await boot_stack(config_overrides=_retention_config())
    try:
        pc = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()

        # Hold step 2 so we can cancel before the chain terminates.
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                scope=FailureScope.UPSTREAM_FILES_UPLOAD,
                latency_ms=10_000,
            ),
        )

        chain_id = uuid4()
        await _submit_happy(
            pc,
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            bearer=stack.fake_security_token(),
        )
        # Cancel mid-flight.
        await pc.cancel(chain_id)
        await assert_chain_reaches_state(
            pc,
            chain_id,
            state="cancelled",
            timeout_seconds=TERMINAL_WAIT_SECONDS,
        )
        terminal_at = time.monotonic()
        emulator.clear_failures()

        await _assert_present(pc, chain_id)

        await sleep_until_monotonic(
            terminal_at + CANCELLED_METADATA_S + REAPER_SETTLE_MARGIN_S,
        )
        await _assert_absent(pc, chain_id)
    finally:
        await stack.tear_down()


async def _assert_present(pc: PhantomClient, chain_id: UUID) -> None:
    """Assert the row is visible via get_upload."""
    response = await pc.get_upload(chain_id)
    assert response.chain_id == chain_id


async def _assert_absent(pc: PhantomClient, chain_id: UUID) -> None:
    """Assert the row is gone (404 from the admin surface).

    The admin GET /v1/admin/chains/{chain_id} endpoint emits the
    canonical :class:`ErrorEnvelope` with ``error.code='not_found'``;
    phantom-client maps that to :class:`PhantomNotFoundError`.
    """
    try:
        await pc.get_upload(chain_id)
    except PhantomNotFoundError:
        return
    raise AssertionError(f"row {chain_id} still present; expected reaper sweep")


async def _submit_happy(
    pc: PhantomClient,
    *,
    chain_id: UUID,
    emulator_url: str,
    bearer: str,
) -> None:
    """Submit a happy two-step envelope using the driver's builder."""
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    await pc.submit_chain(
        envelope,
        body_refs={"body": BODY_BYTES},
        uid=SHARED_SUB,
        auth_token=f"Bearer {bearer}",
    )


async def _submit_bad_path_chain(
    pc: PhantomClient,
    *,
    chain_id: UUID,
    emulator_url: str,
    bearer: str,
) -> None:
    """Submit a single-step chain at a non-existent upstream path.

    The emulator's FastAPI default 404 for unmatched routes is the
    only test-harness path to state='failed' — the failure injection
    surface emits 401/503/5xx but not 4xx, and the chain parser
    catches malformed templates at ingress before the executor runs.
    """
    step = ChainStep(
        name="bad_path_step",
        method="POST",
        url=f"{emulator_url}/v1/files/this-path-does-not-exist",
        headers={"Content-Type": "application/json"},
        body=ChainBodyJson(
            kind="json",
            value={
                "metadata": {
                    "keyValueStore": {"phantom_local_uuid": str(chain_id)},
                },
            },
        ),
        capture=[],
        idempotency_header=None,
    )
    envelope = ChainEnvelope(
        chain_id=chain_id,
        idempotency_key=str(chain_id),
        steps=[step],
        default_target=None,
    )
    await pc.submit_chain(
        envelope,
        body_refs=None,
        uid=SHARED_SUB,
        auth_token=f"Bearer {bearer}",
    )
