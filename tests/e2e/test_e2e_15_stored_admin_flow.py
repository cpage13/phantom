"""E2E-15 — ``stored`` → admin recovery flow.

Submits envelopes whose step 1 capture has a 1 s TTL; injects retryable
5xx on step 2 so the executor's first attempt fails, the retry interval
elapses, and the next attempt's TTL check sees the capture expired.
With ``capture_reexecution: false`` (the default per ADR-011) the row
then transitions to ``stored``.

Two recovery paths exercised:

- (a) **Cancel** — :meth:`PhantomClient.cancel` flips the row to
  ``cancelled``; the emulator never sees a successful step 2 for this
  chain_id.
- (b) **Replay** — clearing failures and calling
  :meth:`PhantomClient.replay` re-queues the chain; step 1 re-runs,
  fresh capture is obtained, step 2 succeeds.

The load-bearing knob for triggering capture expiry is the in-envelope
``ttl_seconds`` (the executor's TTL check uses the ``expires_at``
recorded on the captured value, which is ``now_at_capture +
ttl_seconds``).
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_client.models.chain import ChainCapture, ChainEnvelope
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import settle_for

# Body bytes — small; this test is about state transitions, not throughput.
BODY_BYTES: bytes = b"phantom-e2e-stored-admin-flow"

# Capture TTL on step 1 — short enough that the retry-back-off interval
# carries us past the expiry. The retry strategy intervals are
# [0, 1, 2, 5, 10] in phantom-config.yml, so a 1 s TTL plus a single
# step-2 failure (which schedules a 1 s back-off) reliably crosses the
# boundary.
CAPTURE_TTL_SECONDS: int = 1

# Wait budgets.
STORED_WAIT_SECONDS: float = 20.0
SUCCEEDED_WAIT_SECONDS: float = 15.0
CANCELLED_WAIT_SECONDS: float = 5.0


@pytest.mark.e2e
async def test_e2e_15_stored_cancel_flow() -> None:
    """Capture TTL elapsed + capture_reexecution=False → row stored → cancel."""
    stack = await _boot_with_stored_config()
    try:
        pc = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()

        # 5xx on step 2 so the executor's first attempt fails; the
        # retry back-off (intervals[1]=1 s) carries us past the 1 s
        # capture TTL, and the next attempt's TTL check trips the
        # capture-expired path.
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                scope=FailureScope.UPSTREAM_FILES_UPLOAD,
                error_rate_5xx=1.0,
            ),
        )

        chain_id = uuid4()
        await _submit_one(
            pc,
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            bearer=stack.fake_security_token(),
        )

        # 1. Wait for the row to reach `stored` (capture expired
        #    while step 2 was held in flight; ADR-011 default of
        #    capture_reexecution=false routes the row there).
        stored_chain = await assert_chain_reaches_state(
            pc,
            chain_id,
            state="stored",
            timeout_seconds=STORED_WAIT_SECONDS,
        )
        assert stored_chain.state == "stored"

        # Snapshot the emulator's received log — should NOT have a
        # successful PUT for this chain_id (the PUT either never
        # completed or returned 403 SignatureMismatch due to expiry).
        emulator.clear_received()

        # 2. Cancel via admin verb.
        await pc.cancel(chain_id)
        cancelled = await assert_chain_reaches_state(
            pc,
            chain_id,
            state="cancelled",
            timeout_seconds=CANCELLED_WAIT_SECONDS,
        )
        assert cancelled.state == "cancelled"

        # 3. After cancel, no further step 2 PUT should be issued by
        #    phantom (the row is terminal). This is a negative
        #    assertion (absence of PUT), so there is no observable
        #    predicate to await on — we settle for one second to give
        #    any racing in-flight worker a chance to perform a (wrong)
        #    PUT, then assert the received-log is clean.
        await settle_for(
            1.0,
            reason="negative-assertion: window for any racing in-flight PUT to land",
        )
        for entry in emulator.received():
            assert entry.metadata_kvs.get("phantom_local_uuid") != str(chain_id), (
                f"emulator received a body for cancelled chain_id={chain_id}"
            )
    finally:
        await stack.tear_down()


@pytest.mark.e2e
async def test_e2e_15_stored_replay_flow() -> None:
    """Capture TTL elapsed → row stored → clear failures, replay → succeeded."""
    stack = await _boot_with_stored_config()
    try:
        pc = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()

        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                scope=FailureScope.UPSTREAM_FILES_UPLOAD,
                error_rate_5xx=1.0,
            ),
        )

        chain_id = uuid4()
        await _submit_one(
            pc,
            chain_id=chain_id,
            emulator_url=stack.emulator_url,
            bearer=stack.fake_security_token(),
        )

        # 1. Wait for stored.
        await assert_chain_reaches_state(
            pc,
            chain_id,
            state="stored",
            timeout_seconds=STORED_WAIT_SECONDS,
        )

        # 2. Clear failures so the replayed chain's step 2 succeeds.
        emulator.clear_failures()

        # 3. Replay re-queues the chain; step 1 re-runs and produces
        #    a fresh capture; step 2 succeeds.
        await pc.replay(chain_id)
        succeeded = await assert_chain_reaches_state(
            pc,
            chain_id,
            state="succeeded",
            timeout_seconds=SUCCEEDED_WAIT_SECONDS,
        )
        assert succeeded.state == "succeeded"

        # 4. Emulator received exactly one body for this chain_id
        #    (the post-replay execution). The pre-replay PUT may
        #    have landed with bad signature (which yields a 403,
        #    not a recorded body) or may have been abandoned
        #    mid-flight; either way the received log should have
        #    exactly one entry for the post-replay run.
        received_for_chain = [
            e
            for e in emulator.received()
            if e.metadata_kvs.get("phantom_local_uuid") == str(chain_id)
        ]
        assert len(received_for_chain) == 1, (
            f"expected exactly one received entry for chain_id={chain_id}, "
            f"got {len(received_for_chain)}: {[e.body_size for e in received_for_chain]}"
        )
    finally:
        await stack.tear_down()


async def _boot_with_stored_config() -> E2EStack:
    """Boot a stack with ``capture_reexecution: false`` on the instance."""
    return await boot_stack(
        config_overrides={
            "instances": [
                {
                    "id": "primary",
                    "host_prefixes": ["emulator", "127.0.0.1", "localhost"],
                    "data_dir": "primary",
                    "capture_reexecution": False,  # ADR-011 default; explicit here
                    "routes": [
                        {
                            "name": "emulator",
                            "hosts": ["emulator", "127.0.0.1", "localhost"],
                            "auth_mode": "phantom_bearer",
                        },
                    ],
                },
            ],
        },
    )


async def _submit_one(
    pc: PhantomClient,
    *,
    chain_id: UUID,
    emulator_url: str,
    bearer: str,
) -> None:
    """Build an envelope with a short capture TTL and submit it.

    Reuses the driver's envelope builder for the step shapes, then
    overrides each capture's ``ttl_seconds`` to the short test value
    (the builder hard-codes the upstream 7-day default). The override is
    cosmetic on the wire — the executor's TTL check consults the
    ``ttl_seconds`` field on each :class:`ChainCapture` directly.
    """
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    envelope = _with_short_capture_ttl(envelope, ttl_seconds=CAPTURE_TTL_SECONDS)
    await pc.submit_chain(
        envelope,
        body_refs={"body": BODY_BYTES},
        uid="00000000-0000-0000-0000-000000000001",
        auth_token=f"Bearer {bearer}",
    )


def _with_short_capture_ttl(envelope: ChainEnvelope, *, ttl_seconds: int) -> ChainEnvelope:
    """Return a copy of ``envelope`` with every capture's TTL clamped."""
    new_steps = []
    for step in envelope.steps:
        new_captures: list[ChainCapture] = []
        for cap in step.capture:
            data: dict[str, Any] = cap.model_dump(by_alias=True)
            data["ttl_seconds"] = ttl_seconds
            new_captures.append(ChainCapture.model_validate(data))
        new_steps.append(step.model_copy(update={"capture": new_captures}))
    return envelope.model_copy(update={"steps": new_steps})
