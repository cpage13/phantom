"""E2E-11 — Memory-tier saturation cap.

Submits N envelopes against a Phantom whose instance ``saturation.max_in_flight``
is intentionally low; emulator is throttled so step 2 stays in flight long
enough for all N to overlap. The (N+1)-th submit must trip the ingress
saturation gate and surface as ``saturation_cap`` on the wire. After the
trickle is cleared and one chain finishes, the same (N+1)-th submit
succeeds — proving the gate releases on terminal-success.

The saturation gate's row-count cap is the load-bearing knob here.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from phantom_client import (
    ChainResponse,
    PhantomClient,
    PhantomUnavailableError,
)
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import boot_stack
from .helpers.timing import await_until

# Saturation cap pinned to a small value so the test runs in seconds.
# Five is large enough to exercise concurrent flight (so the gate's
# release-after-success path matters) without being so large that
# every claim_due tick has to scan a long queue.
MAX_IN_FLIGHT: int = 5

# Latency injected on the PUT step (in milliseconds). Big enough that
# all five chains hold their in-flight slot through the 6th submit;
# small enough that the test still finishes inside the suite budget.
PUT_LATENCY_MS: int = 4_000

# Body bytes per envelope. Small bytes — we're testing row-count cap,
# not bytes cap.
BODY_BYTES: bytes = b"phantom-e2e-saturation-cap-body"


async def _submit_one(
    pc: PhantomClient,
    *,
    emulator_url: str,
    bearer: str,
    uid: str,
) -> ChainResponse:
    """Build a fresh chain envelope and submit it through phantom-client.

    Bypasses the driver so 503 surfaces as :class:`PhantomUnavailableError`
    on the SDK side without going through the driver's return path. The
    envelope shape is identical to what the driver
    constructs in ``in_memory_upload`` (same builder).
    """
    request = build_create_file_request(file_name=f"e2e_{uuid4().hex[:12]}")
    local_uuid = uuid4()
    request.metadata.key_value_store["phantom_local_uuid"] = str(local_uuid)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=local_uuid,
    )
    return await pc.submit_chain(
        envelope,
        body_refs={"body": BODY_BYTES},
        uid=uid,
        auth_token=f"Bearer {bearer}",
    )


@pytest.mark.e2e
async def test_e2e_11_saturation_cap() -> None:
    """Saturation cap fires at the configured row count; releases on terminal-success."""
    stack = await boot_stack(
        config_overrides={
            "saturation": {"max_in_flight": MAX_IN_FLIGHT},
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
        },
    )
    try:
        emulator = stack.emulator
        pc = stack.phantom_client
        emulator.clear_received()
        emulator.clear_failures()

        # 1. Throttle step 2 so each successful chain occupies an
        #    in-flight slot for ~PUT_LATENCY_MS.
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                scope=FailureScope.UPSTREAM_FILES_UPLOAD,
                latency_ms=PUT_LATENCY_MS,
            ),
        )

        bearer = stack.fake_security_token()
        uid = "00000000-0000-0000-0000-000000000001"

        # 2. Submit MAX_IN_FLIGHT envelopes back-to-back. Each is
        #    admitted (the gate has headroom) and the sender pool
        #    picks them up; the latency injection holds them in
        #    flight.
        responses: list[ChainResponse] = []
        for _ in range(MAX_IN_FLIGHT):
            resp = await _submit_one(
                pc,
                emulator_url=stack.emulator_url,
                bearer=bearer,
                uid=uid,
            )
            responses.append(resp)
        assert len(responses) == MAX_IN_FLIGHT

        # Wait briefly so the senders have actually claimed the rows
        # and started the latency-injected PUT — i.e., the saturation
        # gate is genuinely full before we try the 6th.
        await _await_saturated(pc, expected=MAX_IN_FLIGHT)

        # 3. Sixth submit must trip the ingress saturation gate.
        with pytest.raises(PhantomUnavailableError) as excinfo:
            await _submit_one(
                pc,
                emulator_url=stack.emulator_url,
                bearer=bearer,
                uid=uid,
            )
        assert excinfo.value.error_code == "saturation_cap", (
            f"expected error_code='saturation_cap', got {excinfo.value.error_code!r}"
        )
        assert excinfo.value.status_code == 503

        # Gate self-report consistency.
        stats = await pc.get_stats()
        assert stats.saturation.saturated is True
        assert stats.saturation.max_in_flight == MAX_IN_FLIGHT
        # in_flight count reflects the rows currently occupying slots.
        # The breakdown lives on stats.in_flight (TierBreakdown), not on
        # the saturation block (the SDK contract surfaces only the cap
        # and the boolean).
        assert stats.in_flight.count == MAX_IN_FLIGHT

        # 4. Clear the latency throttle. The first sender's PUT will
        #    return immediately, the chain succeeds, the gate releases
        #    one slot. A fresh submit should be admitted.
        emulator.clear_failures()
        for resp in responses:
            await assert_chain_reaches_state(
                pc,
                resp.chain_id,
                state="succeeded",
                timeout_seconds=30.0,
            )

        # 5. Sixth submit now succeeds.
        sixth = await _submit_one(
            pc,
            emulator_url=stack.emulator_url,
            bearer=bearer,
            uid=uid,
        )
        final = await assert_chain_reaches_state(
            pc,
            sixth.chain_id,
            state="succeeded",
            timeout_seconds=15.0,
        )
        assert final.state == "succeeded"
    finally:
        await stack.tear_down()


async def _await_saturated(
    pc: PhantomClient,
    *,
    expected: int,
    timeout_seconds: float = 10.0,
) -> None:
    """Wait until ``stats.saturation.saturated`` flips to True.

    The senders pick up claimed rows out-of-band, so saturating the
    gate is not instantaneous after the last ingress 202; this poll
    guards the rare-but-real case where the 6th submission races
    ahead of the 5th sender claim.
    """

    async def _saturated() -> bool:
        stats = await pc.get_stats()
        return stats.in_flight.count >= expected and stats.saturation.saturated

    await await_until(
        _saturated,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=0.05,
        message=(
            f"saturation gate never reached saturated=True with in_flight>={expected} "
            f"within {timeout_seconds}s"
        ),
    )
