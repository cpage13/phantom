"""Invariant #16 end to end: the saturation ledger idles at zero (T4).

The R8-4 / R8-6 unit pins prove each release path in isolation; this
test proves the COMPOSITION over a live wire: a mixed workload that
exercises every ledger leg the resumed loop fixed - healthy delivery
(the sender's release), replay of a delivered row (the R8-6 re-admit
followed by a second release), admin cancel of an in-flight row (the
R8-4 cancel-owned release), and admin delete of an in-flight row (the
R8-4 removal release) - after which the gate's ``saturation_balance``
gauge must read exactly zero. Any future leak path that survives the
per-path unit pins shows up here as a non-zero residue.
"""

from __future__ import annotations

import secrets
from uuid import UUID

import httpx
import pytest
from phantom_client import PhantomClient
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from ._driver import PhantomDriver
from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until

pytestmark = pytest.mark.e2e

# Body size for the probe uploads; small keeps the lane quick.
PROBE_BODY_BYTES: int = 1024

# Budget for one upload to reach a terminal state on a healthy upstream
# (e2e retry cadence fixed_intervals [0, 1, 2, 5, 10]).
TERMINAL_BUDGET_SECONDS: float = 30.0

# Budget for a held-down row to record its first attempt (proves it is
# genuinely in flight before the admin action races nothing).
FIRST_ATTEMPT_BUDGET_SECONDS: float = 10.0

# Budget for the gauge to settle at zero once every chain is terminal.
IDLE_BUDGET_SECONDS: float = 10.0

# 5xx rate for the hold-down policy. 1.0 = every upstream call fails.
FORCE_5XX_RATE: float = 1.0

# Reaper effectively disabled so rows survive the whole scenario, and
# succeeded bodies retained so the replay leg is reachable (the suite
# default discards on success, which makes replay refuse by design).
RETENTION_HOLD_SECONDS: int = 3600


def _driver_for(stack: E2EStack) -> PhantomDriver:
    """Build the public test driver bound to ``stack``."""
    return PhantomDriver(
        stack.phantom_client,
        files_api=stack.emulator_url,
        get_security_token=stack.fake_security_token,
    )


async def _saturation_balance(admin_url: str) -> float:
    """Read the live ``saturation_balance`` gauge off the admin API.

    Wire shape (GaugesResponse): ``{"gauges": [{"name", "description",
    "values": {"<label>": <float>}}]}``; the empty-string bucket is the
    no-label total.
    """
    async with httpx.AsyncClient() as http:
        response = await http.get(f"{admin_url}/v1/admin/observability/gauges")
    response.raise_for_status()
    for entry in response.json()["gauges"]:
        if entry["name"] == "saturation_balance":
            return float(entry["values"][""])
    raise AssertionError("saturation_balance gauge missing from the gauges response")


async def _await_first_attempt(pc: PhantomClient, chain_id: UUID) -> None:
    """Poll until the held-down row records an attempt (it is in flight)."""

    async def _attempted() -> bool:
        detail = await pc.get_upload(chain_id)
        return detail.attempts > 0

    await await_until(
        _attempted,
        timeout_seconds=FIRST_ATTEMPT_BUDGET_SECONDS,
        message="the held-down row never recorded an attempt",
    )


async def test_mixed_workload_drains_the_gate_to_zero() -> None:
    """Deliver, replay, cancel, and delete; the ledger must idle at zero."""
    stack = await boot_stack(
        config_overrides={
            "retention": {
                "reaper_interval_seconds": RETENTION_HOLD_SECONDS,
                "succeeded_body_seconds": RETENTION_HOLD_SECONDS,
                "succeeded_metadata_seconds": RETENTION_HOLD_SECONDS,
            },
        },
    )
    try:
        pc: PhantomClient = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        driver = _driver_for(stack)

        # Leg 1 - two healthy deliveries (admission charge, sender release).
        delivered: list[UUID] = []
        for idx in range(2):
            result = await driver.in_memory_upload(
                build_create_file_request(file_name=f"idle-balance-{idx}.bin"),
                secrets.token_bytes(PROBE_BODY_BYTES),
            )
            delivered.append(UUID(str(result.id)))
        for chain_id in delivered:
            await assert_chain_reaches_state(
                pc, chain_id, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
            )

        # Leg 2 - replay one delivered row: the R8-6 re-admit charges the
        # gate again; the second delivery releases again.
        await pc.replay(delivered[0])
        await assert_chain_reaches_state(
            pc, delivered[0], state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )

        # Legs 3 + 4 - two rows parked against a dead upstream (slots
        # held), then removed by the two admin paths whose releases R8-4
        # added: cancel and hard delete.
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # fields have defaults; mypy lacks the pydantic plugin
                scope=FailureScope.GLOBAL,
                error_rate_5xx=FORCE_5XX_RATE,
            )
        )
        parked: list[UUID] = []
        for idx in range(2):
            result = await driver.in_memory_upload(
                build_create_file_request(file_name=f"idle-balance-parked-{idx}.bin"),
                secrets.token_bytes(PROBE_BODY_BYTES),
            )
            parked.append(UUID(str(result.id)))
        for chain_id in parked:
            await _await_first_attempt(pc, chain_id)

        await pc.cancel(parked[0])
        await pc.delete_upload(parked[1])
        emulator.clear_failures()

        # The ledger must drain to exactly zero - no residue from any leg.
        async def _idle() -> bool:
            return await _saturation_balance(stack.phantom_admin_url) == 0.0

        await await_until(
            _idle,
            timeout_seconds=IDLE_BUDGET_SECONDS,
            message=(
                "saturation_balance never returned to zero after the mixed "
                "workload drained; some ledger leg leaked (invariant #16)"
            ),
        )
    finally:
        await stack.tear_down()
