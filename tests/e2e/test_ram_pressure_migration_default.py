"""RAM-pressure migration in the DEFAULT e2e run (TEST-2).

The two existing RAM-pressure e2e (``stress/test_ram_pressure`` and
``all_ram/test_r9_pm_allram_ram_ceiling``) are both behind the
stress/load markers, so the standard per-PR e2e battery never exercises
the over-ceiling -> watcher signal -> migrate-oldest-first path. That
path is a real reliability surface (SD-card-wear control plus the
``ram_pressure_signal_total`` counter the operator watches). This module
is a small, fast, DEFAULT-run (unmarked) variant that proves the watcher
fires at least once and the persist controller migrates a body
RAM->disk - so the gated suite no longer skips the whole surface.

It is deliberately lean next to the stress test: a tiny RAM ceiling and
a few small bodies held against a dead (5xx) upstream so they linger in
RAM long enough for the watcher to observe the breach. It asserts the
two load-bearing signals - ``ram_pressure_signal_total`` bumps on the
admin observability wire, and at least one row reaches
``body_location='file'`` - then clears the hold and lets the chains
drain so teardown is clean. The exhaustive admission-unblock + full-burst
drain assertions stay in the marker-gated stress test.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.e2e

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000002"

# The single declared body_ref name (matches the driver's envelope name "body").
BODY_REF_NAME: str = "body"

# A small RAM ceiling and a handful of small bodies that together exceed
# it - keeps the lane fast while still breaching the cap. 4 bodies of
# 4 KiB = 16 KiB against an 8 KiB ceiling.
RAM_CEILING_BYTES: int = 8 * 1024
BODY_BYTES_PER_CHAIN: int = 4 * 1024
BURST_SIZE: int = 4

# Tight watcher poll so the breach is observed within the test window.
RAM_PRESSURE_POLL_SECONDS: float = 0.25

# Canonical RAM-pressure signal counter the watcher bumps on each breach.
COUNTER_RAM_SIGNAL_TOTAL: str = "ram_pressure_signal_total"

# Every upstream call fails 5xx so the bodies stay buffered in RAM
# (queued/attempting, not delivered) while the watcher observes them.
FORCE_5XX_RATE: float = 1.0

# Budget for the watcher to signal AND a body to migrate to disk.
MIGRATION_BUDGET_SECONDS: float = 20.0

# Per-chain drain budget once the hold is lifted (so teardown is clean).
PER_CHAIN_TERMINAL_BUDGET_SECONDS: float = 30.0


async def _submit_one(
    pc: PhantomClient,
    *,
    emulator_url: str,
    bearer: str,
    body: bytes,
    chain_id: UUID,
) -> None:
    """Submit one chain at a small-but-non-trivial body size."""
    request = build_create_file_request(file_name=f"ram-default-{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    await pc.submit_chain(
        envelope,
        body_refs={BODY_REF_NAME: body},
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )


async def _ram_signal_total(stack: E2EStack) -> int:
    """Read the ``ram_pressure_signal_total`` no-label bucket off the wire."""
    response = await stack.phantom_client.get_observability_counters()
    for counter in response.counters:
        if counter.name == COUNTER_RAM_SIGNAL_TOTAL:
            return counter.values.get("", 0)
    raise AssertionError(
        f"{COUNTER_RAM_SIGNAL_TOTAL} counter missing from the observability surface; "
        f"present counters={[c.name for c in response.counters]}"
    )


async def test_ram_ceiling_breach_signals_and_migrates_in_default_run() -> None:
    """A RAM-ceiling breach signals the watcher and migrates a body to disk."""
    stack = await boot_stack(
        config_overrides={
            "storage": {
                "body_store": {
                    "mode": "hybrid",
                    "ram_ceiling_bytes": RAM_CEILING_BYTES,
                    "ram_pressure_poll_seconds": RAM_PRESSURE_POLL_SECONDS,
                },
            },
            "saturation": {
                # Headroom so the saturation gate does not 503 the burst
                # before it ever reaches RAM (this test targets RAM
                # pressure, not the saturation cap).
                "max_in_flight": BURST_SIZE * 4,
                "max_in_flight_bytes": BURST_SIZE * BODY_BYTES_PER_CHAIN * 8,
            },
        },
    )
    chain_ids: list[UUID] = [uuid4() for _ in range(BURST_SIZE)]
    bodies: list[bytes] = [secrets.token_bytes(BODY_BYTES_PER_CHAIN) for _ in range(BURST_SIZE)]
    try:
        pc = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        bearer = stack.fake_security_token(sub=DEFAULT_SUB)

        # Baseline: no pressure signalled yet.
        baseline_signals = await _ram_signal_total(stack)

        # Hold every upstream call at 5xx so the bodies linger in RAM
        # (buffered, retryable) for the watcher to observe over the cap.
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields default; mypy lacks the pydantic plugin
                scope=FailureScope.GLOBAL,
                error_rate_5xx=FORCE_5XX_RATE,
            ),
        )

        await asyncio.gather(
            *(
                _submit_one(
                    pc,
                    emulator_url=stack.emulator_url,
                    bearer=bearer,
                    body=bodies[i],
                    chain_id=chain_ids[i],
                )
                for i in range(BURST_SIZE)
            )
        )

        # Load-bearing signal 1: the watcher observed the breach and bumped
        # ram_pressure_signal_total on the admin wire.
        async def _signalled() -> bool:
            return await _ram_signal_total(stack) > baseline_signals

        await await_until(
            _signalled,
            timeout_seconds=MIGRATION_BUDGET_SECONDS,
            message=(
                "ram_pressure_signal_total never advanced under a RAM-ceiling breach; "
                "the RamPressureWatcher did not signal"
            ),
        )

        # Load-bearing signal 2: the persist controller migrated at least
        # one oldest body RAM->disk in response.
        async def _at_least_one_persisted() -> bool:
            rows, _ = await pc.list_uploads(limit=BURST_SIZE * 2)
            return any(row.body_location == "file" for row in rows)

        await await_until(
            _at_least_one_persisted,
            timeout_seconds=MIGRATION_BUDGET_SECONDS,
            message="no body migrated RAM->disk under RAM pressure",
        )

        # Lift the hold and let the chains drain so teardown is clean (no
        # rows left mid-flight, no leaked daemon work).
        emulator.clear_failures()
        await asyncio.gather(
            *(
                assert_chain_reaches_state(
                    pc,
                    cid,
                    state="succeeded",
                    timeout_seconds=PER_CHAIN_TERMINAL_BUDGET_SECONDS,
                )
                for cid in chain_ids
            )
        )
    finally:
        await stack.tear_down()
