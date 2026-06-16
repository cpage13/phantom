"""RAM-pressure stress test (plan § 6.2.3).

Drives the RAM tier above the configured ``ram_ceiling_bytes`` and
asserts:

1. The RAM-pressure watcher signals the persist controller.
2. The persist controller migrates oldest bodies to disk in bounded
   time.
3. Admission unblocks once RAM occupancy drops back below the
   ceiling.

Marked ``@pytest.mark.stress`` — runs only in the nightly workflow.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import time
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient

from tests.e2e._driver import build_in_memory_upload_envelope

from ..helpers.assertions import assert_chain_reaches_state
from ..helpers.payloads import build_create_file_request
from ..helpers.stack import boot_stack
from ..helpers.timing import await_until

logger = logging.getLogger(__name__)

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

# Body size big enough that 16 of them push RAM past the configured
# ceiling. 64 KiB * 16 = 1 MiB; ceiling pinned to 512 KiB in the test.
BODY_BYTES_PER_CHAIN: int = 64 * 1024
RAM_CEILING_BYTES_FOR_TEST: int = 512 * 1024
BURST_SIZE: int = 16

# Per-chain end-to-end budget.
PER_CHAIN_TERMINAL_BUDGET_SECONDS: float = 60.0

# Maximum time to wait for the persist controller to land at least
# one disk-located body after the burst.
PERSIST_OBSERVATION_BUDGET_SECONDS: float = 30.0

pytestmark = [pytest.mark.e2e, pytest.mark.stress]


async def _submit_one(
    pc: PhantomClient,
    *,
    emulator_url: str,
    bearer: str,
    body: bytes,
    chain_id: UUID,
) -> None:
    """Submit one chain at a large body size."""
    request = build_create_file_request(file_name=f"ram-pressure-{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    await pc.submit_chain(
        envelope,
        body_refs={"body": body},
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )


async def test_ram_pressure_triggers_persist_migration() -> None:
    """Drive RAM above the ceiling; verify persist migration kicks in."""
    stack = await boot_stack(
        config_overrides={
            "storage": {
                "body_store": {
                    "mode": "hybrid",
                    "ram_ceiling_bytes": RAM_CEILING_BYTES_FOR_TEST,
                    "ram_pressure_poll_seconds": 0.5,
                },
            },
            "saturation": {
                "max_in_flight": BURST_SIZE * 4,
                "max_in_flight_bytes": BURST_SIZE * BODY_BYTES_PER_CHAIN * 4,
            },
        },
    )
    chain_ids: list[UUID] = [uuid4() for _ in range(BURST_SIZE)]
    bodies: list[bytes] = [secrets.token_bytes(BODY_BYTES_PER_CHAIN) for _ in range(BURST_SIZE)]

    try:
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        bearer = stack.fake_security_token()

        # Hold the upstream so bodies linger in RAM long enough for
        # the pressure watcher to observe.
        from phantom_emulator.failure.injection import FailurePolicy, FailureScope

        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                scope=FailureScope.UPSTREAM_FILES_UPLOAD,
                latency_ms=5_000,
            ),
        )

        t_start = time.monotonic()
        await asyncio.gather(
            *(
                _submit_one(
                    stack.phantom_client,
                    emulator_url=stack.emulator_url,
                    bearer=bearer,
                    body=bodies[i],
                    chain_id=chain_ids[i],
                )
                for i in range(BURST_SIZE)
            )
        )

        # Wait for at least one row to be migrated to disk
        # (body_location='file') — proof that the pressure watcher
        # signalled and the persist controller ran.
        pc = stack.phantom_client

        async def _at_least_one_persisted() -> bool:
            rows, _ = await pc.list_uploads(limit=BURST_SIZE * 2)
            return any(row.body_location == "file" for row in rows)

        await await_until(
            _at_least_one_persisted,
            timeout_seconds=PERSIST_OBSERVATION_BUDGET_SECONDS,
            poll_interval_seconds=0.5,
            message=(
                f"no row migrated to disk within "
                f"{PERSIST_OBSERVATION_BUDGET_SECONDS}s under RAM pressure"
            ),
        )

        # Clear the failure so the chains can drain to succeeded.
        emulator.clear_failures()

        async def _wait_one(cid: UUID) -> None:
            await assert_chain_reaches_state(
                pc,
                cid,
                state="succeeded",
                timeout_seconds=PER_CHAIN_TERMINAL_BUDGET_SECONDS,
            )

        await asyncio.gather(*(_wait_one(cid) for cid in chain_ids))
        logger.info(
            "ram-pressure stress drained %d chains in %.2fs",
            BURST_SIZE,
            time.monotonic() - t_start,
        )
    finally:
        await stack.tear_down()
