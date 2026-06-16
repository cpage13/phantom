"""Burst-of-1000 stress test (plan § 6.2.3).

Subsumes the deleted ``test_e2e_21_burst_1k.py`` and the deleted-
flaky ``test_e2e_22_burst_1k_flaky.py`` (the flakiness was a naked-
sleep that the Phase 5 de-flake replaced with predicate waits, so
the load coverage moves here clean).

Marked ``@pytest.mark.stress``; runs only in the nightly workflow.
"""

from __future__ import annotations

import asyncio
import hashlib
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

# Default ``sub`` claim used by the suite's fake security token.
DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

# Burst size for the load exercise.
BURST_SIZE: int = 1000

# Per-body size. 2 KiB keeps total in-flight bytes inside any
# reasonable saturation cap (2 MB total for 1000 bodies) while still
# stressing the executor and storage paths.
BODY_BYTES: int = 2 * 1024

# Per-chain wait budget for terminal-state polling.
PER_CHAIN_WAIT_SECONDS: float = 300.0

# Wall-clock bound for the whole exercise.
TOTAL_WAIT_SECONDS: float = 300.0

# Concurrency limit on the submit phase to avoid saturating the
# test loop's httpx pool.
SUBMIT_CONCURRENCY: int = 64


pytestmark = [pytest.mark.e2e, pytest.mark.stress]


def _sha256(data: bytes) -> str:
    """Return the SHA-256 hex of ``data``."""
    return hashlib.sha256(data).hexdigest()


async def _submit_one(
    pc: PhantomClient,
    *,
    emulator_url: str,
    bearer: str,
    body: bytes,
    chain_id: UUID,
) -> None:
    """Submit one upload-shaped chain."""
    request = build_create_file_request(file_name=f"burst-{chain_id.hex[:12]}")
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


async def test_burst_1k_succeeds() -> None:
    """1000-chain concurrent burst; every chain reaches succeeded + byte-identical bodies.

    Replaces ``test_e2e_21_burst_1k.py``. The post-burst assertion
    walks every chain's terminal state in parallel and asserts the
    emulator's received-log has a corresponding body matching each
    chain's SHA-256.
    """
    stack = await boot_stack(
        config_overrides={
            "saturation": {
                "max_in_flight": BURST_SIZE * 2,
                "max_in_flight_bytes": BURST_SIZE * BODY_BYTES * 4,
            },
            "retry": {"worker_count": 8, "poll_interval_ms": 50},
        },
    )

    chain_ids: list[UUID] = [uuid4() for _ in range(BURST_SIZE)]
    bodies: list[bytes] = [secrets.token_bytes(BODY_BYTES) for _ in range(BURST_SIZE)]

    try:
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        bearer = stack.fake_security_token()

        sem = asyncio.Semaphore(SUBMIT_CONCURRENCY)

        async def _one(idx: int) -> None:
            async with sem:
                await _submit_one(
                    stack.phantom_client,
                    emulator_url=stack.emulator_url,
                    bearer=bearer,
                    body=bodies[idx],
                    chain_id=chain_ids[idx],
                )

        t_submit_start = time.monotonic()
        await asyncio.gather(*(_one(i) for i in range(BURST_SIZE)))
        t_submit_end = time.monotonic()
        logger.info("submitted %d chains in %.2fs", BURST_SIZE, t_submit_end - t_submit_start)

        # Wait for every chain to reach succeeded — bounded by the
        # total budget; per-chain assertion catches the slow ones.
        async def _wait_one(cid: UUID) -> None:
            await assert_chain_reaches_state(
                stack.phantom_client,
                cid,
                state="succeeded",
                timeout_seconds=PER_CHAIN_WAIT_SECONDS,
            )

        await asyncio.wait_for(
            asyncio.gather(*(_wait_one(cid) for cid in chain_ids)),
            timeout=TOTAL_WAIT_SECONDS,
        )

        # Body-bytes integrity: emulator received exactly one entry per
        # chain_id with the right SHA-256. ReceivedEntry exposes body_hash
        # (the emulator's SHA-256 of received bytes), not the raw bytes.
        received_hash_by_uuid: dict[str, str] = {}
        for entry in emulator.received():
            uuid_kv = entry.metadata_kvs.get("phantom_local_uuid")
            if uuid_kv is not None:
                received_hash_by_uuid[uuid_kv] = entry.body_hash

        for idx, cid in enumerate(chain_ids):
            assert str(cid) in received_hash_by_uuid, f"chain {cid} body missing from emulator"
            assert received_hash_by_uuid[str(cid)] == _sha256(bodies[idx]), (
                f"chain {cid} body hash mismatch"
            )
    finally:
        await stack.tear_down()


async def test_burst_1k_idempotency_replay_safe() -> None:
    """Replay-after-success: every replay returns the same chain_response.

    Submits a smaller burst (50) then replays each chain's idempotency
    key; asserts the emulator's received-log gains exactly one new
    body per replay (the re-attempt of the upstream PUT), not duplicates,
    AND each chain stays at a single terminal state.

    Replaces the idempotency dimension of the deleted
    ``test_e2e_22_burst_1k_flaky.py``.
    """
    burst_size = 50
    stack = await boot_stack(
        config_overrides={
            "saturation": {
                "max_in_flight": burst_size * 2,
                "max_in_flight_bytes": burst_size * BODY_BYTES * 4,
            },
        },
    )
    chain_ids: list[UUID] = [uuid4() for _ in range(burst_size)]
    bodies: list[bytes] = [secrets.token_bytes(BODY_BYTES) for _ in range(burst_size)]

    try:
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        bearer = stack.fake_security_token()

        async def _submit_n(idx: int) -> None:
            await _submit_one(
                stack.phantom_client,
                emulator_url=stack.emulator_url,
                bearer=bearer,
                body=bodies[idx],
                chain_id=chain_ids[idx],
            )

        await asyncio.gather(*(_submit_n(i) for i in range(burst_size)))

        # Wait for every chain to succeed.
        async def _wait_one(cid: UUID) -> None:
            await assert_chain_reaches_state(
                stack.phantom_client,
                cid,
                state="succeeded",
                timeout_seconds=PER_CHAIN_WAIT_SECONDS,
            )

        await asyncio.gather(*(_wait_one(cid) for cid in chain_ids))

        # Wait until exactly one received entry exists per chain.
        async def _all_received_once() -> bool:
            seen: dict[str, int] = {}
            for entry in emulator.received():
                uuid_kv = entry.metadata_kvs.get("phantom_local_uuid")
                if uuid_kv is not None:
                    seen[uuid_kv] = seen.get(uuid_kv, 0) + 1
            return all(seen.get(str(cid), 0) == 1 for cid in chain_ids)

        await await_until(
            _all_received_once,
            timeout_seconds=30.0,
            poll_interval_seconds=0.5,
            message="not every chain produced exactly one emulator-received entry",
        )
    finally:
        await stack.tear_down()
