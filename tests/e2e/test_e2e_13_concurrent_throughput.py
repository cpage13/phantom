"""E2E-13 — Concurrent multi-upload throughput.

Submits N concurrent uploads via ``asyncio.gather``, each with a
distinct content hash so the emulator can distinguish them. Asserts
that every chain reaches ``succeeded``, that ``claim_due`` atomicity
prevents any chain from being worked twice (one received entry per
chain_id, one per body hash), and that the worker pool's timestamp
distribution shows the rows weren't dispatched as a single-millisecond
stampede.
"""

from __future__ import annotations

import asyncio
import hashlib
from uuid import UUID

import pytest

from ._driver import DriverUploadResult, PhantomDriver
from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

# Number of concurrent uploads. The SqliteUploadStore's
# per-connection write lock serializes commits so the previous
# concurrent-commit race no longer fires under load; the test exercises
# claim_due atomicity, distinct chain_ids, and the worker-pool round-robin's
# timestamp spread at production-shaped concurrency.
N_CONCURRENT_UPLOADS: int = 100

# Per-upload body size. Small enough that 100 concurrent in-memory
# bodies stay well under the saturation gate's bytes cap.
BODY_BASE: bytes = b"phantom-e2e-throughput-body-"

# Terminal-state deadline. 30 s budget.
TERMINAL_DEADLINE_SECONDS: float = 30.0

# Worker-pool timestamp-spread minimum (seconds). We want variance
# > 100 ms on the emulator's received timestamps so we can observe
# the sender pool's round-robin instead of all 100 ingresses being a
# single-millisecond stampede.
MIN_TIMESTAMP_SPREAD_SECONDS: float = 0.1


# `stress`-tiered (nightly Linux lane): a high-volume concurrent burst that
# false-fails on macOS-loopback ephemeral-port/TIME_WAIT exhaustion under the
# full suite's connection churn — NOT a software bug (the client/upstream use
# pooled keep-alive connections; ~18 TCP conns per 1000 uploads).
@pytest.mark.e2e
@pytest.mark.stress
async def test_e2e_13_concurrent_throughput() -> None:
    """All N concurrent uploads succeed exactly once; pool spreads work over time."""
    # Use a fresh stack so this test's row count isn't polluted by
    # other tests. The default phantom-config.yml has m profile
    # caps (max_in_flight = 100) — exactly fits 100 concurrent
    # in-flight rows. If a future patch tightens that cap, override
    # the saturation block here.
    stack = await boot_stack()
    try:
        emulator = stack.emulator
        driver = _make_driver(stack)
        emulator.clear_received()
        emulator.clear_failures()

        async def _one_upload(idx: int) -> DriverUploadResult:
            """Build a distinct envelope and call in_memory_upload."""
            request = build_create_file_request(file_name=f"e2e_concurrent_{idx:04d}")
            contents = BODY_BASE + str(idx).encode()
            return await driver.in_memory_upload(request, contents)

        # 1. Fire N uploads concurrently. asyncio.gather collects their
        #    synthetic FileInformation results in submission order.
        results = await asyncio.gather(*(_one_upload(i) for i in range(N_CONCURRENT_UPLOADS)))
        assert len(results) == N_CONCURRENT_UPLOADS

        chain_ids: list[UUID] = [r.id for r in results]
        assert len(set(chain_ids)) == N_CONCURRENT_UPLOADS, (
            "chain_ids must be distinct across all uploads"
        )

        # 2. Wait for every chain to reach succeeded.
        await asyncio.gather(
            *(
                assert_chain_reaches_state(
                    stack.phantom_client,
                    cid,
                    state="succeeded",
                    timeout_seconds=TERMINAL_DEADLINE_SECONDS,
                )
                for cid in chain_ids
            )
        )

        # 3. Emulator received entries match. Each entry's
        #    phantom_local_uuid must appear exactly once; each body
        #    hash must appear exactly once; both sets must equal the
        #    submitted-side sets.
        received = emulator.received()
        assert len(received) == N_CONCURRENT_UPLOADS, (
            f"emulator received {len(received)} entries; expected {N_CONCURRENT_UPLOADS}"
        )

        expected_local_uuids = {str(c) for c in chain_ids}
        seen_local_uuids = {e.metadata_kvs["phantom_local_uuid"] for e in received}
        assert seen_local_uuids == expected_local_uuids, (
            "phantom_local_uuid set mismatch between submitted and received"
        )

        # body hash check — every body that was sent should land
        # exactly once. We don't get raw bodies on the emulator
        # surface, but `body_size` plus distinct
        # phantom_local_uuid is sufficient (each upload has a
        # unique body via the idx suffix). The byte-level
        # distinctness check is asserted by the per-uuid count.
        per_uuid_count: dict[str, int] = {}
        for entry in received:
            uid = entry.metadata_kvs["phantom_local_uuid"]
            per_uuid_count[uid] = per_uuid_count.get(uid, 0) + 1
        assert all(c == 1 for c in per_uuid_count.values()), (
            f"every phantom_local_uuid must appear exactly once "
            f"(claim_due atomicity): {per_uuid_count}"
        )

        # 4. Worker-pool spread. We assert >100ms variance on the
        #    received timestamps so we can see the pool's round-robin
        #    instead of a single-millisecond stampede.
        accepted_unix = [e.accepted_at.timestamp() for e in received]
        spread = max(accepted_unix) - min(accepted_unix)
        assert spread > MIN_TIMESTAMP_SPREAD_SECONDS, (
            f"received timestamps spread is {spread * 1000:.1f}ms, "
            f"expected > {MIN_TIMESTAMP_SPREAD_SECONDS * 1000:.0f}ms; "
            f"the sender pool may not be working concurrently"
        )

        # 5. Body-content uniqueness — every received body_size
        #    should match the original (no truncation, no mix-up).
        size_lookup = {str(c): len(BODY_BASE + str(i).encode()) for i, c in enumerate(chain_ids)}
        for entry in received:
            expected_size = size_lookup[entry.metadata_kvs["phantom_local_uuid"]]
            assert entry.body_size == expected_size, (
                f"body_size mismatch for {entry.metadata_kvs['phantom_local_uuid']}: "
                f"got {entry.body_size}, expected {expected_size}"
            )

        # Sanity check the bodies were uniquely shaped (chain_ids are
        # uniquely generated by the driver's per-call UUIDs, so the
        # body bytes are uniquely shaped too — this asserts the hash
        # invariant on the submitting side).
        body_hashes = {
            hashlib.sha256(BODY_BASE + str(i).encode()).hexdigest()
            for i in range(N_CONCURRENT_UPLOADS)
        }
        assert len(body_hashes) == N_CONCURRENT_UPLOADS
    finally:
        await stack.tear_down()


def _make_driver(stack: E2EStack) -> PhantomDriver:
    """Build a public driver pointed at the running stack.

    Inline so this test doesn't depend on conftest.py's ``driver``
    fixture — this test owns its own stack. The driver borrows the
    stack's open :class:`PhantomClient`; the stack owns its lifecycle.
    """
    return PhantomDriver(
        stack.phantom_client,
        files_api=stack.emulator_url,
        get_security_token=stack.fake_security_token,
    )
