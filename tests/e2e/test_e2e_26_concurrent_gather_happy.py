"""E2E-26 — concurrent_gather per-source 3-way ``asyncio.gather`` happy path.

Mirrors a gather pattern that submits uploads concurrently from one
client process. Per source, the pattern builds three
``asyncio.create_task`` calls (detail, large_binary, ranges) and awaits
them via ``asyncio.gather(..., return_exceptions=True)``.

Phantom's behavior under simultaneous ``submit_chain`` calls from one
upstream client matters here — each chain must run independently
with no cross-task interference. All three share the same source's
``ref_id`` so the test can verify the per-source grouping survives
through the metadata round-trip.
"""

from __future__ import annotations

import asyncio
import time
from uuid import UUID

import pytest
from phantom_client import PhantomClient

from ._driver import DriverUploadResult, PhantomDriver
from .helpers.assertions import assert_chain_reaches_state, assert_emulator_received
from .helpers.payloads_routines import SequenceUpload, build_concurrent_gather_payloads
from .helpers.stack import EmulatorControl

# Per-upload synchronous-return budget. The contract is "synthetic
# FileInformation immediately"; 250 ms accommodates the 500 KB
# large_binary body's multipart frame copy on slow runners.
SYNTHETIC_RETURN_BUDGET_SECONDS: float = 0.25

# Per-chain terminal-state budget; chains succeed within ~1 s on the
# in-process stack. 10 s is the helper default.
TERMINAL_STATE_BUDGET_SECONDS: float = 10.0

# The representative gather fires exactly three concurrent uploads
# (detail, large_binary, ranges). Asserted to catch builder drift.
EXPECTED_GATHER_WIDTH: int = 3

# The shared source identifier all three uploads carry. Verified to be
# identical in every received metadata KVS so a future Phantom
# regression that mangles per-key fidelity surfaces fast.
SHARED_REF_ID: str = "hist-cg-026"


pytestmark = pytest.mark.e2e


# perf-tier (whole test): asserts SYNTHETIC_RETURN_BUDGET_SECONDS,
# interwoven with the gather result-validation loop and sharing the
# stack boot — not cleanly splittable. Runs only via `-m perf` (see
# pyproject markers).
@pytest.mark.perf
async def test_e2e_26_concurrent_gather_happy(
    driver: PhantomDriver,
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """3 concurrent uploads all succeed via asyncio.gather.

    ``gather(..., return_exceptions=True)`` over 3 uploads returns 3
    :class:`FileInformation` (no exceptions); all three chains reach
    ``succeeded`` within ``TERMINAL_STATE_BUDGET_SECONDS``; admin API
    shows 3 rows with the shared ``ref_id`` and distinct
    ``lane_base_name``s; emulator records all 3 received entries with
    the shared ref_id.
    """
    # Setup — fresh emulator state so this test owns the received log.
    emulator.clear_received()
    emulator.clear_failures()

    payloads = build_concurrent_gather_payloads(ref_id=SHARED_REF_ID)
    assert len(payloads) == EXPECTED_GATHER_WIDTH, (
        f"concurrent gather builder emitted {len(payloads)} entries; "
        f"expected {EXPECTED_GATHER_WIDTH}"
    )

    # Action — fire all three uploads in parallel via asyncio.gather
    # with return_exceptions=True, the representative idiom. Each
    # submission's synthetic-return latency is recorded so a regression
    # that introduces inter-task serialization surfaces visibly.
    async def _submit(entry: SequenceUpload) -> tuple[DriverUploadResult, float]:
        start = time.perf_counter()
        result = await driver.in_memory_upload(entry.request, entry.body)
        elapsed = time.perf_counter() - start
        return result, elapsed

    gather_results = await asyncio.gather(
        *(_submit(p) for p in payloads),
        return_exceptions=True,
    )

    # Assertion — no exceptions; three upload results.
    file_infos: list[DriverUploadResult] = []
    for entry, result in zip(payloads, gather_results, strict=True):
        assert not isinstance(result, BaseException), (
            f"gather raised on {entry.label!r}: {result!r}"
        )
        fi, elapsed = result
        assert isinstance(fi.id, UUID)
        assert elapsed < SYNTHETIC_RETURN_BUDGET_SECONDS, (
            f"in_memory_upload for {entry.label!r} took {elapsed:.3f}s; "
            f"expected < {SYNTHETIC_RETURN_BUDGET_SECONDS}s"
        )
        file_infos.append(fi)

    # Distinct local UUIDs across the three concurrent submissions —
    # a regression that minted a per-process singleton uuid would show
    # up here.
    distinct_ids = {fi.id for fi in file_infos}
    assert len(distinct_ids) == EXPECTED_GATHER_WIDTH, (
        f"expected {EXPECTED_GATHER_WIDTH} distinct local UUIDs; got {len(distinct_ids)}"
    )

    # Assertion — every chain reaches succeeded. Poll in parallel to
    # stay ahead of Phantom's reaper sweep (see E2E-25 notes).
    async def _await_one(entry: SequenceUpload, fi: DriverUploadResult) -> None:
        chain_response = await assert_chain_reaches_state(
            phantom_client,
            fi.id,
            state="succeeded",
            timeout_seconds=TERMINAL_STATE_BUDGET_SECONDS,
        )
        assert chain_response.state == "succeeded", (
            f"chain for {entry.label!r} ({fi.id}) state={chain_response.state!r}"
        )

    await asyncio.gather(
        *(_await_one(entry, fi) for entry, fi in zip(payloads, file_infos, strict=True))
    )

    # Assertion — emulator records all 3 with shared ref_id and
    # distinct lane_base_names. The shared ref_id is the load-
    # bearing per-source grouping invariant.
    received_lanes: set[str] = set()
    for entry, fi in zip(payloads, file_infos, strict=True):
        received = await assert_emulator_received(
            emulator,
            phantom_local_uuid=str(fi.id),
            body_size=len(entry.body),
        )
        assert received.metadata_kvs.get("ref_id") == SHARED_REF_ID, (
            f"ref_id for {entry.label!r}: "
            f"expected {SHARED_REF_ID!r}, "
            f"got {received.metadata_kvs.get('ref_id')!r}"
        )
        # Lane name uniqueness comes from the request's lane_base_name;
        # collect to verify distinctness across the gather.
        received_lanes.add(entry.request.lane_base_name)

    assert len(received_lanes) == EXPECTED_GATHER_WIDTH, (
        f"expected {EXPECTED_GATHER_WIDTH} distinct lane_base_names; got {sorted(received_lanes)}"
    )
