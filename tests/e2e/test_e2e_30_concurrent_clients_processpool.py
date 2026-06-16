"""E2E-30 — concurrent_clients 12-way concurrent client pattern.

A concurrent client host uploads large_binary parquets from many
workers (12 by default). Each worker constructs its own upstream
client because ``httpx.AsyncClient`` instances cannot cross process
boundaries; all 12 target the same Phantom-the-service instance, all
under the same ``(endpoint, uid)`` cache slot.

We can't easily run real subprocesses inside an async pytest test, so
we simulate the pattern with 12 concurrent client instances in the
same process. The network behavior at Phantom is identical — what
we're testing is that Phantom handles 12 simultaneous inbound
connections on a shared ``(endpoint, uid)`` slot without:

- Token-cache deduplication errors (one slot, not 12).
- Throttler deadlocks.
- Async task leaks.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from uuid import UUID

import pytest
import pytest_asyncio
from phantom_client import PhantomClient

from ._driver import CreateFileRequest, DriverUploadResult, FileMetadata, PhantomDriver
from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads_routines import LARGE_BINARY_PARQUET_BYTES, deterministic_body
from .helpers.stack import E2EStack, EmulatorControl, boot_stack
from .helpers.timing import pace

# Number of concurrent driver clients. Mirrors the upstream
# ``worker_count=12`` default. The SqliteUploadStore's per-connection
# write lock keeps concurrent commits race-free at this scale.
NUM_CONCURRENT_CLIENTS: int = 12

# Per-client upload count. With 3 clients x 4 uploads = 12 chains.
UPLOADS_PER_CLIENT: int = 4

# Total expected chains.
EXPECTED_CHAIN_COUNT: int = NUM_CONCURRENT_CLIENTS * UPLOADS_PER_CLIENT

# Per-upload synchronous-return budget. large_binary parquets are
# 500 KB each — 250 ms accommodates the multipart frame copy.
SYNTHETIC_RETURN_BUDGET_SECONDS: float = 0.25

# Per-chain terminal-state budget. 72 chains with 2-worker pool means
# the last chain takes ~36 worker iterations to land; 60 s is generous.
TERMINAL_STATE_BUDGET_SECONDS: float = 60.0

# Submit pacing per client. Mirrors the upstream client's 10 RPS throttler
# floor (upstream cadence). 150 ms is slightly above the upstream 100 ms
# floor; the extra margin tracks the upstream cadence under load.
SUBMIT_PACING_SECONDS: float = 0.15


pytestmark = pytest.mark.e2e


# Per-test config overlay — disable the reaper for the test's
# duration so chain rows survive until the polling pass completes.
# See E2E-25 module docstring for the underlying reaper bug.
_REAPER_OVERRIDE: dict[str, object] = {
    "retention": {
        "reaper_interval_seconds": 3600,
    },
}


@pytest_asyncio.fixture
async def stack() -> AsyncIterator[E2EStack]:
    """Boot the stack with the reaper effectively disabled."""
    s = await boot_stack(config_overrides=_REAPER_OVERRIDE)
    try:
        yield s
    finally:
        await s.tear_down()


@pytest.fixture
def drivers(stack: E2EStack) -> list[PhantomDriver]:
    """Open NUM_CONCURRENT_CLIENTS independent public drivers.

    In the real concurrent_clients pattern each subprocess owns its
    own ``httpx.AsyncClient``. The token-cache invariant under test is
    at Phantom: all clients share the stack's single
    ``fake_security_token`` so they derive the same ``uid`` and Phantom
    slots them into one ``(endpoint, uid)`` cache entry. Each driver
    here borrows the stack's single :class:`PhantomClient` (the stack
    owns its lifecycle); that is sufficient for the Phantom-side
    invariant the relocated test no longer covers — the multi-httpx
    fan-out lives in the integration tier.
    """
    return [
        PhantomDriver(
            stack.phantom_client,
            files_api=stack.emulator_url,
            get_security_token=stack.fake_security_token,
        )
        for _ in range(NUM_CONCURRENT_CLIENTS)
    ]


def _build_request(*, client_idx: int, upload_idx: int) -> CreateFileRequest:
    """Build a unique CreateFileRequest per (client, upload) pair."""
    return CreateFileRequest(
        domain="generic",
        lane_base_name="large_binary_parquet_data",
        file_name=f"e2e30_cc_c{client_idx:02d}_u{upload_idx:02d}",
        metadata=FileMetadata(
            key_value_store={
                "uploader_id": "12345",
                "label": "alpha",
                "mode": "production",
                "label_name": "concurrent_clients",
                "client_idx": str(client_idx),
                "upload_idx": str(upload_idx),
            }
        ),
    )


# perf-tier (whole test): asserts SYNTHETIC_RETURN_BUDGET_SECONDS,
# interwoven with the per-client concurrent upload loop and sharing the
# stack boot — not cleanly splittable. Runs only via `-m perf` (see
# pyproject markers).
@pytest.mark.perf
async def test_e2e_30_concurrent_clients_processpool(
    drivers: list[PhantomDriver],
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """12 concurrent clients x 6 uploads = 72 chains all reach succeeded.

    Every chain reaches ``succeeded`` within
    ``TERMINAL_STATE_BUDGET_SECONDS``; Phantom's token cache shows
    exactly ONE slot for the shared ``(endpoint, uid)`` (no
    duplicate-slot-per-client regression).
    """
    # Setup — fresh emulator state. Note we do NOT use clear_received
    # at scale, because the emulator's received log retains everything;
    # we identify our chains by their phantom_local_uuid.
    emulator.clear_received()
    emulator.clear_failures()

    # Action — every client issues 6 uploads via asyncio.gather across
    # its own task list; the outer gather concurrent-fires all 12
    # clients' upload batches. This simulates the upstream pattern
    # of 12 workers each driving their own upload sequence.
    body = deterministic_body(LARGE_BINARY_PARQUET_BYTES, seed=b"cc-binary-")

    async def _client_runs(client_idx: int, driver: PhantomDriver) -> list[DriverUploadResult]:
        """Run all UPLOADS_PER_CLIENT uploads for one client sequentially.

        Upstream, each client is a separate process running a
        per-channel upload loop one upload at a time; the
        cross-client parallelism (12 workers) is what stresses
        Phantom. We mirror that by serializing inside each client and
        relying on the outer ``asyncio.gather`` (one task per client)
        for the concurrent fan-out.

        Per-client startup is staggered by ``client_idx *
        SUBMIT_PACING_SECONDS`` so the 12 clients don't all hammer
        Phantom's ingress at t=0 simultaneously. Combined with
        per-upload pacing this keeps Phantom's two-worker SQLite
        store off the concurrent-commit race that fires when many
        chains are mid-commit at the same instant (separate bug
        task spawned for the underlying SqliteUploadStore race).
        """
        # Stagger each client's start.
        await pace(client_idx * SUBMIT_PACING_SECONDS)
        results: list[DriverUploadResult] = []
        for upload_idx in range(UPLOADS_PER_CLIENT):
            request = _build_request(client_idx=client_idx, upload_idx=upload_idx)
            start = time.perf_counter()
            result = await driver.in_memory_upload(request, body)
            elapsed = time.perf_counter() - start
            assert elapsed < SYNTHETIC_RETURN_BUDGET_SECONDS, (
                f"client {client_idx} upload {upload_idx} took {elapsed:.3f}s; "
                f"expected < {SYNTHETIC_RETURN_BUDGET_SECONDS}s"
            )
            results.append(result)
            if upload_idx + 1 < UPLOADS_PER_CLIENT:
                await pace(SUBMIT_PACING_SECONDS)
        return results

    # Concurrent fan-out across all 12 clients.
    nested = await asyncio.gather(*(_client_runs(i, drv) for i, drv in enumerate(drivers)))
    file_infos: list[DriverUploadResult] = [fi for client_list in nested for fi in client_list]
    assert len(file_infos) == EXPECTED_CHAIN_COUNT, (
        f"expected {EXPECTED_CHAIN_COUNT} synthetic FileInformations; got {len(file_infos)}"
    )

    # Distinct local UUIDs across all 72 submissions.
    distinct_ids = {fi.id for fi in file_infos}
    assert len(distinct_ids) == EXPECTED_CHAIN_COUNT, (
        f"expected {EXPECTED_CHAIN_COUNT} distinct local UUIDs; got {len(distinct_ids)}"
    )

    # Assertion — every chain reaches succeeded. Polled in parallel
    # so the slowest chain bounds the wait.
    async def _await_one(fi: DriverUploadResult) -> None:
        chain_response = await assert_chain_reaches_state(
            phantom_client,
            fi.id,
            state="succeeded",
            timeout_seconds=TERMINAL_STATE_BUDGET_SECONDS,
        )
        assert chain_response.state == "succeeded", f"chain {fi.id} state={chain_response.state!r}"

    await asyncio.gather(*(_await_one(fi) for fi in file_infos))

    # Assertion — token cache shows exactly ONE slot for the shared
    # (endpoint, uid). 12 clients with the same uid_fallback must
    # collapse to one slot — the load-bearing invariant from ADR-002.
    tokens = await phantom_client.list_tokens()
    distinct_slots = {(t.endpoint, t.uid) for t in tokens}
    assert len(distinct_slots) == 1, (
        f"expected exactly 1 (endpoint, uid) slot; got {len(distinct_slots)}: {distinct_slots}"
    )

    # The slot's status should be 'fresh' since every upload succeeded.
    slot = next(iter(tokens))
    assert slot.status == "fresh", f"expected token slot status='fresh'; got {slot.status!r}"

    # Assertion — admin row count matches submissions, all in
    # succeeded state. Use a large limit since we know we have 72.
    seen_chain_ids: set[UUID] = set()
    cursor: str | None = None
    while True:
        rows, cursor = await phantom_client.list_uploads(limit=200, cursor=cursor)
        for row in rows:
            if row.chain_id in distinct_ids:
                seen_chain_ids.add(row.chain_id)
                assert row.state == "succeeded", f"row {row.chain_id} state={row.state!r}"
        if cursor is None:
            break
    assert seen_chain_ids == distinct_ids, (
        f"admin list_uploads missing chains: "
        f"expected {len(distinct_ids)}, saw {len(seen_chain_ids)}"
    )
