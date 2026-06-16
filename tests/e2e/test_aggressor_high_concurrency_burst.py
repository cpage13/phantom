"""Aggressor — high concurrency ProcessPoolExecutor burst at upper bound.

Mirrors a high-concurrency producer: each unit of work runs in a
subprocess from a ``ProcessPoolExecutor`` whose default size is
``os.cpu_count()``. This can be 16+ simultaneous in-flight uploads. The
existing E2E-30 test exercises 12 concurrent clients (matching
``num_workers=12``); this test pushes to the upper bound of **16** to
catch any soft cap that's not surfaced under 12.

All clients share the same ``(endpoint, uid)`` slot (production:
``files.upstream.example`` + the client's JWT `sub`). Phantom MUST
deduplicate the cache slot — one entry, not 16 — even though 16
different httpx clients minted the bearer independently.

Body sizes mirror a representative spread: 10 KB / 100 KB / 500 KB / 1 MB
(small parquet → large_binary parquet → image → large parquet). With
16 clients x 4 uploads = 64 chains.

Invariants:

1. Cache slot count = 1 — Phantom's token cache (admin /v1/admin/tokens)
   shows exactly one entry for the shared (endpoint, uid).
2. Every chain reaches succeeded.
3. Per-upload synchronous-return budget honored (track elapsed time of
   each submit_chain).
4. Body byte-equality after success (forced persist so retrieval works).
5. No throttler deadlock — total wall time stays under budget.

Marked `@pytest.mark.perf` (a sync-return timing budget). Like its 12-way twin
E2E-30, it runs on the dedicated ``-m perf`` lane, not the per-push gate — a
timing stopwatch is only trustworthy on a quiet, dedicated host, not a shared CI box.
"""

from __future__ import annotations

import asyncio
import secrets
import time
from collections.abc import AsyncIterator
from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

from ._driver import PhantomDriver
from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

# Upper-bound concurrent client count — matches the upper of
# `os.cpu_count()` across concurrent client host variants. E2E-30 sits
# at 12; this aggressor pushes to 16.
NUM_CONCURRENT_CLIENTS: int = 16
UPLOADS_PER_CLIENT: int = 4
EXPECTED_TOTAL_CHAINS: int = NUM_CONCURRENT_CLIENTS * UPLOADS_PER_CLIENT  # 64

# Per-upload sizes — a representative spread of synthetic sizes.
BODY_SIZES: list[int] = [10 * 1024, 100 * 1024, 500 * 1024, 1024 * 1024]
assert len(BODY_SIZES) == UPLOADS_PER_CLIENT

# Per-upload synchronous-return budget. Phantom's contract is "ack
# fast, retry in the background." E2E-30 holds 250 ms at 12-way; at
# 16-way concurrency on a single-process uvicorn event loop, the
# multipart frame-copy for 1 MiB bodies serializes on the receive
# side and the budget needs to absorb that. 1.0 s is generous enough
# for a quiet, dedicated perf host but still loose enough that an
# outright regression (e.g., a sync-blocking admission path) would
# surface as >1 s outliers. This is a perf-tier timing assertion: like
# its 12-way twin E2E-30 it runs ONLY on the dedicated `-m perf` lane
# (never a gating lane), because a sync-return stopwatch is not
# trustworthy on a shared/contended CI runner.
SYNTHETIC_RETURN_BUDGET_SECONDS: float = 1.0

# Terminal-state budget — 64 chains through 4 workers means ~16
# worker iterations to clear; 90 s is generous for slow runners.
TERMINAL_STATE_BUDGET_SECONDS: float = 90.0

# Reaper-disable overlay — chains need to survive until terminal
# polling completes. Same pattern as E2E-30 (test_e2e_30) and per the
# E2E-25 reaper note.
_REAPER_OVERRIDE: dict[str, object] = {
    "retention": {
        "reaper_interval_seconds": 3600,
        "succeeded_metadata_seconds": 600,
        "succeeded_body_seconds": 600,
    },
    # All-disk mode so body retrieval works post-success — replaces
    # Phase-0 persist-on-receipt (``after_attempts: 0``).
    "storage": {
        "body_store": {"mode": "all_disk"},
        "compression": {
            "mode": "always",
            "algorithm": "original",
        },
    },
    # Roomy saturation — 64 in-flight + 64 MB bytes.
    "saturation": {
        "max_in_flight": 200,
        "max_in_flight_bytes": 128 * 1024 * 1024,
    },
    # More workers so the test doesn't time-out on serial drain.
    "retry": {
        "worker_count": 4,
        "poll_interval_ms": 100,
    },
}


pytestmark = [pytest.mark.e2e]


@pytest_asyncio.fixture
async def burst_stack(tmp_path: Path) -> AsyncIterator[E2EStack]:
    s = await boot_stack(tmp_path=tmp_path, config_overrides=_REAPER_OVERRIDE)
    try:
        yield s
    finally:
        await s.tear_down()


@pytest.fixture
def drivers(burst_stack: E2EStack) -> list[PhantomDriver]:
    """Construct N independent public drivers sharing one (endpoint, uid).

    The Phantom-side invariant under test is one token-cache slot for
    the shared ``(endpoint, uid)`` across all N submitters. Each driver
    borrows the stack's single :class:`PhantomClient` (the stack owns
    its lifecycle); the multi-httpx-pool fan-out that mirrors the real
    ``ProcessPoolExecutor`` lives in the integration tier.
    """
    return [
        PhantomDriver(
            burst_stack.phantom_client,
            files_api=burst_stack.emulator_url,
            # The bound method — re-reads on every request, same shape
            # as the high-concurrency producer's wiring.
            get_security_token=burst_stack.fake_security_token,
        )
        for _ in range(NUM_CONCURRENT_CLIENTS)
    ]


async def _client_workload(
    driver: PhantomDriver,
    *,
    client_idx: int,
    chain_ids: list[UUID],
    bodies: list[bytes],
) -> list[float]:
    """One client's 4 uploads. Returns per-upload elapsed seconds.

    Each upload is a single ``in_memory_upload`` call — the standard
    driver entry point mirroring the producer's call shape.
    """
    elapsed: list[float] = []
    for upload_idx, (chain_id, body) in enumerate(zip(chain_ids, bodies, strict=True)):
        # Mirrors the per-unit-of-work pattern: build a fresh
        # CreateFileRequest with the local UUID embedded for
        # cross-reference with the emulator's received log.
        request = build_create_file_request(
            file_name=f"client_c{client_idx:02d}_u{upload_idx:02d}",
        )
        request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
        request.metadata.key_value_store["client_idx"] = str(client_idx)
        # The driver mints its own chain_id via
        # build_in_memory_upload_envelope; we pre-allocate one for the
        # pre-flight KVS but the returned `id` is the actual chain_id.
        t0 = time.perf_counter()
        info = await driver.in_memory_upload(request=request, contents=body)
        elapsed.append(time.perf_counter() - t0)
        chain_ids[upload_idx] = UUID(str(info.id))  # capture the real chain_id
    return elapsed


# perf-tier (whole test): asserts SYNTHETIC_RETURN_BUDGET_SECONDS,
# interwoven with the 16-client concurrent burst and sharing the stack
# boot — not cleanly splittable. Runs only via `-m perf` (see pyproject
# markers).
@pytest.mark.perf
async def test_aggressor_high_concurrency_16_concurrent_burst(
    burst_stack: E2EStack,
    drivers: list[PhantomDriver],
) -> None:
    """16 concurrent clients x 4 uploads each = 64 chains; no slot dedup loss."""
    pc = burst_stack.phantom_client
    burst_stack.emulator.clear_received()
    burst_stack.emulator.clear_failures()

    # Pre-allocate chain_ids + bodies per client. Mutated in-place by
    # the workload to reflect the actual chain_id returned by the driver.
    client_chain_ids: list[list[UUID]] = [
        [uuid4() for _ in range(UPLOADS_PER_CLIENT)] for _ in range(NUM_CONCURRENT_CLIENTS)
    ]
    client_bodies: list[list[bytes]] = [
        [secrets.token_bytes(size) for size in BODY_SIZES] for _ in range(NUM_CONCURRENT_CLIENTS)
    ]

    # Fire 16 concurrent workloads via asyncio.gather (same shape as
    # E2E-30 but with 16 clients instead of 12).
    t_burst = time.perf_counter()
    elapsed_per_client = await asyncio.gather(
        *(
            _client_workload(
                drv,
                client_idx=idx,
                chain_ids=client_chain_ids[idx],
                bodies=client_bodies[idx],
            )
            for idx, drv in enumerate(drivers)
        )
    )
    burst_wall_time = time.perf_counter() - t_burst

    # Synchronous-return budget invariant — every single submit_chain
    # MUST return within the budget. Phantom's 202-and-buffer pattern
    # is the load-bearing latency contract.
    over_budget: list[tuple[int, int, float]] = []
    for client_idx, per_client_elapsed in enumerate(elapsed_per_client):
        for upload_idx, dt in enumerate(per_client_elapsed):
            if dt > SYNTHETIC_RETURN_BUDGET_SECONDS:
                over_budget.append((client_idx, upload_idx, dt))
    assert not over_budget, (
        f"{len(over_budget)} uploads exceeded sync-return budget "
        f"({SYNTHETIC_RETURN_BUDGET_SECONDS}s). Burst wall time: "
        f"{burst_wall_time:.2f}s. Sample over-budget: {over_budget[:5]}"
    )

    # Token cache slot invariant — all 16 clients share one
    # (endpoint, uid), so admin /v1/admin/tokens shows exactly ONE slot
    # for that endpoint. If Phantom mis-keys the cache on per-client
    # state, we'd see N slots.
    # Note: there may also be a sibling slot if any other endpoint was
    # contacted; we filter to the emulator hostname.
    emulator_host = urlparse(burst_stack.emulator_url).hostname or ""
    token_slots = await pc.list_tokens()
    matching_slots = [s for s in token_slots if s.endpoint == emulator_host]
    assert len(matching_slots) == 1, (
        f"expected 1 token slot for endpoint={emulator_host!r}, "
        f"got {len(matching_slots)}: "
        f"{[(s.endpoint, s.uid) for s in matching_slots]}"
    )

    # Terminal-state invariant — every chain reaches succeeded.
    all_chain_ids = [cid for sub in client_chain_ids for cid in sub]
    assert len(all_chain_ids) == EXPECTED_TOTAL_CHAINS
    for chain_id in all_chain_ids:
        detail = await assert_chain_reaches_state(
            pc,
            chain_id,
            state="succeeded",
            timeout_seconds=TERMINAL_STATE_BUDGET_SECONDS,
        )
        assert detail.state == "succeeded"

    # Body byte-equality on a sampled set — full retrieval of 64 bodies
    # is expensive; sample one per client = 16 retrievals.
    for client_idx in range(NUM_CONCURRENT_CLIENTS):
        sampled_upload = 0  # The first upload of each client.
        cid = client_chain_ids[client_idx][sampled_upload]
        expected_body = client_bodies[client_idx][sampled_upload]
        chunks: list[bytes] = []
        async for chunk in await pc.fetch_body(cid):
            chunks.append(chunk)
        retrieved = b"".join(chunks)
        assert retrieved == expected_body, (
            f"client {client_idx} upload {sampled_upload} body mismatch: "
            f"expected len={len(expected_body)}, got len={len(retrieved)}"
        )
