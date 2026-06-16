"""E2E-28 — long_sequence sequential 30-upload sequence.

The largest sequential upload sequence Phantom handles. The builder
emits a long chain of uploads: per-cycle entries plus a per-cycle
2-parquet large_binary pair and a per-cycle JSON. With
``num_cycles=5``, the builder emits 43 sequential uploads totalling
~5.4 MiB.

The upstream client's 10 RPS throttler paces real callers at 100 ms
per call. The driver overrides ``in_memory_upload`` so the original
throttler is bypassed — Phantom's ingress sees uploads as fast as the
test can issue them. To stay faithful to upstream cadence (and to give
Phantom's two sender workers time to commit each row's SQLite
transactions before the next ingress hits), the test paces
submissions at 100 ms apart.

The total resolves to 5x history + 5x summary + 1 primary_result + 5x
+ 5x + 5x per-cycle + 10 cycle large_binary + 1 HTML + 1 batch + 5
JSON = 43. The test asserts the exact count.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from uuid import UUID

import pytest
import pytest_asyncio
from phantom_client import PhantomClient

from ._driver import DriverUploadResult, PhantomDriver
from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads_routines import SequenceUpload, build_long_sequence
from .helpers.stack import E2EStack, EmulatorControl, boot_stack
from .helpers.timing import pace

# Per-upload synchronous-return budget. Body sizes range from 1.3 KB
# (summary) to 500 KB (large_binary, HTML); 250 ms accommodates the
# larger entries.
SYNTHETIC_RETURN_BUDGET_SECONDS: float = 0.25

# Per-chain terminal-state budget. Chains succeed within ~1-2 s on
# the in-process stack; 10 s is the helper default and gives slow
# runners headroom while staying well under the reaper-sweep race.
TERMINAL_STATE_BUDGET_SECONDS: float = 10.0

# Cycle count for the sequence. 5 gives the documented
# bytes-and-shape ratios.
NUM_CYCLES: int = 5

# Expected total uploads — 9 + 4N + N (history/summary plus three
# more per-cycle entries, plus 2 large_binary parquets and 1 JSON
# per cycle, plus 1 primary + 1 HTML + 1 batch). With N=5 that
# resolves to 43.
EXPECTED_UPLOAD_COUNT: int = 43

# Wall-clock budget for the full submit-and-await loop. We hold the
# 30 s line (43 uploads at 10 RPS = 4.3 s floor before Phantom-side
# completion).
WALL_CLOCK_BUDGET_SECONDS: float = 30.0

# Per-submit pacing — mirrors the upstream client's throttler 10
# RPS cap. The driver bypasses the original throttler,
# so the test imposes the equivalent at the call site to keep the
# load realistic.
SUBMIT_PACING_SECONDS: float = 0.1


pytestmark = pytest.mark.e2e


# Per-test config overlay: push the reaper interval far past the test's
# wall-clock so the (currently buggy) reaper never sweeps mid-test
# and prunes a row whose poll task is still mid-flight. Once the
# reaper's UTC-vs-local-time comparison is fixed (separate bug task),
# this override becomes redundant.
_REAPER_OVERRIDE: dict[str, object] = {
    "retention": {
        "reaper_interval_seconds": 3600,
    },
}


@pytest_asyncio.fixture
async def stack() -> AsyncIterator[E2EStack]:
    """Boot the stack with the reaper effectively disabled for this test.

    Shadows the conftest.py ``stack`` fixture so downstream
    ``driver``/``phantom_client``/``emulator`` fixtures pick this
    one up.
    """
    s = await boot_stack(config_overrides=_REAPER_OVERRIDE)
    try:
        yield s
    finally:
        await s.tear_down()


@pytest.fixture
def driver(stack: E2EStack) -> PhantomDriver:
    """Public driver against the overridden (reaper-disabled) stack."""
    return PhantomDriver(
        stack.phantom_client,
        files_api=stack.emulator_url,
        get_security_token=stack.fake_security_token,
    )


# perf-tier (whole test): asserts SYNTHETIC_RETURN_BUDGET_SECONDS,
# interwoven with the sequential upload flow and sharing the stack
# boot — not cleanly splittable. Runs only via `-m perf` (see pyproject
# markers).
@pytest.mark.perf
async def test_e2e_28_long_sequence_sequential(
    driver: PhantomDriver,
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """43 sequential long-sequence uploads all reach succeeded under budget.

    Every upload returns synthetic FileInformation within
    ``SYNTHETIC_RETURN_BUDGET_SECONDS``; every chain reaches
    ``succeeded`` within ``TERMINAL_STATE_BUDGET_SECONDS``; the total
    wall clock from first submit to last terminal is under
    ``WALL_CLOCK_BUDGET_SECONDS``.
    """
    # Setup — fresh emulator state.
    emulator.clear_received()
    emulator.clear_failures()

    sequence = build_long_sequence(num_cycles=NUM_CYCLES)
    assert len(sequence) == EXPECTED_UPLOAD_COUNT, (
        f"long_sequence builder emitted {len(sequence)} uploads; expected {EXPECTED_UPLOAD_COUNT}"
    )

    # Action — submit sequentially in the documented upstream order.
    # Each upload awaits the previous's synthetic FileInformation
    # return; this matches the upload sequence exactly.
    #
    # Per-chain polling runs as a background task started immediately
    # after each submit, so every chain's state is observed within a
    # couple of seconds of its submit time — well inside the
    # reaper-sweep race window. (See E2E-25 module docstring for the
    # reaper bug context.)
    wall_start = time.perf_counter()
    file_infos: list[DriverUploadResult] = []
    poll_tasks: list[asyncio.Task[None]] = []

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

    for idx, entry in enumerate(sequence):
        start = time.perf_counter()
        result = await driver.in_memory_upload(entry.request, entry.body)
        elapsed = time.perf_counter() - start
        assert elapsed < SYNTHETIC_RETURN_BUDGET_SECONDS, (
            f"in_memory_upload for {entry.label!r} took {elapsed:.3f}s; "
            f"expected < {SYNTHETIC_RETURN_BUDGET_SECONDS}s"
        )
        assert isinstance(result.id, UUID)
        file_infos.append(result)
        # Start polling this chain immediately so its state is captured
        # before the reaper sweep can prune the row.
        poll_tasks.append(asyncio.create_task(_await_one(entry, result)))
        # Upstream-faithful pacing — 100 ms per call matches the
        # upstream client's throttler 10 RPS cap, which the driver
        # bypasses. Skip on the final entry to keep the
        # wall-clock measurement honest.
        if idx + 1 < len(sequence):
            await pace(SUBMIT_PACING_SECONDS)

    assert len(file_infos) == EXPECTED_UPLOAD_COUNT

    # Wait for every chain's poll task to complete.
    await asyncio.gather(*poll_tasks)

    wall_elapsed = time.perf_counter() - wall_start
    assert wall_elapsed < WALL_CLOCK_BUDGET_SECONDS, (
        f"full sequence took {wall_elapsed:.2f}s; expected < {WALL_CLOCK_BUDGET_SECONDS}s"
    )
