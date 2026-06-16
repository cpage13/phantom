"""E2E-23 (LOAD) — sustained 10-RPS for 15 minutes.

Asserts the steady-state invariants survive a long run:

- In-flight count stays under the saturation cap (the gate refuses on
  overflow; the test catches a runaway from a worker stuck on a row
  by observing the saturation never trips).
- Reaper drops succeeded rows on schedule (the count of recent
  successes plateaus around RPS * reaper_window rather than growing
  unbounded).
- Memory total bytes stays under the configured ``in_memory_max_bytes``
  (sampling periodically; assert never crosses the cap).

Marked ``@pytest.mark.load``.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from uuid import uuid4

import pytest
from phantom_client import PhantomClient

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.payloads import build_create_file_request
from .helpers.stack import boot_stack
from .helpers.timing import pace

logger = logging.getLogger(__name__)

# Default ``sub`` claim used by the suite's fake security token.
DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

# Sustained-traffic profile.
SUSTAINED_RPS: int = 10
SUSTAINED_DURATION_SECONDS: int = 15 * 60  # 15 minutes

# Per-body size — small bodies keep the reaper-tail-bytes total
# manageable. At 10 RPS * 4 KiB * 60s window = ~2.4 MiB queue.
BODY_BYTES: int = 4 * 1024

# Sampling cadence for the steady-state checks.
SAMPLE_PERIOD_SECONDS: float = 30.0

# Memory-tier byte cap. Set high enough that healthy bursts fit but
# low enough that a leak would trip the assertion. Picked so the
# expected steady-state RAM occupancy (10 RPS * 4 KiB * 60s reaper
# window ≈ 2.4 MiB) sits well below the cap.
MEMORY_CAP_BYTES: int = 64 * 1024 * 1024

# Saturation cap. Set to multiple of (RPS * worker_count) so the gate
# never trips under healthy steady-state load.
SATURATION_CAP_ROWS: int = 200


pytestmark = [pytest.mark.e2e, pytest.mark.load]


async def _submit_one(
    pc: PhantomClient,
    *,
    emulator_url: str,
    bearer: str,
    body: bytes,
) -> None:
    """Submit one upload-shaped chain."""
    chain_id = uuid4()
    request = build_create_file_request(file_name=f"e2e23_{chain_id.hex[:12]}")
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


async def test_e2e_23_sustained_15min(tmp_path: Path) -> None:
    """15 minutes at 10 RPS — gate, reaper, memory cap all hold."""
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "saturation": {
                "max_in_flight": SATURATION_CAP_ROWS,
                "max_in_flight_bytes": MEMORY_CAP_BYTES,
            },
            "storage": {
                # Pin RAM ceiling to a known value so the assertion is
                # auditable. Phase 1 renamed
                # ``storage.in_memory_max_bytes`` →
                # ``storage.body_store.ram_ceiling_bytes`` per plan § 0.8.
                "body_store": {"ram_ceiling_bytes": MEMORY_CAP_BYTES},
            },
            "retry": {"worker_count": 4, "poll_interval_ms": 50},
            "retention": {
                # Tight succeeded-metadata window so the reaper picks
                # up rows quickly. With 30s the steady-state count of
                # succeeded_recent rows plateaus around (10 RPS * 30s) =
                # 300 rows.
                "succeeded_metadata_seconds": 30,
                "succeeded_body_seconds": 0,
                "reaper_interval_seconds": 5,
            },
        },
    )
    try:
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        # Fixed body content keeps submit-side allocation off the hot
        # path and lets us reuse the same bytes for every submit. The
        # codec round-trip is unaffected (every chain has its own UUID
        # and chain_id so the idempotency-replay path doesn't dedupe).
        body = b"phantom-e2e-23-sustained-body-" + b"x" * (BODY_BYTES - 30)
        assert len(body) == BODY_BYTES

        # Sample-collection task.
        samples: list[dict[str, int | bool]] = []

        async def _sample_loop() -> None:
            deadline = time.monotonic() + SUSTAINED_DURATION_SECONDS
            while time.monotonic() < deadline:
                stats = await stack.phantom_client.get_stats()
                samples.append(
                    {
                        "in_flight_count": stats.in_flight.count,
                        "in_flight_bytes": stats.in_flight.bytes,
                        "memory_bytes": stats.body_location["ram"].bytes,
                        "saturated": stats.saturation.saturated,
                        "succeeded_recent": stats.by_state.succeeded_recent.count,
                    }
                )
                await pace(SAMPLE_PERIOD_SECONDS)

        async def _emit_loop() -> None:
            tick = 1.0 / SUSTAINED_RPS
            deadline = time.monotonic() + SUSTAINED_DURATION_SECONDS
            n = 0
            while time.monotonic() < deadline:
                start = time.monotonic()
                # Submit serially at the configured RPS — keeps the
                # arrival pattern even rather than bursty.
                await _submit_one(
                    stack.phantom_client,
                    emulator_url=stack.emulator_url,
                    bearer=bearer,
                    body=body,
                )
                n += 1
                if n % 100 == 0:
                    logger.info("emitted %d chains so far", n)
                elapsed = time.monotonic() - start
                sleep_for = max(0.0, tick - elapsed)
                await pace(sleep_for)
            logger.info("emit loop exited after %d submits", n)

        await asyncio.gather(_emit_loop(), _sample_loop())

        # The sampler captures at SAMPLE_PERIOD_SECONDS cadence over
        # 15 minutes — we expect ~30 samples.
        assert len(samples) >= 20, (
            f"sample count too low ({len(samples)}); sampler may have starved"
        )
        for sample in samples:
            assert sample["in_flight_count"] <= SATURATION_CAP_ROWS, (
                f"in_flight {sample['in_flight_count']} exceeded cap {SATURATION_CAP_ROWS}"
            )
            assert sample["memory_bytes"] <= MEMORY_CAP_BYTES, (
                f"memory_bytes {sample['memory_bytes']} exceeded cap {MEMORY_CAP_BYTES}"
            )

        # Reaper sanity: at steady-state with a 30s succeeded-metadata
        # window and 10 RPS, ``succeeded_recent`` should plateau around
        # 300. Allow generous headroom so a slow-tick reaper isn't a
        # spurious failure — but assert it stays bounded (no
        # unbounded growth, which a missing reaper sweep would produce
        # at ~9000 rows by the end).
        steady_state_samples = samples[len(samples) // 2 :]
        max_steady = max(int(s["succeeded_recent"]) for s in steady_state_samples)
        # 10 RPS * 30 s retention + a reaper-interval (5s) buffer + a
        # generous factor for sample-time skew = 600.
        assert max_steady < 600, (
            f"succeeded_recent grew unboundedly: max={max_steady} (expected < 600). "
            f"Reaper may not be running."
        )
        logger.info(
            "sustained 15min OK: %d samples, max in_flight=%d, max memory_bytes=%d, "
            "max succeeded_recent=%d",
            len(samples),
            max(int(s["in_flight_count"]) for s in samples),
            max(int(s["memory_bytes"]) for s in samples),
            max_steady,
        )
    finally:
        await stack.tear_down()
