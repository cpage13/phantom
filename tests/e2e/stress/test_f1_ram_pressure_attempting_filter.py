"""Regression test for aggressor finding F-1 / validation F-P8-A (adopted Round 2).

Asserts that sustained RAM pressure on rows in ``attempting`` state does
NOT leave the RAM body store above the ceiling indefinitely. Round 1
found that ``RamPressureWatcher`` skipped every mid-attempt candidate
unconditionally, so a slow/unreachable upstream that pinned the oldest
RAM rows in ``attempting`` let RAM blow past ``ram_ceiling_bytes``
unbounded (OOM risk on a Pi-class host).

Defender Round 2 fix: TIME-BOUNDED attempting skip. The watcher skips a
mid-attempt candidate only while its attempt is *fresh* (started within
~2x ``ram_pressure_poll_seconds``, with a 1 s floor); an attempt stalled
longer is enqueued anyway. ``updated_at`` is the attempt-start time
(set on the queued->attempting flip by ``claim_due``). Migrating a
stalled-attempt body is safe — the HybridBodyStore fsyncs to disk before
deleting RAM, so the sender's RAM-first read falls back to disk and the
controller dedupes a later failure-handler re-enqueue. See
``phantom.workers.ram_pressure._is_fresh_attempt``.

This test asserts a quantitative "drop below ceiling within N seconds"
SLA under sustained attempting. The ``@pytest.mark.stress`` mark keeps
it out of the per-PR run.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import secrets
from uuid import UUID, uuid4

import httpx
import pytest

from tests.e2e._driver import build_in_memory_upload_envelope
from tests.e2e.helpers.payloads import build_create_file_request
from tests.e2e.helpers.stack import boot_stack
from tests.e2e.helpers.timing import await_until

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.e2e, pytest.mark.stress]


BODY_BYTES = 128 * 1024
BURST_SIZE = 4
RAM_CEILING = 256 * 1024
# After this many seconds under sustained pressure, the watcher MUST
# have driven the RAM total back below the ceiling. The current
# implementation keeps the total >= ceiling indefinitely under this
# attack — the test fails until the watcher is repaired.
DRAIN_DEADLINE_SECONDS = 8.0


async def test_ram_pressure_drained_within_deadline_under_sustained_attempting() -> None:
    """Sustained slow upstream + tight ceiling must drop below ceiling within DRAIN_DEADLINE."""
    from phantom_emulator.failure.injection import FailurePolicy, FailureScope

    stack = await boot_stack(
        config_overrides={
            "storage": {
                "body_store": {
                    "mode": "hybrid",
                    "ram_ceiling_bytes": RAM_CEILING,
                    "ram_pressure_poll_seconds": 0.25,
                    "linger_seconds": 30,
                },
            },
            "saturation": {
                "max_in_flight": BURST_SIZE * 4,
                "max_in_flight_bytes": BODY_BYTES * BURST_SIZE * 4,
            },
            "retry": {
                "worker_count": BURST_SIZE,
                "poll_interval_ms": 100,
            },
        },
    )
    try:
        emulator = stack.emulator
        pc = stack.phantom_client
        bearer = stack.fake_security_token()
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]
                scope=FailureScope.UPSTREAM_FILES_CREATE,
                latency_ms=30_000,
            ),
        )
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]
                scope=FailureScope.UPSTREAM_FILES_UPLOAD,
                latency_ms=30_000,
            ),
        )

        chain_ids: list[UUID] = [uuid4() for _ in range(BURST_SIZE)]
        bodies = [secrets.token_bytes(BODY_BYTES) for _ in range(BURST_SIZE)]
        for chain_id, body in zip(chain_ids, bodies, strict=True):
            req = build_create_file_request(file_name=f"f1-regression-{chain_id.hex[:8]}")
            req.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
            envelope, _ = build_in_memory_upload_envelope(
                request=req,
                files_api_base=stack.emulator_url,
                local_uuid=chain_id,
            )
            await pc.submit_chain(
                envelope,
                body_refs={"body": body},
                uid="00000000-0000-0000-0000-000000000001",
                auth_token=f"Bearer {bearer}",
            )

        # Poll the ram_pressure endpoint until ram_body_store_bytes drops
        # below the ceiling, via the project's await_until helper (the
        # hook-approved bounded-poll idiom — no naked sleep). The watcher
        # must enqueue the stalled-attempting rows and the controller must
        # migrate them within the deadline.
        last_bytes = -1

        async def _ram_below_ceiling() -> bool:
            nonlocal last_bytes
            async with httpx.AsyncClient() as http:
                r = await http.get(f"{stack.phantom_url}/v1/admin/observability/ram_pressure")
            if r.status_code != 200:
                return False
            last_bytes = int(r.json().get("ram_body_store_bytes", 0))
            return last_bytes < RAM_CEILING

        await await_until(
            _ram_below_ceiling,
            timeout_seconds=DRAIN_DEADLINE_SECONDS,
            poll_interval_seconds=0.25,
            message=(
                f"RAM ceiling violated for {DRAIN_DEADLINE_SECONDS}s — "
                f"ram_body_store_bytes stayed >= ram_ceiling_bytes={RAM_CEILING}. "
                "The persist controller did not drain under sustained "
                "'attempting' state (F-P8-A)."
            ),
        )
        assert last_bytes < RAM_CEILING, last_bytes
    finally:
        stack.emulator.clear_failures()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(stack.emulator.drain(), timeout=2.0)
        await stack.tear_down()
