"""Performance baselines + regression gates over the multi-instance stack.

Cycle-7 plan 06_09 task 7.2 (baselines CAPTURED first, gates second):
this module runs one measured workload, the multi-instance burst with
concurrent admin-read load on the new read surfaces, and then either

* CAPTURES this machine's baseline (first run on a machine: no
  assertion against a reference, the file under
  ``tests/e2e/perf_baselines/<machine-key>.json`` is written and
  committed as the reference), or
* GATES the measured metrics against the stored baseline with the
  tolerance band defined and justified in
  :mod:`tests.e2e.helpers.perf_baseline` (throughput floor 0.5x,
  read-p95 ceiling 3x).

Marked ``perf``: wall-clock assertions belong on a quiet lane
(``pytest -m perf tests/e2e/test_perf_baseline_gates.py``), not in the
default suite where a loaded host produces false failures. The
correctness twin of this workload (no clocks, every byte verified)
is ``test_multi_instance_throughput.py``.
"""

from __future__ import annotations

import asyncio
import logging
import random
import secrets
import time
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_emulator.auth.modes import AuthMode

from .helpers.assertions import assert_chain_reaches_state
from .helpers.perf_baseline import (
    READ_LATENCY_QUANTILE,
    PerfMetrics,
    baseline_path_for,
    capture_baseline,
    collect_machine_facts,
    evaluate_gates,
    load_baseline,
    machine_key,
    percentile,
)
from .helpers.stack import E2EStack, boot_stack
from .test_multi_instance_throughput import (
    _INSTANCE_IDS,
    _SHARED_SUB,
    _multi_instance_overrides,
    _submit_to_instance,
)

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.e2e, pytest.mark.perf]

# Uploads per instance in the MEASURED burst: 16 x 3 instances = 48,
# larger than the correctness burst so the throughput number averages
# over a steadier window, still seconds-scale on a developer machine.
PERF_UPLOADS_PER_INSTANCE: int = 16

# Total measured volume (derived; stored in the baseline as the
# workload identity, so a resized workload forces a re-capture).
PERF_TOTAL_UPLOADS: int = PERF_UPLOADS_PER_INSTANCE * len(_INSTANCE_IDS)

# Concurrent admin-reader lanes sampling latencies during the burst.
PERF_READER_LANES: int = 2

# Body size for measured uploads (matches the correctness burst).
PERF_BODY_BYTES: int = 2 * 1024

# Deadlock backstop for the measured burst; NOT a gated metric.
PERF_BURST_BUDGET_SECONDS: float = 120.0

# Seeded RNG for the reader op mix (reproducible measurement shape).
PERF_RNG_SEED: int = 13

# List page size for the list-read op.
PERF_LIST_PAGE_LIMIT: int = 100


async def _measured_burst(stack: E2EStack) -> PerfMetrics:
    """Run the measured workload; return its metrics.

    The clock starts when the concurrent submissions fire and stops when
    the LAST upload reaches succeeded; reader lanes sample admin-read
    latencies (rollup / lookup / list / detail) for that whole window.
    """
    pc: PhantomClient = stack.phantom_client
    emulators = [stack.emulator, *stack.extra_emulators]
    emulator_urls = [stack.emulator_url, *stack.extra_emulator_urls]
    assert len(emulators) == len(_INSTANCE_IDS)
    for emu in emulators:
        emu.clear_received()
        emu.clear_failures()
        emu.set_auth_mode(AuthMode.NONE)
    bearer = stack.fake_security_token(sub=_SHARED_SUB)

    groups: dict[str, UUID] = {instance_id: uuid4() for instance_id in _INSTANCE_IDS}
    chains: dict[str, list[UUID]] = {instance_id: [] for instance_id in _INSTANCE_IDS}
    # Chains whose 202 has returned, per instance: row-addressed reads
    # (detail / lookup / rollup) may only target these, or an honest
    # 404 on a not-yet-landed POST would poison the measurement.
    admitted: dict[str, list[UUID]] = {instance_id: [] for instance_id in _INSTANCE_IDS}

    async def _submit_and_record(instance_id: str, chain_id: UUID, emulator_url: str) -> None:
        """Submit one upload and record its admission for the readers."""
        await _submit_to_instance(
            pc,
            chain_id=chain_id,
            body=secrets.token_bytes(PERF_BODY_BYTES),
            emulator_url=emulator_url,
            bearer=bearer,
            instance_id=instance_id,
            group_id=groups[instance_id],
        )
        admitted[instance_id].append(chain_id)

    submit_coros = []
    for idx, instance_id in enumerate(_INSTANCE_IDS):
        for _ in range(PERF_UPLOADS_PER_INSTANCE):
            chain_id = uuid4()
            chains[instance_id].append(chain_id)
            submit_coros.append(_submit_and_record(instance_id, chain_id, emulator_urls[idx]))

    read_latencies: list[float] = []
    read_faults: list[str] = []
    settled = asyncio.Event()

    async def _reader_lane(rng: random.Random) -> None:
        """Sample mixed admin reads until the burst settles."""
        ops = ("rollup", "lookup", "list", "detail")
        while not settled.is_set():
            instance_id = _INSTANCE_IDS[rng.randrange(len(_INSTANCE_IDS))]
            op = rng.choice(ops)
            landed = admitted[instance_id]
            if op != "list" and not landed:
                # No admitted row to address on this instance yet.
                await asyncio.sleep(0)  # pre-commit-allow: sleep (zero-second yield)
                continue
            begin = time.perf_counter()
            try:
                if op == "rollup":
                    await pc.get_group_status(groups[instance_id], instance=instance_id)
                elif op == "lookup":
                    await pc.find_by_local_uuid(rng.choice(landed))
                elif op == "list":
                    await pc.list_uploads(instance=instance_id, limit=PERF_LIST_PAGE_LIMIT)
                else:
                    await pc.get_upload(rng.choice(landed))
            except Exception as exc:  # measurement collector; re-surfaced below
                read_faults.append(f"{op}: {type(exc).__name__}: {exc}")
            finally:
                read_latencies.append(time.perf_counter() - begin)
            await asyncio.sleep(0)  # pre-commit-allow: sleep (zero-second yield)

    rng = random.Random(PERF_RNG_SEED)
    burst_began = time.perf_counter()
    async with asyncio.TaskGroup() as tg:
        for _ in range(PERF_READER_LANES):
            tg.create_task(_reader_lane(rng))
        try:
            await asyncio.gather(*submit_coros)
            await asyncio.gather(
                *(
                    assert_chain_reaches_state(
                        pc,
                        chain_id,
                        state="succeeded",
                        timeout_seconds=PERF_BURST_BUDGET_SECONDS,
                    )
                    for per_instance in chains.values()
                    for chain_id in per_instance
                )
            )
        finally:
            settled.set()
    burst_seconds = time.perf_counter() - burst_began

    assert not read_faults, (
        f"a measured run with {len(read_faults)} read fault(s) is invalid: {read_faults[:10]}"
    )
    return PerfMetrics(
        delivered_throughput_uploads_per_second=PERF_TOTAL_UPLOADS / burst_seconds,
        admin_read_p95_seconds=percentile(read_latencies, READ_LATENCY_QUANTILE),
        admin_read_count=len(read_latencies),
        uploads_total=PERF_TOTAL_UPLOADS,
    )


async def test_perf_baseline_capture_then_gate(tmp_path: Path) -> None:
    """First run on a machine captures its baseline; later runs gate against it."""
    stack = await boot_stack(
        tmp_path=tmp_path,
        extra_emulators=len(_INSTANCE_IDS) - 1,
        config_overrides=_multi_instance_overrides(),
    )
    try:
        metrics = await _measured_burst(stack)
    finally:
        await stack.tear_down()

    key = machine_key()
    path = baseline_path_for(key)
    baseline = load_baseline(path)
    if baseline is None:
        captured = capture_baseline(path, collect_machine_facts(), metrics)
        logger.info(
            "perf baseline CAPTURED for machine %s (no gate on the capture run): "
            "throughput=%.2f uploads/s, read p95=%.4fs over %d reads",
            key,
            captured.metrics.delivered_throughput_uploads_per_second,
            captured.metrics.admin_read_p95_seconds,
            captured.metrics.admin_read_count,
        )
        return

    breaches = evaluate_gates(baseline, metrics)
    logger.info(
        "perf gates for machine %s: measured throughput=%.2f uploads/s "
        "(baseline %.2f), read p95=%.4fs (baseline %.4fs) over %d reads",
        key,
        metrics.delivered_throughput_uploads_per_second,
        baseline.metrics.delivered_throughput_uploads_per_second,
        metrics.admin_read_p95_seconds,
        baseline.metrics.admin_read_p95_seconds,
        metrics.admin_read_count,
    )
    assert not breaches, "; ".join(breaches)
