"""E2E-24 (LOAD) — abrupt-kill idempotency + crash-survival under a real SIGKILL.

A burst of chains is in flight; mid-burst, a genuine ``os.kill(pid, SIGKILL)``
terminates the Phantom OS subprocess — no lifespan teardown, the SQLite WAL
interrupted mid-write, exactly as a power-loss or OOM-kill on a Pi would leave
it. Phantom is restarted against the same ``data_dir``; the recovery sweep cleans
up partial persists, and the sender pool drives every surviving chain to a
terminal state. The emulator — which stays alive in the parent process, since it
is the surviving upstream — dedupes duplicate uploads via its Idempotency-Key
cache, so each chain appears at most once in the received log even when retries
replay across the restart.

This test runs Phantom as a real subprocess via the shared
``tests/e2e/_harness/subprocess_harness`` so the kill is a TRUE SIGKILL. The
earlier in-process variant (``uvicorn force_exit`` on a serve task sharing the
test process) could not deliver a real kill: its lifespan teardown still ran and
raced the restart for the SQLite lock — an artifact of the in-process harness,
not a Phantom bug. The production recovery path's own real-SIGKILL contract (no
"database is locked" over a hot WAL) is pinned separately by
``tests/e2e/crash_recovery/test_crash_sigkill_recovery_no_database_locked.py``;
this test layers the idempotency contract on top of the same real-subprocess
mechanism.

Two deliberate harness choices keep this load test deterministic rather than
flaky:

* The kill fires on a DETERMINISTIC precondition — at least ``MIN_BUFFERED_ROWS``
  upload rows durably committed — not a fixed wall-clock timer. A fixed sleep
  raced admission: each ``submit_chain`` round-trip is ~2 s, so a short sleep
  could kill the process before a single row landed.
* The post-restart drain polls the ON-DISK census directly (``count_rows_by_state``
  over a separate WAL reader), NOT the HTTP admin surface. Polling the admin
  endpoint for every chain concurrently starves the in-process emulator's event
  loop, stalling the very deliveries the drain waits on. The reaper is deferred
  so the quiescent census is reaper-independent (the pinned config reaps every
  5 s by default).

Marked ``@pytest.mark.load``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import secrets
import time
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from phantom.storage import TERMINAL_STATES
from phantom_client import PhantomClient
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._harness.subprocess_harness import (
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    count_rows_by_state,
    fake_security_token,
    submit_one,
    write_phantom_config,
)

logger = logging.getLogger(__name__)

# Burst size for the load exercise.
BURST_SIZE: int = 100

# Per-body size — small bodies keep the restart phase tractable while still
# exercising the body-store / hash-verification machinery.
BODY_BYTES: int = 4 * 1024

# Concurrency cap on the submit burst.
SUBMIT_CONCURRENCY: int = 32

# Light upstream flakiness during the burst so some chains carry outstanding
# retries at the kill instant — the load-bearing dimension (idempotency under a
# retry storm replaying across the restart). Cleared after the kill so the
# restarted sender drains every surviving chain to a terminal state.
UPSTREAM_5XX_RATE: float = 0.2

# Deterministic kill trigger: SIGKILL only once at least this many upload rows
# are durably committed, so recovery has real buffered work on restart.
MIN_BUFFERED_ROWS: int = 20
PRECONDITION_TIMEOUT_SECONDS: float = 90.0
PRECONDITION_POLL_SECONDS: float = 0.2

# Post-restart drain budget (the buffered backlog is small; generous headroom).
DRAIN_TOTAL_BUDGET_SECONDS: float = 180.0
DRAIN_POLL_SECONDS: float = 0.5

# Reaper interval for the restarted stack — pushed far out so terminal rows are
# not reaped before the census is read (the pinned config reaps every 5 s).
REAPER_DEFERRAL_SECONDS: int = 3600


pytestmark = [pytest.mark.conformance, pytest.mark.e2e, pytest.mark.load]


def _overrides() -> dict[str, Any]:
    """all_disk config with generous caps, immediate retry, reaper deferred.

    all_disk pins every body to disk at admission so it survives the SIGKILL
    (RAM bodies could not survive process death by definition). Immediate retry
    (``fixed_intervals`` of 0 s) drains the backlog fast once the failure
    injection is cleared on restart. The reaper interval is pushed far out so the
    post-restart census reflects every recovered row rather than a reaper race.
    """
    return {
        "storage": {"body_store": {"mode": "all_disk"}},
        "saturation": {
            "max_in_flight": BURST_SIZE * 2,
            "max_in_flight_bytes": BURST_SIZE * BODY_BYTES * 4,
        },
        "retry": {
            "worker_count": 4,
            "poll_interval_ms": 50,
            "default_strategy": {
                "type": "fixed_intervals",
                "intervals_seconds": [0, 0, 0, 0, 0, 0, 0, 0],
            },
        },
        "retention": {"reaper_interval_seconds": REAPER_DEFERRAL_SECONDS},
    }


async def _await_buffered_rows(data_dir: Path, *, minimum: int) -> int:
    """Poll the on-disk census until ``minimum`` upload rows are buffered.

    Reads the persisted ``uploads`` table from a separate connection (WAL mode,
    so this never blocks the live writer in the subprocess) and returns the
    observed total once the precondition holds. Raises ``AssertionError`` on
    timeout so a failure to land load (rather than a recovery bug) is
    unambiguous in the report.

    Args:
        data_dir: The Phantom data root passed to ``write_phantom_config``.
        minimum: The buffered-row count that must be reached before the kill.

    Returns:
        The observed total upload-row count once it reaches ``minimum``.
    """
    deadline = time.monotonic() + PRECONDITION_TIMEOUT_SECONDS
    total = 0
    while time.monotonic() < deadline:
        census = await count_rows_by_state(data_dir)
        total = sum(census.values())
        if total >= minimum:
            return total
        await asyncio.sleep(PRECONDITION_POLL_SECONDS)  # pre-commit-allow: sleep
    raise AssertionError(
        f"precondition not met: only {total} upload rows buffered after "
        f"{PRECONDITION_TIMEOUT_SECONDS}s (need >= {minimum}); the burst never "
        "landed enough load to exercise the crash"
    )


async def _await_drained(data_dir: Path) -> dict[str, int]:
    """Poll the on-disk census until no upload row is in a non-terminal state.

    Reads the persisted ``uploads`` table directly (WAL mode → never blocks the
    live writer) rather than polling the HTTP admin surface, so the drain does
    not starve the in-process emulator's event loop and stall the deliveries it
    is waiting on. Raises ``AssertionError`` on timeout with the stuck census so
    a wedged row is unambiguous.

    Args:
        data_dir: The Phantom data root passed to ``write_phantom_config``.

    Returns:
        The final ``{state: count}`` census once every row is terminal.
    """
    deadline = time.monotonic() + DRAIN_TOTAL_BUDGET_SECONDS
    census: dict[str, int] = {}
    while time.monotonic() < deadline:
        census = await count_rows_by_state(data_dir)
        non_terminal = sum(n for state, n in census.items() if state not in TERMINAL_STATES)
        if non_terminal == 0:
            return census
        await asyncio.sleep(DRAIN_POLL_SECONDS)  # pre-commit-allow: sleep
    raise AssertionError(
        f"post-restart drain did not reach quiescence within "
        f"{DRAIN_TOTAL_BUDGET_SECONDS}s; final census={census} — non-terminal rows "
        "remain (recovery/sender did not drain the backlog)"
    )


async def test_e2e_24_sigkill_idempotency(tmp_path: Path) -> None:
    """Real SIGKILL mid-burst, restart — no duplicate uploads, no wedged chains."""
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    port = allocate_port()
    cfg = write_phantom_config(data_dir=data_dir, bind_port=port, config_overrides=_overrides())
    p1 = PhantomSubprocess.make(cfg, port)
    p2: PhantomSubprocess | None = None

    chain_ids: list[UUID] = [uuid4() for _ in range(BURST_SIZE)]
    bodies: list[bytes] = [secrets.token_bytes(BODY_BYTES) for _ in range(BURST_SIZE)]

    try:
        await p1.start()
        emu.clear_received()
        emu.clear_failures()
        emu.inject_failure(
            FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=UPSTREAM_5XX_RATE)  # type: ignore[call-arg]
        )
        bearer = fake_security_token(emu)

        sem = asyncio.Semaphore(SUBMIT_CONCURRENCY)

        async def _one(idx: int) -> None:
            async with sem:
                # Best-effort submit: a submit landing on the network during the
                # kill window surfaces as a transport error. Absorb it — those
                # chains simply never reached Phantom's storage.
                try:
                    async with PhantomClient(p1.url) as client:
                        await submit_one(
                            client,
                            emulator_url=emu.url,
                            bearer=bearer,
                            body=bodies[idx],
                            chain_id=chain_ids[idx],
                            file_prefix="e2e24",
                        )
                except Exception as exc:
                    logger.warning("submit %d failed during kill window: %r", idx, exc)

        async def _submit_all() -> None:
            await asyncio.gather(*(_one(i) for i in range(BURST_SIZE)), return_exceptions=True)

        submit_task: asyncio.Task[None] = asyncio.create_task(_submit_all())

        # Deterministic kill: wait until enough rows are durably buffered, then
        # deliver a genuine SIGKILL with the rest of the burst still in flight.
        buffered = await _await_buffered_rows(data_dir, minimum=MIN_BUFFERED_ROWS)
        logger.info("KILL: %d rows buffered; delivering SIGKILL to pid=%s", buffered, p1.pid)
        p1.sigkill()

        submit_task.cancel()
        with contextlib.suppress(Exception, asyncio.CancelledError):
            await submit_task

        # Clear the injected flakiness so the restarted sender drives every
        # surviving chain to a terminal state.
        emu.clear_failures()

        # Restart on a fresh port + the SAME data_dir. Recovery runs at startup
        # before the sender pool is allowed to claim any row. A fresh port avoids
        # racing the dead process's TCP listener still in TIME_WAIT.
        port2 = allocate_port()
        cfg2 = write_phantom_config(
            data_dir=data_dir, bind_port=port2, config_overrides=_overrides()
        )
        p2 = PhantomSubprocess.make(cfg2, port2)
        try:
            await p2.start()
        except RuntimeError as exc:  # pragma: no cover - defensive; recovery must come back
            tail = p2._read_log_tail(80)  # type: ignore[attr-defined]
            pytest.fail(f"restart after SIGKILL failed to come up: {exc}\nLog tail:\n{tail}")

        # Drain to quiescence via the on-disk census (NOT the HTTP admin surface).
        t_start = time.monotonic()
        census = await _await_drained(data_dir)
        logger.info(
            "post-restart drain quiescent in %.2fs: census=%s",
            time.monotonic() - t_start,
            census,
        )

        # No chain is wedged (guaranteed terminal by _await_drained), the retry
        # budget is not exhausted, and the pipeline actually delivered some
        # buffered chain end-to-end after the crash.
        assert census.get("failed", 0) == 0, (
            f"unexpected failed rows after restart drain: census={census} "
            "(retry budget should not exhaust once the failure injection is cleared)"
        )
        assert census.get("succeeded", 0) > 0, (
            f"no chain reached 'succeeded' post-restart: census={census} "
            "(the pipeline did not deliver any buffered chain end-to-end)"
        )

        # Idempotency: the emulator's Idempotency-Key cache dedupes replays, so
        # each chain_id appears at most once in the received log even though
        # retries replay across the restart.
        per_chain_count: dict[str, int] = {}
        for entry in emu.received():
            cid_str = entry.metadata_kvs.get("phantom_local_uuid")
            if cid_str is None:
                continue
            per_chain_count[cid_str] = per_chain_count.get(cid_str, 0) + 1
        duplicates = {cid: n for cid, n in per_chain_count.items() if n > 1}
        assert not duplicates, f"emulator received duplicate entries for chains: {duplicates}"
        assert per_chain_count, "emulator received no uploads post-restart — nothing was delivered"
        logger.info(
            "idempotency held: %d unique chains in received log; no duplicates",
            len(per_chain_count),
        )
    finally:
        p1.terminate()
        if p2 is not None:
            p2.terminate()
        await emu.stop()
