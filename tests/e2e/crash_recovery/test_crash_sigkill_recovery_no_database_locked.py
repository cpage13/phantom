"""Real-OS-SIGKILL crash-recovery test (aggressor R5 findings V1 + V2).

This is the first crash test in the suite that delivers a GENUINE
``os.kill(pid, SIGKILL)`` to a real Phantom OS subprocess. The rest of
``tests/e2e/crash_recovery/`` drives recovery at the storage-component
level and closes the store cleanly (``store.stop()`` → WAL checkpointed),
and E2E-24 only does ``uvicorn force_exit`` (which STILL runs the lifespan
teardown → clean WAL close). E2E-24's own docstring conceded a true SIGKILL
"would require running phantom in a subprocess (out of scope)". This test
closes exactly that gap, and pins the V1+V2 fix as permanent coverage.

THE BUG (V1, HIGH): a real SIGKILL under retry load leaves a LARGE hot
``uploads.db-wal`` (WAL mode is the config default; the killed process never
checkpointed). On restart, ``run_recovery`` walked ``store.iter_rows()`` with
an OPEN read cursor and, the instant it found a row whose body bytes were gone
(RAM-lost in hybrid; file-missing-on-disk in all_disk — both ALWAYS exist after
a real crash), it issued a WRITE via ``mark_corrupted`` on the SAME connection,
mid-iteration. Over a hot WAL past SQLite's ``wal_autocheckpoint`` threshold,
that write triggered an autocheckpoint that collided with the still-open cursor
→ ``sqlite3.OperationalError: database is locked`` → recovery raised out of the
lifespan → ``Application startup failed. Exiting.`` → the service never came
back and every buffered upload was stranded. The 5 s ``busy_timeout`` does NOT
cover it (same-connection cursor-vs-checkpoint, not a cross-process BUSY).

V2 widened it: the crash is NOT hybrid/RAM-specific. ANY recovery quarantine
triggers it, including the all_disk ``file_body_missing_on_recovery`` path
(a body mid-write to disk at SIGKILL leaves a ``body_location='file'`` row whose
file is absent). Hence the ``mode`` parametrization below covers BOTH quarantine
paths (RAM-lost in hybrid, file-missing in all_disk) under a real SIGKILL.

THE FIX: ``run_recovery`` now COLLECTS the ``(chain_id, reason)`` quarantine
targets during the ``iter_rows`` walk and issues every ``mark_corrupted`` write
only AFTER the cursor is drained — no write while a read cursor is open. The
store ALSO checkpoints + truncates a hot WAL at ``start()`` (defense in depth),
so the WAL is cold before recovery's sweep runs.

DESIRED behavior this test asserts: after a real SIGKILL mid-load, restarting on
the same data_dir brings the service back HEALTHY (recovery completes without
``database is locked``) AND every upload that was durably buffered before the
crash survives. RED before the fix (restart subprocess fails to become healthy;
its log carries the recovery lock). GREEN after.

Determinism: the kill fires only AFTER the on-disk census shows at least
``_MIN_BUFFERED_ROWS`` committed rows, NOT on a fixed wall-clock timer. Each
``submit_chain`` round-trip is ~2 s, so a fixed short sleep raced admission and
could SIGKILL the process before a single row landed (recovery then had nothing
to quarantine and the WAL was empty). The test does NOT assert a "hot WAL" at
kill: the fix checkpoints + TRUNCATEs the WAL at ``store.start()``, so recovery
always runs against a cold WAL on restart. The WAL size at the crash instant is
neither externally observable in a stable way nor relevant to the contract.

Durability bar (review finding S12-2). ``_MIN_BUFFERED_ROWS`` is the KILL
TRIGGER and nothing else. The survival assertion compares the post-restart
census against ``buffered``, the count this run actually MEASURED on disk just
before the kill, which at ``_BURST`` 200 and concurrency 32 is routinely far
above the trigger floor. Asserting against the floor instead let a recovery
regression lose 125 of 150 buffered uploads and still pass, which is the exact
failure this module exists to catch, and it certified the trailing Go port's
crash recovery through the ``conformance`` marker as well.
"""

from __future__ import annotations

import asyncio
import contextlib
import secrets
import time
from pathlib import Path
from uuid import uuid4

import pytest

from tests.e2e._harness.subprocess_harness import (
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    count_rows_by_state,
    fake_security_token,
    submit_one,
    write_phantom_config,
)

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]

_BURST = 200
_BODY_BYTES = 4 * 1024
_UPSTREAM_5XX_RATE = 0.85
# Deterministic kill TRIGGER, and only that. A fixed wall-clock sleep RACED
# admission: each ``submit_chain`` round-trip is ~2 s (slow on a loaded host or
# Pi), so the process could be SIGKILLed before a single body was buffered.
# Recovery then had nothing to quarantine and the test asserted against an empty
# DB. Instead we SIGKILL only AFTER at least this many uploads are durably
# committed in a non-terminal state, guaranteeing recovery has real buffered
# work to perform.
#
# This constant is NOT the durability bar (finding S12-2). The survival
# assertion uses the count actually measured on disk at kill time, which is
# routinely far larger than this floor; comparing against the floor made the
# assertion unfailable for any regression that left 25 rows behind.
_MIN_BUFFERED_ROWS = 25
_PRECONDITION_TIMEOUT_SECONDS = 90.0
_PRECONDITION_POLL_SECONDS = 0.2


def _overrides(mode: str) -> dict:
    """Config for ``mode`` with generous caps, immediate retry, reaper deferred.

    A high 5xx rate keeps admitted bodies un-delivered at kill time
    (RAM-resident in hybrid; file-backed in all_disk) so the recovered DB still
    holds them and — in hybrid — recovery is FORCED down the ``mark_corrupted``
    quarantine WRITE path (the path that crashed before the fix). Immediate
    retry (``intervals_seconds=[0,...]``) keeps rows churning
    queued↔attempting so recovery's attempting→queued reset write also fires.
    The reaper interval is pushed far out AND the one short terminal-state
    metadata window is pinned to forever, so the post-recovery census is
    independent of reaper timing. This test pins crash recovery, not
    terminal-state reaping.
    """
    return {
        "storage": {"body_store": {"mode": mode}},
        "saturation": {
            "max_in_flight": _BURST * 4,
            "max_in_flight_bytes": _BURST * _BODY_BYTES * 8,
        },
        "retry": {
            "worker_count": 6,
            "poll_interval_ms": 20,
            "default_strategy": {
                "type": "fixed_intervals",
                "intervals_seconds": [0, 0, 0, 0, 0, 0],
            },
        },
        "retention": {
            "reaper_interval_seconds": 3600,
            # The reaper still sweeps ONCE at boot however long the interval
            # is, and ``succeeded_metadata_seconds`` is the one default short
            # enough (180 s) that a row which succeeded early in the burst
            # could legitimately age out of it before the post-restart census.
            # The durability assertion compares that census against the
            # measured pre-crash count, so pin the window to forever and the
            # count becomes reaper-independent by construction rather than by
            # timing luck. Every other terminal-state metadata window already
            # defaults to 30 days or forever.
            "succeeded_metadata_seconds": -1,
        },
    }


async def _await_buffered_rows(data_dir: Path) -> int:
    """Poll the on-disk census until ``_MIN_BUFFERED_ROWS`` rows are buffered.

    Reads the persisted ``uploads`` table from a separate connection (WAL mode,
    so this never blocks the live writer) and returns the observed total once
    the precondition holds. Raises ``AssertionError`` on timeout so a failure to
    land load (rather than a recovery bug) is unambiguous in the report.
    """
    deadline = time.monotonic() + _PRECONDITION_TIMEOUT_SECONDS
    total = 0
    while time.monotonic() < deadline:
        census = await count_rows_by_state(data_dir)
        total = sum(census.values())
        if total >= _MIN_BUFFERED_ROWS:
            return total
        await asyncio.sleep(_PRECONDITION_POLL_SECONDS)  # pre-commit-allow: sleep
    raise AssertionError(
        f"precondition not met: only {total} upload rows buffered after "
        f"{_PRECONDITION_TIMEOUT_SECONDS}s (need >= {_MIN_BUFFERED_ROWS}); the burst "
        "never landed enough load to exercise recovery"
    )


def _assert_no_buffered_rows_lost(*, census: dict[str, int], buffered: int, mode: str) -> None:
    """Objective: no upload durably buffered before the SIGKILL is missing after recovery.

    Expected outcome: the post-restart on-disk census totals AT LEAST
    ``buffered``, the count this run measured on disk in the instant
    before the kill. Recovery quarantines RAM-lost rows to ``corrupted``
    (hybrid) and resumes file-backed rows (all_disk); it resets
    ``attempting`` rows to ``queued``. Every one of those outcomes KEEPS
    the row, so the total may only grow (rows admitted between the census
    read and the kill), never shrink. The reaper interval is pushed to an
    hour by ``_overrides`` so no retention pass can legitimately delete a
    row inside the test window.

    The comparison is against the MEASURED pre-crash count, deliberately
    not against ``_MIN_BUFFERED_ROWS`` (finding S12-2): the floor is the
    kill trigger, and a recovery regression that aborts the ``iter_rows``
    walk at the first quarantine target leaves exactly the floor's worth
    of rows behind. Comparing to the floor passed that regression.

    Args:
        census: ``{state: count}`` read from the on-disk uploads table
            after the restarted process answered ``/v1/healthz``.
        buffered: Rows durably committed before the kill, as measured by
            :func:`_await_buffered_rows`.
        mode: The parametrized body-store mode, for the failure message.

    Raises:
        AssertionError: When recovery dropped rows that were durable.
    """
    total = sum(census.values())
    assert total >= buffered, (
        f"[{mode}] recovery LOST durably buffered rows: {buffered} rows were "
        f"committed on disk before the SIGKILL, only {total} survived the restart "
        f"({buffered - total} gone). census={census}"
    )


@pytest.mark.parametrize("mode", ["hybrid", "all_disk"])
async def test_sigkill_under_load_then_restart_recovers_healthy(tmp_path: Path, mode: str) -> None:
    """Objective: a real SIGKILL mid-load must cost neither availability nor rows.

    Expected outcome, both halves asserted:

    1. Restarting on the same data_dir brings the service back HEALTHY,
       so recovery completed without ``database is locked``.
    2. The post-restart on-disk census holds at least as many rows as
       were measured durably buffered in the instant before the kill.

    ``hybrid`` exercises V1 (RAM-lost ``mark_corrupted`` quarantine);
    ``all_disk`` exercises V2 (file-missing ``mark_corrupted``
    quarantine). Before the fix BOTH crash in ``run_recovery`` with
    ``database is locked`` over the SIGKILL-hot WAL and never become
    healthy. After the fix both recover cleanly.

    Falsifier for part 2: make recovery abandon its ``iter_rows`` walk at
    the first quarantine target, or delete quarantined rows instead of
    marking them ``corrupted``, and the surviving count drops below the
    measured pre-crash count, so the run goes RED.
    """
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    port = allocate_port()
    cfg = write_phantom_config(data_dir=data_dir, bind_port=port, config_overrides=_overrides(mode))
    p1 = PhantomSubprocess.make(cfg, port)
    p2: PhantomSubprocess | None = None
    try:
        from phantom_client import PhantomClient
        from phantom_emulator.failure.injection import FailurePolicy, FailureScope

        await p1.start()
        bearer = fake_security_token(emu)
        emu.inject_failure(
            FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=_UPSTREAM_5XX_RATE)  # type: ignore[call-arg]
        )

        sem = asyncio.Semaphore(32)

        async def _one(_: int) -> None:
            async with sem:
                try:
                    async with PhantomClient(p1.url) as c:
                        await submit_one(
                            c,
                            emulator_url=emu.url,
                            bearer=bearer,
                            body=secrets.token_bytes(_BODY_BYTES),
                            chain_id=uuid4(),
                            file_prefix=f"sigkill-{mode}",
                        )
                except Exception:
                    pass

        async def _all() -> None:
            await asyncio.gather(*(_one(i) for i in range(_BURST)), return_exceptions=True)

        t = asyncio.create_task(_all())
        # Wait for the DETERMINISTIC precondition — at least _MIN_BUFFERED_ROWS
        # uploads durably committed in a non-terminal state — THEN SIGKILL
        # mid-flight, with the rest of the burst still in flight. This replaces
        # the old fixed wall-clock sleep, which raced admission: each
        # submit_chain round-trip is ~2 s, so a 2.5 s sleep could kill the
        # process before a single row landed, leaving recovery nothing to do.
        buffered = await _await_buffered_rows(data_dir)
        p1.sigkill()
        t.cancel()
        with contextlib.suppress(Exception, asyncio.CancelledError):
            await t

        # NOTE: we deliberately do NOT assert the WAL is "hot" at kill. The fix's
        # defense-in-depth checkpoints + TRUNCATEs the WAL at store.start(), so by
        # the time recovery runs on restart the WAL is always cold regardless of
        # how hot it was at the crash — an external WAL-size assertion would be
        # both timing-dependent and measure an internal the fix neutralizes. The
        # contract this test pins is OBSERVABLE: restart recovers HEALTHY and the
        # buffered rows survive the crash.

        # Restart on a fresh port + the SAME data_dir. This is the assertion:
        # the service MUST come back up. Before the fix .start() raised because
        # recovery died with 'database is locked' over the hot WAL.
        port2 = allocate_port()
        cfg2 = write_phantom_config(
            data_dir=data_dir, bind_port=port2, config_overrides=_overrides(mode)
        )
        p2 = PhantomSubprocess.make(cfg2, port2)
        try:
            await p2.start()
        except RuntimeError as exc:  # pragma: no cover - the pre-fix RED path
            tail = p2._read_log_tail(80)  # type: ignore[attr-defined]
            pytest.fail(
                f"[{mode}] restart after SIGKILL failed to come up — recovery "
                f"crashed over the hot WAL. Boot error: {exc}\nLog tail:\n{tail}"
            )

        # GREEN: the restart subprocess answered /v1/healthz, so recovery
        # completed without 'database is locked'. Durability check: every row we
        # confirmed buffered before the crash must still be present, measured
        # against the count this run OBSERVED, not against the kill trigger.
        # Recovery quarantines RAM-lost rows to 'corrupted' (hybrid) and resumes
        # file-backed rows (all_disk); it never silently drops them, and the
        # reaper is deferred so this count is reaper-independent.
        census = await count_rows_by_state(data_dir)
        _assert_no_buffered_rows_lost(census=census, buffered=buffered, mode=mode)
    finally:
        p1.terminate()
        if p2 is not None:
            p2.terminate()
        await emu.stop()
