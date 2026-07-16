"""Recovery boot survives an external DB lock held past busy_timeout (R9-V6-2).

Aggressor R9, vector V6 ("the lock"). This is the HIGH finding: a transient
external holder of the ``uploads.db`` write lock during a restart used to crash
startup recovery and WEDGE the service, stranding every buffered upload.

THE BUG (R9-V6-2, HIGH): on a clean restart with buffered rows on disk, a
sibling connection holding the WAL write lock (a stray ``sqlite3 uploads.db``
admin session, a backup/snapshot tool, a second instance mis-sharing the
data_dir, a stale prior process whose connection lingers) past Phantom's
``busy_timeout`` turned recovery's FIRST write — ``reset_attempting_to_queued``
at the top of ``run_recovery`` — into an uncaught
``sqlite3.OperationalError: database is locked``. That propagated out of the
composition-root lifespan → uvicorn ``Application startup failed. Exiting.`` →
the service never answered health and the buffered backlog was UNREACHABLE
until an operator cleared the lock and restarted by hand. This is the V1/V2
recovery-write-vs-lock failure CLASS on a genuinely different trigger
(cross-process ``SQLITE_BUSY``, not the same-connection cursor-vs-checkpoint of
V1/V2), which the V1/V2 cursor-drain fix does NOT cover.

THE FIX: ``run_recovery`` wraps its write path
(``reset_attempting_to_queued`` + each ``mark_corrupted``) in a bounded
retry-with-backoff keyed on the shared ``is_transient_lock_error`` classifier.
A transient holder is RIDDEN OUT until it releases (recovery is idempotent —
R7-3 — so re-running a lock-rejected write loses nothing), preserving the
"recovery completes before serving traffic" invariant. Only a holder that
outlasts the generous bounded budget surfaces a clean, loud
``RecoveryLockError`` (a clean startup failure the supervisor restarts into),
never a raw traceback.

DESIRED behavior this test asserts: a fresh boot on a seeded data_dir, launched
WHILE an external lock is held past ``busy_timeout``, STILL becomes healthy
(recovery rides out the lock and completes once it clears) AND every seeded row
survives. RED before the fix (the boot subprocess dies during the hold with
``database is locked`` / ``Application startup failed`` and never answers
health). GREEN after.

DETERMINISM (the hard-won harness lesson baked into V6b): the lock-hold window
must OUTLAST both (a) the deterministic buffered-rows precondition (we POLL
``count_rows_by_state`` — never a fixed wall-clock sleep that races the ~2.2 s
``submit_chain`` round-trips) AND (b) the fresh subprocess's ~16 s cold-start,
so recovery's first write genuinely contends with the held lock. A 10/20 s hold
was INCONCLUSIVE (cold-start outran it and recovery ran after release, never
contending); 45 s is decisive. The boot is launched WITHOUT the harness's 30 s
health-wait (which would fire mid-hold and mislabel correct ``busy_timeout``
blocking as a failure); the test classifies death-vs-blocking itself and polls
health with a generous post-release budget.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from pathlib import Path
from uuid import uuid4

import pytest

from tests.e2e._harness.subprocess_harness import (
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    count_rows_by_state,
    db_path_for,
    fake_security_token,
    submit_one,
    write_phantom_config,
)

# Worktree root: the cwd for the ``python -m phantom`` subprocess the
# no-health-wait boot helper spawns below.
_REPO_ROOT = Path(__file__).resolve().parents[3]

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio, pytest.mark.e2e]

_SEED_BURST = 24
_BODY_BYTES = 2 * 1024
_SUBMIT_CONCURRENCY = 12
_MIN_SEEDED_ROWS = 6
# Near-total upstream failure so seeded bodies stay buffered (queued) rather
# than draining to a terminal state before the seeder is stopped.
_UPSTREAM_5XX_RATE = 0.98
# Must DECISIVELY exceed both the fresh subprocess's ~16 s cold-start AND
# busy_timeout on top, so recovery's first write genuinely contends with the
# held lock for well over busy_timeout (see the module docstring's determinism
# note — a 10/20 s hold was inconclusive; 45 s is decisive).
_LOCK_HOLD_SECONDS = 45.0
_PRECONDITION_TIMEOUT_SECONDS = 90.0
_PRECONDITION_POLL_SECONDS = 0.25
# Generous post-release budget for the boot to finish recovery + become
# healthy once the lock clears.
_POST_RELEASE_HEALTH_BUDGET_SECONDS = 45.0


def _overrides() -> dict:
    """Hybrid mode, generous saturation, gentle retry — shared by both boots."""
    return {
        "storage": {"body_store": {"mode": "hybrid"}},
        "saturation": {
            "max_in_flight": _SEED_BURST * 8,
            "max_in_flight_bytes": _SEED_BURST * _BODY_BYTES * 16,
        },
        "retry": {"worker_count": 2, "poll_interval_ms": 100},
        "retention": {"reaper_interval_seconds": 3600},
    }


class _ExternalLock:
    """Holds a genuine cross-process WAL write lock on the on-disk uploads.db.

    Runs the blocking sqlite3 connection on its own thread (sqlite3 is
    synchronous). ``acquire`` returns only once the write lock is actually
    held; ``release`` lets the held transaction commit and closes the
    connection.
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._acquired = threading.Event()
        self._release = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._error: BaseException | None = None

    def _run(self) -> None:
        try:
            conn = sqlite3.connect(str(self._db_path), timeout=5.0)
            try:
                conn.execute("PRAGMA busy_timeout=5000;")
                # IMMEDIATE acquires the write lock now; the throwaway UPDATE
                # forces the actual WAL write lock so other writers see
                # SQLITE_BUSY. WHERE 0 touches no rows but still takes the lock.
                conn.execute("BEGIN IMMEDIATE;")
                conn.execute("UPDATE uploads SET updated_at = updated_at WHERE 0;")
                self._acquired.set()
                self._release.wait()
                conn.commit()
            finally:
                conn.close()
        except BaseException as exc:  # surface to the main thread
            self._error = exc
            self._acquired.set()

    def acquire(self, timeout: float = 30.0) -> None:
        self._thread.start()
        if not self._acquired.wait(timeout=timeout):
            raise RuntimeError("external lock holder failed to acquire within timeout")
        if self._error is not None:
            raise RuntimeError(f"external lock holder errored on acquire: {self._error!r}")

    def release(self) -> None:
        self._release.set()
        self._thread.join(timeout=10.0)


async def _await_health_or_death(p: PhantomSubprocess, *, budget_seconds: float) -> bool:
    """Poll health until 200 (True) or the proc dies / budget expires (False)."""
    import httpx

    deadline = time.monotonic() + budget_seconds
    async with httpx.AsyncClient(timeout=2.0) as client:
        while time.monotonic() < deadline:
            if p._proc is not None and p._proc.poll() is not None:  # type: ignore[attr-defined]
                return False
            try:
                r = await client.get(f"{p.url}/v1/healthz")
                if r.status_code == 200:
                    return True
            except httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError:
                pass
            await asyncio.sleep(0.2)  # pre-commit-allow: sleep
    return False


async def _await_buffered_rows(data_dir: Path, want: int) -> int:
    """Poll the on-disk census until >= ``want`` rows are buffered.

    Reads from a separate connection (WAL mode, so it never blocks the live
    writer). Deterministic precondition — replaces a fixed wall-clock sleep
    that would race the ~2.2 s ``submit_chain`` round-trips.
    """
    deadline = time.monotonic() + _PRECONDITION_TIMEOUT_SECONDS
    total = 0
    while time.monotonic() < deadline:
        census = await count_rows_by_state(data_dir)
        total = sum(census.values())
        if total >= want:
            return total
        await asyncio.sleep(_PRECONDITION_POLL_SECONDS)  # pre-commit-allow: sleep
    raise AssertionError(
        f"seed precondition not met: only {total} rows buffered after "
        f"{_PRECONDITION_TIMEOUT_SECONDS}s (need >= {want}); the burst never landed load"
    )


async def _seed(data_dir: Path, emu, port: int) -> int:
    """Boot Phantom, land a buffered burst, stop CLEANLY; return seeded count."""
    from phantom_client import PhantomClient
    from phantom_emulator.failure.injection import FailurePolicy, FailureScope

    cfg = write_phantom_config(data_dir=data_dir, bind_port=port, config_overrides=_overrides())
    seeder = PhantomSubprocess.make(cfg, port)
    await seeder.start()
    bearer = fake_security_token(emu)
    emu.inject_failure(
        FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=_UPSTREAM_5XX_RATE)  # type: ignore[call-arg]
    )
    sem = asyncio.Semaphore(_SUBMIT_CONCURRENCY)

    async def _one() -> None:
        async with sem:
            try:
                async with PhantomClient(seeder.url) as c:
                    await submit_one(
                        c,
                        emulator_url=emu.url,
                        bearer=bearer,
                        body=b"s" * _BODY_BYTES,
                        chain_id=uuid4(),
                        file_prefix="v6b",
                    )
            except Exception:  # buffering is what matters here, not the response
                pass

    await asyncio.gather(*(_one() for _ in range(_SEED_BURST)), return_exceptions=True)
    seeded = await _await_buffered_rows(data_dir, want=_MIN_SEEDED_ROWS)
    # Clean stop → lifespan teardown → WAL checkpointed, rows durable on disk.
    # This is NOT a hot-WAL crash (V1/V2 own that); it isolates the
    # cross-process lock trigger.
    seeder.terminate()
    emu.clear_failures()
    return seeded


async def test_recovery_boot_survives_external_lock_past_busy_timeout(tmp_path: Path) -> None:
    """Fresh boot under a held external DB lock STILL comes up healthy (R9-V6-2).

    Seeds a data_dir with buffered rows (clean stop), holds an external WAL
    write lock past ``busy_timeout`` while a fresh Phantom boots on the same
    data_dir, and asserts the service becomes healthy and every seeded row
    survives recovery. Before the fix the boot dies with ``database is locked``
    during the hold and never answers health.
    """
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    seed_port = allocate_port()
    lock: _ExternalLock | None = None
    p2: PhantomSubprocess | None = None
    try:
        seeded = await _seed(data_dir, emu, seed_port)

        # Seize the external lock on the seeded DB and hold it past busy_timeout.
        lock = _ExternalLock(db_path_for(data_dir))
        lock.acquire()

        # Boot a FRESH Phantom on the seeded data_dir WHILE the lock is held.
        # Launch directly (NOT .start(), whose 30 s health poll would fire
        # mid-hold and mislabel correct busy_timeout blocking as a failure).
        port2 = allocate_port()
        cfg2 = write_phantom_config(
            data_dir=data_dir, bind_port=port2, config_overrides=_overrides()
        )
        # spawn() (not start()): the held lock can legitimately block startup
        # recovery past the harness's 30 s health budget. That is correct
        # blocking (the fix riding out the lock), not a failure, so the
        # health observation is owned below by _await_health_or_death.
        p2 = PhantomSubprocess.make(cfg2, port2)
        p2.spawn(label=f"no-health-wait config={p2.config_path}")

        # Hold the full window so recovery's first write contends for well over
        # busy_timeout (the hold outlasts cold-start).
        await asyncio.sleep(_LOCK_HOLD_SECONDS)  # pre-commit-allow: sleep

        # Decisive break signal: did the boot subprocess DIE during the hold?
        # (Pre-fix: startup recovery raised 'database is locked' → uvicorn
        # exits.) After the fix the boot is ALIVE — recovery is riding out the
        # lock with bounded backoff.
        crashed_during_hold = (
            p2._proc is not None and p2._proc.poll() is not None  # type: ignore[attr-defined]
        )
        lock.release()
        lock = None

        if crashed_during_hold:
            tail = p2._read_log_tail(80)  # type: ignore[attr-defined]
            pytest.fail(
                "boot subprocess DIED while the external DB lock was held — startup "
                "recovery crashed instead of riding out the transient lock. "
                f"('database is locked' in log={'database is locked' in tail}, "
                f"'Application startup failed' in log={'Application startup failed' in tail}). "
                f"{seeded} buffered upload(s) would be stranded. Log tail:\n{tail}"
            )

        # Alive through the hold — it must now finish recovery and become
        # healthy now that the lock is gone.
        became_healthy = await _await_health_or_death(
            p2, budget_seconds=_POST_RELEASE_HEALTH_BUDGET_SECONDS
        )
        if not became_healthy:
            tail = p2._read_log_tail(80)  # type: ignore[attr-defined]
            pytest.fail(
                "boot did not become healthy within "
                f"{_POST_RELEASE_HEALTH_BUDGET_SECONDS}s after the external lock released — "
                f"recovery never completed. Log tail:\n{tail}"
            )

        # GREEN: recovery completed under (then after) the lock. Durability
        # check: every seeded row must still be present (recovery quarantines
        # RAM-lost rows to 'corrupted' but NEVER silently drops them; the
        # reaper is deferred so this count is reaper-independent).
        post_census = await count_rows_by_state(data_dir)
        post_total = sum(post_census.values())
        assert post_total >= seeded, (
            f"recovered DB lost buffered rows: seeded {seeded}, found {post_total} "
            f"after recovery under external-lock contention, census={post_census}"
        )
    finally:
        if lock is not None:
            lock.release()
        if p2 is not None:
            p2.terminate()
        await emu.stop()
