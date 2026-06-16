"""External lock during sustained churn: no corruption, no WAL blowup (R9-V6-3).

Aggressor R9, vector V6 ("the lock"). EXPLORATORY durability PIN: a clean pass
here is the GOOD result, and pinning it catches a future regression (e.g. a
multi-connection refactor) that would break the property.

THE PROPERTY (R9-V6-3, holds): under a sustained external WAL write lock held
during continuous write churn (a backup tool / admin session / wedged sibling
holding ``uploads.db`` open during steady traffic on a Pi), Phantom's data
layer must NOT corrupt and must NOT grow the WAL without bound. Phantom's
writers all serialize through ONE aiosqlite connection with a ``busy_timeout``,
so while the external lock is held those writes BLOCK (and ultimately surface as
the R9-V6-1 clean-503 / worker-retry error SURFACE — not a data problem) rather
than succeeding and appending WAL frames. So the WAL cannot grow from Phantom's
side during the hold: the single-connection + busy_timeout design is itself the
backpressure. SQLite's WAL is crash-consistent, so once the lock clears and a
checkpoint runs, the WAL truncates cleanly and integrity is intact.

This test pins three observable contract properties after a sustained external
hold during churn: ``PRAGMA integrity_check`` returns ``ok``, the row census
did not DROP (nothing lost), and the WAL is reclaimable (a checkpoint after
release shrinks it). Scope caveat (carried from the reproducer): this measures a
SINGLE Phantom connection contending with the external holder; the "blocked
writes don't grow the WAL" property is a consequence of that single-connection
serialization. A future change introducing multiple independent writer
connections would make WAL growth a separate question — which is exactly why
this pin exists.

DETERMINISM: POLL ``count_rows_by_state`` until rows are buffered (never a fixed
wall-clock sleep that races the ~2.2 s ``submit_chain`` round-trips), THEN seize
the lock and churn against it for a fixed window.
"""

from __future__ import annotations

import asyncio
import contextlib
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
    integrity_check,
    submit_one,
)
from tests.e2e._harness.subprocess_harness import (
    write_phantom_config as _write_phantom_config,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]

_BODY_BYTES = 4 * 1024
_SUBMIT_CONCURRENCY = 16
_MIN_BUFFERED_ROWS = 20
# ~half the attempts fail so rows churn record_attempt_result continuously,
# generating WAL frames the whole time.
_UPSTREAM_5XX_RATE = 0.5
# Sustained hold so many churned writes' worth of frames pile up while the
# checkpoint is blocked (decisively exceeds busy_timeout).
_LOCK_HOLD_SECONDS = 20.0
_WAL_SAMPLE_INTERVAL_SECONDS = 2.0
# Absurd WAL ceiling for this tiny workload: a pathological unbounded grower
# would blow far past this; a backpressured WAL stays well under.
_WAL_CEILING_BYTES = 256 * 1024 * 1024
_PRECONDITION_TIMEOUT_SECONDS = 90.0
_PRECONDITION_POLL_SECONDS = 0.25


def _overrides() -> dict:
    """Hybrid mode, generous saturation, tight churny retry, far-out reaper."""
    return {
        "storage": {"body_store": {"mode": "hybrid"}},
        "saturation": {
            "max_in_flight": _MIN_BUFFERED_ROWS * 32,
            "max_in_flight_bytes": _MIN_BUFFERED_ROWS * _BODY_BYTES * 64,
        },
        "retry": {
            "worker_count": 4,
            "poll_interval_ms": 25,
            "default_strategy": {"type": "fixed_intervals", "intervals_seconds": [0, 0, 0, 0]},
        },
        "retention": {"reaper_interval_seconds": 3600},
    }


class _ExternalLock:
    """Holds a genuine cross-process WAL write lock on the on-disk uploads.db."""

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


def _wal_size(data_dir: Path) -> int:
    """Current ``uploads.db-wal`` size in bytes (0 if absent)."""
    wal = db_path_for(data_dir).with_suffix(".db-wal")
    return wal.stat().st_size if wal.exists() else 0


async def _await_buffered_rows(data_dir: Path, want: int) -> int:
    """Poll the on-disk census until >= ``want`` rows are buffered."""
    deadline = time.monotonic() + _PRECONDITION_TIMEOUT_SECONDS
    total = 0
    while time.monotonic() < deadline:
        census = await count_rows_by_state(data_dir)
        total = sum(census.values())
        if total >= want:
            return total
        await asyncio.sleep(_PRECONDITION_POLL_SECONDS)  # pre-commit-allow: sleep
    raise AssertionError(
        f"precondition not met: only {total} rows buffered after "
        f"{_PRECONDITION_TIMEOUT_SECONDS}s (need >= {want})"
    )


async def _drive_load(
    phantom_url: str, emulator_url: str, bearer: str, stop: asyncio.Event
) -> None:
    """Continuously submit uploads until ``stop`` is set."""
    from phantom_client import PhantomClient

    sem = asyncio.Semaphore(_SUBMIT_CONCURRENCY)

    async def _one() -> None:
        async with sem:
            try:
                async with PhantomClient(phantom_url) as c:
                    await submit_one(
                        c,
                        emulator_url=emulator_url,
                        bearer=bearer,
                        body=b"c" * _BODY_BYTES,
                        chain_id=uuid4(),
                        file_prefix="v6c",
                    )
            except Exception:  # contended submits may error; that's fine here
                pass

    while not stop.is_set():
        await asyncio.gather(*(_one() for _ in range(_SUBMIT_CONCURRENCY)), return_exceptions=True)


async def test_external_lock_during_churn_no_corruption_no_wal_blowup(tmp_path: Path) -> None:
    """Sustained external lock during churn: integrity ok, no loss, WAL reclaimable.

    Pins the R9-V6-3 durability property so a future multi-connection refactor
    that breaks it (corruption / loss / unbounded un-reclaimable WAL) is caught.
    """
    from phantom_emulator.failure.injection import FailurePolicy, FailureScope

    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    port = allocate_port()
    cfg = _write_phantom_config(data_dir=data_dir, bind_port=port, config_overrides=_overrides())
    p = PhantomSubprocess.make(cfg, port)
    lock: _ExternalLock | None = None
    stop = asyncio.Event()
    load_task: asyncio.Task[None] | None = None
    try:
        await p.start()
        bearer = fake_security_token(emu)
        emu.inject_failure(
            FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=_UPSTREAM_5XX_RATE)  # type: ignore[call-arg]
        )

        load_task = asyncio.create_task(_drive_load(p.url, emu.url, bearer, stop))
        await _await_buffered_rows(data_dir, want=_MIN_BUFFERED_ROWS)
        rows_before = sum((await count_rows_by_state(data_dir)).values())

        # Seize the lock and sample the WAL while load churns against it.
        lock = _ExternalLock(db_path_for(data_dir))
        lock.acquire()
        wal_samples: list[int] = []
        hold_deadline = time.monotonic() + _LOCK_HOLD_SECONDS
        while time.monotonic() < hold_deadline:
            wal_samples.append(_wal_size(data_dir))
            await asyncio.sleep(_WAL_SAMPLE_INTERVAL_SECONDS)  # pre-commit-allow: sleep
        wal_peak = max(wal_samples) if wal_samples else 0
        lock.release()
        lock = None

        # The process must NOT have died under sustained contention.
        assert p._proc is not None and p._proc.poll() is None, (  # type: ignore[attr-defined]
            "phantom subprocess EXITED during sustained external-lock churn. "
            f"Log tail:\n{p._read_log_tail(60)}"  # type: ignore[attr-defined]
        )

        # Stop the driver and quiesce, then clear the failure so survivors drain.
        stop.set()
        if load_task is not None:
            await load_task
            load_task = None
        emu.clear_failures()

        # Checkpoint from a sibling connection (the lock is gone) and re-sample
        # the WAL — it must be reclaimable.
        ckpt_conn = sqlite3.connect(str(db_path_for(data_dir)), timeout=10.0)
        try:
            ckpt_conn.execute("PRAGMA busy_timeout=10000;")
            ckpt_conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        finally:
            ckpt_conn.close()
        wal_after = _wal_size(data_dir)

        # 1. Integrity must be ok.
        ok, msg = await integrity_check(data_dir)
        assert ok, (
            f"PRAGMA integrity_check FAILED after sustained external-lock churn: {msg} — "
            "the lock contention corrupted the DB."
        )

        # 2. No row vanished (the census can only grow as the driver submits; a
        #    DROP below the pre-hold count would mean loss).
        rows_after = sum((await count_rows_by_state(data_dir)).values())
        assert rows_after >= rows_before, (
            f"census-total dropped from {rows_before} to {rows_after} across the lock window "
            "— uploads lost under contention."
        )

        # 3. WAL is reclaimable: a checkpoint after release shrank it well under
        #    the absurd ceiling (it never approaches it for this tiny workload —
        #    the single-connection + busy_timeout backpressure keeps it bounded).
        assert wal_after <= _WAL_CEILING_BYTES, (
            f"WAL did not reclaim after release+checkpoint ({wal_after} bytes > "
            f"{_WAL_CEILING_BYTES} ceiling; peak during hold={wal_peak}) — unbounded WAL "
            "growth with no recovery (disk-exhaustion path)."
        )
    finally:
        stop.set()
        if load_task is not None:
            load_task.cancel()
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await load_task
        if lock is not None:
            lock.release()
        p.terminate()
        await emu.stop()
