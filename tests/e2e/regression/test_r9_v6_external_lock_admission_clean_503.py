"""External-lock admission returns a clean 503, not a naked 5xx (R9-V6-1).

Aggressor R9, vector V6 ("the lock").

THE BUG (R9-V6-1, Medium): a sibling connection holding the ``uploads.db`` WAL
write lock past Phantom's ``busy_timeout`` (a stray ``sqlite3 uploads.db`` admin
session, a backup tool mid-snapshot, a second instance mis-sharing the
data_dir) made admission's INSERT raise ``sqlite3.OperationalError: database is
locked``. Pre-fix that was neither the ``IntegrityError`` (chain_id collision)
nor the ``OSError`` (body-store fault) admission caught, so it escaped past the
``/send`` handler's ``except ChainAdmissionError`` and became a NAKED HTTP 500
(the D-1/R3-2/R7-1 naked-500 class on a new trigger) — tripping the upstream
client's 5xx fallback instead of preserving the client's buffered retry. The former 5 s
``busy_timeout`` also AMPLIFIED the burst: each contended writer monopolized the
store's single ``_write_lock`` for the full window, so admissions queued behind
multiple 5 s busy-waits and the client's HTTP read timed out before admission could
return its 503 — the burst surfaced as bare ``PhantomTimeoutError``s too.

THE FIX (two parts, both exercised here):
1. Admission catches the transient-lock ``OperationalError``
   (``is_transient_lock_error``) and maps it to the ADR-017
   ``storage_unavailable`` 503 + Retry-After — the same arm/shape as the
   ``OSError`` → 503 fix.
2. ``SQLITE_BUSY_TIMEOUT_MS`` lowered 5000 → 1000. Every store writer
   serializes through one ``_write_lock``, so internal write-vs-write
   contention is impossible; the busy_timeout only governs EXTERNAL
   contention, where failing FAST (→ a clean retryable) beats monopolizing the
   single writer slot. Contended admissions now return clean 503s well within
   the client's read timeout instead of timing out.

DESIRED behavior this test asserts: every admission fired DURING a held
external lock returns either a success (admitted — block-then-succeed under
busy_timeout) or a CLEAN retryable 503 (ADR-017 ``storage_unavailable``,
``PhantomUnavailableError``) — NEVER a naked non-503 5xx and NEVER a bare
transport timeout — and the service fully recovers after release. RED before
the fix (naked 500 / PhantomTimeoutError). GREEN after.

DETERMINISM: warm up >= 1 buffered row (so uploads.db + the table exist for the
sibling to lock) via a POLL on ``count_rows_by_state`` — never a fixed
wall-clock sleep that races the ~2.2 s ``submit_chain`` round-trips. The lock
hold (9 s) decisively exceeds busy_timeout so a contended writer genuinely
contends.
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

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]

_WARMUP = 2
_BURST = 8
_BODY_BYTES = 2 * 1024
# Must decisively exceed busy_timeout so a contended writer actually contends
# (and, pre-fix, times out into OperationalError) rather than block-then-succeed.
_LOCK_HOLD_SECONDS = 9.0
_PRECONDITION_TIMEOUT_SECONDS = 60.0
_PRECONDITION_POLL_SECONDS = 0.25


def _overrides() -> dict:
    """Hybrid (default) mode, generous saturation so the gate never refuses."""
    return {
        "storage": {"body_store": {"mode": "hybrid"}},
        "saturation": {
            "max_in_flight": _BURST * 8,
            "max_in_flight_bytes": _BURST * _BODY_BYTES * 16,
        },
        "retry": {"worker_count": 2, "poll_interval_ms": 50},
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


def _extract_status(exc: BaseException) -> int | None:
    """Best-effort extraction of an HTTP status code from a client exception."""
    resp = getattr(exc, "response", None)
    code = getattr(resp, "status_code", None)
    if isinstance(code, int):
        return code
    text = str(exc)
    for token in text.replace(",", " ").split():
        if token.isdigit() and len(token) == 3 and token[0] in "45":
            return int(token)
    return None


async def _submit_classified(
    phantom_url: str,
    emulator_url: str,
    bearer: str,
) -> str:
    """Submit one chain; return 'ok' | 'http_<code>' | 'exc:<type>'.

    ``submit_one`` raises on a non-2xx phantom-client response; we catch and
    classify the HTTP status so a 503 (clean retryable) is distinguished from a
    naked 500 (break) or a bare transport error (break).
    """
    from phantom_client import PhantomClient

    try:
        async with PhantomClient(phantom_url) as c:
            await submit_one(
                c,
                emulator_url=emulator_url,
                bearer=bearer,
                body=b"x" * _BODY_BYTES,
                chain_id=uuid4(),
                file_prefix="v6a",
            )
        return "ok"
    except Exception as exc:  # classify the failure surface
        status = _extract_status(exc)
        if status is not None:
            return f"http_{status}"
        return f"exc:{type(exc).__name__}"


async def test_external_lock_admission_returns_clean_503_not_naked_5xx(tmp_path: Path) -> None:
    """Admissions during a held external DB lock return clean 503s (R9-V6-1).

    Every contended admission must be ACCEPTABLE — a success (admitted) or a
    clean 503 ``storage_unavailable`` retryable — never a naked non-503 5xx and
    never a bare ``PhantomTimeoutError``. The process survives and a fresh
    upload admits cleanly after the lock releases.
    """
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    port = allocate_port()
    cfg = write_phantom_config(data_dir=data_dir, bind_port=port, config_overrides=_overrides())
    p = PhantomSubprocess.make(cfg, port)
    lock: _ExternalLock | None = None
    try:
        await p.start()
        bearer = fake_security_token(emu)

        # Warm up: land a couple of rows so uploads.db + schema + table exist
        # for the sibling connection to lock (deterministic precondition).
        for _ in range(_WARMUP):
            await _submit_classified(p.url, emu.url, bearer)
        await _await_buffered_rows(data_dir, want=1)

        # Seize the external write lock and HOLD it past busy_timeout.
        lock = _ExternalLock(db_path_for(data_dir))
        lock.acquire()

        # Fire the contended burst WHILE the lock is held.
        labels = await asyncio.gather(
            *(_submit_classified(p.url, emu.url, bearer) for _ in range(_BURST)),
        )

        lock.release()
        lock = None

        # The process must NOT have died.
        assert p._proc is not None and p._proc.poll() is None, (  # type: ignore[attr-defined]
            "phantom subprocess EXITED during/after the contended burst — a transient "
            f"external DB lock crashed the process. Log tail:\n{p._read_log_tail(60)}"  # type: ignore[attr-defined]
        )

        # Classify: ACCEPTABLE = 'ok' (admitted / blocked-then-succeeded) or
        # 'http_503' (clean retryable). BREAK = any other 5xx (naked 500 from an
        # uncaught OperationalError) or a bare transport error
        # (PhantomTimeoutError — the busy_timeout amplification).
        unacceptable = [lbl for lbl in labels if not (lbl == "ok" or lbl == "http_503")]
        assert not unacceptable, (
            "contended admission(s) returned a non-503 5xx / bare transport error under "
            f"external lock contention: {unacceptable} (all labels: {labels}). Expected "
            "every admission to be a success or a clean 503 storage_unavailable retryable. "
            f"Log tail:\n{p._read_log_tail(60)}"  # type: ignore[attr-defined]
        )

        # Post-release recovery: a fresh submit must cleanly admit.
        recov = await _submit_classified(p.url, emu.url, bearer)
        assert recov == "ok", (
            f"after the external lock released, a fresh upload did not cleanly admit "
            f"(got {recov}) — the service did not fully recover."
        )
    finally:
        if lock is not None:
            lock.release()
        p.terminate()
        await emu.stop()
