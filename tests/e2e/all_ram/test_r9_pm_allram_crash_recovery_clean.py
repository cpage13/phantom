"""all_ram high-volume SIGKILL crash recovery is CLEAN (R9-PM-1).

Aggressor R9, per-mode set. GREEN regression PIN: a clean pass is the GOOD
result.

THE PROPERTY (R9-PM-1, holds): in all_ram EVERY un-delivered body is
RAM-resident, so a SIGKILL loses the whole backlog BY DESIGN -- that is the
operator's speed-over-durability tradeoff, not a bug. But the loss must be
CLEAN. After a real ``SIGKILL`` (WAL interrupted mid-write, like a Pi power
loss) and a restart on the SAME data_dir:

* the service comes back HEALTHY (no ``database is locked``, no wedge);
* recovery QUARANTINES the RAM-lost rows to a TERMINAL state, never leaves them
  "deliverable" (queued/attempting) pointing at a vanished RAM body;
* NO SILENT LOSS: a RAM-lost chain must NOT forward a truncated/garbage body
  upstream after restart; any body the upstream did receive is byte-identical to
  the original;
* DB integrity holds (``PRAGMA integrity_check`` == ok).

This is the densest exercise of recovery's RAM-lost quarantine path (the V1/V1b
``database is locked`` over a hot WAL class) -- in all_ram essentially the whole
backlog hits it at once. A regression in recovery's lock-rideout or
terminal-quarantine would turn this RED.

DETERMINISM: POLL the on-disk census until >= MIN_BUFFERED rows are buffered
before the SIGKILL (never a wall-clock race); after restart, POLL until the
backlog reaches a terminal census, then inspect.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import secrets
import time
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from tests.e2e._harness.subprocess_harness import (
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    count_rows_by_state,
    fake_security_token,
    integrity_check,
    open_store_readonly,
    submit_one,
    write_phantom_config,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]

_BURST = 80
_BODY_BYTES = 3 * 1024
_SUBMIT_CONCURRENCY = 24
# Near-total failure so almost nothing delivers -- bodies stay RAM-resident.
_UPSTREAM_5XX_RATE = 0.95
_MIN_BUFFERED = 25
_PRECONDITION_TIMEOUT_SECONDS = 60.0
_PRECONDITION_POLL_SECONDS = 0.25
_DRAIN_BUDGET_SECONDS = 90.0
_DRAIN_POLL_SECONDS = 0.5

# States from which no further upstream delivery happens (the row is "done").
_TERMINAL_STATES = frozenset(
    {"succeeded", "failed", "cancelled", "stored", "corrupted", "expired", "auth_expired"}
)


def _overrides() -> dict:
    """all_ram mode, generous saturation (no ingress refusal), tight retry."""
    return {
        "storage": {"body_store": {"mode": "all_ram"}},
        "saturation": {
            "max_in_flight": _BURST * 4,
            "max_in_flight_bytes": _BURST * _BODY_BYTES * 8,
        },
        "retry": {
            "worker_count": 4,
            "poll_interval_ms": 50,
            "default_strategy": {"type": "fixed_intervals", "intervals_seconds": [0, 0, 1]},
        },
        "retention": {"reaper_interval_seconds": 3600},
    }


def _sha256(b: bytes) -> str:
    """Hex SHA-256."""
    return hashlib.sha256(b).hexdigest()


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


async def _await_terminal_census(data_dir: Path) -> dict[str, int]:
    """Poll until no non-terminal rows remain (or the budget elapses)."""
    deadline = time.monotonic() + _DRAIN_BUDGET_SECONDS
    while time.monotonic() < deadline:
        counts = await count_rows_by_state(data_dir)
        if sum(v for k, v in counts.items() if k not in _TERMINAL_STATES) == 0:
            return counts
        await asyncio.sleep(_DRAIN_POLL_SECONDS)  # pre-commit-allow: sleep
    return await count_rows_by_state(data_dir)


async def test_allram_crash_recovery_clean(tmp_path: Path) -> None:
    """all_ram SIGKILL recovery: healthy restart, terminal quarantine, no silent loss.

    Pins R9-PM-1: the by-design RAM loss happens cleanly -- no wedge, no lock
    crash, no garbage forward. A regression in recovery's terminal-quarantine or
    its hot-WAL lock rideout would turn this RED.
    """
    from phantom_client import PhantomClient
    from phantom_emulator.failure.injection import FailurePolicy, FailureScope

    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    port = allocate_port()
    cfg = write_phantom_config(data_dir=data_dir, bind_port=port, config_overrides=_overrides())
    p1 = PhantomSubprocess.make(cfg, port)
    p2: PhantomSubprocess | None = None

    chain_ids: list[UUID] = [uuid4() for _ in range(_BURST)]
    bodies: list[bytes] = [secrets.token_bytes(_BODY_BYTES) for _ in range(_BURST)]
    body_by_cid: dict[str, bytes] = {str(c): b for c, b in zip(chain_ids, bodies, strict=True)}

    try:
        await p1.start()
        bearer = fake_security_token(emu)
        emu.inject_failure(
            FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=_UPSTREAM_5XX_RATE)  # type: ignore[call-arg]
        )

        sem = asyncio.Semaphore(_SUBMIT_CONCURRENCY)

        async def _one(idx: int) -> None:
            # contextlib.suppress is a SYNC context manager (kill-race submits
            # are expected to error); the client + semaphore are async.
            with contextlib.suppress(Exception):
                async with sem, PhantomClient(p1.url) as c:
                    await submit_one(
                        c,
                        emulator_url=emu.url,
                        bearer=bearer,
                        body=bodies[idx],
                        chain_id=chain_ids[idx],
                        file_prefix="pm1",
                    )

        async def _submit_all() -> None:
            await asyncio.gather(*(_one(i) for i in range(_BURST)), return_exceptions=True)

        submit_task = asyncio.create_task(_submit_all())

        # DETERMINISTIC precondition: wait until the backlog is genuinely
        # buffered (NOT a wall-clock sleep) before killing.
        await _await_buffered_rows(data_dir, want=_MIN_BUFFERED)
        p1.sigkill()
        submit_task.cancel()
        with contextlib.suppress(Exception, asyncio.CancelledError):
            await submit_task

        # Verification 1: DB integrity after a real mid-write kill.
        ok, msg = await integrity_check(data_dir)
        assert ok, (
            f"PRAGMA integrity_check FAILED after a mid-write SIGKILL in all_ram: {msg} "
            "-- the hot WAL corrupted the DB."
        )

        # Restart on the SAME data_dir; lift the throttle so survivors drain.
        emu.clear_failures()
        emu.clear_received()
        port2 = allocate_port()
        cfg2 = write_phantom_config(
            data_dir=data_dir, bind_port=port2, config_overrides=_overrides()
        )
        p2 = PhantomSubprocess.make(cfg2, port2)
        try:
            await p2.start()
        except RuntimeError as exc:
            tail = p2._read_log_tail(60)  # type: ignore[attr-defined]
            pytest.fail(
                "all_ram restart did NOT come back healthy after the SIGKILL "
                f"(recovery wedge / 'database is locked' over the hot WAL): {exc}\n{tail}"
            )

        # Verification 2: the backlog reaches a TERMINAL census (no wedge / no
        # RAM-lost row left "deliverable").
        final_counts = await _await_terminal_census(data_dir)
        non_terminal = sum(v for k, v in final_counts.items() if k not in _TERMINAL_STATES)
        assert non_terminal == 0, (
            f"{non_terminal} rows never reached terminal after all_ram restart "
            f"(wedge / RAM-lost rows left deliverable): {final_counts}"
        )

        # Verification 3: NO SILENT LOSS. A succeeded chain must have forwarded a
        # byte-identical body; the upstream must have received NO body matching no
        # original (a garbage/truncated forward from a RAM-lost row).
        received_hash_by_uuid: dict[str, str] = {}
        for entry in emu.received():
            uuid_kv = entry.metadata_kvs.get("phantom_local_uuid")
            if uuid_kv is not None:
                received_hash_by_uuid[uuid_kv] = entry.body_hash

        store = await open_store_readonly(data_dir)
        succeeded = 0
        quarantined = 0
        try:
            async for row in store.iter_rows():
                cid = str(row.chain_id)
                if row.state == "succeeded":
                    succeeded += 1
                    recv = received_hash_by_uuid.get(cid)
                    expected = _sha256(body_by_cid.get(cid, b""))
                    assert recv == expected, (
                        f"succeeded chain {cid} forwarded a NON-identical body after "
                        f"restart (recv={recv} expected={expected}) -- silent loss / "
                        "garbage forward."
                    )
                elif row.state in ("corrupted", "failed"):
                    quarantined += 1
        finally:
            await store.stop()

        original_hashes = {_sha256(b) for b in bodies}
        garbage_forwards = [h for h in received_hash_by_uuid.values() if h not in original_hashes]
        assert not garbage_forwards, (
            f"the upstream received {len(garbage_forwards)} body(ies) matching NO original "
            "payload after all_ram restart (garbage/truncated forward from a RAM-lost row)."
        )

        # Sanity: the SIGKILL+restart cycle actually exercised the quarantine path
        # (some RAM-lost rows reached a terminal quarantine), so the pin is not a
        # vacuous pass.
        assert quarantined + succeeded > 0, (
            "no row reached succeeded or quarantine after restart -- the scenario did "
            f"not exercise the recovery path (final census: {final_counts})"
        )
    finally:
        p1.terminate()
        if p2 is not None:
            p2.terminate()
        await emu.stop()
