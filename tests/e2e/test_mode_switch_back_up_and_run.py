"""End-to-end mode-switch coverage (plan § 5 Part 5.A).

Item-4's conversion proved the all_ram-over-populated guard backs up and runs;
this module adds the surrounding positive coverage Part 5.A calls for, all
through the REAL boot path (the killable ``python -m phantom`` subprocess harness
restarted on the SAME ``data_dir``, the way an operator flips a mode):

* :func:`test_safe_switch_hybrid_all_disk_roundtrip_no_backup` - a SAFE
  switch (``hybrid`` <-> ``all_disk``, both disk-backed) must NOT back up:
  no ``mode_switch`` artifact appears and a row buffered before the switch
  still delivers after it.
* :func:`test_unsafe_switch_to_all_ram_then_restore_roundtrip` - the UNSAFE
  switch (``-> all_ram`` over a populated tree) backs up + boots fresh
  (backup visible ``reason=mode_switch``), then the one-call admin restore
  (§ 1.5) stages the backup back and a restart in a disk-backed mode serves
  the restored row.
* :func:`test_service_always_boots_for_every_mode_and_prior_state` - the
  always-boots invariant across (configured mode x prior on-disk state).
* :func:`test_crash_mid_backup_is_reconciled_on_next_boot` - a backup halted
  mid-move (case (a), every halt point) is finished forward by
  :func:`phantom.storage.integrity.reconcile_interrupted_backup_move` on the
  next real boot, which still comes up; the body bytes survive.

Public e2e-light lane: drives only the generic subprocess
``submit_one`` shape + the emulator (plan § 5.0). The
subprocess harness is deterministic and fast, so these run in the default lane
like the existing real-SIGKILL crash tests.
"""

from __future__ import annotations

import asyncio
import secrets
import time
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._harness.subprocess_harness import (
    DEFAULT_INSTANCE,
    EmulatorHandle,
    PhantomSubprocess,
    boot_emulator,
    count_rows_by_state,
    db_path_for,
    fake_security_token,
    instance_dir,
    submit_one,
)
from tests.e2e.helpers.mode_switch import (
    BackupHaltStep,
    halt_mode_switch_backup_after,
    restart_phantom_on_data_dir,
)

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]

_BODY_BYTES = 2 * 1024
_PRECONDITION_TIMEOUT_SECONDS = 60.0
_PRECONDITION_POLL_SECONDS = 0.2
_DELIVERY_TIMEOUT_SECONDS = 60.0

# A high 5xx rate keeps an admitted body un-delivered (buffered) across a mode
# switch so the "still delivers after the switch" half is observable; lifting
# the failure lets the sender complete it.
_BLOCK_UPSTREAM = FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=1.0)  # type: ignore[call-arg]

# A non-exhausting retry cadence: many short intervals so a blocked row stays
# in a RETRYABLE state (queued/attempting) for the whole test window rather than
# exhausting its budget to the terminal ``stored`` state. Once the upstream
# recovers the next ~1 s attempt delivers it. Both halves of the safe-switch
# test depend on the row still being retryable after the switch.
_RETRY_BUDGET_ATTEMPTS = 600
_DURABLE_RETRY: dict[str, object] = {
    "retry": {
        "worker_count": 4,
        "poll_interval_ms": 50,
        "default_strategy": {
            "type": "fixed_intervals",
            "intervals_seconds": [1] * _RETRY_BUDGET_ATTEMPTS,
        },
    },
    "retention": {"reaper_interval_seconds": 3600},
}


def _mode_overrides(mode: str) -> dict[str, object]:
    """Durable-retry config pinned to ``body_store.mode``."""
    return {**_DURABLE_RETRY, "storage": {"body_store": {"mode": mode}}}


def _persist_immediately_overrides(mode: str) -> dict[str, object]:
    """``_mode_overrides`` that also forces every body straight to disk.

    A 1-byte persist threshold makes ``hybrid`` migrate the body to disk at
    admission. This matters for any cross-restart test: a hybrid body left in
    RAM is lost on restart, so the body must be on disk to survive a switch
    (and, for the unsafe case, to give the all_ram guard a populated
    ``bodies/`` tree to back up).
    """
    overrides = _mode_overrides(mode)
    storage = overrides["storage"]
    assert isinstance(storage, dict)
    storage["persist_trigger"] = {"body_size_threshold_bytes": 1}
    return overrides


async def _await_buffered(data_dir: Path, *, minimum: int = 1) -> int:
    """Poll the on-disk census until at least ``minimum`` rows are buffered."""
    deadline = time.monotonic() + _PRECONDITION_TIMEOUT_SECONDS
    total = 0
    while time.monotonic() < deadline:
        census = await count_rows_by_state(data_dir)
        total = sum(census.values())
        if total >= minimum:
            return total
        await asyncio.sleep(_PRECONDITION_POLL_SECONDS)  # pre-commit-allow: sleep
    raise AssertionError(
        f"precondition not met: only {total} rows buffered after "
        f"{_PRECONDITION_TIMEOUT_SECONDS}s (need >= {minimum})"
    )


async def _await_delivery(emu: EmulatorHandle, *, minimum: int = 1) -> None:
    """Poll the emulator until it has accepted at least ``minimum`` bodies."""
    deadline = time.monotonic() + _DELIVERY_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if len(emu.received()) >= minimum:
            return
        await asyncio.sleep(_PRECONDITION_POLL_SECONDS)  # pre-commit-allow: sleep
    raise AssertionError(
        f"upstream never received >= {minimum} bodies within {_DELIVERY_TIMEOUT_SECONDS}s "
        f"(got {len(emu.received())})"
    )


async def _submit_blocked_body(
    proc: PhantomSubprocess, emu: EmulatorHandle, chain_id: UUID
) -> None:
    """Submit one chain whose upstream is blocked, so the row buffers."""
    bearer = fake_security_token(emu)
    async with PhantomClient(proc.url) as client:
        await submit_one(
            client,
            emulator_url=emu.url,
            bearer=bearer,
            body=secrets.token_bytes(_BODY_BYTES),
            chain_id=chain_id,
            file_prefix="modeswitch",
        )


def _has_mode_switch_sibling(data_dir: Path) -> bool:
    """True if a ``.mode_switch.`` backup artifact exists in the instance root."""
    inst_root = instance_dir(data_dir)
    return any(".mode_switch." in p.name for p in inst_root.iterdir())


async def test_safe_switch_hybrid_all_disk_roundtrip_no_backup(tmp_path: Path) -> None:
    """A hybrid <-> all_disk switch never backs up and never drops a buffered row.

    Both modes are disk-backed, so ``check_body_store_mode`` is a no-op: no
    ``mode_switch`` artifact must appear. A row buffered (upstream down) before
    the switch must still deliver once the upstream recovers, proving the safe
    switch preserves in-flight work.
    """
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    procs: list[PhantomSubprocess] = []
    try:
        emu.inject_failure(_BLOCK_UPSTREAM)

        # Boot hybrid, buffer one row (upstream blocked). Persist-immediately so
        # the body is on disk and survives the cross-restart switch (a hybrid
        # RAM body would be lost on restart, masking the delivery assertion).
        p1 = await restart_phantom_on_data_dir(
            data_dir, config_overrides=_persist_immediately_overrides("hybrid")
        )
        procs.append(p1)
        chain_id = uuid4()
        await _submit_blocked_body(p1, emu, chain_id)
        await _await_buffered(data_dir, minimum=1)
        await _await_body_on_disk(instance_dir(data_dir) / "bodies")
        p1.terminate()

        # Switch to all_disk on the SAME data_dir. No backup; the row survives.
        p2 = await restart_phantom_on_data_dir(
            data_dir, config_overrides=_mode_overrides("all_disk")
        )
        procs.append(p2)
        assert not _has_mode_switch_sibling(data_dir), (
            "a safe hybrid->all_disk switch must NOT create a mode_switch backup"
        )
        # quarantine inventory is an admin route on the single listener
        # (p2.url).
        async with PhantomClient(p2.url) as client:
            inv = await client.get_quarantine_inventory(instance=DEFAULT_INSTANCE)
            assert not [e for e in inv.quarantines if e.reason == "mode_switch"], inv.quarantines
        assert sum((await count_rows_by_state(data_dir)).values()) >= 1, "buffered row vanished"
        p2.terminate()

        # Switch back to hybrid; still no backup. Now let the upstream recover
        # and confirm the buffered row finally delivers.
        p3 = await restart_phantom_on_data_dir(data_dir, config_overrides=_mode_overrides("hybrid"))
        procs.append(p3)
        assert not _has_mode_switch_sibling(data_dir), (
            "a safe all_disk->hybrid switch-back must NOT create a mode_switch backup"
        )
        emu.clear_failures()
        await _await_delivery(emu, minimum=1)
    finally:
        for proc in procs:
            proc.terminate()
        await emu.stop()


async def test_unsafe_switch_to_all_ram_then_restore_roundtrip(tmp_path: Path) -> None:
    """all_ram over a populated tree backs up; the one-call restore brings it back.

    hybrid persists a body to disk; switching to all_ram backs the live data up
    to a ``mode_switch`` pair (visible ``reason=mode_switch``) and boots fresh.
    The admin restore (§ 1.5) stages the backup back (``restart_required``), and
    a restart in a disk-backed mode serves the restored row again.
    """
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    procs: list[PhantomSubprocess] = []
    try:
        emu.inject_failure(_BLOCK_UPSTREAM)

        # hybrid + 1-byte persist threshold => the body lands on disk.
        p1 = await restart_phantom_on_data_dir(
            data_dir, config_overrides=_persist_immediately_overrides("hybrid")
        )
        procs.append(p1)
        chain_id = uuid4()
        await _submit_blocked_body(p1, emu, chain_id)
        await _await_buffered(data_dir, minimum=1)
        bodies_root = instance_dir(data_dir) / "bodies"
        await _await_body_on_disk(bodies_root)
        p1.terminate()

        # Switch to all_ram: back-up-and-run. The backup is visible via the SDK.
        p2 = await restart_phantom_on_data_dir(
            data_dir, config_overrides=_mode_overrides("all_ram")
        )
        procs.append(p2)
        # No cross-second pause needed: backup identity is a uuid (cycle-7
        # seam 1), so a restore's interim backup landing in the same
        # wall-clock second as the boot backup cannot collide by construction.
        # quarantine inventory + restore are admin routes on the single
        # listener (p2.url).
        async with PhantomClient(p2.url) as client:
            inv = await client.get_quarantine_inventory(instance=DEFAULT_INSTANCE)
            mode_switch = [e for e in inv.quarantines if e.reason == "mode_switch"]
            assert mode_switch, f"all_ram switch must back up; inventory={inv.quarantines!r}"
            # ONE manifested backup entry carrying the whole pair (seam 2).
            assert len(mode_switch) == 1, "one backup => one inventory entry"
            backup = mode_switch[0]
            assert backup.anomaly is False
            assert backup.backup_id is not None
            assert backup.has_db and backup.has_body, "the backup pair must be on disk"
            # The live tree booted fresh.
            assert sum((await count_rows_by_state(data_dir)).values()) == 0, "live DB not fresh"

            # One-call restore by IDENTITY stages the backup back; restart required.
            restore = await client.restore_quarantine_backup(
                backup_id=backup.backup_id, instance=DEFAULT_INSTANCE
            )
            assert restore.restart_required is True
        p2.terminate()

        # Restart in a disk-backed mode: the restored row is live again.
        p3 = await restart_phantom_on_data_dir(
            data_dir, config_overrides=_mode_overrides("all_disk")
        )
        procs.append(p3)
        assert sum((await count_rows_by_state(data_dir)).values()) >= 1, (
            "the restored backup must put the buffered row back into the live DB"
        )
    finally:
        for proc in procs:
            proc.terminate()
        await emu.stop()


@pytest.mark.parametrize("configured_mode", ["hybrid", "all_disk", "all_ram"])
@pytest.mark.parametrize("prior_mode", ["fresh", "hybrid"])
async def test_service_always_boots_for_every_mode_and_prior_state(
    tmp_path: Path, configured_mode: str, prior_mode: str
) -> None:
    """The service ALWAYS boots for every (configured mode x prior on-disk state).

    ``prior_mode='fresh'`` boots over an empty dir; ``prior_mode='hybrid'``
    first lays down a real hybrid-persisted body, then boots the configured
    mode over it. Every combination must come up healthy: a safe switch is a
    no-op, and the unsafe all_ram-over-populated switch backs up and boots
    fresh (never refuses, never crash-loops).
    """
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    procs: list[PhantomSubprocess] = []
    try:
        if prior_mode == "hybrid":
            emu.inject_failure(_BLOCK_UPSTREAM)
            p0 = await restart_phantom_on_data_dir(
                data_dir, config_overrides=_persist_immediately_overrides("hybrid")
            )
            procs.append(p0)
            await _submit_blocked_body(p0, emu, uuid4())
            await _await_buffered(data_dir, minimum=1)
            await _await_body_on_disk(instance_dir(data_dir) / "bodies")
            p0.terminate()
            emu.clear_failures()

        # The assertion is simply that .start() returns (health answered).
        proc = await restart_phantom_on_data_dir(
            data_dir, config_overrides=_mode_overrides(configured_mode)
        )
        procs.append(proc)
        async with PhantomClient(proc.url) as client:
            health = await client.get_health()
            assert health.status == "ok"
    finally:
        for proc in procs:
            proc.terminate()
        await emu.stop()


@pytest.mark.parametrize(
    "stop_after",
    [BackupHaltStep.MARKER_ONLY, BackupHaltStep.BODY_MOVED, BackupHaltStep.DB_MOVED],
)
async def test_crash_mid_backup_is_reconciled_on_next_boot(
    tmp_path: Path, stop_after: BackupHaltStep
) -> None:
    """A backup halted mid-move is finished forward on the next boot (case (a)).

    Seeds a real hybrid-persisted data dir, then drives § 1's marked backup
    mover BY HAND and halts after each step (marker-only / body-moved /
    db-moved), leaving a genuinely half-finished backup + its in-progress
    marker on disk. The next REAL boot (``python -m phantom``) runs
    ``reconcile_interrupted_backup_move``, which must finish the move forward
    and let the service come up healthy. The persisted body bytes must survive
    in the completed backup; the live tree boots clean.

    Falsifier: drop the reconcile step from the lifespan -> the half-moved
    backup is never completed and either the boot trips over the marker or the
    live tree is left inconsistent.
    """
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    procs: list[PhantomSubprocess] = []
    try:
        # Seed: one hybrid-persisted body on disk.
        emu.inject_failure(_BLOCK_UPSTREAM)
        p0 = await restart_phantom_on_data_dir(
            data_dir, config_overrides=_persist_immediately_overrides("hybrid")
        )
        procs.append(p0)
        await _submit_blocked_body(p0, emu, uuid4())
        await _await_buffered(data_dir, minimum=1)
        body_file = await _await_body_on_disk(instance_dir(data_dir) / "bodies")
        persisted_bytes = body_file.read_bytes()
        p0.terminate()

        # Halt a mode_switch backup partway, leaving the marker for the reconciler.
        halt = halt_mode_switch_backup_after(data_dir, stop_after=stop_after)
        assert halt.marker_path.exists(), "the in-progress marker must be present pre-boot"

        # Next REAL boot reconciles the interrupted move, then comes up. Boot a
        # disk-backed mode so the reconciled live tree is served (not re-backed).
        proc = await restart_phantom_on_data_dir(
            data_dir, config_overrides=_mode_overrides("all_disk")
        )
        procs.append(proc)

        # The reconciler cleared the marker and completed the backup pair.
        assert not halt.marker_path.exists(), "boot must clear the in-progress marker"
        assert halt.quarantined_db.exists(), "boot must finish the DB half of the backup"
        assert halt.quarantined_bodies.is_dir(), "boot must finish the body half of the backup"
        # The persisted bytes survive in the completed backup body tree.
        preserved = [
            p
            for p in halt.quarantined_bodies.rglob("*")
            if p.is_file() and p.read_bytes() == persisted_bytes
        ]
        assert preserved, "the persisted body bytes must survive the reconciled backup"
        # The service is healthy and the live DB is the fresh post-backup tree.
        async with PhantomClient(proc.url) as client:
            assert (await client.get_health()).status == "ok"
        assert (
            not db_path_for(data_dir).exists()
            or sum((await count_rows_by_state(data_dir)).values()) == 0
        ), "the live tree must be fresh after the backup completed"
    finally:
        for proc in procs:
            proc.terminate()
        await emu.stop()


async def _await_body_on_disk(bodies_root: Path) -> Path:
    """Poll until at least one COMPLETED body file lands under ``bodies_root``.

    Excludes the ``.tmp/`` staging directory: FileBodyStore stages writes
    there and atomically renames them into the shard tree, so a staging entry
    can vanish between this scan and the caller's read. CI first lost that
    race on the Linux runners (FileNotFoundError on a ``.tmp`` path); a
    staged file is not a persisted body.
    """
    deadline = time.monotonic() + _PRECONDITION_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        files = [
            p
            for p in bodies_root.rglob("*")
            if p.is_file() and ".tmp" not in p.relative_to(bodies_root).parts
        ]
        if files:
            return files[0]
        await asyncio.sleep(_PRECONDITION_POLL_SECONDS)  # pre-commit-allow: sleep
    raise AssertionError(f"no body file landed under {bodies_root} within the precondition window")
