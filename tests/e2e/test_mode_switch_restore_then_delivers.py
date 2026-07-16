"""ADR-025 restore round trip DELIVERS the restored upload (adversary round 1, M-2).

The existing mode-switch coverage
(``test_mode_switch_back_up_and_run.py::test_unsafe_switch_to_all_ram_then_restore_roundtrip``)
proves the one-call admin restore puts a buffered row back into the live DB, but
it STOPS at "a row is present on disk" - it never lifts the upstream block to
confirm the restored, still-buffered upload actually DELIVERS. ADR-025's whole
promise is that an unsafe ``all_ram``-over-populated mode switch preserves
undelivered uploads RECOVERABLY: a restore that stages a non-deliverable row
(body bytes not restored, row stuck in a bad state, hashes broken) would satisfy
the existing assertion yet still lose the upload in practice. This test closes
that gap by exercising the FULL durability leg.

The arc, all over the real ``python -m phantom`` subprocess harness on one shared
``data_dir`` (the way an operator's persistent volume is handed between
freshly-deployed images):

1. boot ``hybrid`` with a 1-byte persist threshold and a blocked upstream;
   submit one chain, so the body lands on disk and the row buffers undelivered;
2. switch to ``all_ram`` over the populated ``bodies/`` tree -> back-up-and-run:
   the live DB + body tree move to a ``reason=mode_switch`` backup and the
   instance boots fresh (loud, never refuses);
3. the one-call admin restore stages the backup back (``restart_required``);
4. restart in a disk-backed mode (``all_disk``) so the restored tree is served;
5. LIFT the upstream block and assert the emulator RECEIVES the restored body
   byte-identically - the upload was never lost, merely parked through a
   backup/restore round trip.

Public e2e-light lane (plan § 5.0): generic ``submit_one`` shapes, no
``PHANTOM_ENABLED``.

Falsifier: make the restore stage a non-deliverable row (e.g. drop the body half
of ``restore_mode_switch_backup``, or regress the restored row's body hashes) ->
the restored row never delivers -> the emulator receives nothing -> RED. Equally,
break the restore entirely -> step 4 boots a fresh empty DB, nothing to deliver
-> RED.
"""

from __future__ import annotations

import asyncio
import hashlib
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
    fake_security_token,
    instance_dir,
    submit_one,
)
from tests.e2e.helpers.mode_switch import restart_phantom_on_data_dir

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]

# A 1:1 5xx rate keeps an admitted body buffered (undelivered) across the switch
# + restore; clearing the policy lets the next retry complete it.
_BLOCK_UPSTREAM = FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=1.0)  # type: ignore[call-arg]

# Body size big enough to be unmistakable on the wire, small enough to be fast.
_BODY_BYTES = 4 * 1024

# A long, non-exhausting retry cadence so the buffered row stays RETRYABLE
# (queued/attempting) for the whole window rather than exhausting to ``stored``.
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
    # Keep the reaper out of the way for the test window.
    "retention": {"reaper_interval_seconds": 3600},
}

_PRECONDITION_TIMEOUT_SECONDS = 60.0
_PRECONDITION_POLL_SECONDS = 0.2
_DELIVERY_TIMEOUT_SECONDS = 60.0


def _mode_overrides(mode: str) -> dict[str, object]:
    """Durable-retry config pinned to ``body_store.mode``."""
    return {**_DURABLE_RETRY, "storage": {"body_store": {"mode": mode}}}


def _persist_immediately_overrides(mode: str) -> dict[str, object]:
    """``_mode_overrides`` that also forces every body straight to disk.

    A 1-byte persist threshold makes ``hybrid`` migrate the body to disk at
    admission, so the body survives the cross-restart switch (a hybrid RAM body
    is lost on restart) and gives the ``all_ram`` guard a populated ``bodies/``
    tree to back up.
    """
    overrides = _mode_overrides(mode)
    storage = overrides["storage"]
    assert isinstance(storage, dict)
    storage["persist_trigger"] = {"body_size_threshold_bytes": 1}
    return overrides


async def _submit_blocked_body(
    proc: PhantomSubprocess, emu: EmulatorHandle, chain_id: UUID, body: bytes
) -> None:
    """Submit one chain whose upstream is blocked, so the row buffers undelivered."""
    bearer = fake_security_token(emu)
    async with PhantomClient(proc.url) as client:
        await submit_one(
            client,
            emulator_url=emu.url,
            bearer=bearer,
            body=body,
            chain_id=chain_id,
            file_prefix="restore-delivers",
        )


async def _await_buffered(data_dir: Path, *, minimum: int = 1) -> None:
    """Poll the on-disk census until at least ``minimum`` rows are buffered."""
    deadline = time.monotonic() + _PRECONDITION_TIMEOUT_SECONDS
    total = 0
    while time.monotonic() < deadline:
        total = sum((await count_rows_by_state(data_dir)).values())
        if total >= minimum:
            return
        await asyncio.sleep(_PRECONDITION_POLL_SECONDS)  # pre-commit-allow: sleep
    raise AssertionError(
        f"precondition not met: only {total} rows buffered after "
        f"{_PRECONDITION_TIMEOUT_SECONDS}s (need >= {minimum})"
    )


async def _await_body_on_disk(bodies_root: Path) -> None:
    """Poll until at least one COMPLETED body file lands under ``bodies_root``.

    Excludes the ``.tmp/`` staging directory (FileBodyStore's atomic-rename
    staging): a staged entry is not a persisted body and can vanish between
    scan and use, so counting it satisfies the precondition prematurely.
    Same fix as the back-up-and-run module's helper.
    """
    deadline = time.monotonic() + _PRECONDITION_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        completed = (
            p
            for p in bodies_root.rglob("*")
            if p.is_file() and ".tmp" not in p.relative_to(bodies_root).parts
        )
        if any(completed):
            return
        await asyncio.sleep(_PRECONDITION_POLL_SECONDS)  # pre-commit-allow: sleep
    raise AssertionError(f"no body file landed under {bodies_root} within the precondition window")


async def _await_received_body(emu: EmulatorHandle, chain_id: UUID, expected: bytes) -> None:
    """Poll the emulator until it accepts the restored chain's body.

    Matches on the emulator's ``ReceivedEntry`` shape: the body is identified by
    ``metadata_kvs["phantom_local_uuid"]`` (``submit_one`` stamps it to
    ``str(chain_id)``) AND the SHA-256 ``body_hash`` of the accepted bytes, so a
    match means the EXACT restored upload arrived byte-identically (transparent
    on the wire, invariant #11) - not merely that some upload landed.
    """
    expected_hash = hashlib.sha256(expected).hexdigest()
    expected_uuid = str(chain_id)
    deadline = time.monotonic() + _DELIVERY_TIMEOUT_SECONDS
    # ``EmulatorHandle.received()`` is typed ``list[Any]`` by the harness; each
    # entry is the emulator's ``ReceivedEntry`` (``.metadata_kvs`` / ``.body_hash``).
    seen = emu.received()
    while time.monotonic() < deadline:
        seen = emu.received()
        for entry in seen:
            if (
                entry.metadata_kvs.get("phantom_local_uuid") == expected_uuid
                and entry.body_hash == expected_hash
            ):
                return
        await asyncio.sleep(_PRECONDITION_POLL_SECONDS)  # pre-commit-allow: sleep
    raise AssertionError(
        f"upstream never received the restored body (chain {expected_uuid}, "
        f"sha256={expected_hash[:12]}...) within {_DELIVERY_TIMEOUT_SECONDS}s; "
        f"{len(seen)} record(s) accepted"
    )


async def test_unsafe_switch_backup_then_restore_then_delivers(tmp_path: Path) -> None:
    """A buffered upload survives an unsafe mode switch backup + restore AND delivers.

    The full ADR-025 durability leg: back up (mode switch), restore (admin), then
    deliver once the upstream recovers. The restored upload must reach the
    emulator byte-identically - proving the restore stages a genuinely
    DELIVERABLE row, not merely a present one.
    """
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    body = secrets.token_bytes(_BODY_BYTES)
    procs: list[PhantomSubprocess] = []
    try:
        emu.inject_failure(_BLOCK_UPSTREAM)

        # 1. hybrid + 1-byte persist threshold => the body lands on disk and the
        #    row buffers undelivered (upstream blocked).
        p1 = await restart_phantom_on_data_dir(
            data_dir, config_overrides=_persist_immediately_overrides("hybrid")
        )
        procs.append(p1)
        chain_id = uuid4()
        await _submit_blocked_body(p1, emu, chain_id, body)
        await _await_buffered(data_dir, minimum=1)
        await _await_body_on_disk(instance_dir(data_dir) / "bodies")
        p1.terminate()

        # 2. Switch to all_ram over the populated tree => back-up-and-run.
        p2 = await restart_phantom_on_data_dir(
            data_dir, config_overrides=_mode_overrides("all_ram")
        )
        procs.append(p2)
        # No cross-second pause needed: backup identity is a uuid (cycle-7
        # seam 1), so same-second names cannot collide by construction. This
        # test exercises the DELIVERY leg; the same-second case has its own
        # dedicated test.
        # quarantine inventory + restore are admin routes on the single
        # listener (p2.url).
        async with PhantomClient(p2.url) as client:
            inv = await client.get_quarantine_inventory(instance=DEFAULT_INSTANCE)
            mode_switch = [e for e in inv.quarantines if e.reason == "mode_switch"]
            assert mode_switch, f"all_ram switch must back up; inventory={inv.quarantines!r}"
            backup_id = mode_switch[0].backup_id
            assert backup_id is not None
            # The live tree booted fresh (the buffered row is now only in the backup).
            assert sum((await count_rows_by_state(data_dir)).values()) == 0, (
                "all_ram boot must start fresh; the buffered row should be in the backup only"
            )

            # 3. One-call restore by IDENTITY stages the backup back.
            restore = await client.restore_quarantine_backup(
                backup_id=backup_id, instance=DEFAULT_INSTANCE
            )
            assert restore.restart_required is True
        p2.terminate()

        # 4. Restart in a disk-backed mode so the restored tree is served.
        p3 = await restart_phantom_on_data_dir(
            data_dir, config_overrides=_mode_overrides("all_disk")
        )
        procs.append(p3)
        assert sum((await count_rows_by_state(data_dir)).values()) >= 1, (
            "the restore must put the buffered row back into the live DB before delivery"
        )

        # 5. THE DURABILITY LEG: lift the block; the restored upload delivers.
        emu.clear_failures()
        await _await_received_body(emu, chain_id, body)
    finally:
        for proc in procs:
            proc.terminate()
        await emu.stop()
