"""DB-integrity fail_open=false aborts boot on corruption (TEST-8).

The default posture (``db_integrity.fail_open=true`` -> quarantine the
corrupt DB and serve a fresh empty one) is e2e-covered in a real
subprocess by ``crash_recovery/test_corrupt_db_quarantine_boot``. The
opt-in fail-CLOSED posture (``fail_open=false`` -> quarantine, then ABORT
startup) is pinned only at the unit/integration tier. This module proves
the fail-closed path in a REAL ``python -m phantom`` subprocess, the
exact deployment surface an operator who pins ``fail_open=false`` relies
on.

Sequence (mirrors the quarantine-boot e2e, flipped to fail-closed):

1. Boot a real Phantom subprocess; buffer a few uploads on disk (5xx
   upstream keeps them un-delivered, so the DB carries real rows).
2. Clean stop (SIGTERM -> WAL checkpointed -> a valid DB on disk).
3. Overwrite ``<data>/primary/uploads.db`` with garbage and drop the
   -wal/-shm siblings (the power-loss analogue the gate must catch).
4. Reboot on the SAME data dir with ``db_integrity.fail_open=false``.

Assertions (both halves of the fail-closed contract):

* the reboot ABORTS - ``.start()`` raises because the process exits
  early and never answers health (the loud, visible boot failure an
  operator wants), and the exit code is non-zero; and
* the quarantine STILL fired before the abort - a
  ``uploads.corrupted.<iso>.db`` artifact exists under the per-instance
  dir (the config contract: "the quarantine still fires; the process
  exits after"), so the corrupt image is preserved for retrieval rather
  than left in place or silently dropped.

Falsifier: flip the override to ``fail_open=true`` -> the reboot becomes
healthy and ``.start()`` does not raise -> RED.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from uuid import uuid4

import pytest

from tests.e2e._harness.subprocess_harness import (
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    fake_security_token,
    instance_dir,
    submit_one,
    write_phantom_config,
)

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]

# A few buffered uploads so the pre-corruption DB carries real rows; 5xx
# keeps them un-delivered and durably on disk.
PRECORRUPT_UPLOADS: int = 3
BODY_BYTES: bytes = b"phantom-fail-closed-e2e-body"

# The fail-CLOSED override: quarantine the corrupt DB, then abort boot.
# db_integrity is nested under the storage block (StorageCfg.db_integrity),
# not at the top level - a top-level key is rejected by Settings'
# extra="forbid".
FAIL_CLOSED_OVERRIDE: dict[str, dict[str, dict[str, bool]]] = {
    "storage": {"db_integrity": {"fail_open": False}}
}

# Pattern to pull the exit code out of the harness's early-exit message
# ("phantom subprocess exited early (code=<N>); ...").
_EXIT_CODE_RE: re.Pattern[str] = re.compile(r"code=(-?\d+)")


async def test_fail_closed_aborts_boot_after_quarantine(tmp_path: Path) -> None:
    """A corrupt DB with fail_open=false quarantines then aborts the boot."""
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    inst_dir = instance_dir(data_dir)  # <data>/primary
    db_path = inst_dir / "uploads.db"

    port1 = allocate_port()
    cfg1 = write_phantom_config(data_dir=data_dir, bind_port=port1)
    p1 = PhantomSubprocess.make(cfg1, port1)
    p2: PhantomSubprocess | None = None
    try:
        from phantom_client import PhantomClient
        from phantom_emulator.failure.injection import FailurePolicy, FailureScope

        # 1. Boot + buffer a few uploads (5xx keeps them on disk).
        await p1.start()
        bearer = fake_security_token(emu)
        emu.inject_failure(
            FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=1.0)  # type: ignore[call-arg]
        )
        async with PhantomClient(p1.url) as client:
            for _ in range(PRECORRUPT_UPLOADS):
                await submit_one(
                    client,
                    emulator_url=emu.url,
                    bearer=bearer,
                    body=BODY_BYTES,
                    chain_id=uuid4(),
                    file_prefix="fail-closed",
                )
        assert db_path.exists(), f"expected a live DB at {db_path} after buffering uploads"

        # 2. Clean stop (SIGTERM) -> WAL checkpointed -> valid DB on disk.
        p1.terminate()

        # 3. Corrupt the on-disk DB (the power-loss analogue): overwrite the
        #    whole main file with garbage AND drop the -wal/-shm siblings, so
        #    there is no recoverable WAL for SQLite's integrity_check to shrug
        #    the smudge off with.
        original_size = db_path.stat().st_size
        db_path.write_bytes(b"\xde\xad\xbe\xef" * (original_size // 4 + 1))
        for sibling_suffix in ("-wal", "-shm"):
            sibling = db_path.with_name(db_path.name + sibling_suffix)
            if sibling.exists():
                sibling.unlink()

        # 4. Reboot on the SAME data dir + a fresh port, fail-CLOSED.
        port2 = allocate_port()
        cfg2 = write_phantom_config(
            data_dir=data_dir,
            bind_port=port2,
            config_overrides=FAIL_CLOSED_OVERRIDE,
        )
        p2 = PhantomSubprocess.make(cfg2, port2)

        # Half 1: the boot ABORTS - the process exits early and never
        # answers health, so start() raises loudly.
        with pytest.raises(RuntimeError) as excinfo:
            await p2.start()
        msg = str(excinfo.value)
        assert "exited early" in msg.lower(), (
            f"expected a fail-closed early-exit at boot; got: {excinfo.value!r}"
        )
        # The exit code is non-zero (an abort, not a clean shutdown).
        code_match = _EXIT_CODE_RE.search(msg)
        assert code_match is not None, f"could not read the exit code from: {msg!r}"
        assert int(code_match.group(1)) != 0, (
            f"fail-closed boot exited with code 0 (expected non-zero abort): {msg!r}"
        )

        # Half 2: the quarantine STILL fired before the abort - the corrupt
        # image is preserved for retrieval (the config contract).
        quarantined = list(inst_dir.glob("uploads.corrupted.*.db"))
        assert len(quarantined) == 1, (
            f"expected exactly one quarantined DB under {inst_dir} after a fail-closed "
            f"abort (quarantine fires, then the process exits); found {quarantined}"
        )
    finally:
        p1.terminate()
        if p2 is not None:
            p2.terminate()
        await emu.stop()
