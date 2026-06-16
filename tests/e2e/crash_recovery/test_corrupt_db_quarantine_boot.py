"""E2E falsifier — the DB-integrity gate quarantines a corrupt DB on boot.

§9.2 case 1 (2026-05-29 refinement). The integrity gate moved into
``app.py``'s production lifespan (per-instance, before the store opens).
This test proves it fires in a REAL ``python -m phantom`` subprocess —
not the dead-path ``compose_and_run`` — by reusing the existing
``crash_recovery`` subprocess seam (§9.2 "reuse the existing
crash_recovery seam").

Sequence (the genuine operator scenario — a power-loss-corrupted SQLite
on a Pi):

1. Boot a real Phantom subprocess; land a few buffered uploads on disk.
2. Stop it cleanly (SIGTERM → WAL checkpointed → a *valid* DB on disk).
3. Overwrite ``<data>/primary/uploads.db`` with garbage (the corruption).
4. Reboot on the SAME data dir.

Assertions (the gate fired in production):

* the service comes back HEALTHY (fail_open default → fresh empty DB);
* a ``uploads.corrupted.<iso>.db`` quarantine artifact exists under the
  per-instance dir — asserting the *quarantine path specifically*, not
  merely that recovery coped (a loose "it booted" assertion would be
  vacuously green, since the pre-guard path also boots);
* ``db_quarantine_total == 1`` via the admin counters endpoint;
* a fresh upload submitted after the reboot succeeds end-to-end.

Falsifier: remove ``run_integrity_gate`` from ``_build_instance_context``
→ the store opens the corrupt file, no quarantine artifact appears, the
counter stays 0 (and the boot may crash-loop) → RED.
"""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import httpx
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

pytestmark = [pytest.mark.asyncio]

# A few buffered uploads so the pre-corruption DB carries real rows
# (5xx upstream keeps them un-delivered, so they are durably on disk).
_PRECORRUPT_UPLOADS = 3
_BODY = b"phantom-corrupt-db-e2e-body"


async def _counter_value(url: str, name: str) -> int:
    """Read one counter's empty-bucket value via the admin endpoint."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(f"{url}/v1/admin/observability/counters")
        resp.raise_for_status()
        for counter in resp.json()["counters"]:
            if counter["name"] == name:
                # The empty-label bucket is keyed "" in the values map.
                return int(counter["values"].get("", 0))
    return 0


async def test_corrupt_db_quarantined_on_real_subprocess_boot(tmp_path: Path) -> None:
    """A corrupt on-disk DB is quarantined when a real Phantom subprocess reboots."""
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
        async with PhantomClient(p1.url) as c:
            for _ in range(_PRECORRUPT_UPLOADS):
                await submit_one(
                    c,
                    emulator_url=emu.url,
                    bearer=bearer,
                    body=_BODY,
                    chain_id=uuid4(),
                    file_prefix="corrupt-db",
                )
        assert db_path.exists(), f"expected a live DB at {db_path} after buffering uploads"

        # 2. Clean stop (SIGTERM) → WAL checkpointed → valid DB on disk.
        p1.terminate()

        # 3. Corrupt the on-disk DB (the power-loss analogue). Overwrite the
        #    WHOLE main file with garbage AND drop the -wal/-shm siblings.
        #    Both are load-bearing: WAL mode + a clean checkpoint can leave
        #    enough valid structure (or a recoverable WAL) that SQLite's
        #    integrity_check shrugs off a header-only smudge, so a full-file
        #    overwrite with no WAL to recover from is the deterministic
        #    "disk image is malformed" the gate must catch.
        original_size = db_path.stat().st_size
        db_path.write_bytes(b"\xde\xad\xbe\xef" * (original_size // 4 + 1))
        for sibling_suffix in ("-wal", "-shm"):
            sibling = db_path.with_name(db_path.name + sibling_suffix)
            if sibling.exists():
                sibling.unlink()

        # 4. Reboot on the SAME data dir + a fresh port.
        port2 = allocate_port()
        cfg2 = write_phantom_config(data_dir=data_dir, bind_port=port2)
        p2 = PhantomSubprocess.make(cfg2, port2)
        await p2.start()  # raises if it never becomes healthy (the pre-fix RED path)

        # The integrity gate fired: a quarantine artifact exists under the
        # per-instance dir, and the counter bumped exactly once. The counter
        # is read from an admin endpoint on the single listener (p2.url).
        quarantined = list(inst_dir.glob("uploads.corrupted.*.db"))
        assert len(quarantined) == 1, (
            f"expected exactly one quarantined DB under {inst_dir}; found {quarantined}"
        )
        assert await _counter_value(p2.url, "db_quarantine_total") == 1

        # The live DB is fresh + present at the canonical path.
        assert db_path.exists()

        # 5. A fresh upload after the reboot succeeds end-to-end (service
        #    is genuinely serving, not just answering /v1/healthz). Intake
        #    (submit_one) and the poll's admin route both ride the single
        #    listener, so one client covers both.
        emu.clear_failures()
        emu.clear_received()
        fresh_id = uuid4()
        async with PhantomClient(p2.url) as client:
            await submit_one(
                client,
                emulator_url=emu.url,
                bearer=bearer,
                body=_BODY,
                chain_id=fresh_id,
                file_prefix="post-quarantine",
            )
            detail = await client.poll_until(
                fresh_id,
                terminal_states=frozenset({"succeeded"}),
                deadline=_deadline(30.0),
            )
        assert detail.state == "succeeded"
    finally:
        p1.terminate()
        if p2 is not None:
            p2.terminate()
        await emu.stop()


def _deadline(seconds: float):  # type: ignore[no-untyped-def]
    """UTC deadline ``seconds`` from now (PhantomClient.poll_until shape)."""
    from datetime import UTC, datetime, timedelta

    return datetime.now(UTC) + timedelta(seconds=seconds)
