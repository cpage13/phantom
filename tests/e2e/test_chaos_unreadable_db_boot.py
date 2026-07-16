"""Operational-chaos E2E: an unreadable DB isolates and boots fresh (plan § 5.2 Part 5.C, § 4D).

Adversary 2, the unreadable-database chaos. § 4D generalizes "the service
always boots" to ANY database that cannot be OPENED at all (a permission / I/O
fault, an unopenable WAL, a lock past budget). The service isolates the bad DB
as ``reason=corrupted`` (an un-openable DB is functionally corrupt from the
service's view; the precise cause is in the log), bumps ``db_quarantine_total``,
and boots fresh. Unlike the schema gate (§ 4S, which DELETES), the isolate
PRESERVES the DB - it may be a real buffer we merely cannot open right now.

This drives the PERMISSION-unreadable case over the real ``python -m phantom``
subprocess harness: ``chmod 000`` on a valid, current-schema on-disk ``uploads.db``
(the SD-card-permission-glitch / wrong-owner-after-redeploy analogue). The
crux § 4D assertion: the service BOOTS (no crash-loop), the bad DB is ISOLATED
as ``reason=corrupted`` and visible via ``GET /v1/admin/quarantine``,
``db_quarantine_total`` bumped, and a fresh DB SERVES a new upload.

HONEST SCOPE NOTE (recorded in the execution log). A ``chmod 000`` file is
caught at the boot integrity probe (``check_integrity``'s ``aiosqlite.connect``
raises a permission ``OSError``, which it catches), so this E2E proves the
isolate-and-boot-fresh OUTCOME that the crux specifies (reason=corrupted,
counter bumped, fresh serves) via the integrity-gate path. The § 4D open-guard
at ``store.start()`` is the belt-and-suspenders net for an open-time fault that
slips PAST the integrity probe; it is hard to trigger with on-disk-only tricks
in a subprocess (SQLite silently ignores a garbage WAL, and a permission fault
is caught earlier by the probe), so that distinct branch is proven by the
landed UNIT tests (``test_startup_guards_prod_path.py`` patches ``start`` to
raise). Both converge on the SAME observable - reason=corrupted + fresh boot -
which is exactly what this E2E asserts for real.

Public e2e-light lane (plan § 5.0): generic subprocess ``submit_one`` + the
emulator. Deterministic + fast -> default lane.

Falsifier: drop the boot DB-isolation entirely (no integrity gate, no § 4D
open-guard) -> the unreadable file reaches ``store.start()`` and the subprocess
never becomes healthy (crash-loop) -> RED.
"""

from __future__ import annotations

import contextlib
import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from phantom_client import PhantomClient

from tests.e2e._harness.subprocess_harness import (
    PhantomSubprocess,
    allocate_port,
    boot_emulator,
    db_path_for,
    fake_security_token,
    instance_dir,
    submit_one,
    write_phantom_config,
)

pytestmark = [pytest.mark.conformance, pytest.mark.asyncio]

_BODY = b"phantom-unreadable-db-chaos-body"
# A few buffered uploads so the pre-chmod DB carries real rows (5xx keeps them
# un-delivered, durably on disk) - the isolate preserves a non-empty buffer.
_PRECHMOD_UPLOADS = 3


async def _counter_value(url: str, name: str) -> int:
    """Read one counter's empty-label-bucket value via the admin endpoint."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(f"{url}/v1/admin/observability/counters")
        resp.raise_for_status()
        for counter in resp.json()["counters"]:
            if counter["name"] == name:
                return int(counter["values"].get("", 0))
    return 0


def _deadline(seconds: float) -> datetime:
    """UTC deadline ``seconds`` from now (PhantomClient.poll_until shape)."""
    return datetime.now(UTC) + timedelta(seconds=seconds)


async def test_unreadable_db_is_isolated_corrupted_and_serves_fresh(tmp_path: Path) -> None:
    """A permission-unreadable on-disk DB is isolated (corrupted) and the service serves fresh.

    Sequence (the genuine operator scenario - a redeploy lands the image as a
    different uid, or the SD card hands back a wrong-owner DB):

    1. Boot a real subprocess; buffer a few uploads on disk (5xx keeps them).
    2. Clean stop (SIGTERM -> WAL checkpointed -> a valid DB on disk).
    3. ``chmod 000`` the on-disk ``uploads.db`` (the unreadable fault).
    4. Reboot on the SAME data dir.

    Assertions (the crux § 4D unreadable-DB property): the service comes back
    HEALTHY, a ``uploads.corrupted.<iso>.db`` quarantine artifact exists,
    ``db_quarantine_total == 1``, ``/quarantine`` reports ``reason=corrupted``,
    and a fresh upload succeeds end-to-end.
    """
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    inst_dir = instance_dir(data_dir)
    db_path = db_path_for(data_dir)

    port1 = allocate_port()
    cfg1 = write_phantom_config(data_dir=data_dir, bind_port=port1)
    p1 = PhantomSubprocess.make(cfg1, port1)
    p2: PhantomSubprocess | None = None
    chmodded = False
    try:
        from phantom_emulator.failure.injection import FailurePolicy, FailureScope

        # 1. Boot + buffer a few uploads (5xx keeps them on disk).
        await p1.start()
        bearer = fake_security_token(emu)
        emu.inject_failure(
            FailurePolicy(scope=FailureScope.GLOBAL, error_rate_5xx=1.0)  # type: ignore[call-arg]
        )
        async with PhantomClient(p1.url) as c:
            for _ in range(_PRECHMOD_UPLOADS):
                await submit_one(
                    c,
                    emulator_url=emu.url,
                    bearer=bearer,
                    body=_BODY,
                    chain_id=uuid4(),
                    file_prefix="unreadable-db",
                )
        assert db_path.exists(), f"expected a live DB at {db_path} after buffering uploads"

        # 2. Clean stop -> a valid, current-schema DB on disk.
        p1.terminate()

        # 3. Make the on-disk DB unreadable (permission fault, NOT structural
        #    corruption - distinct from the garbage-overwrite integrity test).
        os.chmod(db_path, 0o000)
        chmodded = True

        # 4. Reboot on the SAME data dir. .start() raises if it crash-loops.
        port2 = allocate_port()
        cfg2 = write_phantom_config(data_dir=data_dir, bind_port=port2)
        p2 = PhantomSubprocess.make(cfg2, port2)
        await p2.start()

        # The bad DB was isolated as corrupted (a .corrupted. artifact exists),
        # the counter bumped, and the live DB is fresh + present.
        quarantined = list(inst_dir.glob("uploads.corrupted.*.db"))
        assert len(quarantined) == 1, (
            f"expected exactly one isolated DB under {inst_dir}; found {quarantined}"
        )
        # Restore read perms on the isolated artifact so teardown / inspection works.
        os.chmod(quarantined[0], stat.S_IRUSR | stat.S_IWUSR)
        chmodded = False
        # The counter + /quarantine + poll_until are admin routes, and submit
        # is intake - all on the single listener (p2.url), so one client covers
        # everything.
        assert await _counter_value(p2.url, "db_quarantine_total") == 1
        assert db_path.exists(), "the live DB must be fresh + present at the canonical path"

        async with PhantomClient(p2.url) as client:
            # /quarantine reports the isolated DB with reason=corrupted.
            inv = await client.get_quarantine_inventory(instance="primary")
            corrupted = [e for e in inv.quarantines if e.reason == "corrupted"]
            assert corrupted, (
                f"an unreadable DB must be isolated reason=corrupted; inventory={inv.quarantines!r}"
            )
            assert any(e.has_db for e in corrupted), "the isolated DB half must be present"

            # A fresh upload after the reboot succeeds end-to-end.
            emu.clear_failures()
            emu.clear_received()
            fresh_id = uuid4()
            await submit_one(
                client,
                emulator_url=emu.url,
                bearer=bearer,
                body=_BODY,
                chain_id=fresh_id,
                file_prefix="post-isolate",
            )
            detail = await client.poll_until(
                fresh_id,
                terminal_states=frozenset({"succeeded"}),
                deadline=_deadline(30.0),
            )
        assert detail.state == "succeeded"
    finally:
        if chmodded:
            # Defensive: restore perms so teardown can clean tmp_path.
            with contextlib.suppress(OSError):
                os.chmod(db_path, stat.S_IRUSR | stat.S_IWUSR)
        p1.terminate()
        if p2 is not None:
            p2.terminate()
        await emu.stop()
