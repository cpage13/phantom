"""Operational-chaos E2E: a wrong-schema DB never crashes boot (plan § 5.2 Part 5.C, § 4S).

Adversary 2, the schema-mismatch chaos. § 4S stamps a schema version
(``PRAGMA user_version = SCHEMA_VERSION`` inside ``start()``) and runs a
boot-time gate (``run_schema_gate``) that, on a pre-version / wrong-schema
on-disk database, DELETES it (population of zero - no field DB can hold real
undelivered uploads yet) and boots fresh. This is the DELETE-not-backup path:
unlike corruption (§ 4D) and a mode switch (§ 1), the schema gate writes NO
quarantine sibling - it just unlinks the db + its ``-wal``/``-shm`` siblings.

Three properties, all through the REAL boot path (the killable ``python -m
phantom`` subprocess harness restarted on the same ``data_dir``, the way an
operator's old SD card would hand a stale DB to a freshly-deployed image):

* :func:`test_old_schema_db_is_deleted_and_service_serves_fresh` - stage a
  pre-Phase-1 ``uploads`` table (old ``tier``/``committed`` columns, missing
  ``body_location`` + ``chain_id_at_ingress``, ``user_version=0``) under the
  instance data_root, boot, and assert: the service COMES UP (no crash-loop),
  the old DB is GONE, the live DB is the fresh stamped schema
  (``PRAGMA user_version == SCHEMA_VERSION``), ``schema_discard_total`` bumped,
  NO quarantine sibling was created, and a new upload serves end-to-end.
* :func:`test_fresh_boot_stamps_schema_version` - a clean first boot stamps
  ``user_version = SCHEMA_VERSION`` (the forward-going invariant).
* :func:`test_db_file_without_uploads_table_boots_as_is` - a DB file present
  but WITHOUT an ``uploads`` table is fresh-enough: ``CREATE TABLE IF NOT
  EXISTS`` builds it, nothing is deleted.

Public e2e-light lane (plan § 5.0): generic subprocess ``submit_one`` shape +
the emulator. The subprocess harness is deterministic and fast, so
this runs in the default lane like the existing real-SIGKILL crash tests.

Falsifier: drop ``run_schema_gate`` from ``_build_instance_context`` -> the old
DB reaches ``store.start()``'s ``executescript``, which crashes on
``CREATE INDEX ... ON uploads(chain_id_at_ingress)`` (the index references a
column the old table lacks) -> the subprocess never becomes healthy -> RED.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from phantom.runtime.startup_checks import SCHEMA_DISCARD_COUNTER_NAME
from phantom.storage.sqlite_store import SCHEMA_VERSION
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

_BODY = b"phantom-schema-gate-chaos-body"

# A clearly pre-Phase-1 ``uploads`` shape: the old ``tier`` + ``committed``
# columns, and crucially MISSING ``body_location`` and ``chain_id_at_ingress``
# (the Phase-1 rename the schema gate's column subset test catches). One row is
# inserted so the test also proves the discard fires even with data present
# (the population-of-zero rationale is about FIELD deployments; in the test the
# point is "delete fires regardless of the old DB's contents").
_OLD_SCHEMA_DDL = """
CREATE TABLE uploads (
    chain_id    TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    state       TEXT NOT NULL,
    tier        TEXT NOT NULL,
    committed   INTEGER NOT NULL DEFAULT 0
);
"""


def _stage_old_schema_db(db_path: Path) -> None:
    """Write a pre-version, wrong-column ``uploads.db`` at ``db_path``.

    ``user_version`` is left at the default ``0`` (pre-version), so the gate
    discards it even though the table name matches - both the version stamp and
    the column subset must hold for ``BOOT_AS_IS``.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(_OLD_SCHEMA_DDL)
        conn.execute(
            "INSERT INTO uploads (chain_id, instance_id, state, tier, committed) "
            "VALUES (?, ?, ?, ?, ?)",
            (str(uuid4()), "primary", "queued", "disk", 1),
        )
        conn.commit()
        observed = conn.execute("PRAGMA user_version").fetchone()[0]
        assert observed == 0, f"staged old DB should be unstamped (user_version=0), got {observed}"
    finally:
        conn.close()


def _read_user_version(db_path: Path) -> int:
    """Read ``PRAGMA user_version`` off an on-disk SQLite file."""
    conn = sqlite3.connect(str(db_path))
    try:
        return int(conn.execute("PRAGMA user_version").fetchone()[0])
    finally:
        conn.close()


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


async def test_old_schema_db_is_deleted_and_service_serves_fresh(tmp_path: Path) -> None:
    """A pre-version / wrong-column DB is DELETED on boot; the service serves fresh.

    The crux § 4S assertion: the schema gate deletes (does NOT back up) a
    wrong-schema DB and boots fresh. Proven over the real subprocess boot path.
    """
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    inst_dir = instance_dir(data_dir)
    db_path = db_path_for(data_dir)
    proc: PhantomSubprocess | None = None
    try:
        # Stage the stale DB under the per-instance data_root BEFORE boot.
        _stage_old_schema_db(db_path)
        assert db_path.exists()

        # Boot the real service over the stale DB. .start() raises if the
        # service never becomes healthy (the pre-fix RED path: executescript
        # crashes on the missing-column index).
        port = allocate_port()
        cfg = write_phantom_config(data_dir=data_dir, bind_port=port)
        proc = PhantomSubprocess.make(cfg, port)
        await proc.start()

        # The live DB is the FRESH stamped schema (the gate deleted the old one,
        # then start() recreated + stamped it).
        assert db_path.exists(), "the gate deletes the old DB; start() recreates a fresh one"
        assert _read_user_version(db_path) == SCHEMA_VERSION, (
            "the fresh DB must carry PRAGMA user_version == SCHEMA_VERSION"
        )

        # schema_discard_total bumped exactly once.
        assert await _counter_value(proc.url, SCHEMA_DISCARD_COUNTER_NAME) == 1

        # CRITICAL: NO quarantine sibling was created (the schema gate DELETES,
        # it never writes a .mode_switch. / .corrupted. backup pair).
        siblings = [
            p.name
            for p in inst_dir.iterdir()
            if ".mode_switch." in p.name or ".corrupted." in p.name
        ]
        assert not siblings, f"schema discard must write NO quarantine sibling; found {siblings}"
        # Quarantine inventory + poll_until are admin routes and submit is
        # intake - all on the single listener, so one client covers everything.
        async with PhantomClient(proc.url) as client:
            inv = await client.get_quarantine_inventory(instance="primary")
            assert inv.quarantines == [], (
                f"a schema discard must add no quarantine entry; inventory={inv.quarantines!r}"
            )

            # The service genuinely SERVES a new upload end-to-end (not merely
            # answering /v1/healthz).
            emu.clear_received()
            emu.clear_failures()
            fresh_id = uuid4()
            await submit_one(
                client,
                emulator_url=emu.url,
                bearer=fake_security_token(emu),
                body=_BODY,
                chain_id=fresh_id,
                file_prefix="post-schema-discard",
            )
            detail = await client.poll_until(
                fresh_id,
                terminal_states=frozenset({"succeeded"}),
                deadline=_deadline(30.0),
            )
        assert detail.state == "succeeded"
    finally:
        if proc is not None:
            proc.terminate()
        await emu.stop()


async def test_fresh_boot_stamps_schema_version(tmp_path: Path) -> None:
    """A clean first boot stamps ``user_version = SCHEMA_VERSION``.

    Falsifier: drop the ``PRAGMA user_version = {SCHEMA_VERSION}`` stamp from
    ``start()`` -> the fresh DB stays at user_version 0 -> RED.
    """
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    db_path = db_path_for(data_dir)
    proc: PhantomSubprocess | None = None
    try:
        assert not db_path.exists(), "fresh data_dir must have no DB before boot"
        port = allocate_port()
        cfg = write_phantom_config(data_dir=data_dir, bind_port=port)
        proc = PhantomSubprocess.make(cfg, port)
        await proc.start()
        assert db_path.exists(), "boot must create the DB"
        assert _read_user_version(db_path) == SCHEMA_VERSION, (
            "a fresh boot must stamp PRAGMA user_version == SCHEMA_VERSION"
        )
        # No discard fired on a fresh boot.
        assert await _counter_value(proc.url, SCHEMA_DISCARD_COUNTER_NAME) == 0
    finally:
        if proc is not None:
            proc.terminate()
        await emu.stop()


async def test_db_file_without_uploads_table_boots_as_is(tmp_path: Path) -> None:
    """A DB file present but WITHOUT an ``uploads`` table is fresh-enough.

    ``decide_schema_action`` treats an empty observed-column set (no ``uploads``
    table) as fresh: ``CREATE TABLE IF NOT EXISTS`` builds it and nothing is
    deleted. Proves the gate does not over-eagerly discard an unrelated /
    half-initialized SQLite file.

    Falsifier: make the gate discard on an absent ``uploads`` table -> the
    counter bumps -> RED.
    """
    emu = await boot_emulator()
    data_dir = tmp_path / "phantom-data"
    data_dir.mkdir()
    db_path = db_path_for(data_dir)
    proc: PhantomSubprocess | None = None
    try:
        # A valid SQLite file with some OTHER table and no uploads table.
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute("CREATE TABLE unrelated (k TEXT)")
            conn.commit()
        finally:
            conn.close()

        port = allocate_port()
        cfg = write_phantom_config(data_dir=data_dir, bind_port=port)
        proc = PhantomSubprocess.make(cfg, port)
        await proc.start()

        # Nothing was discarded; the DB is now the stamped current schema.
        assert await _counter_value(proc.url, SCHEMA_DISCARD_COUNTER_NAME) == 0
        assert _read_user_version(db_path) == SCHEMA_VERSION
        async with PhantomClient(proc.url) as client:
            assert (await client.get_health()).status == "ok"
    finally:
        if proc is not None:
            proc.terminate()
        await emu.stop()
