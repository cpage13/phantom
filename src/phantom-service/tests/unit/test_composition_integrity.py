"""Production-path integrity-gate no-op cases + the umask-ordering guard.

Plan § 5.2.2 — the composition root runs ``PRAGMA integrity_check``
BEFORE opening the persistent SqliteUploadStore. The corrupt-DB
quarantine + counter-bump + fail-open cases live in
``test_startup_guards_prod_path.py``; this file pins the two cases that
file does not:

* the integrity gate is a NO-OP on a missing or clean DB (the counter
  stays at 0 — no spurious quarantine on the happy path); and
* ``os.umask(0o077)`` (plan § 5.2.4 / WS-4 Finding 6) runs BEFORE the
  first ``SqliteUploadStore.start()`` — the bare-metal trust boundary is
  set before any file opens.

Provenance: these assertions used to drive the dead-path
``compose_and_run`` (R-2 deleted it). They now drive the REAL composition
root — ``create_app``'s FastAPI lifespan, entered via
``app.router.lifespan_context`` (the exact async context manager the ASGI
server runs). The umask-ordering assertion in particular had NO prod-path
equivalent before R-2 (``test_startup_guards_prod_path.py`` asserted only
the umask *effect*, ``os.umask == 0o077`` after startup); this file closes
that ordering gap directly against the production path.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from phantom.app import create_app
from phantom.config.settings import (
    BodyStoreCfg,
    DbIntegrityCfg,
    InstanceCfg,
    RouteCfg,
    Settings,
    StorageCfg,
)

if TYPE_CHECKING:
    from fastapi import FastAPI

# The suite's default instance id + its per-instance data subdir.
_INSTANCE_ID = "primary"
_INSTANCE_DATA_DIR = "primary"
# Generous ceiling so a hung teardown surfaces as a test failure.
_LIFESPAN_TIMEOUT_SECONDS = 30.0


def _instance() -> InstanceCfg:
    """Build a minimal single-route InstanceCfg for the default instance."""
    hosts = ["files.example.com"]
    return InstanceCfg(
        id=_INSTANCE_ID,
        host_prefixes=hosts,
        data_dir=_INSTANCE_DATA_DIR,
        routes=[RouteCfg(name="files", hosts=hosts, auth_mode="phantom_bearer")],
    )


def _settings(*, data_root: Path, fail_open: bool = True) -> Settings:
    """Build a production-shaped hybrid Settings rooted at ``data_root``."""
    return Settings(
        storage=StorageCfg(
            data_dir=str(data_root),
            body_store=BodyStoreCfg(mode="hybrid", ram_ceiling_bytes=1024 * 1024),
            db_integrity=DbIntegrityCfg(fail_open=fail_open),
        ),
        instances=[_instance()],
    )


def _lifespan_of(app: FastAPI) -> AbstractAsyncContextManager[None]:
    """Return the app's production lifespan context manager."""
    return app.router.lifespan_context(app)


async def test_integrity_gate_no_op_when_db_path_missing(tmp_path: Path) -> None:
    """Fresh deployment (no DB file yet) → no quarantine, counter at 0.

    Falsifier: if the gate quarantined unconditionally the counter would
    read 1 on a clean boot → RED.
    """
    app = create_app(_settings(data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        counter = app.state.metrics_registry.counters.get("db_quarantine_total")
        assert counter is not None
        assert counter.snapshot()[""] == 0


async def test_integrity_gate_no_op_on_clean_existing_db(tmp_path: Path) -> None:
    """A pre-existing valid DB passes integrity check → no quarantine event.

    First boot creates the store; the second reopens it cleanly. The
    counter stays 0 and the live DB remains at its canonical per-instance
    path.
    """
    instance_root = tmp_path / _INSTANCE_DATA_DIR
    # First boot creates a clean store.
    app1 = create_app(_settings(data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app1):
        pass
    assert (instance_root / "uploads.db").exists()

    # Second boot reopens it — still clean.
    app2 = create_app(_settings(data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app2):
        counter = app2.state.metrics_registry.counters["db_quarantine_total"]
        assert counter.snapshot()[""] == 0
    # Live DB still present, no quarantine artifacts.
    assert (instance_root / "uploads.db").exists()
    assert not list(instance_root.glob("uploads.corrupted.*.db"))


async def test_umask_077_precedes_store_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan § 5.2.4 / WS-4 Finding 6 — bare-metal trust boundary ORDERING.

    The composition root calls ``os.umask(0o077)`` at the top of the
    lifespan (process-wide) BEFORE any per-instance ``SqliteUploadStore``
    opens, so the SQLite DB, WAL, body store, and token cache are created
    owner-only rather than inheriting a group/world-readable default. This
    spies on both call sites with one event log and asserts the umask call
    lands with the documented argument AND precedes the first store open.

    Falsifier: drop ``apply_umask`` from the lifespan, or move it after the
    instance loop, → the umask event is absent or follows the store open →
    RED. (``test_startup_guards_prod_path.py`` asserts the umask *effect*;
    this asserts the *ordering* the effect alone cannot prove.)

    umask is process-global; the spy delegates to the real call and the
    test save/restores around itself so it does not leak.
    """
    from phantom.storage.sqlite_store import SqliteUploadStore

    events: list[tuple[str, int | None]] = []
    real_umask = os.umask
    real_start = SqliteUploadStore.start

    def umask_spy(mode: int) -> int:
        events.append(("umask", mode))
        return real_umask(mode)

    async def start_spy(self: SqliteUploadStore) -> None:
        events.append(("sqlite_store.start", None))
        await real_start(self)

    # apply_umask() calls os.umask in runtime/startup_checks; patch the
    # real call site there (NOT app.os, which app.py never touches).
    monkeypatch.setattr("phantom.runtime.startup_checks.os.umask", umask_spy)
    monkeypatch.setattr(SqliteUploadStore, "start", start_spy)

    saved = os.umask(0o022)
    try:
        app = create_app(_settings(data_root=tmp_path))
        async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
            pass
    finally:
        os.umask(saved)

    # umask(0o077) ran at least once with the documented argument.
    assert ("umask", 0o077) in events, f"umask(0o077) not observed; saw: {events!r}"
    # The first umask call must precede the first SqliteUploadStore.start()
    # — proof the trust boundary is set before any file opens.
    umask_index = next(i for i, (name, _) in enumerate(events) if name == "umask")
    start_index = next(i for i, (name, _) in enumerate(events) if name == "sqlite_store.start")
    assert umask_index < start_index, f"umask must precede sqlite_store.start; events={events!r}"
