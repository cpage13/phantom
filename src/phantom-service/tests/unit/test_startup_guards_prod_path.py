"""Production-path falsifiers for the five startup guards + instance isolation.

These tests drive the REAL composition root — ``create_app``'s FastAPI
lifespan, entered via its async context manager
(``app.router.lifespan_context``) — NOT the dead-path ``compose_and_run``.
That is the whole point of the 2026-05-29 refinement: the guards were
previously tested only against ``compose_and_run`` while production
(``app.py``'s lifespan) ran unguarded. Each test is a genuine falsifier
— removing the corresponding wiring in ``app.py`` turns it RED:

* integrity gate (per-instance): corrupt DB → quarantined + counter
  bumped + service boots fresh;
* all_ram over a populated ``bodies/`` (per-instance): the live DB + body
  tree are backed up to a ``mode_switch`` quarantine pair, the counter
  bumps, and the service boots fresh (back-up-and-run, plan § 1 / ADR-025);
* retention-floor (process-wide): boot refused;
* umask (process-wide): ``os.umask`` is ``0o077`` after startup;
* cold-backup (per-instance): an artifact appears when ``backup_enabled``;
* instance-isolation (process-wide): duplicate ``id`` / ``data_dir`` /
  ``host_prefix`` → boot refused.

Driver note: every test enters the lifespan via
``app.router.lifespan_context(app)`` — the exact async context manager
the ASGI server runs. Entering without raising IS the "boots" assertion;
a refusal is asserted with ``pytest.raises`` around the ``async with``.
(Starlette's sync ``TestClient`` deadlocks when a lifespan raises during
startup, and mixing it with ``async def`` tests under
``asyncio_mode=auto`` triggers an event-loop interaction, so the whole
file drives the lifespan directly and uniformly.)

The instance-isolation pure-function edge cases (resolve normalization,
component-vs-prefix nesting) are exercised directly against
:func:`check_instance_isolation`.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import TYPE_CHECKING

import aiosqlite
import pytest
from phantom.app import create_app
from phantom.config.settings import (
    BodyStoreCfg,
    DbIntegrityCfg,
    InstanceCfg,
    RetentionCfg,
    RouteCfg,
    Settings,
    StorageCfg,
)
from phantom.runtime.lock_retry import retry_on_transient_lock
from phantom.runtime.startup_checks import (
    MODE_SWITCH_BACKUP_COUNTER_NAME,
    ConfigInvariantError,
    DegradeReason,
    check_instance_isolation,
)
from phantom.storage.integrity import list_quarantines
from phantom.storage.sqlite_store import SqliteUploadStore
from phantom.storage.token_cache import SqliteTokenCache

if TYPE_CHECKING:
    from fastapi import FastAPI

# The suite's default instance id + its per-instance data subdir.
_INSTANCE_ID = "primary"
_INSTANCE_DATA_DIR = "primary"
# Generous ceiling so a hung teardown surfaces as a test failure, not a
# wedged worker. The clean boot/teardown is sub-second locally.
_LIFESPAN_TIMEOUT_SECONDS = 30.0


def _instance(
    *,
    instance_id: str = _INSTANCE_ID,
    data_dir: str = _INSTANCE_DATA_DIR,
    host_prefixes: list[str] | None = None,
) -> InstanceCfg:
    """Build a minimal single-route InstanceCfg."""
    hosts = host_prefixes if host_prefixes is not None else ["files.example.com"]
    return InstanceCfg(
        id=instance_id,
        host_prefixes=hosts,
        data_dir=data_dir,
        routes=[RouteCfg(name="files", hosts=hosts, auth_mode="phantom_bearer")],
    )


def _settings(
    *,
    data_root: Path,
    mode: str = "hybrid",
    instances: list[InstanceCfg] | None = None,
    db_integrity: DbIntegrityCfg | None = None,
    retention: RetentionCfg | None = None,
) -> Settings:
    """Build a production-shaped Settings rooted at ``data_root``."""
    storage_kwargs: dict[str, object] = {
        "data_dir": str(data_root),
        "body_store": BodyStoreCfg(mode=mode, ram_ceiling_bytes=1024 * 1024),
    }
    if db_integrity is not None:
        storage_kwargs["db_integrity"] = db_integrity
    return Settings(
        storage=StorageCfg(**storage_kwargs),  # type: ignore[arg-type]
        instances=instances if instances is not None else [_instance()],
        retention=retention if retention is not None else RetentionCfg(),
    )


def _lifespan_of(app: FastAPI) -> AbstractAsyncContextManager[None]:
    """Return the app's production lifespan context manager."""
    return app.router.lifespan_context(app)


async def _write_corrupt_db(db_path: Path) -> None:
    """Create a real SQLite file then corrupt its header in place."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(str(db_path)) as conn:
        await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        await conn.commit()
    with db_path.open("r+b") as fh:
        fh.write(b"\x00\xff" * 256)


def _populate_bodies(bodies_root: Path) -> Path:
    """Lay down one fake chain body dir under ``bodies_root`` (shard layout)."""
    chain_dir = bodies_root / "ab" / "abcdef00-0000-0000-0000-000000000000"
    chain_dir.mkdir(parents=True, exist_ok=True)
    body_file = chain_dir / "body"
    body_file.write_bytes(b"pre-existing-on-disk-body")
    return body_file


# ---------------------------------------------------------------------
# 1. Integrity gate (per-instance).
# ---------------------------------------------------------------------


async def test_corrupt_db_quarantined_and_counter_bumped_and_boots_fresh(tmp_path: Path) -> None:
    """A corrupt per-instance DB is quarantined; counter bumps; service boots.

    Falsifier: drop ``run_integrity_gate`` from ``_build_instance_context``
    → the store opens the corrupt file, no quarantine dir appears and the
    counter stays 0 → RED.
    """
    instance_root = tmp_path / _INSTANCE_DATA_DIR
    db_path = instance_root / "uploads.db"
    await _write_corrupt_db(db_path)
    body_file = _populate_bodies(instance_root / "bodies")

    app = create_app(_settings(data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        # The service came up (fail_open default True) — the counter
        # bumped exactly once on the quarantine.
        counter = app.state.metrics_registry.counters["db_quarantine_total"]
        assert counter.snapshot()[""] == 1

    # Live DB is fresh + present at the canonical path.
    assert db_path.exists()
    # Quarantine artifacts exist under the per-instance root with the
    # PRODUCTION ``bodies`` naming (NOT ``body_store``).
    assert list(instance_root.glob("uploads.corrupted.*.db"))
    quarantined_bodies = list(instance_root.glob("bodies.quarantine.*"))
    assert len(quarantined_bodies) == 1
    # The pre-quarantine body bytes are preserved in the artifact.
    assert (quarantined_bodies[0] / "ab" / body_file.parent.name / "body").exists()


async def test_corrupt_db_fail_closed_refuses_boot(tmp_path: Path) -> None:
    """``fail_open=False`` → boot raises after quarantining (artifact survives).

    Falsifier: drop the gate → no raise, no quarantine → RED.
    """
    instance_root = tmp_path / _INSTANCE_DATA_DIR
    db_path = instance_root / "uploads.db"
    await _write_corrupt_db(db_path)

    app = create_app(_settings(data_root=tmp_path, db_integrity=DbIntegrityCfg(fail_open=False)))
    with pytest.raises(RuntimeError, match=r"fail_open=False"):
        async with _lifespan_of(app):
            pass  # pragma: no cover — lifespan startup must raise

    # The quarantine artifact is on disk despite the refusal.
    assert list(instance_root.glob("uploads.corrupted.*.db"))


# ---------------------------------------------------------------------
# 2. all_ram over a populated bodies/ (per-instance).
# ---------------------------------------------------------------------


async def test_all_ram_over_populated_bodies_backs_up_and_boots_fresh(tmp_path: Path) -> None:
    """Booting all_ram over a populated bodies/ tree backs up and runs.

    Plan § 1.2 / ADR-025 (back-up-and-run, supersedes the prior fail-closed
    A-3/F-2 decision): ``check_body_store_mode`` no longer refuses to boot.
    It relocates the live DB + body tree to a recoverable ``mode_switch``
    quarantine pair (no data loss — the bytes are preserved, not condemned to
    ``corrupted`` nor leaked), bumps ``mode_switch_backup_total``, and the
    service boots fresh over the now-empty live tree.

    Falsifier: drop ``check_body_store_mode`` → all_ram boots over the
    populated tree, no ``mode_switch`` artifact appears, the counter stays 0,
    and recovery condemns the on-disk row to ``corrupted`` → RED.
    """
    instance_root = tmp_path / _INSTANCE_DATA_DIR
    body_file = _populate_bodies(instance_root / "bodies")
    pre_existing_bytes = body_file.read_bytes()

    app = create_app(_settings(data_root=tmp_path, mode="all_ram"))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        # The service came up (back-up-and-run) — the counter bumped once on
        # the backup, mirroring the integrity gate's db_quarantine_total.
        counter = app.state.metrics_registry.counters[MODE_SWITCH_BACKUP_COUNTER_NAME]
        assert counter.snapshot()[""] == 1
        assert [inst.cfg.id for inst in app.state.instances] == [_INSTANCE_ID]

    # The pre-existing tree was moved to ONE manifested mode_switch backup
    # (cycle-7 seam 2), visible via the same inventory the
    # /v1/admin/quarantine route serves; the live bodies/ tree is fresh (no
    # leftover chain dirs the all_ram store ignores).
    mode_switch_entries = [e for e in list_quarantines(instance_root) if e.reason == "mode_switch"]
    assert mode_switch_entries, "all_ram-over-populated must leave a mode_switch backup"
    backup = mode_switch_entries[0]
    assert backup.anomaly is False and backup.backup_id is not None
    assert backup.has_body, "the backup must include the relocated body tree"
    assert backup.body_path is not None
    # The bytes are preserved in the backup, not lost.
    preserved = [
        p
        for p in backup.body_path.rglob("*")
        if p.is_file() and p.read_bytes() == pre_existing_bytes
    ]
    assert preserved, "the pre-existing body bytes must survive in the backup"
    # The live tree is empty of pre-existing chain bodies (booted fresh).
    live_bodies = instance_root / "bodies"
    assert not [p for p in live_bodies.rglob("*") if p.is_file()]


async def test_all_ram_clean_dir_boots(tmp_path: Path) -> None:
    """all_ram on a fresh (empty) data dir boots normally — guard is targeted."""
    app = create_app(_settings(data_root=tmp_path, mode="all_ram"))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        assert app.state.instances[0].cfg.id == _INSTANCE_ID


# ---------------------------------------------------------------------
# 2b. The typed degrade vocabulary (cycle-7 seam 3).
# ---------------------------------------------------------------------


def test_every_degrade_reason_has_a_distinct_operator_hint() -> None:
    """Every DegradeReason member yields a non-empty, distinct action hint.

    The static half of the seam-3 exhaustiveness guarantee lives in mypy
    (the ``assert_never``-closed match in ``degrade_action_hint`` fails
    type-checking for an unhandled member). This runtime half walks the
    REAL enum so a member added with a lazy duplicate or empty hint is
    caught even before the type gate runs.
    """
    from phantom.runtime.startup_checks import degrade_action_hint

    hints = {reason: degrade_action_hint(reason) for reason in DegradeReason}
    assert all(hint.strip() for hint in hints.values()), hints
    assert len(set(hints.values())) == len(hints), (
        f"operator hints must be distinct per reason; got {hints!r}"
    )


# ---------------------------------------------------------------------
# 3. Retention-floor (process-wide).
# ---------------------------------------------------------------------


async def test_retention_floor_violation_refuses_boot(tmp_path: Path) -> None:
    """A body-outlives-metadata retention config refuses to boot.

    Falsifier: drop ``check_retention_floor`` from the lifespan → boot
    succeeds → RED.
    """
    bad_retention = RetentionCfg(
        succeeded_body_seconds=600,
        succeeded_metadata_seconds=60,
    )
    app = create_app(_settings(data_root=tmp_path, retention=bad_retention))
    with pytest.raises(ConfigInvariantError, match=r"succeeded"):
        async with _lifespan_of(app):
            pass  # pragma: no cover — lifespan startup must raise


# ---------------------------------------------------------------------
# 4. umask (process-wide).
# ---------------------------------------------------------------------


async def test_umask_is_owner_only_after_startup(tmp_path: Path) -> None:
    """After lifespan startup the process umask is ``0o077``.

    Falsifier: drop ``apply_umask`` from the lifespan → the inherited
    default (commonly ``0o022``) remains → RED.

    umask is process-global; save/restore around the test so it does not
    leak into other tests.
    """
    saved = os.umask(0o022)
    try:
        app = create_app(_settings(data_root=tmp_path))
        async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
            observed = os.umask(0o022)  # read-and-restore in one move
        assert observed == 0o077, f"expected umask 0o077 after startup, got {observed:#o}"
    finally:
        os.umask(saved)


# ---------------------------------------------------------------------
# 5. Cold-backup (per-instance).
# ---------------------------------------------------------------------


async def test_cold_backup_artifact_appears_when_enabled(tmp_path: Path) -> None:
    """With ``backup_enabled`` + a short period, a backup artifact appears.

    Falsifier: drop the ``ColdBackupScheduler`` spawn from app.py's
    TaskGroup → no artifact ever appears → RED.
    """
    backups_root = tmp_path / _INSTANCE_DATA_DIR / "backups"
    app = create_app(
        _settings(
            data_root=tmp_path,
            db_integrity=DbIntegrityCfg(backup_enabled=True, backup_period_seconds=1),
        )
    )
    artifacts: list[Path] = []
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        deadline = asyncio.get_running_loop().time() + 8.0
        while asyncio.get_running_loop().time() < deadline:
            if backups_root.is_dir():
                artifacts = list(backups_root.glob("uploads.backup.*.db"))
                if artifacts:
                    break
            await asyncio.sleep(0.1)
    assert artifacts, f"no cold-backup artifact under {backups_root} within the period"


async def test_cold_backup_no_artifact_when_disabled(tmp_path: Path) -> None:
    """With ``backup_enabled`` False (default) no backups dir is produced."""
    backups_root = tmp_path / _INSTANCE_DATA_DIR / "backups"
    app = create_app(_settings(data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        await asyncio.sleep(0.5)
        assert not backups_root.exists() or not list(backups_root.glob("uploads.backup.*.db"))


# ---------------------------------------------------------------------
# 6. Instance isolation (process-wide).
# ---------------------------------------------------------------------


async def test_duplicate_instance_id_refuses_boot(tmp_path: Path) -> None:
    """Two instances with the same id refuse to boot.

    Falsifier: drop ``check_instance_isolation`` → boot succeeds with two
    instances sharing an id (dispatcher silently collapses one) → RED.
    """
    instances = [
        _instance(instance_id="dup", data_dir="a", host_prefixes=["a.example.com"]),
        _instance(instance_id="dup", data_dir="b", host_prefixes=["b.example.com"]),
    ]
    app = create_app(_settings(data_root=tmp_path, instances=instances))
    with pytest.raises(ConfigInvariantError, match=r"[Dd]uplicate instance id"):
        async with _lifespan_of(app):
            pass  # pragma: no cover — lifespan startup must raise


async def test_duplicate_data_dir_refuses_boot(tmp_path: Path) -> None:
    """Two instances sharing a data_dir refuse to boot (single-writer)."""
    instances = [
        _instance(instance_id="one", data_dir="shared", host_prefixes=["one.example.com"]),
        _instance(instance_id="two", data_dir="shared", host_prefixes=["two.example.com"]),
    ]
    app = create_app(_settings(data_root=tmp_path, instances=instances))
    with pytest.raises(ConfigInvariantError, match=r"share data_dir"):
        async with _lifespan_of(app):
            pass  # pragma: no cover — lifespan startup must raise


async def test_duplicate_host_prefix_refuses_boot(tmp_path: Path) -> None:
    """Two instances declaring the same host_prefix (case-insensitive) refuse."""
    instances = [
        _instance(instance_id="one", data_dir="a", host_prefixes=["Files.Example.com"]),
        _instance(instance_id="two", data_dir="b", host_prefixes=["files.example.COM"]),
    ]
    app = create_app(_settings(data_root=tmp_path, instances=instances))
    with pytest.raises(ConfigInvariantError, match=r"host_prefix"):
        async with _lifespan_of(app):
            pass  # pragma: no cover — lifespan startup must raise


async def test_two_isolated_instances_boot(tmp_path: Path) -> None:
    """Two fully-isolated instances boot cleanly (the guard is not overzealous)."""
    instances = [
        _instance(instance_id="prod", data_dir="prod", host_prefixes=["prod.example.com"]),
        _instance(instance_id="staging", data_dir="staging", host_prefixes=["stg.example.com"]),
    ]
    app = create_app(_settings(data_root=tmp_path, instances=instances))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        ids = {inst.cfg.id for inst in app.state.instances}
        assert ids == {"prod", "staging"}


# ---------------------------------------------------------------------
# check_instance_isolation — pure-function edge cases (§9.11 tweaks).
# ---------------------------------------------------------------------


def test_isolation_sibling_data_dirs_are_distinct() -> None:
    """``a/b`` and ``a/bc`` are siblings, NOT nested — must pass.

    The §9.11 load-bearing fix: raw-string prefix comparison would have
    falsely rejected ``a/bc`` as "nested under ``a/b``". Path-component
    nesting via Path.parents keeps them distinct.
    """
    check_instance_isolation(
        [
            _instance(instance_id="one", data_dir="a/b", host_prefixes=["one.example.com"]),
            _instance(instance_id="two", data_dir="a/bc", host_prefixes=["two.example.com"]),
        ]
    )


def test_isolation_resolve_normalizes_dot_and_trailing_slash() -> None:
    """``foo`` and ``./foo`` resolve to the same dir → collision.

    Raw-string comparison would have missed the ``./foo`` form.
    """
    with pytest.raises(ConfigInvariantError, match=r"share data_dir"):
        check_instance_isolation(
            [
                _instance(instance_id="one", data_dir="foo", host_prefixes=["one.example.com"]),
                _instance(instance_id="two", data_dir="./foo", host_prefixes=["two.example.com"]),
            ]
        )


def test_isolation_true_nesting_is_rejected() -> None:
    """A data_dir nested *inside* another instance's data_dir is rejected."""
    with pytest.raises(ConfigInvariantError, match=r"nested inside"):
        check_instance_isolation(
            [
                _instance(instance_id="parent", data_dir="a", host_prefixes=["p.example.com"]),
                _instance(instance_id="child", data_dir="a/b", host_prefixes=["c.example.com"]),
            ]
        )


def test_isolation_glob_overlap_is_permitted() -> None:
    """Glob *overlap* (not exact dup) stays permitted by declaration order."""
    check_instance_isolation(
        [
            _instance(instance_id="catchall", data_dir="a", host_prefixes=["*.example.com"]),
            _instance(instance_id="specific", data_dir="b", host_prefixes=["api.example.com"]),
        ]
    )


# ---------------------------------------------------------------------
# § 4D.1 / § 4D.2 — boot-open retry-then-isolate + degraded boot.
#
# These drive the SAME production lifespan as the integrity-gate tests
# above. The failures are injected by patching the boot DB OPEN
# (``SqliteUploadStore.start`` / ``SqliteTokenCache.start``): a transient
# lock that clears is ridden out; an unrecoverable open is isolated and the
# instance boots fresh; an unwritable substrate degrades the instance
# (no context, a typed DegradedInstance outcome) instead of crashing.
# The /ready + /health + /send surfacing of the degraded set is Part 4D.B;
# here we assert the BOOT side (the map is populated, boot continues).
# ---------------------------------------------------------------------

# A non-transient message: NOT one of is_transient_lock_error's fragments,
# so it classifies as an UNRECOVERABLE open (-> isolate), not a retry.
_UNRECOVERABLE_OPEN_MESSAGE = "disk I/O error"
# A transient SQLITE_BUSY message fragment (is_transient_lock_error True).
_TRANSIENT_LOCK_MESSAGE = "database is locked"


async def _make_valid_db(db_path: Path) -> None:
    """Create a real, openable SQLite file at ``db_path`` (something to isolate)."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(str(db_path)) as conn:
        await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        await conn.commit()


def _patch_start_failing[StoreT](
    monkeypatch: pytest.MonkeyPatch,
    cls: type[StoreT],
    *,
    exc: BaseException,
    fail_times: int,
) -> None:
    """Patch ``cls.start`` to raise ``exc`` on the first ``fail_times`` calls.

    Subsequent calls delegate to the REAL ``start`` (so the post-isolate /
    post-retry fresh open succeeds). The guard builds a fresh store per
    attempt, so the class-level patch sees every attempt; a shared counter
    across instances mimics "the open fails N times then the substrate is fine".
    """
    original_start = cls.start  # type: ignore[attr-defined]
    calls = {"n": 0}

    async def _failing_start(self: StoreT) -> None:
        calls["n"] += 1
        if calls["n"] <= fail_times:
            raise exc
        await original_start(self)

    monkeypatch.setattr(cls, "start", _failing_start)


def _patch_start_failing_while_present[StoreT](
    monkeypatch: pytest.MonkeyPatch,
    cls: type[StoreT],
    *,
    exc: BaseException,
    db_path: Path,
) -> None:
    """Patch ``cls.start`` to raise ``exc`` WHILE ``db_path`` still exists.

    Models a fault on a SPECIFIC live file: every attempt fails while the
    original DB is at its path; once the guard isolates it (renames it aside),
    the original path is gone, so the post-isolate fresh open succeeds. Robust
    against the exact retry-attempt COUNT (unlike a fixed ``fail_times``), which
    matters when a tiny budget makes the count timing-dependent.
    """
    original_start = cls.start  # type: ignore[attr-defined]

    async def _failing_start(self: StoreT) -> None:
        if db_path.exists():
            raise exc
        await original_start(self)

    monkeypatch.setattr(cls, "start", _failing_start)


def _attach_capture(logger_name: str) -> list[logging.LogRecord]:
    """Attach a record-capturing handler to ``logger_name`` and return the list.

    ``create_app`` calls ``configure_logging`` which does ``root.handlers.clear()``,
    so pytest's ``caplog`` root handler is removed before the lifespan logs.
    Attaching directly to the named logger (which is not cleared) captures its
    records reliably. The handler stays attached for the test's duration (the
    process-global logger is fine for a single-threaded unit test).
    """
    records: list[logging.LogRecord] = []

    class _ListHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger(logger_name)
    logger.addHandler(_ListHandler())
    logger.setLevel(logging.WARNING)
    return records


def _patch_fast_lock_budget(monkeypatch: pytest.MonkeyPatch, *, budget_seconds: float) -> None:
    """Shrink the boot-open lock-retry budget so a "past budget" test is fast.

    The budget is a default-bound arg of ``retry_on_transient_lock``, so
    patching the module constant would not take effect; instead replace the
    name the boot-open guard calls (``phantom.app.retry_on_transient_lock``)
    with a thin forwarder that injects a tiny ``budget_seconds``.
    """

    async def _fast[T](
        op: Callable[[], Awaitable[T]],
        *,
        description: str,
        on_budget_exhausted: Callable[[BaseException], BaseException],
    ) -> T:
        return await retry_on_transient_lock(
            op,
            description=description,
            on_budget_exhausted=on_budget_exhausted,
            budget_seconds=budget_seconds,
        )

    monkeypatch.setattr("phantom.app.retry_on_transient_lock", _fast)


async def test_unrecoverable_upload_open_isolated_and_boots_fresh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unrecoverable ``store.start()`` open isolates the DB and boots fresh.

    Falsifier: drop the boot-open guard's isolate branch from
    ``_build_instance_context`` -> the open error propagates and crashes the
    lifespan; no ``.corrupted.`` artifact, counter stays 0 -> RED.
    """
    instance_root = tmp_path / _INSTANCE_DATA_DIR
    db_path = instance_root / "uploads.db"
    # A real DB on disk so the isolate has something to rename aside. It opens
    # for the integrity probe (valid SQLite); the injected open fault is what
    # § 4D handles (a fault that slips past the integrity + schema gates).
    await _make_valid_db(db_path)

    # Fail the open while the original DB is present; once isolated (renamed
    # away) the post-isolate fresh open succeeds.
    _patch_start_failing_while_present(
        monkeypatch,
        SqliteUploadStore,
        exc=aiosqlite.OperationalError(_UNRECOVERABLE_OPEN_MESSAGE),
        db_path=db_path,
    )
    records = _attach_capture("phantom.app")

    app = create_app(_settings(data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        # The service came up on a fresh DB; the counter bumped once.
        counter = app.state.metrics_registry.counters["db_quarantine_total"]
        assert counter.snapshot()[""] == 1
        # A real, healthy instance context was built (not degraded).
        assert [inst.cfg.id for inst in app.state.instances] == [_INSTANCE_ID]
        assert list(app.state.degraded_boot) == []

    # The original DB was isolated to a .corrupted. sibling (preserved, not
    # deleted) and a fresh live DB exists at the canonical path.
    assert list(instance_root.glob("uploads.corrupted.*.db"))
    assert db_path.exists()
    # The WARNING names the exact open error.
    warning_text = "\n".join(r.getMessage() for r in records)
    assert _UNRECOVERABLE_OPEN_MESSAGE in warning_text
    assert "isolated" in warning_text.lower()


async def test_transient_lock_at_open_clears_within_budget_nothing_isolated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient lock at open is ridden out; the open succeeds, nothing isolated.

    Complements recovery's existing WRITE-path lock retry with OPEN-path
    coverage. Falsifier: drop the retry from the boot-open guard -> the first
    lock error is treated as unrecoverable and the DB is needlessly isolated
    (a .corrupted. artifact appears) -> RED.
    """
    instance_root = tmp_path / _INSTANCE_DATA_DIR

    # Fail the FIRST upload-store open with a transient lock, then succeed. One
    # retry sleeps ~_RETRY_BACKOFF_BASE (0.5s) — fast enough for a unit test.
    _patch_start_failing(
        monkeypatch,
        SqliteUploadStore,
        exc=sqlite3.OperationalError(_TRANSIENT_LOCK_MESSAGE),
        fail_times=1,
    )

    app = create_app(_settings(data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        # The instance booted normally — the lock cleared on retry.
        assert [inst.cfg.id for inst in app.state.instances] == [_INSTANCE_ID]
        assert list(app.state.degraded_boot) == []
        # Nothing was isolated: the lock was transient, not a fault.
        counter = app.state.metrics_registry.counters["db_quarantine_total"]
        assert counter.snapshot()[""] == 0

    assert not list(instance_root.glob("uploads.corrupted.*.db"))


async def test_transient_lock_past_budget_is_isolated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lock that OUTLASTS the budget is treated as unrecoverable -> isolate.

    Falsifier: make the budget unbounded / never raise BootOpenLockError ->
    the boot hangs (or never isolates) -> the test times out -> RED.
    """
    instance_root = tmp_path / _INSTANCE_DATA_DIR
    db_path = instance_root / "uploads.db"
    await _make_valid_db(db_path)

    # Shrink the budget so the persistent lock exhausts it quickly.
    _patch_fast_lock_budget(monkeypatch, budget_seconds=0.05)
    # The lock holds for the whole (tiny) budget: every retry-phase attempt
    # fails while the original DB is present, exhausting the budget ->
    # BootOpenLockError -> isolate -> rename away -> the post-isolate fresh open
    # (on the now-absent path) succeeds. Keyed on the file's presence so it is
    # robust to the exact retry-attempt count the tiny budget produces.
    _patch_start_failing_while_present(
        monkeypatch,
        SqliteUploadStore,
        exc=sqlite3.OperationalError(_TRANSIENT_LOCK_MESSAGE),
        db_path=db_path,
    )

    app = create_app(_settings(data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        # The lock past budget became BootOpenLockError -> unrecoverable ->
        # isolate -> boot fresh. The instance is up (not degraded) and the
        # counter bumped once.
        assert [inst.cfg.id for inst in app.state.instances] == [_INSTANCE_ID]
        counter = app.state.metrics_registry.counters["db_quarantine_total"]
        assert counter.snapshot()[""] == 1

    assert list(instance_root.glob("uploads.corrupted.*.db"))


async def test_token_cache_isolate_uses_isolate_db_file_no_body_tree_moved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unrecoverable token-cache open isolates ONLY the cache DB, not bodies.

    The token cache is body-less, so the coupled ``quarantine(...)`` (which
    moves the body tree first) is WRONG for it (m-4). Falsifier: isolate the
    token cache via ``quarantine(db_path, bodies_root)`` instead of
    ``isolate_db_file`` -> the upload body tree is relocated to a
    ``bodies.corrupted.*`` sibling and the live ``bodies/`` is emptied -> RED.
    """
    instance_root = tmp_path / _INSTANCE_DATA_DIR
    token_cache_db = instance_root / "token_cache.db"
    await _make_valid_db(token_cache_db)
    # A populated upload body tree that MUST be left untouched by the
    # token-cache isolate.
    body_file = _populate_bodies(instance_root / "bodies")

    # Fail ONLY the token-cache open (the upload store opens fresh + fine).
    _patch_start_failing(
        monkeypatch,
        SqliteTokenCache,
        exc=aiosqlite.OperationalError(_UNRECOVERABLE_OPEN_MESSAGE),
        fail_times=1,
    )

    app = create_app(_settings(data_root=tmp_path))
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        assert [inst.cfg.id for inst in app.state.instances] == [_INSTANCE_ID]
        assert list(app.state.degraded_boot) == []

    # The token cache DB was isolated to a .corrupted. sibling.
    assert list(instance_root.glob("token_cache.corrupted.*.db"))
    # The body tree was NOT moved: the live body file is intact and NO
    # bodies.corrupted.* / bodies.quarantine.* sibling was created.
    assert body_file.exists()
    assert not list(instance_root.glob("bodies.corrupted.*"))
    assert not list(instance_root.glob("bodies.quarantine.*"))


@pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0,
    reason="chmod 0o555 read-only dir is bypassable by root; the degraded fault "
    "cannot be reproduced as uid 0",
)
async def test_unwritable_substrate_degrades_boot_no_crash(tmp_path: Path) -> None:
    """A genuinely read-only data_dir degrades the instance instead of crashing.

    The bug-guard for the § 5.C operational-chaos finding: this drives the REAL
    physics (a ``chmod 0o555`` instance ``data_root``), NOT a monkeypatched isolate
    OSError. A read-only directory makes ``store.start()`` raise
    ``sqlite3.OperationalError("unable to open database file")`` — an
    ``aiosqlite.Error`` that is NOT an ``OSError`` — and the post-isolate recreate
    raises the SAME thing. The boot-open guard must classify the unwritable
    substrate by PROBING ``data_root`` (not by the exception type) and degrade:
    return a typed ``DegradedInstance``, build no context, and let the
    lifespan ENTER (no crash).

    Falsifier (the exact bug § 5.C caught): narrow the guard's post-isolate
    ``except`` back to ``OSError`` only -> the recreate's ``sqlite3.OperationalError``
    escapes uncaught and the lifespan CRASHES on boot -> RED. The earlier version
    of this test masked the bug by raising ``OSError`` from a patched ``quarantine``
    over a WRITABLE dir, so it never exercised the real read-only-dir code path.
    """
    instance_root = tmp_path / _INSTANCE_DATA_DIR
    instance_root.mkdir(parents=True)
    # Read-only BEFORE boot: store.start() cannot create uploads.db (real
    # sqlite3.OperationalError), the isolate has nothing to move, and the
    # post-isolate recreate cannot create the file either -> degrade.
    os.chmod(instance_root, 0o555)

    app = create_app(_settings(data_root=tmp_path))
    try:
        # The lifespan ENTERS without raising — the degraded boot does not crash.
        async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
            # The instance is degraded: recorded in the map, absent from instances.
            degraded = {d.instance_id: d for d in app.state.degraded_boot}
            assert _INSTANCE_ID in degraded
            assert degraded[_INSTANCE_ID].reason is DegradeReason.SUBSTRATE_UNWRITABLE
            assert "unwritable" in degraded[_INSTANCE_ID].detail.lower()
            assert app.state.instances == []
    finally:
        os.chmod(instance_root, 0o755)


async def test_data_root_prep_substrate_fault_degrades_boot_no_crash(tmp_path: Path) -> None:
    """A non-directory at the per-instance data_root degrades at the directory-prep stage.

    Unit-level coverage of the M4-A directory-prep degrade path
    (:func:`phantom.app._ensure_data_root_writable`), the FIRST per-instance boot
    op and the earliest place a substrate fault can surface. A regular FILE sits
    where the instance ``data_root`` directory must be, so ``data_root.mkdir``
    raises ``FileExistsError`` (an ``OSError``) BEFORE the DB-open guard; the helper
    probes the substrate, finds it unwritable, and raises
    ``_StorageSubstrateUnwritableError`` so the instance boots DEGRADED. Unlike the
    read-only-``data_root`` degrade test above, this fault is a physical
    filesystem-type conflict, so it reproduces under root too (NO skip): the exact
    Balena / ARM / root-CI environment the e2e companion targets.

    Falsifier: narrow the M4-A degrade decision back to the DB-open stage (drop
    ``_ensure_data_root_writable``'s probe-classify) -> ``mkdir`` raises the bare
    ``FileExistsError`` out of the build loop and the lifespan CRASHES on boot ->
    RED (the degrade map stays empty and entering the lifespan raises).
    """
    # A FILE where the instance data_root directory must be (euid-independent).
    instance_root = tmp_path / _INSTANCE_DATA_DIR
    instance_root.write_text("corrupted-filesystem: a file where the data_root directory must be")

    app = create_app(_settings(data_root=tmp_path))
    # The lifespan ENTERS without raising; the directory-prep fault degrades.
    async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
        degraded = {d.instance_id: d for d in app.state.degraded_boot}
        assert _INSTANCE_ID in degraded, (
            "a file at data_root is an unwritable substrate; the instance must boot "
            "DEGRADED at the directory-prep stage, not crash the lifespan"
        )
        assert degraded[_INSTANCE_ID].reason is DegradeReason.SUBSTRATE_UNWRITABLE
        assert "unwritable" in degraded[_INSTANCE_ID].detail.lower()
        assert app.state.instances == []


async def test_data_root_prep_mkdir_failure_over_writable_dir_does_not_degrade(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient mkdir failure over a WRITABLE data_root re-raises, never degrades.

    Falsifier for the "never silently degrade a real fault" half of
    :func:`phantom.app._ensure_data_root_writable` (the branch the L-1 lesson says
    to verify rather than assume dead): when ``data_root.mkdir`` raises an
    ``OSError`` but the substrate PROBE finds the directory genuinely writable
    (a transient mkdir glitch that nonetheless left a usable directory, or a
    concurrent creator), the helper must RE-RAISE the original error rather than
    convert it into a degrade: a degrade here would hide a real, non-substrate
    fault behind a "storage unavailable" boot.

    Drives it by pre-creating a writable ``data_root`` (so the probe succeeds) and
    patching ``Path.mkdir`` to raise an ``OSError`` for THAT path only. The error
    must propagate out of the lifespan (a crash, the honest outcome for an
    unclassified fault), and the instance must NOT be recorded degraded.

    Falsifier: make ``_ensure_data_root_writable`` degrade on ANY ``mkdir`` OSError
    (drop the probe re-classify) -> the writable-dir mkdir glitch is silently
    turned into a degrade -> the typed degraded set is populated and the lifespan
    does NOT raise -> RED.
    """
    # A genuinely writable, present data_root so the substrate probe returns False.
    instance_root = tmp_path / _INSTANCE_DATA_DIR
    instance_root.mkdir(parents=True)

    sentinel_message = "transient mkdir glitch over a writable substrate"
    real_mkdir = Path.mkdir

    def _mkdir_raising_for_instance_root(
        self: Path, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        # Only the per-instance data_root prep raises; every other boot mkdir
        # (bodies/, shard dirs, ...) delegates to the real implementation.
        if self == instance_root:
            raise OSError(sentinel_message)
        real_mkdir(self, mode, parents=parents, exist_ok=exist_ok)

    monkeypatch.setattr(Path, "mkdir", _mkdir_raising_for_instance_root)

    app = create_app(_settings(data_root=tmp_path))
    # The unclassified fault must propagate (crash), not be swallowed into a degrade.
    with pytest.raises(OSError, match=sentinel_message):
        async with asyncio.timeout(_LIFESPAN_TIMEOUT_SECONDS), _lifespan_of(app):
            pass
    assert list(app.state.degraded_boot) == [], (
        "a mkdir failure over a WRITABLE data_root is a real fault to re-raise, not "
        "a substrate degrade; the degraded map must stay empty"
    )
