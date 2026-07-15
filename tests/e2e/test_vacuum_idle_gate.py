"""Running-service VACUUM idle gate through the composed scheduler (audit T10 / G5).

The cron parser and the decision predicate have unit coverage, but nothing ever
crossed the production store/saturation boundary: no test proved that the
COMPOSED scheduler (the instance ``app.py`` spawns under the TaskGroup) skips
VACUUM while an upload is in flight, fires exactly once on an idle matching
minute, dedups the same minute, and actually reclaims SQLite free pages.

Lane (the audit's sanctioned seam): production gained one private refactor,
``VacuumScheduler._tick(now)``, holding the complete existing decision
(slot dedup + cron match + ``saturation.in_flight == 0``) and the ``_vacuum``
call; ``run`` is loop coordination around it. The test patches the
``phantom.app.VacuumScheduler`` symbol before ``create_app`` with a subclass
that captures the composed instance, injects a mutable clock, and overrides
ONLY loop coordination (a test-owned wake event replaces the fixed 30 s wait).
No decision logic is copied; every tick that runs is the inherited production
``_tick``.

Free-page preconditions are real: the uploads database is bloated and a
scratch table dropped BEFORE boot (schema stays canonical for the boot schema
gate), so ``PRAGMA freelist_count > 0`` going in, and the idle VACUUM must
bring it to zero without growing ``page_count``.

The exception arm proves supervision (invariant #15): a raising
``store.vacuum`` on the captured composed instance escapes the inherited
``_tick``, aborts the composition root's TaskGroup, and tears down the app.
That arm enters the production lifespan directly
(``app.router.lifespan_context``), because in-process uvicorn would keep
serving after a lifespan failure; the abort itself is the proof (the
production CLI's fatal-worker bridge rides exactly this propagation).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar
from uuid import uuid4

import pytest
from phantom.app import create_app
from phantom.config.settings import InstanceCfg, RouteCfg, Settings, StorageCfg
from phantom.storage.sqlite_store import SqliteUploadStore
from phantom.workers.vacuum import VacuumScheduler
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._harness.subprocess_harness import submit_one
from tests.e2e.helpers.stack import boot_stack
from tests.e2e.helpers.timing import await_until

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]

# The suite's pinned cron is "0 3 * * 0" (Sunday 03:00). 2026-01-04 and
# 2026-01-11 are Sundays; seconds vary to show sub-minute times still match.
_HELD_MATCHING_TIME = datetime(2026, 1, 4, 3, 0, 30, tzinfo=UTC)
_IDLE_MATCHING_TIME = datetime(2026, 1, 11, 3, 0, 15, tzinfo=UTC)
_SAME_SLOT_AGAIN = datetime(2026, 1, 11, 3, 0, 45, tzinfo=UTC)
_NON_MATCHING_TIME = datetime(2026, 1, 11, 3, 5, 0, tzinfo=UTC)

# Bloat scale: 512 x 4 KiB zeroblob rows dropped pre-boot leaves a freelist
# far larger than anything boot-time activity could consume.
_BLOAT_ROWS = 512
_BLOAT_BLOB_BYTES = 4096

# One failed attempt then hour-scale backoff: the held row stays non-terminal
# (slot held, in_flight == 1) for the whole held-phase without racing the
# retry ladder toward the `failed` terminal state.
_HOLD_RETRY_INTERVALS = [0, 3600, 3600, 3600, 3600]

_READY_BUDGET_SECONDS = 10.0
_TICK_BUDGET_SECONDS = 5.0
_ATTEMPT_BUDGET_SECONDS = 15.0
_SUCCEEDED_BUDGET_SECONDS = 20.0
_SUPERVISION_BUDGET_SECONDS = 30.0
_LIFESPAN_BUDGET_SECONDS = 60.0
# Wake-poll granularity of the controlled run loop (coordination only).
_WAKE_POLL_SECONDS = 0.05

_VACUUM_LOG_MESSAGE = "Running SQLite VACUUM on persistent store"
_VACUUM_LOGGER = "phantom.workers.vacuum"


class _VacuumBoomError(RuntimeError):
    """Marker exception injected through the captured store's vacuum."""


class _ControlledVacuumScheduler(VacuumScheduler):
    """Production ``_tick``, test-owned loop coordination.

    Constructed by ``app.py`` itself (the patched symbol receives the real
    composed kwargs). ``run`` signals ``ready`` and then fires the inherited
    production ``_tick`` only when the test sets ``wake`` — so the immediate
    startup tick of the production loop cannot race the test's phase setup,
    and no decision logic is duplicated.
    """

    instances: ClassVar[list[_ControlledVacuumScheduler]] = []

    def __init__(self, *, instance: Any, cron_spec: str, clock: Any = None) -> None:
        self.now: datetime = _HELD_MATCHING_TIME
        super().__init__(instance=instance, cron_spec=cron_spec, clock=lambda: self.now)
        self.composed_instance = instance
        self.ready = asyncio.Event()
        self.wake = asyncio.Event()
        self.ticked = asyncio.Event()
        type(self).instances.append(self)

    async def run(self, stop_event: asyncio.Event) -> None:
        """Wake-event loop coordination; every tick is the inherited one."""
        self.ready.set()
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=_WAKE_POLL_SECONDS)
            except TimeoutError:
                continue
            self.wake.clear()
            await self._tick(self._clock())
            self.ticked.set()


async def _wake_once(scheduler: _ControlledVacuumScheduler, at: datetime) -> None:
    """Drive exactly one production tick at the injected time ``at``."""
    scheduler.now = at
    scheduler.ticked.clear()
    scheduler.wake.set()
    await asyncio.wait_for(scheduler.ticked.wait(), timeout=_TICK_BUDGET_SECONDS)


async def _seed_free_pages(db_path: Path) -> tuple[int, int]:
    """Create the canonical schema, then real free pages (bloat + drop).

    The scratch table is dropped before boot, so ``sqlite_master`` is
    byte-canonical for the boot schema gate while the freed pages stay on
    the freelist (no VACUUM has run).

    Returns:
        ``(freelist_count, page_count)`` measured post-seed via a fresh
        read-only connection.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteUploadStore(str(db_path))
    await store.start()
    await store.stop()
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        conn.execute("CREATE TABLE _bloat (x BLOB)")
        conn.execute(
            "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c WHERE i < ?) "
            "INSERT INTO _bloat SELECT zeroblob(?) FROM c",
            (_BLOAT_ROWS, _BLOAT_BLOB_BYTES),
        )
        conn.commit()
        conn.execute("DROP TABLE _bloat")
        conn.commit()
    freelist, pages = _db_pragmas(db_path)
    return freelist, pages


def _db_pragmas(db_path: Path) -> tuple[int, int]:
    """Read ``(freelist_count, page_count)`` via a fresh read-only connection."""
    with contextlib.closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)) as conn:
        freelist = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        pages = int(conn.execute("PRAGMA page_count").fetchone()[0])
    return freelist, pages


def _vacuum_log_count(caplog: pytest.LogCaptureFixture) -> int:
    """Count captured production VACUUM log records."""
    return sum(
        1
        for record in caplog.records
        if record.name == _VACUUM_LOGGER and record.getMessage() == _VACUUM_LOG_MESSAGE
    )


async def test_composed_scheduler_gates_on_in_flight_and_fires_idle_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The composed scheduler skips under load, fires once idle, dedups, reclaims.

    Objective: matching minute + one held in-flight upload -> no VACUUM and
    unchanged page/freelist counts; after release + next matching minute ->
    exactly one VACUUM, freelist reclaimed to zero, page_count bounded by the
    pre-boot bloated value; same minute again -> no repeat; non-matching
    minute -> nothing; a fresh upload still delivers afterward.
    """
    db_path = tmp_path / "primary" / "uploads.db"
    freelist_seeded, pages_seeded = await _seed_free_pages(db_path)
    assert freelist_seeded > 0, "precondition failed: seeding produced no free pages"

    _ControlledVacuumScheduler.instances.clear()
    monkeypatch.setattr("phantom.app.VacuumScheduler", _ControlledVacuumScheduler)

    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "retry": {
                "default_strategy": {
                    "type": "fixed_intervals",
                    "intervals_seconds": _HOLD_RETRY_INTERVALS,
                }
            }
        },
    )
    # The app's boot ran configure_logging, which CLEARS root handlers
    # (observability/logging.py) and thereby evicts caplog's root catching
    # handler. Attach caplog's handler to the vacuum logger directly so the
    # audit's log-count assertions observe the production records regardless
    # of the root handler state.
    vacuum_logger = logging.getLogger(_VACUUM_LOGGER)
    vacuum_logger.addHandler(caplog.handler)
    try:
        with caplog.at_level(logging.INFO, logger=_VACUUM_LOGGER):
            assert len(_ControlledVacuumScheduler.instances) == 1, (
                "expected exactly one composed scheduler (single-instance config)"
            )
            scheduler = _ControlledVacuumScheduler.instances[0]
            await asyncio.wait_for(scheduler.ready.wait(), timeout=_READY_BUDGET_SECONDS)
            saturation = scheduler.composed_instance.saturation

            # Hold one upload in flight: every upstream create 5xxes, and the
            # retry ladder's hour-scale backoff keeps the row non-terminal.
            stack.emulator.inject_failure(
                FailurePolicy(  # type: ignore[call-arg]  # pydantic defaults; plugin unavailable
                    scope=FailureScope.UPSTREAM_FILES_CREATE,
                    error_rate_5xx=1.0,
                )
            )
            held_chain_id = uuid4()
            await submit_one(
                stack.phantom_client,
                emulator_url=stack.emulator_url,
                bearer=stack.fake_security_token(),
                body=b"t10-held-body",
                chain_id=held_chain_id,
            )

            async def _first_attempt_recorded() -> bool:
                detail = await stack.phantom_client.get_upload(held_chain_id)
                return detail.attempts >= 1

            await await_until(_first_attempt_recorded, timeout_seconds=_ATTEMPT_BUDGET_SECONDS)
            assert saturation.in_flight == 1, (
                f"expected the held upload to hold one slot, in_flight={saturation.in_flight}"
            )

            # Baseline AFTER boot + held submit so phase deltas are exact.
            freelist_before, pages_before = _db_pragmas(db_path)
            assert freelist_before > 0, (
                "precondition failed: boot activity consumed the entire freelist"
            )

            # Phase A: matching minute, held in flight -> gate refuses.
            await _wake_once(scheduler, _HELD_MATCHING_TIME)
            assert _vacuum_log_count(caplog) == 0, "VACUUM ran while an upload was in flight"
            assert _db_pragmas(db_path) == (freelist_before, pages_before), (
                "database changed on a held tick"
            )

            # Release: operator cancel is terminal -> the slot frees.
            await stack.phantom_client.cancel(held_chain_id)

            async def _idle() -> bool:
                return int(saturation.in_flight) == 0

            await await_until(_idle, timeout_seconds=_ATTEMPT_BUDGET_SECONDS)

            # Phase B: next matching minute, idle -> exactly one VACUUM.
            await _wake_once(scheduler, _IDLE_MATCHING_TIME)
            assert _vacuum_log_count(caplog) == 1, (
                "idle matching tick did not produce exactly one VACUUM log"
            )
            freelist_after, pages_after = _db_pragmas(db_path)
            assert freelist_after == 0, (
                f"VACUUM left {freelist_after} free pages (expected full reclaim)"
            )
            assert pages_after <= pages_seeded, (
                f"page_count grew past the pre-boot bloated value: {pages_after} > {pages_seeded}"
            )

            # Phase C: same minute slot again -> deduped, no file change.
            await _wake_once(scheduler, _SAME_SLOT_AGAIN)
            assert _vacuum_log_count(caplog) == 1, "same-minute tick re-ran VACUUM"
            assert _db_pragmas(db_path) == (freelist_after, pages_after), (
                "database changed on a deduped tick"
            )

            # Phase D: non-matching minute -> negative control.
            await _wake_once(scheduler, _NON_MATCHING_TIME)
            assert _vacuum_log_count(caplog) == 1, "non-matching minute fired VACUUM"

            # Availability: the service still delivers after the VACUUM cycle.
            stack.emulator.clear_failures()
            fresh_chain_id = uuid4()
            await submit_one(
                stack.phantom_client,
                emulator_url=stack.emulator_url,
                bearer=stack.fake_security_token(),
                body=b"t10-post-vacuum-body",
                chain_id=fresh_chain_id,
            )

            async def _fresh_succeeded() -> bool:
                detail = await stack.phantom_client.get_upload(fresh_chain_id)
                return detail.state == "succeeded"

            await await_until(_fresh_succeeded, timeout_seconds=_SUCCEEDED_BUDGET_SECONDS)
    finally:
        vacuum_logger.removeHandler(caplog.handler)
        await stack.tear_down()


def _exception_arm_settings(data_root: Path) -> Settings:
    """Production-shaped single-instance Settings for the supervision arm."""
    hosts = ["files.example.com"]
    return Settings(
        storage=StorageCfg(data_dir=str(data_root)),
        instances=[
            InstanceCfg(
                id="primary",
                host_prefixes=hosts,
                data_dir="primary",
                routes=[RouteCfg(name="upstream-files", hosts=hosts, auth_mode="phantom_bearer")],
            )
        ],
    )


async def test_vacuum_exception_escapes_scheduler_and_aborts_composition_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A raising ``store.vacuum`` tears down the composed app (invariant #15).

    Objective: replace ONLY the captured composed instance's ``store.vacuum``
    with a raising wrapper, wake one idle matching tick, and require the
    inherited production ``_tick`` exception to escape the scheduler task,
    abort the composition root's TaskGroup (this test task is cancelled at
    its wait point), and surface as an ExceptionGroup carrying the injected
    error out of the production lifespan. Nothing in the controlled subclass
    catches it.
    """
    _ControlledVacuumScheduler.instances.clear()
    monkeypatch.setattr("phantom.app.VacuumScheduler", _ControlledVacuumScheduler)

    app = create_app(_exception_arm_settings(tmp_path))

    async def _boom() -> None:
        raise _VacuumBoomError("t10 injected vacuum fault")

    with pytest.raises(BaseExceptionGroup) as excinfo:
        async with asyncio.timeout(_LIFESPAN_BUDGET_SECONDS):
            async with app.router.lifespan_context(app):
                assert len(_ControlledVacuumScheduler.instances) == 1
                scheduler = _ControlledVacuumScheduler.instances[0]
                await asyncio.wait_for(scheduler.ready.wait(), timeout=_READY_BUDGET_SECONDS)
                scheduler.composed_instance.store.vacuum = _boom
                scheduler.now = _IDLE_MATCHING_TIME
                scheduler.wake.set()
                # The TaskGroup abort must cancel this wait; reaching the
                # timeout instead means supervision failed to propagate.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        asyncio.Event().wait(), timeout=_SUPERVISION_BUDGET_SECONDS
                    )
                raise AssertionError("composition root kept running after the vacuum fault")
    assert excinfo.value.subgroup(_VacuumBoomError) is not None, (
        f"lifespan raised a group without the injected fault: {excinfo.value!r}"
    )
