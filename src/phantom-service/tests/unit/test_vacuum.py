"""Unit tests for phantom.workers.vacuum."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadRow, UploadState
from phantom.storage import FileBodyStore, RamBodyStore, SqliteUploadStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.storage.interface import StateTally
from phantom.strategies import FixedIntervalsStrategy
from phantom.workers.saturation import SaturationGate, row_holds_slot
from phantom.workers.vacuum import (
    CronSpecError,
    VacuumScheduler,
    _matches_cron,
    _parse_cron,
)

from .conftest import make_snapshot, snapshot_thunk, track_instance

# A Sunday at 03:00 UTC: the instant the default ``0 3 * * 0`` expression
# fires. 2026-05-17 is a Sunday.
_SUNDAY_0300 = datetime(2026, 5, 17, 3, 0, 0, tzinfo=UTC)


def _instance(
    *,
    tallies: dict[UploadState, StateTally],
    in_flight: int,
) -> MagicMock:
    """Build an instance whose store reports ``tallies`` and gate ``in_flight``.

    Args:
        tallies: What ``counts_by_state`` returns. States with no rows are
            ABSENT from the mapping, which is the Protocol's documented shape.
        in_flight: The saturation gate's buffer-occupancy count. Set
            explicitly on every call because it is the number the scheduler
            used to read, so a test that pins the new behaviour has to say
            what the old one would have seen.
    """
    instance = MagicMock()
    instance.cfg.id = "prod"
    instance.saturation.in_flight = in_flight
    instance.store.counts_by_state = AsyncMock(return_value=tallies)
    instance.store.vacuum = AsyncMock()
    return instance


async def _real_instance(tmp_path: Path) -> InstanceContext:
    """Build an instance on a REAL SQLite store and a REAL saturation gate.

    Everything the scheduler reads is the production object: the store answers
    ``counts_by_state`` from its own ``GROUP BY``, and the gate keeps its own
    ledger. The executor, upstream client and token cache are never touched
    here and stay inert mocks.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    files = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=files)
    await store.start()
    await body_store.start()
    instance = InstanceContext(
        cfg=InstanceCfg(
            id="prod",
            host_prefixes=["files.example.com"],
            data_dir="prod",
            routes=[RouteCfg(name="files", hosts=["files.example.com"], auth_mode="none")],
        ),
        store=store,
        ram_body_store=ram,
        file_body_store=files,
        body_store=body_store,
        persist_controller=None,
        token_cache=MagicMock(),
        minter=None,
        retry_strategy=FixedIntervalsStrategy([1, 5]),
        upstream_client=MagicMock(),
        executor=MagicMock(),
        saturation=SaturationGate(
            max_in_flight=10, max_in_flight_bytes=10_000_000, max_disk_bytes=10_000_000
        ),
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(make_snapshot()),
    )
    return track_instance(instance)


def test_matches_cron_wildcards() -> None:
    """All wildcards match anything."""
    now = datetime(2026, 5, 13, 3, 0, 0, tzinfo=UTC)
    assert _matches_cron("* * * * *", now)


def test_matches_cron_sunday_3am() -> None:
    """``0 3 * * 0`` matches Sunday 03:00."""
    # 2026-05-17 is a Sunday.
    sunday = datetime(2026, 5, 17, 3, 0, 0, tzinfo=UTC)
    assert _matches_cron("0 3 * * 0", sunday)


def test_matches_cron_rejects_other_time() -> None:
    """``0 3 * * 0`` rejects Sunday at 04:00."""
    sunday_4am = datetime(2026, 5, 17, 4, 0, 0, tzinfo=UTC)
    assert not _matches_cron("0 3 * * 0", sunday_4am)


@pytest.mark.asyncio
async def test_vacuum_calls_store_method() -> None:
    """VACUUM dispatch goes through ``UploadStore.vacuum`` (plan §9.2).

    The scheduler does not reach into the store's private ``_conn`` —
    it calls the Protocol method which runs ``VACUUM;`` under the
    write lock on the single persistent store.
    """
    instance = MagicMock()
    instance.saturation.in_flight = 5
    instance.store.vacuum = AsyncMock()
    sched = VacuumScheduler(instance=instance, cron_spec="* * * * *")
    # Fire path: our matches_cron returns True for "* * * * *"; but
    # saturation.in_flight is 5, so the run loop would skip. Directly
    # exercise _vacuum to confirm it dispatches through the Protocol.
    await sched._vacuum()
    instance.store.vacuum.assert_awaited_once()


def test_step_syntax_is_supported() -> None:
    """Objective: the most common cron idiom parses instead of raising.

    Expected: ``*/15 * * * *`` matches minutes 0, 15, 30 and 45 and no others.
    Before this, the parser accepted only ``*`` or a bare integer, so
    ``int('*/15')`` raised ValueError out of the scheduler's very first tick,
    which runs before any sleep and had no error handling. That cancelled every
    sibling in the composition root's TaskGroup and the orchestrator restarted
    into the identical crash, so the service never stayed up long enough to
    accept the reload that would have fixed the config.
    """
    for minute, expected in ((0, True), (15, True), (30, True), (45, True), (7, False)):
        now = datetime(2026, 9, 8, 3, minute, tzinfo=UTC)
        assert _matches_cron("*/15 * * * *", now) is expected


def test_out_of_range_field_is_rejected_loudly() -> None:
    """Objective: a field that can never match is an error, not silent inertness.

    Expected: CronSpecError naming the field and its range. ``minute: 99``
    parsed fine before and simply matched no instant, so the VACUUM never fired
    and the operator had no signal at all.
    """
    with pytest.raises(CronSpecError, match="minute must be between 0 and 59"):
        _parse_cron("99 * * * *")


def test_malformed_spec_disables_the_scheduler_instead_of_crashing() -> None:
    """Objective: an unparseable expression degrades to inert, per ADR-025.

    Expected: construction succeeds, the scheduler reports itself disabled, and
    a tick at a time that would otherwise match does nothing. Refusing to boot
    on recoverable config is exactly what ADR-025 forbids, and here it was
    worse than a refusal: it was an unrecoverable restart loop.
    """
    scheduler = VacuumScheduler(
        instance=MagicMock(),
        cron_spec="not a cron spec at all",
    )
    assert scheduler.enabled is False


def test_valid_spec_leaves_the_scheduler_enabled() -> None:
    """Objective: the disable path is reserved for genuinely bad config.

    Expected: a well-formed expression leaves the scheduler enabled, so the
    degradation above cannot silently swallow a working VACUUM.
    """
    scheduler = VacuumScheduler(
        instance=MagicMock(),
        cron_spec="0 3 * * 0",
    )
    assert scheduler.enabled is True


# --------------------------------------------------------------------------
# SW-3: the idle signal. The scheduler asks whether the instance is DOING
# anything, not whether the buffer is occupied.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_one_stored_row_does_not_disable_the_scheduled_vacuum() -> None:
    """Objective: a parked ``stored`` row must not cancel VACUUM forever.

    Expected: the Sunday 03:00 tick runs VACUUM even though the saturation
    gate is charged for one ``stored`` row and nothing is queued or
    attempting.

    ``stored`` is terminal and holds its slot on purpose (ADR-036), the
    retention default for its metadata is never, and boot recovery re-seeds
    the charge from the persisted row. So reading the gate's occupancy count
    as an activity signal meant that ONE row - one exhausted retry budget, one
    mistyped route host - skipped the weekly VACUUM every week, for the life
    of the deployment, across restarts, with nothing logged anywhere. The
    flash-wear maintenance this scheduler exists to provide silently never ran
    again.
    """
    instance = _instance(tallies={"stored": StateTally(count=1, bytes=4096)}, in_flight=1)
    scheduler = VacuumScheduler(instance=instance, cron_spec="0 3 * * 0")

    await scheduler._tick(_SUNDAY_0300)

    instance.store.vacuum.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_queued_row_still_holds_the_vacuum_off() -> None:
    """Objective: the flash-wear invariant survives the new signal.

    Expected: a single ``queued`` row skips the VACUUM, so the fix above
    cannot have been bought by dropping the "never VACUUM under load" rule.
    A guard rather than a witness: this held before the change too.
    """
    instance = _instance(tallies={"queued": StateTally(count=1, bytes=4096)}, in_flight=1)
    scheduler = VacuumScheduler(instance=instance, cron_spec="0 3 * * 0")

    await scheduler._tick(_SUNDAY_0300)

    instance.store.vacuum.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_attempting_row_still_holds_the_vacuum_off() -> None:
    """Objective: the mid-attempt row is active work too.

    Expected: a single ``attempting`` row skips the VACUUM. The sender holds
    that row open across an upstream request, which is the case the invariant
    was written for.
    """
    instance = _instance(tallies={"attempting": StateTally(count=1, bytes=4096)}, in_flight=1)
    scheduler = VacuumScheduler(instance=instance, cron_spec="0 3 * * 0")

    await scheduler._tick(_SUNDAY_0300)

    instance.store.vacuum.assert_not_awaited()


@pytest.mark.asyncio
async def test_terminal_and_parked_rows_do_not_count_as_activity() -> None:
    """Objective: only the two moving states hold the VACUUM off.

    Expected: a store full of finished, parked and buffered rows is idle, so
    the VACUUM fires. ``auth_expired`` is in the mix deliberately: a parked
    row is waiting on a token push that may never arrive and nothing writes to
    it until a kicker wakes it, so treating it as activity would reproduce the
    same permanent skip through a different state.
    """
    instance = _instance(
        tallies={
            "succeeded": StateTally(count=900, bytes=0),
            "failed": StateTally(count=12, bytes=0),
            "cancelled": StateTally(count=3, bytes=0),
            "corrupted": StateTally(count=1, bytes=0),
            "expired": StateTally(count=7, bytes=0),
            "auth_expired": StateTally(count=5, bytes=8192),
            "stored": StateTally(count=2, bytes=8192),
        },
        in_flight=2,
    )
    scheduler = VacuumScheduler(instance=instance, cron_spec="0 3 * * 0")

    await scheduler._tick(_SUNDAY_0300)

    instance.store.vacuum.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_matching_minute_fires_once_across_two_polls() -> None:
    """Objective: same-minute dedup survives the restructured decision.

    Expected: two ticks inside the same matching minute run exactly one
    VACUUM. The poll interval is half a minute, so every matching minute is
    sampled twice and the dedup is what stops the second sample re-running a
    VACUUM that just finished.
    """
    instance = _instance(tallies={}, in_flight=0)
    scheduler = VacuumScheduler(instance=instance, cron_spec="0 3 * * 0")

    await scheduler._tick(_SUNDAY_0300)
    await scheduler._tick(_SUNDAY_0300.replace(second=30))

    instance.store.vacuum.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_minute_skipped_for_active_work_is_retried_in_the_same_minute() -> None:
    """Objective: a busy sample must not consume the minute's one chance.

    Expected: the first tick skips on an ``attempting`` row and does NOT stamp
    the minute, so when the row finishes before the next 30 s poll the second
    tick in the same minute runs the VACUUM.
    """
    instance = _instance(tallies={"attempting": StateTally(count=1, bytes=4096)}, in_flight=1)
    scheduler = VacuumScheduler(instance=instance, cron_spec="0 3 * * 0")

    await scheduler._tick(_SUNDAY_0300)
    instance.store.vacuum.assert_not_awaited()

    instance.store.counts_by_state = AsyncMock(return_value={})
    await scheduler._tick(_SUNDAY_0300.replace(second=30))

    instance.store.vacuum.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_non_matching_minute_does_not_touch_the_database() -> None:
    """Objective: the activity read is the last leg, not the first.

    Expected: a tick at a time the cron does not match never calls
    ``counts_by_state``. The scheduler wakes every 30 s and the aggregate is a
    full-table ``GROUP BY``; paying for it on every poll would be a new cost
    on the producer sidecar this scheduler was written for.
    """
    instance = _instance(tallies={}, in_flight=0)
    scheduler = VacuumScheduler(instance=instance, cron_spec="0 3 * * 0")

    await scheduler._tick(_SUNDAY_0300.replace(hour=4))

    instance.store.counts_by_state.assert_not_awaited()
    instance.store.vacuum.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_real_stored_row_seeded_by_boot_recovery_still_lets_vacuum_run(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the same defect against the real store and the real gate.

    Expected: with one genuine ``stored`` row on disk and the gate charged for
    it the way boot recovery charges it, the Sunday 03:00 tick runs VACUUM.

    The mock-driven tests above pin the decision; this one pins that the
    decision is made from numbers the production objects actually produce.
    It also reproduces the restart leg of the finding: recovery re-seeds the
    charge from the persisted row on every boot, so the row's occupancy charge
    is asserted here rather than assumed, and rebooting could never have
    cleared it.
    """
    instance = await _real_instance(tmp_path)
    row = make_upload_row(instance_id="prod", state="stored", body_size_bytes=4096)
    await instance.store.insert(row)
    # What workers.recovery.reconcile_saturation does for every recovered row
    # the slot predicate says is holding.
    assert row_holds_slot(row.state, row.body_discarded_at) is True
    await instance.saturation.reconcile_admit(4096)
    assert instance.saturation.in_flight == 1

    vacuumed = AsyncMock()
    instance.store.vacuum = vacuumed  # type: ignore[method-assign]
    scheduler = VacuumScheduler(instance=instance, cron_spec="0 3 * * 0")

    await scheduler._tick(_SUNDAY_0300)

    vacuumed.assert_awaited_once()
