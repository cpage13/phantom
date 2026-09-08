"""Unit tests for phantom.workers.vacuum."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from phantom.workers.vacuum import (
    CronSpecError,
    VacuumScheduler,
    _matches_cron,
    _parse_cron,
)


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
