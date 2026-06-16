"""Unit tests for phantom.workers.vacuum."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from phantom.workers.vacuum import VacuumScheduler, _matches_cron


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
