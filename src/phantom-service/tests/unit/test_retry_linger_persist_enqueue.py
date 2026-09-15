"""Retry-linger RAM-to-disk migration trigger (review finding S12-3).

``Sender._on_retryable_failure`` ends in the retry-linger block: when a
retryable failure leaves a row whose body is still in RAM, and the row has
been un-delivered for longer than ``body_store.linger_seconds``, the sender
enqueues the chain against the :class:`PersistController` so the body
migrates to disk. That block is THE DEFAULT PRODUCTION TRIGGER for
RAM-to-disk migration in hybrid mode.

Why this file exists: branch coverage over the whole service unit lane
showed the block never executed. The one test that called
``_on_retryable_failure`` used ``FixedIntervalsStrategy([])``, so the retry
budget was already spent, the handler parked the row in ``stored`` and
returned before ever reaching the linger block; every other persist test
called ``PersistController.enqueue`` directly and bypassed the trigger. No
e2e test lowers ``linger_seconds`` either, bar one stress-marked test that
the default suite and every per-PR job exclude.

What an uncovered trigger costs: an inverted comparison, a
minutes-for-seconds units error, or a regression to the ``body_location``
guard all mean RAM bodies NEVER migrate on linger. With an upstream down
for hours in hybrid mode the entire backlog stays in RAM and is lost on the
next crash, which is the precise durability promise CONTEXT.md makes for
hybrid mode.

The four tests below are the four exits of that block: migrate, inside the
window, already on disk, and no controller wired. Together they pin the
comparison direction, its units, and both guards.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from phantom.chain.executor import Failed5xx
from phantom.config.settings import BodyStoreCfg, InstanceCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadRow
from phantom.storage import FileBodyStore, RamBodyStore, SqliteUploadStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.workers.saturation import SaturationGate
from phantom.workers.sender import Sender

from .conftest import make_snapshot, snapshot_thunk, track_instance

# The configured linger window for every test here. The production default
# is also 90 s, so these tests read against the shipped value.
_LINGER_SECONDS: int = 90
# Row age used for the migrate case. Comfortably past the window, yet far
# BELOW ``_LINGER_SECONDS`` interpreted as minutes (5400 s), so a
# minutes-for-seconds units error in the comparison fails this test.
_AGE_PAST_LINGER_SECONDS: int = 120
# Row age used for the no-migrate case. Comfortably inside the window, so
# an inverted comparison fails this test.
_AGE_WITHIN_LINGER_SECONDS: int = 30
# A retry ladder with rungs left, so the handler takes the reschedule path
# rather than parking the row in ``stored`` and returning early. This is
# exactly what the pre-existing caller got wrong: an empty ladder returns
# before the linger block is ever reached.
_RETRY_INTERVALS_SECONDS: tuple[int, ...] = (1, 5)
# Upstream status for the injected retryable failure. Any 5xx does.
_RETRYABLE_STATUS: int = 503


class _RecordingPersistController:
    """Records every ``enqueue`` chain_id; otherwise inert.

    Stands in for :class:`phantom.workers.persist_controller.PersistController`.
    The sender only ever awaits ``enqueue`` and ignores the result, so a
    coroutine returning ``None`` is a faithful duck-typed stand-in and
    keeps the test free of a real migration worker.
    """

    def __init__(self) -> None:
        self.enqueued: list[UUID] = []

    async def enqueue(self, chain_id: UUID) -> None:
        """Record the migration request the sender issued."""
        self.enqueued.append(chain_id)


async def _build_instance(
    tmp_path: Path,
    *,
    persist_controller: _RecordingPersistController | None,
) -> InstanceContext:
    """Minimal real-store hybrid instance wired to a recording controller.

    The store is real because the handler under test writes a row before
    it reaches the linger block, and the write has to land for the handler
    to continue. The executor, upstream client and token cache are never
    touched on this path and stay inert mocks.

    Args:
        tmp_path: Per-test directory for the sqlite store and body tree.
        persist_controller: The migration target, or ``None`` to model the
            ``all_ram`` / ``all_disk`` composition where no controller is
            spawned.

    Returns:
        The tracked :class:`InstanceContext`, registered for teardown.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    await store.start()
    await body_store.start()
    cfg = InstanceCfg(
        id="emu",
        host_prefixes=["files.example.com"],
        data_dir="emu",
        routes=[RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer")],
    )
    saturation = SaturationGate(
        max_in_flight=10, max_in_flight_bytes=10_000_000, max_disk_bytes=10_000_000
    )
    snapshot = make_snapshot(
        body_store=BodyStoreCfg(
            mode="hybrid",
            ram_ceiling_bytes=1_073_741_824,
            linger_seconds=_LINGER_SECONDS,
        )
    )
    instance = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=persist_controller,  # type: ignore[arg-type]  # recording stand-in
        token_cache=MagicMock(),
        minter=None,
        retry_strategy=FixedIntervalsStrategy(list(_RETRY_INTERVALS_SECONDS)),
        upstream_client=MagicMock(),
        executor=MagicMock(),
        saturation=saturation,
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(snapshot),
    )
    return track_instance(instance)


def _claimed_row(
    make_upload_row: Callable[..., UploadRow], *, age_seconds: int, body_location: str
) -> UploadRow:
    """Build an ``attempting`` row of a given age and body location.

    ``received_at`` is what the handler measures the linger window
    against, so the age is set there rather than on ``updated_at``.
    """
    received_at = datetime.now(tz=UTC) - timedelta(seconds=age_seconds)
    return make_upload_row(
        state="attempting",
        route_name="files",
        attempts=0,
        body_location=body_location,
        received_at=received_at,
    )


@pytest.mark.asyncio
async def test_retry_linger_migrates_a_ram_body_past_the_window(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: a retryable failure past the linger window enqueues the RAM body.

    Expected outcome: the row is rescheduled to ``queued`` with its
    attempt counted, AND the chain is enqueued exactly once against the
    persist controller so the body migrates to disk.

    Falsifier: invert the window comparison, read ``linger_seconds`` as
    minutes, or drop the enqueue, and ``enqueued`` stays empty, so RED.
    """
    controller = _RecordingPersistController()
    instance = await _build_instance(tmp_path, persist_controller=controller)
    row = _claimed_row(make_upload_row, age_seconds=_AGE_PAST_LINGER_SECONDS, body_location="ram")
    await instance.store.insert(row)

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._on_retryable_failure(instance.store, row, Failed5xx(status=_RETRYABLE_STATUS))

    assert controller.enqueued == [row.chain_id], (
        f"a RAM body un-delivered for {_AGE_PAST_LINGER_SECONDS}s past a "
        f"{_LINGER_SECONDS}s linger window must be enqueued for migration; "
        f"the sender enqueued {controller.enqueued}"
    )
    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "queued"
    assert fresh.attempts == 1
    assert fresh.next_attempt_at is not None


@pytest.mark.asyncio
async def test_retry_inside_the_linger_window_leaves_the_body_in_ram(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: a retryable failure inside the linger window must NOT migrate.

    Expected outcome: the row is rescheduled to ``queued`` exactly as
    above, and nothing is enqueued. This is the arm that pins the
    comparison DIRECTION: without it an always-migrate regression, which
    defeats the point of the RAM tier, passes the sibling test above.

    Falsifier: invert the window comparison and this goes RED.
    """
    controller = _RecordingPersistController()
    instance = await _build_instance(tmp_path, persist_controller=controller)
    row = _claimed_row(make_upload_row, age_seconds=_AGE_WITHIN_LINGER_SECONDS, body_location="ram")
    await instance.store.insert(row)

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._on_retryable_failure(instance.store, row, Failed5xx(status=_RETRYABLE_STATUS))

    assert controller.enqueued == [], (
        f"a RAM body only {_AGE_WITHIN_LINGER_SECONDS}s old is inside the "
        f"{_LINGER_SECONDS}s linger window and must stay in RAM; the sender "
        f"enqueued {controller.enqueued}"
    )
    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "queued"
    assert fresh.attempts == 1


@pytest.mark.asyncio
async def test_retry_linger_never_migrates_a_row_already_on_disk(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the linger trigger only ever fires for a RAM-resident body.

    Expected outcome: a row already at ``body_location='file'`` is
    rescheduled normally and never enqueued, however old it is. The
    :class:`PersistController` is the sole writer of the
    ``ram -> file`` flip, and re-enqueueing a row already flipped would
    put a second migration behind a durability commit point that has
    already passed.

    Falsifier: drop the ``body_location`` guard and this goes RED.
    """
    controller = _RecordingPersistController()
    instance = await _build_instance(tmp_path, persist_controller=controller)
    row = _claimed_row(make_upload_row, age_seconds=_AGE_PAST_LINGER_SECONDS, body_location="file")
    await instance.store.insert(row)

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._on_retryable_failure(instance.store, row, Failed5xx(status=_RETRYABLE_STATUS))

    assert controller.enqueued == [], (
        "a row whose body is already on disk must never be enqueued for "
        f"migration; the sender enqueued {controller.enqueued}"
    )
    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "queued"


@pytest.mark.asyncio
async def test_retry_linger_is_inert_without_a_persist_controller(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the trigger is inert in the modes that spawn no controller.

    Expected outcome: with ``persist_controller=None``, which is how
    ``all_ram`` and ``all_disk`` are composed, an aged RAM row still
    completes its reschedule write and the handler returns cleanly.

    Falsifier: drop the ``controller is None`` guard and the handler
    raises ``AttributeError`` on every retryable failure in those two
    production modes, so RED.
    """
    instance = await _build_instance(tmp_path, persist_controller=None)
    row = _claimed_row(make_upload_row, age_seconds=_AGE_PAST_LINGER_SECONDS, body_location="ram")
    await instance.store.insert(row)

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._on_retryable_failure(instance.store, row, Failed5xx(status=_RETRYABLE_STATUS))

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "queued"
    assert fresh.attempts == 1
