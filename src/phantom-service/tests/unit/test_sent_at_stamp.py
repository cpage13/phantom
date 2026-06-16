"""The sent_at write-once stamp (cycle-7 task 2.5).

``record_attempt_result`` carries ``stamp_sent_at: bool = False``; the
UPDATE's CASE guard writes ``sent_at = updated_at`` ONLY while the column
is still NULL. The sender's chain-done success branch is the single
caller passing True, so ``sent_at`` permanently records the moment of
first confirmed upstream delivery.

Four storage-level contracts plus the sender wiring are pinned here:

1. ``sent_at`` stays NULL through every non-stamp transition.
2. A stamped delivery sets ``sent_at`` exactly once, equal to the same
   write's ``updated_at``.
3. The replay regression: succeed, operator replay, re-claim, resucceed.
   The ORIGINAL ``sent_at`` survives (the ``sent_at IS NULL`` guard);
   ``replay`` itself never clears the column.
4. A non-success transition passing ``stamp_sent_at=True`` by accident
   must still not overwrite an existing stamp.

Sender wiring: only ``_on_succeeded``'s ``chain_done`` branch stamps; a
mid-chain step success does not.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from phantom.chain.executor import Succeeded
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.models.upload import CapturedValues, UploadRow
from phantom.storage import FileBodyStore, RamBodyStore, SqliteUploadStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.workers.saturation import SaturationGate
from phantom.workers.sender import Sender

from .conftest import make_snapshot, snapshot_thunk, track_instance, track_started


@pytest.fixture
async def store(tmp_path: Path) -> SqliteUploadStore:
    """Live single-store fixture for the stamp contract."""
    s = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await s.start()
    return track_started(s)


# -----------------------------------------------------------------------------
# Storage-level contract: the CASE guard in record_attempt_result.
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sent_at_stays_null_on_every_non_stamp_transition(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Default ``stamp_sent_at=False`` never touches ``sent_at``.

    Every state the sender writes WITHOUT the stamp (mid-chain queued,
    stored, failed, auth_expired, corrupted) leaves the column NULL:
    sent_at is NULL until delivery.
    """
    for new_state in ("queued", "stored", "failed", "auth_expired", "corrupted"):
        row = make_upload_row(state="attempting")
        await store.insert(row)
        rowcount = await store.record_attempt_result(
            row.chain_id,
            new_state=new_state,
            attempts=1,
            next_attempt_at=None,
            last_error="x",
            upstream_status=None,
            upstream_headers_json=None,
            captured_values=None,
            current_step_index=None,
            last_step_completed=None,
        )
        assert rowcount == 1
        fresh = await store.get(row.chain_id)
        assert fresh is not None
        assert fresh.state == new_state
        assert fresh.sent_at is None, f"sent_at stamped on non-delivery state {new_state!r}"


@pytest.mark.asyncio
async def test_sent_at_set_exactly_once_at_confirmed_delivery(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``stamp_sent_at=True`` on the succeeded write stamps sent_at = updated_at."""
    row = make_upload_row(state="attempting")
    await store.insert(row)
    fetched = await store.get(row.chain_id)
    assert fetched is not None
    assert fetched.sent_at is None  # NULL until delivery

    rowcount = await store.record_attempt_result(
        row.chain_id,
        new_state="succeeded",
        attempts=1,
        next_attempt_at=None,
        last_error=None,
        upstream_status=200,
        upstream_headers_json=None,
        captured_values=None,
        current_step_index=None,
        last_step_completed=None,
        stamp_sent_at=True,
    )
    assert rowcount == 1
    fresh = await store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "succeeded"
    assert fresh.sent_at is not None
    # The stamp is the same timestamp the write put in updated_at.
    assert fresh.sent_at == fresh.updated_at


@pytest.mark.asyncio
async def test_replay_then_resucceed_preserves_original_sent_at(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """The exact replay regression: the ORIGINAL sent_at survives.

    Succeed (stamp), operator replay (reset to queued WITHOUT clearing
    sent_at), re-claim, resucceed with the stamp flag again. The
    ``sent_at IS NULL`` guard keeps the first confirmed-delivery time.
    """
    row = make_upload_row(state="attempting")
    await store.insert(row)
    await store.record_attempt_result(
        row.chain_id,
        new_state="succeeded",
        attempts=1,
        next_attempt_at=None,
        last_error=None,
        upstream_status=200,
        upstream_headers_json=None,
        captured_values=None,
        current_step_index=None,
        last_step_completed=None,
        stamp_sent_at=True,
    )
    first = await store.get(row.chain_id)
    assert first is not None
    original_sent_at = first.sent_at
    assert original_sent_at is not None

    # Operator replay: back to queued; sent_at is NOT cleared.
    replay_outcome = await store.replay(row.chain_id)
    assert replay_outcome is not None
    replayed = replay_outcome.row
    assert replayed is not None
    assert replayed.state == "queued"
    assert replayed.sent_at == original_sent_at

    # Sender re-claims (queued to attempting) and resucceeds with the stamp.
    claimed = await store.claim_due(datetime.now(tz=UTC), limit=1)
    assert [r.chain_id for r in claimed] == [row.chain_id]
    rowcount = await store.record_attempt_result(
        row.chain_id,
        new_state="succeeded",
        attempts=1,
        next_attempt_at=None,
        last_error=None,
        upstream_status=200,
        upstream_headers_json=None,
        captured_values=None,
        current_step_index=None,
        last_step_completed=None,
        stamp_sent_at=True,
    )
    assert rowcount == 1
    fresh = await store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "succeeded"
    # The IS NULL guard held: the original delivery time survives, while
    # updated_at moved past it on the second write.
    assert fresh.sent_at == original_sent_at
    assert fresh.updated_at > original_sent_at


@pytest.mark.asyncio
async def test_accidental_stamp_on_non_success_does_not_overwrite(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """``stamp_sent_at=True`` on a non-success transition cannot move a stamp.

    The store is deliberately state-agnostic; the IS NULL guard alone
    must protect an existing stamp even if a future caller passes the
    flag on a non-success write by mistake.
    """
    row = make_upload_row(state="attempting")
    await store.insert(row)
    await store.record_attempt_result(
        row.chain_id,
        new_state="succeeded",
        attempts=1,
        next_attempt_at=None,
        last_error=None,
        upstream_status=200,
        upstream_headers_json=None,
        captured_values=None,
        current_step_index=None,
        last_step_completed=None,
        stamp_sent_at=True,
    )
    first = await store.get(row.chain_id)
    assert first is not None
    original_sent_at = first.sent_at
    assert original_sent_at is not None

    # Replay, re-claim, then a NON-success transition with the flag
    # accidentally True.
    assert await store.replay(row.chain_id) is not None
    claimed = await store.claim_due(datetime.now(tz=UTC), limit=1)
    assert [r.chain_id for r in claimed] == [row.chain_id]
    rowcount = await store.record_attempt_result(
        row.chain_id,
        new_state="stored",
        attempts=1,
        next_attempt_at=None,
        last_error="budget_exhausted",
        upstream_status=None,
        upstream_headers_json=None,
        captured_values=None,
        current_step_index=None,
        last_step_completed=None,
        stamp_sent_at=True,
    )
    assert rowcount == 1
    fresh = await store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "stored"
    assert fresh.sent_at == original_sent_at


# -----------------------------------------------------------------------------
# Sender wiring: only the chain-done success branch passes the flag.
# -----------------------------------------------------------------------------


async def _build_instance(tmp_path: Path) -> InstanceContext:
    """Minimal real-store instance for driving ``_on_succeeded`` directly.

    The store, body stores, and saturation gate are real (the chain-done
    branch releases saturation and deletes the body); the executor,
    upstream client, and token cache are never touched by the handler
    under test and stay as inert mocks.
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
    instance = InstanceContext(
        cfg=cfg,
        store=store,
        ram_body_store=ram,
        file_body_store=fbs,
        body_store=body_store,
        persist_controller=None,
        token_cache=MagicMock(),
        minter=None,
        retry_strategy=FixedIntervalsStrategy([1, 5]),
        upstream_client=MagicMock(),
        executor=MagicMock(),
        saturation=saturation,
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(make_snapshot()),
    )
    return track_instance(instance)


def _succeeded(*, chain_done: bool) -> Succeeded:
    """Executor success result with the given chain_done flag."""
    return Succeeded(
        captured=CapturedValues(),
        next_step_index=1,
        chain_done=chain_done,
        step_name="step",
        upstream_status=200,
        upstream_headers={},
    )


@pytest.mark.asyncio
async def test_sender_chain_done_success_stamps_sent_at(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """The chain-done success branch is the one caller that stamps."""
    instance = await _build_instance(tmp_path)
    row = make_upload_row(state="attempting", route_name="files", body_size_bytes=7)
    await instance.store.insert(row)
    granted = await instance.saturation.admit(row.body_size_bytes)
    assert granted.__class__.__name__ == "AdmissionGranted", granted

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._on_succeeded(instance.store, row, _succeeded(chain_done=True))

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "succeeded"
    assert fresh.sent_at is not None
    assert fresh.sent_at == fresh.updated_at


@pytest.mark.asyncio
async def test_sender_mid_chain_success_does_not_stamp(
    tmp_path: Path, make_upload_row: Callable[..., UploadRow]
) -> None:
    """A mid-chain step success re-queues WITHOUT stamping sent_at."""
    instance = await _build_instance(tmp_path)
    row = make_upload_row(state="attempting", route_name="files", body_size_bytes=7)
    await instance.store.insert(row)

    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._on_succeeded(instance.store, row, _succeeded(chain_done=False))

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "queued"
    assert fresh.sent_at is None
