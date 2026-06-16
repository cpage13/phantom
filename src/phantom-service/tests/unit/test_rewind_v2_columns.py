"""CaptureExpiredRewind vs the cycle-7 columns (grouping + sent_at).

Round 4 adversary hardening (iteration loop, task 7.3). The sender's
``_on_rewind`` handler (ADR-011 reexecute=True) re-queues a claimed row
at the capture-producing step with ``attempts=0`` (the rewind erases
burned budget BY DESIGN: the chain never reached the upstream). The
cycle-7 contract under attack:

* The rewind UPDATE must not touch ``group_id`` / ``multifile_id`` /
  ``send_order`` (recorded at admission, moved by nobody) and must
  leave ``sent_at`` NULL on a never-delivered row (no fabricated
  delivery time).
* A rewound row that later delivers stamps ``sent_at`` exactly once.
* A REPLAYED delivered row that rewinds mid-replay keeps its ORIGINAL
  ``sent_at`` stamp (write-once survives the rewind path exactly as it
  survives the replay itself).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest
from phantom.chain.executor import CaptureExpiredRewind
from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadRow
from phantom.workers.saturation import AdmissionGranted
from phantom.workers.sender import Sender

from tests.unit.conftest import make_snapshot, snapshot_thunk
from tests.unit.test_sent_at_stamp import _build_instance, _succeeded

# The step index the rewind targets (the capture-producing step).
_REWIND_TARGET_STEP = 0
# Mid-chain index a claimed row sits at when the capture expires.
_MID_CHAIN_STEP = 2
# Worker knobs for the handler-driving Sender (never started; the
# handlers are invoked directly, matching test_sent_at_stamp's idiom).
_WORKER_COUNT = 1
_POLL_INTERVAL_MS = 250
# Body retention for the replay leg: the suite default
# (succeeded_body_seconds=0) discards the body at delivery and the
# subsequent replay then refuses replay_body_discarded BY DESIGN
# (Defender 0). One hour keeps the body so the replay leg is reachable.
_RETAIN_BODY_SECONDS = 3_600


def _rewind() -> CaptureExpiredRewind:
    """The executor's rewind verdict targeting the producing step."""
    return CaptureExpiredRewind(
        producing_step="mint-token",
        rewind_to_step_index=_REWIND_TARGET_STEP,
    )


@pytest.fixture
async def instance(tmp_path: Path) -> AsyncIterator[InstanceContext]:
    """A live minimal instance; the store is stopped on teardown."""
    ctx = await _build_instance(tmp_path)
    yield ctx
    await ctx.store.stop()


@pytest.mark.asyncio
async def test_rewind_preserves_grouping_and_leaves_sent_at_null(
    instance: InstanceContext, make_upload_row: Callable[..., UploadRow]
) -> None:
    """The rewind re-queue moves NOTHING but the execution cursor."""
    row = make_upload_row(
        state="attempting",
        route_name="files",
        current_step_index=_MID_CHAIN_STEP,
        attempts=3,
    )
    await instance.store.insert(row)
    sender = Sender(
        instance=instance, worker_count=_WORKER_COUNT, poll_interval_ms=_POLL_INTERVAL_MS
    )

    await sender._on_rewind(instance.store, row, _rewind())

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "queued"
    assert fresh.current_step_index == _REWIND_TARGET_STEP
    assert fresh.last_error == "rewind:mint-token"
    # ADR-011: the rewound chain never reached the upstream; the
    # attempt budget resets by design.
    assert fresh.attempts == 0
    # The cycle-7 columns are untouched by the rewind UPDATE.
    assert fresh.group_id == row.group_id
    assert fresh.multifile_id == row.multifile_id
    assert fresh.send_order == row.send_order
    assert fresh.sent_at is None


@pytest.mark.asyncio
async def test_rewound_row_delivery_stamps_sent_at_once(
    instance: InstanceContext, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Rewind, re-claim, deliver: sent_at stamps exactly once, at delivery."""
    row = make_upload_row(
        state="attempting",
        route_name="files",
        current_step_index=_MID_CHAIN_STEP,
        body_size_bytes=7,
    )
    await instance.store.insert(row)
    sender = Sender(
        instance=instance, worker_count=_WORKER_COUNT, poll_interval_ms=_POLL_INTERVAL_MS
    )
    await sender._on_rewind(instance.store, row, _rewind())

    claimed = await instance.store.claim_due(datetime.now(tz=UTC), 1)
    assert [c.chain_id for c in claimed] == [row.chain_id]
    assert claimed[0].sent_at is None

    granted = await instance.saturation.admit(claimed[0].body_size_bytes)
    assert isinstance(granted, AdmissionGranted)
    await sender._on_succeeded(instance.store, claimed[0], _succeeded(chain_done=True))

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "succeeded"
    assert fresh.sent_at is not None
    assert fresh.sent_at == fresh.updated_at
    assert fresh.group_id == row.group_id


@pytest.mark.asyncio
async def test_rewind_after_replay_preserves_original_sent_at(
    instance: InstanceContext, make_upload_row: Callable[..., UploadRow]
) -> None:
    """A delivered row replayed then rewound keeps its first delivery stamp."""
    # Retain the delivered body so the replay leg is reachable (the
    # zero-retention default would stamp body_discarded_at at delivery
    # and the replay would 409 replay_body_discarded by design).
    retention = make_snapshot().retention.model_copy(
        update={"succeeded_body_seconds": _RETAIN_BODY_SECONDS}
    )
    instance.current_settings = snapshot_thunk(make_snapshot(retention=retention))
    row = make_upload_row(state="attempting", route_name="files", body_size_bytes=7)
    await instance.store.insert(row)
    sender = Sender(
        instance=instance, worker_count=_WORKER_COUNT, poll_interval_ms=_POLL_INTERVAL_MS
    )

    # First delivery stamps.
    granted = await instance.saturation.admit(row.body_size_bytes)
    assert isinstance(granted, AdmissionGranted)
    await sender._on_succeeded(instance.store, row, _succeeded(chain_done=True))
    delivered = await instance.store.get(row.chain_id)
    assert delivered is not None
    original_stamp = delivered.sent_at
    assert original_stamp is not None

    # Operator replay re-queues; the stamp survives (pinned contract).
    replay_outcome = await instance.store.replay(row.chain_id)
    assert replay_outcome is not None
    replayed = replay_outcome.row
    assert replayed is not None
    assert replayed.sent_at == original_stamp

    # The replayed run's capture expires mid-chain and rewinds: the
    # stamp STILL survives, alongside the grouping columns.
    claimed = await instance.store.claim_due(datetime.now(tz=UTC), 1)
    assert [c.chain_id for c in claimed] == [row.chain_id]
    await sender._on_rewind(instance.store, claimed[0], _rewind())

    fresh = await instance.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "queued"
    assert fresh.sent_at == original_stamp
    assert fresh.group_id == row.group_id
    assert fresh.multifile_id == row.multifile_id
    assert fresh.send_order == row.send_order
