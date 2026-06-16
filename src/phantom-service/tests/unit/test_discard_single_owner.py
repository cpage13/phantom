"""Cycle-7 task 4.7 acceptance (D9 + D10): one body-discard owner per effect.

ONE store operation, ``discard_body_and_zero_accounting``, owns the
row-side body-discard effect (stamps ``body_discarded_at`` AND zeroes
``body_size_bytes``, the zeroing load-bearing for saturation
accounting). Both deletion triggers call it and neither carries its own
variant:

* the sender's IMMEDIATE discard on the chain-done success branch when
  ``retention.succeeded_body_seconds == 0`` (the default);
* the reaper's SCHEDULED discard when a non-zero window elapses.

These tests pin the behavior-identity acceptance: the same rows get
discarded with the same final accounting on both legs, saturation is
released exactly once, the cancel race still discards nothing, the
reaper never re-processes a stamped row, a stamped row refuses operator
replay on either leg while a retained body still replays (cycle-7
phase 7 pre-round defender fix), and static gates prove the operation
has exactly one definition and exactly the two call sites.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from phantom.chain.executor import ChainExecutor, Succeeded
from phantom.config.settings import (
    InstanceCfg,
    PersistTriggerCfg,
    RetentionCfg,
    RouteCfg,
)
from phantom.instances.context import InstanceContext
from phantom.models.upload import (
    BodyHash,
    BodyHashes,
    CapturedValues,
    StorageHash,
    UploadRow,
)
from phantom.routing import resolve_route
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.errors import ReplayBodyDiscardedError
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.transport import UpstreamRequest, UpstreamResponse
from phantom.workers.reaper import Reaper
from phantom.workers.saturation import SaturationGate
from phantom.workers.sender import Sender

from .conftest import make_snapshot, snapshot_thunk, track_instance

_SERVICE_SRC = Path(__file__).resolve().parents[2] / "src" / "phantom"

_TEST_RETENTION_DEFAULTS: dict[str, int] = {
    "succeeded_metadata_seconds": 300,
    "failed_body_seconds": 14 * 86_400,
    "auth_expired_body_seconds": 60 * 86_400,
    "stored_body_seconds": 60 * 86_400,
}


class _FakeUpstream:
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, _req: UpstreamRequest) -> UpstreamResponse:
        return UpstreamResponse(status=200, body=b"{}")


async def _build_instance(
    tmp_path: Path,
    *,
    retention: RetentionCfg | None = None,
) -> InstanceContext:
    """Single-store instance for driving the sender and reaper directly."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await store.start()
    await body_store.start()
    await tokens.start()
    cfg = InstanceCfg(
        id="alpha",
        host_prefixes=["files.example.com"],
        data_dir="alpha",
        routes=[RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer")],
    )
    upstream = _FakeUpstream()
    executor = ChainExecutor(
        token_cache=tokens,
        upstream_client=upstream,
        resolve_route=resolve_route,
        clock=lambda: datetime.now(tz=UTC),
        instance=cfg,
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
        token_cache=tokens,
        minter=None,
        retry_strategy=FixedIntervalsStrategy([1, 5]),
        upstream_client=upstream,
        executor=executor,
        saturation=saturation,
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(
            make_snapshot(
                persist_trigger=PersistTriggerCfg(body_size_threshold_bytes=0),
                retention=retention,
            )
        ),
    )
    return track_instance(instance)


def _seed_hashes(body: bytes) -> dict[str, BodyHashes]:
    digest = hashlib.sha256(body).hexdigest()
    return {
        "body": BodyHashes(
            body_hash=BodyHash(digest),
            storage_hash=StorageHash(digest),
        )
    }


def _chain_done() -> Succeeded:
    return Succeeded(
        captured=CapturedValues(),
        next_step_index=1,
        chain_done=True,
        step_name="step",
        upstream_status=200,
        upstream_headers={},
    )


async def _drive_success(
    instance: InstanceContext,
    make_upload_row: Callable[..., UploadRow],
    *,
    body: bytes,
) -> UploadRow:
    """Insert an attempting row + body, drive the chain-done success."""
    chain_id = uuid4()
    row = make_upload_row(
        chain_id=chain_id,
        state="attempting",
        instance_id="alpha",
        body_size_bytes=len(body),
        body_hashes=_seed_hashes(body),
    )
    await instance.store.insert(row)
    await instance.ram_body_store.put(chain_id, {"body": body})
    sat = await instance.saturation.admit(len(body))
    assert sat.__class__.__name__ == "AdmissionGranted", sat
    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._on_succeeded(instance.store, row, _chain_done())
    fetched = await instance.store.get(chain_id)
    assert fetched is not None
    return fetched


@pytest.mark.asyncio
async def test_immediate_leg_stamps_and_zeroes_via_the_one_op(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """succeeded_body_seconds == 0: body gone AND row discarded in one step.

    Same rows, same accounting, now via the one operation: the body file
    is deleted at the same trigger point as before, saturation releases
    exactly once, and the row records the discard (stamp + zeroed size)
    without waiting for a reaper sweep to mop up.
    """
    instance = await _build_instance(tmp_path)  # default retention: 0
    body = b"payload"
    row = await _drive_success(instance, make_upload_row, body=body)
    assert row.state == "succeeded"
    assert not await instance.body_store.has_body_ref(row.chain_id, "body")
    assert row.body_discarded_at is not None
    assert row.body_size_bytes == 0
    assert instance.saturation.in_flight == 0
    assert instance.saturation.in_flight_bytes == 0


@pytest.mark.asyncio
async def test_nonzero_retention_leaves_row_for_the_scheduled_leg(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """A non-zero window means the sender touches NOTHING discard-wise."""
    retention = RetentionCfg(
        succeeded_body_seconds=60,
        **_TEST_RETENTION_DEFAULTS,  # type: ignore[arg-type]
    )
    instance = await _build_instance(tmp_path, retention=retention)
    body = b"payload"
    row = await _drive_success(instance, make_upload_row, body=body)
    assert row.state == "succeeded"
    assert await instance.body_store.has_body_ref(row.chain_id, "body")
    assert row.body_discarded_at is None
    assert row.body_size_bytes == len(body)
    # Saturation still releases on success regardless of the discard leg.
    assert instance.saturation.in_flight == 0


@pytest.mark.asyncio
async def test_scheduled_leg_converges_to_the_identical_final_state(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """The reaper's scheduled discard lands the SAME final row state.

    Behavior-identity acceptance: both legs end with the body files
    gone, ``body_discarded_at`` stamped, and ``body_size_bytes`` zeroed;
    nothing else on the row differs between the two paths.
    """
    retention = RetentionCfg(
        succeeded_body_seconds=1,
        **_TEST_RETENTION_DEFAULTS,  # type: ignore[arg-type]
    )
    instance = await _build_instance(tmp_path, retention=retention)
    body = b"payload"
    chain_id = uuid4()
    stale = datetime.now(tz=UTC) - timedelta(seconds=5)
    row = make_upload_row(
        chain_id=chain_id,
        state="succeeded",
        instance_id="alpha",
        body_size_bytes=len(body),
        body_hashes=_seed_hashes(body),
        updated_at=stale,
    )
    await instance.store.insert(row)
    await instance.ram_body_store.put(chain_id, {"body": body})

    reaper = Reaper(instances=[instance])
    await reaper._sweep_instance(instance, datetime.now(tz=UTC))

    reaped = await instance.store.get(chain_id)
    assert reaped is not None
    assert not await instance.body_store.has_body_ref(chain_id, "body")
    assert reaped.body_discarded_at is not None
    assert reaped.body_size_bytes == 0
    assert reaped.state == "succeeded"

    # Field-for-field equivalence with the immediate leg, modulo the
    # identity and time fields that necessarily differ per row.
    immediate_root = tmp_path / "immediate"
    immediate_root.mkdir()
    immediate_instance = await _build_instance(immediate_root)
    immediate = await _drive_success(immediate_instance, make_upload_row, body=body)
    varying = {
        "chain_id",
        "group_id",
        "multifile_id",
        "received_at",
        "updated_at",
        "sent_at",
        "next_attempt_at",
        "body_discarded_at",
        "attempts",
        "upstream_status_code",
        "upstream_response_headers_json",
        "last_step_completed",
        "current_step_index",
    }
    reaped_view = reaped.model_dump(exclude=varying)
    immediate_view = immediate.model_dump(exclude=varying)
    assert reaped_view == immediate_view
    assert immediate.body_discarded_at is not None


@pytest.mark.asyncio
async def test_cancel_race_still_discards_nothing(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """rowcount=0 (admin took the row mid-flight): no delete, no stamp."""
    instance = await _build_instance(tmp_path)
    body = b"payload"
    chain_id = uuid4()
    row = make_upload_row(
        chain_id=chain_id,
        state="attempting",
        instance_id="alpha",
        body_size_bytes=len(body),
        body_hashes=_seed_hashes(body),
    )
    await instance.store.insert(row)
    await instance.ram_body_store.put(chain_id, {"body": body})
    # Admin cancel takes the row out of attempting before the sender lands.
    await instance.store.cancel(chain_id)
    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=250)
    await sender._on_succeeded(instance.store, row, _chain_done())
    after = await instance.store.get(chain_id)
    assert after is not None
    assert after.state == "cancelled"
    assert after.body_discarded_at is None
    assert after.body_size_bytes == len(body)
    assert await instance.body_store.has_body_ref(chain_id, "body")


@pytest.mark.asyncio
async def test_reaper_never_reprocesses_a_stamped_row(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """The immediate leg's stamp keeps the row out of every later sweep."""
    instance = await _build_instance(tmp_path)  # succeeded_body_seconds == 0
    row = await _drive_success(instance, make_upload_row, body=b"payload")
    assert row.body_discarded_at is not None
    # The reaper's selection no longer sees the row at all.
    eligible = await instance.store.list_terminal_older_than(
        "succeeded", datetime.now(tz=UTC) + timedelta(seconds=1)
    )
    assert eligible == []
    # A full sweep leaves the original stamp untouched (no re-stamp).
    reaper = Reaper(instances=[instance])
    await reaper._sweep_instance(instance, datetime.now(tz=UTC))
    after = await instance.store.get(row.chain_id)
    assert after is not None
    assert after.body_discarded_at == row.body_discarded_at


@pytest.mark.asyncio
async def test_immediate_leg_row_refuses_replay(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """A row discarded by the sender's immediate leg refuses replay.

    Cycle-7 phase 7 pre-round defender fix: replaying the discarded row
    would hand the sender a row with no bytes and land it in
    ``corrupted`` on the next claim, so the store refuses with the
    typed error and the row stays exactly as the immediate leg left it
    (state succeeded, sent_at preserved, stamp untouched).
    """
    instance = await _build_instance(tmp_path)  # default retention: 0
    row = await _drive_success(instance, make_upload_row, body=b"payload")
    assert row.body_discarded_at is not None
    assert row.sent_at is not None
    with pytest.raises(ReplayBodyDiscardedError) as excinfo:
        await instance.store.replay(row.chain_id)
    assert excinfo.value.chain_id == row.chain_id
    assert excinfo.value.instance_id == "alpha"
    assert excinfo.value.body_discarded_at == row.body_discarded_at
    after = await instance.store.get(row.chain_id)
    assert after is not None
    assert after.model_dump() == row.model_dump()


@pytest.mark.asyncio
async def test_scheduled_leg_row_refuses_replay(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """A row discarded by the reaper's scheduled leg refuses replay too.

    Both legs stamp through the one discard owner, so the refusal
    cannot tell them apart and must hold on either.
    """
    retention = RetentionCfg(
        succeeded_body_seconds=1,
        **_TEST_RETENTION_DEFAULTS,  # type: ignore[arg-type]
    )
    instance = await _build_instance(tmp_path, retention=retention)
    body = b"payload"
    chain_id = uuid4()
    stale = datetime.now(tz=UTC) - timedelta(seconds=5)
    row = make_upload_row(
        chain_id=chain_id,
        state="succeeded",
        instance_id="alpha",
        body_size_bytes=len(body),
        body_hashes=_seed_hashes(body),
        updated_at=stale,
    )
    await instance.store.insert(row)
    await instance.ram_body_store.put(chain_id, {"body": body})
    reaper = Reaper(instances=[instance])
    await reaper._sweep_instance(instance, datetime.now(tz=UTC))
    reaped = await instance.store.get(chain_id)
    assert reaped is not None
    assert reaped.body_discarded_at is not None
    with pytest.raises(ReplayBodyDiscardedError) as excinfo:
        await instance.store.replay(chain_id)
    assert excinfo.value.body_discarded_at == reaped.body_discarded_at
    after = await instance.store.get(chain_id)
    assert after is not None
    assert after.state == "succeeded"
    assert after.body_discarded_at == reaped.body_discarded_at


@pytest.mark.asyncio
async def test_retained_body_row_still_replays(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """No behavior change for rows that still hold bodies (stamp NULL).

    With a non-zero retention window the sender leaves body and row
    alone, so an operator replay of the delivered row re-queues it
    exactly as before the defender fix.
    """
    retention = RetentionCfg(
        succeeded_body_seconds=60,
        **_TEST_RETENTION_DEFAULTS,  # type: ignore[arg-type]
    )
    instance = await _build_instance(tmp_path, retention=retention)
    row = await _drive_success(instance, make_upload_row, body=b"payload")
    assert row.body_discarded_at is None
    replay_outcome = await instance.store.replay(row.chain_id)
    assert replay_outcome is not None
    assert replay_outcome.row.state == "queued"
    assert replay_outcome.row.sent_at == row.sent_at, "replay never clears the delivery stamp"


def test_one_definition_and_exactly_two_call_sites() -> None:
    """Static gate: one owner, two configured triggers, no variants.

    * the old ``discard_body`` name is gone from src (no shadow variant);
    * the ``body_discarded_at`` SET clause lives in exactly ONE store
      method (the single owner of the row-side effect);
    * ``discard_body_and_zero_accounting`` is called from exactly one
      site in the sender and one in the reaper, and nowhere else in
      production code.
    """
    py_files = list(_SERVICE_SRC.rglob("*.py"))
    assert py_files, f"service source not found under {_SERVICE_SRC}"
    old_name_hits: list[str] = []
    set_clause_hits: list[str] = []
    call_sites: dict[str, int] = {}
    for path in py_files:
        text = path.read_text(encoding="utf-8")
        if re.search(r"\.discard_body\(", text):
            old_name_hits.append(str(path))
        # The UPDATE ... SET body_discarded_at writer (multi-line SQL).
        for match in re.finditer(r"SET\s+body_discarded_at", text):
            set_clause_hits.append(f"{path}:{match.start()}")
        calls = len(re.findall(r"\.discard_body_and_zero_accounting\(", text))
        if calls:
            call_sites[path.name] = calls
    assert old_name_hits == [], f"old discard_body name still called: {old_name_hits}"
    assert len(set_clause_hits) == 1, (
        f"body_discarded_at must have ONE SET-clause owner: {set_clause_hits}"
    )
    assert call_sites == {"sender.py": 1, "reaper.py": 1}, call_sites
