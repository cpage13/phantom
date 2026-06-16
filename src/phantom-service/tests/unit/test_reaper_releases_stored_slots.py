"""The reaper's removal passes release a stored row's saturation slot (R8-4).

Defender-authored regression pins for the reaper legs of the R8-4
class. A ``stored`` row deliberately holds its saturation slot (the
buffered body still occupies space). The slot's ownership ends at
exactly ONE of these reaper events, discriminated by the H4 stamp via
:func:`phantom.workers.saturation.row_holds_slot`:

* the BODY-DISCARD pass (``<state>_body_seconds`` elapsed): the space
  the slot represents is freed, so the slot releases here, and the
  stamped row is slotless from then on;
* the METADATA-DELETE pass or the count-cap eviction, when the row is
  removed with its body never separately discarded (body window longer
  than the metadata window): the row leaves the world still holding,
  so the removal releases.

A row that hits BOTH (body discarded first, metadata deleted later)
must release exactly once - the stamp makes the second event a no-op.
Before the R8-4 fix none of the three legs released and every reaped
stored upload leaked its slot until restart.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from phantom.config.settings import InstanceCfg, RetentionCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadRow
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteUploadStore,
)
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies.fixed_intervals import FixedIntervalsStrategy
from phantom.workers.reaper import Reaper
from phantom.workers.saturation import SaturationGate

from .conftest import make_snapshot, snapshot_thunk, track_instance

pytestmark = pytest.mark.asyncio

# Generous caps; the assertions are about the ledger returning to idle.
_GATE_ROW_CAP = 100
_GATE_BYTE_CAP = 10_000_000
_GATE_DISK_CAP = 1_000_000_000

# Declared size admission charged for the stored row.
_DECLARED_BYTES = 4_096

# Ages: the row is comfortably older than any window under test.
_ROW_AGE_SECONDS = 3_600

# Window values selecting which reaper pass fires: 1 second means
# "everything older than a second ago is overdue" (the row is an hour
# old); -1 disables a pass entirely per RetentionCfg semantics.
_OVERDUE_WINDOW_SECONDS = 1
_DISABLED_WINDOW_SECONDS = -1


async def _build_instance(tmp_path: Path, *, retention: RetentionCfg) -> InstanceContext:
    """A real-store instance whose reaper sweep must keep the gate exact."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    await store.start()
    await ram.start()
    await fbs.start()
    await body_store.start()
    cfg = InstanceCfg(
        id="emu",
        host_prefixes=["files.example.com"],
        data_dir="emu",
        routes=[RouteCfg(name="files", hosts=["files.example.com"], auth_mode="phantom_bearer")],
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
        retry_strategy=FixedIntervalsStrategy([1]),
        upstream_client=MagicMock(),
        executor=MagicMock(),
        saturation=SaturationGate(
            max_in_flight=_GATE_ROW_CAP,
            max_in_flight_bytes=_GATE_BYTE_CAP,
            max_disk_bytes=_GATE_DISK_CAP,
        ),
        codec_factory=MagicMock(),
        current_settings=snapshot_thunk(make_snapshot(retention=retention)),
    )
    return track_instance(instance)


def _aged_stored_row(make_row: Callable[..., UploadRow]) -> UploadRow:
    """A stored row aged past every overdue window, holding one slot."""
    aged = datetime.now(tz=UTC) - timedelta(seconds=_ROW_AGE_SECONDS)
    return make_row(
        state="stored",
        route_name="files",
        body_size_bytes=_DECLARED_BYTES,
        received_at=aged,
        updated_at=aged,
    )


def _retention(*, stored_metadata: int, stored_body: int) -> RetentionCfg:
    """Retention with only the stored windows active; other passes disabled."""
    return RetentionCfg(
        succeeded_metadata_seconds=_DISABLED_WINDOW_SECONDS,
        succeeded_body_seconds=_DISABLED_WINDOW_SECONDS,
        failed_metadata_seconds=_DISABLED_WINDOW_SECONDS,
        failed_body_seconds=_DISABLED_WINDOW_SECONDS,
        cancelled_metadata_seconds=_DISABLED_WINDOW_SECONDS,
        cancelled_body_seconds=_DISABLED_WINDOW_SECONDS,
        stored_metadata_seconds=stored_metadata,
        stored_body_seconds=stored_body,
        auth_expired_metadata_seconds=_DISABLED_WINDOW_SECONDS,
        auth_expired_body_seconds=_DISABLED_WINDOW_SECONDS,
        corrupted_metadata_seconds=_DISABLED_WINDOW_SECONDS,
        corrupted_body_seconds=_DISABLED_WINDOW_SECONDS,
    )


async def _seed_held_stored_row(
    instance: InstanceContext, make_row: Callable[..., UploadRow]
) -> UploadRow:
    """Insert the aged stored row and charge the gate as admission did."""
    row = _aged_stored_row(make_row)
    await instance.store.insert(row)
    granted = await instance.saturation.admit(_DECLARED_BYTES)
    assert granted.__class__.__name__ == "AdmissionGranted", granted
    return row


async def test_body_discard_pass_releases_the_stored_slot(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """The discard pass frees the space the slot represents: release here.

    Metadata window disabled, body window overdue. After one sweep the
    row survives (stamped) but the gate is idle; a SECOND sweep must
    not release again (the stamp makes the row slotless).
    """
    instance = await _build_instance(
        tmp_path,
        retention=_retention(
            stored_metadata=_DISABLED_WINDOW_SECONDS, stored_body=_OVERDUE_WINDOW_SECONDS
        ),
    )
    row = await _seed_held_stored_row(instance, make_upload_row)

    reaper = Reaper(instances=[instance])
    await reaper._sweep_once()

    survivor = await instance.store.get(row.chain_id)
    assert survivor is not None and survivor.body_discarded_at is not None, (
        "precondition: the discard pass stamped the row and kept the metadata"
    )
    assert instance.saturation.in_flight == 0, (
        "the discard pass freed the body's space but never released the slot"
    )
    assert instance.saturation.in_flight_bytes == 0

    await reaper._sweep_once()
    assert instance.saturation.in_flight == 0, (
        "a second sweep over the already-stamped row must not double-release "
        "(the gate floors at zero, so drift shows as a wrong bytes total)"
    )


async def test_metadata_delete_pass_releases_a_never_discarded_stored_slot(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Row removed with its body never separately discarded: release at removal."""
    instance = await _build_instance(
        tmp_path,
        retention=_retention(
            stored_metadata=_OVERDUE_WINDOW_SECONDS, stored_body=_DISABLED_WINDOW_SECONDS
        ),
    )
    row = await _seed_held_stored_row(instance, make_upload_row)

    reaper = Reaper(instances=[instance])
    await reaper._sweep_once()

    assert await instance.store.get(row.chain_id) is None, (
        "precondition: the metadata pass deleted the overdue stored row"
    )
    assert instance.saturation.in_flight == 0, (
        "the deleted stored row was still holding its slot; removal must release"
    )
    assert instance.saturation.in_flight_bytes == 0


async def test_count_cap_eviction_releases_a_never_discarded_stored_slot(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """The max_rows backstop evicting a held stored row releases its slot."""
    retention = _retention(
        stored_metadata=_DISABLED_WINDOW_SECONDS, stored_body=_DISABLED_WINDOW_SECONDS
    ).model_copy(update={"max_rows": 0})
    instance = await _build_instance(tmp_path, retention=retention)
    await _seed_held_stored_row(instance, make_upload_row)

    reaper = Reaper(instances=[instance])
    await reaper._sweep_once()

    assert instance.saturation.in_flight == 0, (
        "the cap-evicted stored row was still holding its slot; eviction must release"
    )
    assert instance.saturation.in_flight_bytes == 0
