"""The reaper's body-discard leg must confirm-then-act on live state (R9-5).

Invariant #17 (today's architecture-intent section 5): a worker acting
on another owner's rows re-reads the live truth at the decision instant
instead of trusting a snapshot. The reaper's body-discard pass violates
it end to end (``workers/reaper.py``): ``list_terminal_older_than``
takes a point-in-time snapshot, then PER ROW it (1) deletes the body
files, (2) stamps via ``discard_body_and_zero_accounting`` - whose
UPDATE has NO state guard and NO ``body_discarded_at IS NULL`` guard
(``storage/sqlite_store.py``), and (3) releases the gate keyed on the
SNAPSHOT ``row.state``. Nothing between the list and the three effects
re-validates the row, and the snapshot loop spans one await per row, so
admin actions land inside it routinely.

Two harms, one root:

* An admin replay (or the AuthKicker's wake - the same TOCTOU defeats
  both H4 guards, which check a stamp that does not exist yet) re-queues
  the listed ``stored`` row inside the window. The reaper then deletes
  the LIVE row's body files, stamps it, and zeroes its accounting: the
  sender's next claim takes ``BodyMissingError`` to ``corrupted``
  (``storage_corruption:bodies_missing``). The operator's explicit
  replay is destroyed by the retention sweep and laundered into a
  storage-fault diagnostic - the north-star violation, in exactly the
  R6-1/R7-2/R8-3 stale-identity family the loop has now fixed three
  times in other workers.
* An admin delete/bulk delete removes the listed row inside the window
  and correctly releases its slot on accounting captured atomically
  with the DELETE (R8-4). The reaper's leg then releases AGAIN off its
  stale snapshot state - the double release drains some OTHER live
  row's charge, the gate under-counts, and admission over-admits past
  the operator's caps (invariant #16 broken in the down direction).

MED severity: routine admin/maintenance concurrency, no exotic state.

Both tests drive the REAL ``Reaper`` sweep over a REAL SqliteUploadStore,
SaturationGate, and HybridBodyStore, with the REAL admin routes as the
concurrent actor. A body-store wrapper lands the admin action
deterministically between the sweep's list and its per-row effects (the
established R7-2/R8-3 hook technique). Falsifiability proven both ways
in scratch: today the replayed row ends queued + stamped + bodiless and
the gate drains to zero under a live queued row; a confirm-then-act
variant (live re-read before the irreversible effects, release keyed on
live accounting) preserves the replayed row's bytes and keeps the
ledger exact.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from phantom.config.settings import (
    BodyStoreCfg,
    CompressionCfg,
    InstanceCfg,
    PersistTriggerCfg,
    RetentionCfg,
    RouteCfg,
    SaturationCfg,
)
from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.instances.snapshot import InstanceSettingsSnapshot
from phantom.models.admin import DeleteFilter
from phantom.models.upload import UploadRow
from phantom.routes import admin as admin_routes
from phantom.storage import FileBodyStore, RamBodyStore, SqliteUploadStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.workers.reaper import Reaper
from phantom.workers.saturation import SaturationGate

from .conftest import track_instance

pytestmark = pytest.mark.asyncio

# Stored row's declared bytes (the slot under dispute).
_STORED_BYTES: int = 500

# A bystander queued row's bytes; its charge is what the double release
# drains, so the totals stay unambiguous.
_QUEUED_BYTES: int = 300

# Body retention window for stored rows and the row's age: anything
# older than one second is overdue; the seeded row is an hour old.
_OVERDUE_WINDOW_SECONDS: int = 1
_ROW_AGE_SECONDS: int = 3600

# -1 disables a retention pass entirely per RetentionCfg semantics.
_DISABLED_WINDOW_SECONDS: int = -1

# Generous caps so no admit in the scenario is refused for capacity.
_GATE_ROW_CAP: int = 10
_GATE_BYTE_CAP: int = 10_000_000
_GATE_DISK_CAP: int = 10_000_000

_R9_5_REASON: str = (
    "R9-5: the reaper's body-discard leg acts per row on a stale sweep "
    "snapshot - body files deleted and the stamp written with NO state or "
    "stamp guard, and the gate release keyed on the snapshot row.state - so "
    "an admin replay (or kicker wake) landing mid-sweep has its re-queued "
    "row's bodies destroyed (false corrupted; the north star violated), and "
    "an admin delete landing mid-sweep double-releases and drains a live "
    "row's slot (over-admission); invariants #16 and #17 broken in one leg"
)


def _retention_stored_body_overdue() -> RetentionCfg:
    """Retention with only the stored BODY window active (and overdue)."""
    return RetentionCfg(
        succeeded_metadata_seconds=_DISABLED_WINDOW_SECONDS,
        succeeded_body_seconds=_DISABLED_WINDOW_SECONDS,
        failed_metadata_seconds=_DISABLED_WINDOW_SECONDS,
        failed_body_seconds=_DISABLED_WINDOW_SECONDS,
        cancelled_metadata_seconds=_DISABLED_WINDOW_SECONDS,
        cancelled_body_seconds=_DISABLED_WINDOW_SECONDS,
        stored_metadata_seconds=_DISABLED_WINDOW_SECONDS,
        stored_body_seconds=_OVERDUE_WINDOW_SECONDS,
        auth_expired_metadata_seconds=_DISABLED_WINDOW_SECONDS,
        auth_expired_body_seconds=_DISABLED_WINDOW_SECONDS,
        corrupted_metadata_seconds=_DISABLED_WINDOW_SECONDS,
        corrupted_body_seconds=_DISABLED_WINDOW_SECONDS,
    )


def _snapshot(retention: RetentionCfg) -> InstanceSettingsSnapshot:
    """A snapshot carrying the test retention; other blocks at defaults."""
    return InstanceSettingsSnapshot(
        persist_trigger=PersistTriggerCfg(),
        body_store=BodyStoreCfg(),
        retention=retention,
        compression=CompressionCfg(),
        saturation=SaturationCfg(
            max_in_flight=_GATE_ROW_CAP,
            max_in_flight_bytes=_GATE_BYTE_CAP,
            max_disk_bytes=_GATE_DISK_CAP,
            large_body_threshold_bytes=0,
            max_large_in_flight=0,
        ),
        capture_reexecution=False,
    )


class _AdminActionAfterListStore:
    """Upload-store wrapper landing an admin action inside the sweep window.

    Under the R9-5 confirm-then-act order, the sweep's race window is
    between its ``list_terminal_older_than`` snapshot and the guarded
    stamp; this wrapper fires the admin action ONCE, the moment the
    sweep's snapshot returns non-empty, deterministically inside that
    window (the pre-fix repro hooked ``body_store.delete``, the first
    per-row effect of the OLD delete-then-stamp order). Re-entrant
    calls from the admin route pass straight through via the fired
    guard; everything else proxies to the real store.
    """

    def __init__(self, real: SqliteUploadStore, action: Callable[[], Awaitable[None]]) -> None:
        self._real = real
        self._action = action
        self._fired = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)

    async def list_terminal_older_than(self, state: str, cutoff: Any) -> Any:
        """Return the sweep snapshot, then land the admin action once."""
        rows = await self._real.list_terminal_older_than(state, cutoff)
        if rows and not self._fired:
            self._fired = True
            await self._action()
        return rows


async def _build_instance(tmp_path: Path) -> InstanceContext:
    """A real-store instance whose reaper sweep must keep the ledger exact."""
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
    snapshot = _snapshot(_retention_stored_body_overdue())
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
        current_settings=lambda: snapshot,
    )
    return track_instance(instance)


def _make_row(**overrides: object) -> UploadRow:
    """Build an UploadRow with the producer's defaults."""
    now = datetime.now(tz=UTC)
    chain_id = uuid4()
    base: dict[str, object] = {
        "chain_id": chain_id,
        "instance_id": "emu",
        "group_id": chain_id,
        "multifile_id": None,
        "send_order": 0,
        "route_name": "files",
        "state": "queued",
        "body_location": "ram",
        "received_at": now,
        "updated_at": now,
        "endpoint": "files.example.com",
        "uid": "user-1",
        "chain_envelope_json": "{}",
        "idempotency_key": "k",
        "capture_reexecution_active": False,
    }
    base.update(overrides)
    return UploadRow.model_validate(base)


async def _seed_overdue_stored_row(instance: InstanceContext) -> UploadRow:
    """Insert an aged stored row with bytes present, holding one slot."""
    aged = datetime.now(tz=UTC) - timedelta(seconds=_ROW_AGE_SECONDS)
    row = _make_row(
        state="stored",
        body_size_bytes=_STORED_BYTES,
        received_at=aged,
        updated_at=aged,
        idempotency_key=str(uuid4()),
    )
    await instance.store.insert(row)
    await instance.body_store.put(row.chain_id, {"body": b"buffered-upload-bytes"})
    granted = await instance.saturation.admit(_STORED_BYTES)
    assert granted.__class__.__name__ == "AdmissionGranted", granted
    return row


async def test_replayed_row_survives_a_racing_body_discard_sweep(
    tmp_path: Path,
) -> None:
    """A row replayed mid-sweep must keep its bytes and stay unstamped.

    Attack: a ``stored`` row ages past its body window and lands on the
    sweep's snapshot list. The operator replays it inside the window
    (legal: the row is unstamped and not attempting, so both refusal
    guards pass). The sweep then deletes the LIVE queued row's body
    files, stamps it, and zeroes its accounting - condemning the
    sender's next claim to ``BodyMissingError`` -> ``corrupted``. The
    replayed upload must come out of the sweep deliverable: queued,
    unstamped, bytes present.
    """
    instance = await _build_instance(tmp_path)
    row = await _seed_overdue_stored_row(instance)
    dispatcher = InstanceDispatcher([instance])
    real_store = instance.store
    real_body_store = instance.body_store

    async def admin_replays_the_row() -> None:
        replayed = await admin_routes.replay_upload(row.chain_id, dispatcher)
        assert isinstance(replayed, UploadRow) and replayed.state == "queued", (
            "precondition: the replay re-queued the listed stored row inside "
            "the sweep window (unstamped at decision time, so the "
            "replay_body_discarded guard correctly passed)"
        )

    instance.store = _AdminActionAfterListStore(real_store, admin_replays_the_row)  # type: ignore[assignment]
    reaper = Reaper(instances=[instance])
    await reaper._sweep_once()

    final = await real_store.get(row.chain_id)
    assert final is not None and final.state == "queued", (
        "precondition: the replayed row is live and queued after the sweep"
    )
    assert final.body_discarded_at is None, (
        "the sweep stamped a LIVE queued row off its stale snapshot (the "
        "discard UPDATE has no state guard): the replayed upload is now "
        "carved out of recovery, the kicker, and replay itself"
    )
    assert await real_body_store.has_body_ref(row.chain_id, "body"), (
        "the sweep deleted a LIVE queued row's body files off its stale "
        "snapshot: the sender's next claim takes BodyMissingError to "
        "corrupted - the operator's explicit replay destroyed by the "
        "retention sweep and reported as storage corruption"
    )


async def test_sweep_does_not_double_release_a_row_deleted_mid_sweep(
    tmp_path: Path,
) -> None:
    """A row deleted mid-sweep must release exactly once across both paths.

    Attack: the overdue ``stored`` row is on the sweep's snapshot list
    while a bystander ``queued`` row also holds a charge (gate at two
    slots). An admin bulk delete claims the stored row inside the
    window and releases its slot on accounting captured atomically with
    the DELETE (R8-4 - correct). The sweep's leg then releases AGAIN
    keyed on its stale snapshot state, draining the live queued row's
    charge: the gate reads idle under one live in-flight row and
    over-admits past the operator's caps. After both actors the gate
    must hold exactly the bystander's charge.
    """
    instance = await _build_instance(tmp_path)
    await _seed_overdue_stored_row(instance)
    bystander = _make_row(
        state="queued",
        body_size_bytes=_QUEUED_BYTES,
        idempotency_key=str(uuid4()),
    )
    await instance.store.insert(bystander)
    granted = await instance.saturation.admit(_QUEUED_BYTES)
    assert granted.__class__.__name__ == "AdmissionGranted", granted
    dispatcher = InstanceDispatcher([instance])
    real_store = instance.store

    async def admin_bulk_deletes_stored_rows() -> None:
        response = await admin_routes.bulk_delete_uploads(DeleteFilter(state="stored"), dispatcher)
        assert response.deleted == 1, (
            "precondition: the bulk delete removed the listed stored row "
            "inside the sweep window and released its slot atomically"
        )

    instance.store = _AdminActionAfterListStore(  # type: ignore[assignment]
        real_store, admin_bulk_deletes_stored_rows
    )
    reaper = Reaper(instances=[instance])
    await reaper._sweep_once()

    live = await real_store.get(bystander.chain_id)
    assert live is not None and live.state == "queued", (
        "precondition: the bystander queued row is still live after the sweep"
    )
    assert instance.saturation.in_flight == 1, (
        "the sweep's stale-snapshot release drained the live queued row's "
        f"slot (gate in_flight={instance.saturation.in_flight}, expected 1): "
        "one charge, two releases - the gate under-counts and admission "
        "over-admits past the operator's caps"
    )
    assert instance.saturation.in_flight_bytes == _QUEUED_BYTES, (
        f"the live queued row's {_QUEUED_BYTES} bytes must stay charged, not "
        f"{instance.saturation.in_flight_bytes} - the double release freed "
        "bytes that belong to a live row"
    )
