"""The bearer kicker's no-op leg must release the slot it just admitted (R9-3).

Invariant #16 (the saturation ledger balances) requires exactly one
gate charge per row in the in-flight set. The kicker's wake sequence is
admit-then-write: it charges the gate (``saturation.admit``) BEFORE its
``record_attempt_result(expected_state="auth_expired")`` re-queue. When
that guarded UPDATE returns rowcount 0 - the row moved out of
``auth_expired`` between the kicker's list and its write, which the
code comment itself attributes to "admin cancel/replay, or another
kicker tick" - the kicker logs and skips WITHOUT releasing the slot it
admitted one await earlier (``workers/kicker.py``). The charge
then corresponds to no row forever.

Compare the replay route, which handles its mirror-image store-side
refusals by releasing the freshly admitted slot (R8-6,
``routes/admin.py`` replay_upload); the kicker's no-op leg is the one
admit-then-refuse path with no release.

The triggering interleaving is routine: an operator cancels a parked
``auth_expired`` upload at the same moment a fresh token lands (the
kicker rescans every second AND on every cache write, so the window
recurs on every wake attempt). The cancel route correctly releases
nothing (a parked row holds no slot), the kicker's UPDATE no-ops, and
the admitted slot leaks. MED severity, the R8-4 class: the gate drifts
up by one slot plus the row's bytes per occurrence, never returns, and
eventually 503-refuses fresh ingress (``saturation_cap``) with no live
row behind the count; only a process restart resets it.

The test drives the REAL bearer kicker rescan over a REAL SqliteUploadStore,
SqliteTokenCache, and SaturationGate. A store wrapper lands the admin
cancel deterministically between the kicker's list and its re-queue
write (the established R7-2/R8-3 hook technique). After the dust
settles the row is cancelled and the gate must be idle. Falsifiability
proven both ways in scratch: the real kicker strands in_flight=1 with
the row's bytes; a variant whose no-op leg releases returns the gate to
zero.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadRow
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.workers.kicker import PHANTOM_BEARER_FLAVOUR, Kicker
from phantom.workers.saturation import SaturationGate

from .conftest import track_instance

pytestmark = pytest.mark.asyncio

# Parked row's declared bytes: a visibly non-zero residue when leaked.
_DECLARED_BYTES: int = 500

# Generous caps so the kicker's admit itself is never refused - the
# test is about the unreleased charge, not the admit threshold.
_GATE_ROW_CAP: int = 10
_GATE_BYTE_CAP: int = 10_000_000
_GATE_DISK_CAP: int = 10_000_000

_R9_3_REASON: str = (
    "R9-3: Kicker._rescan admits a saturation slot BEFORE its guarded "
    "auth_expired->queued write; when that write no-ops (rowcount 0 - admin "
    "cancel/replay raced the wake, the comment's own enumeration) the kicker "
    "skips without releasing the slot it just admitted, so the gate drifts up "
    "by one slot + the row's bytes per occurrence and eventually 503-refuses "
    "fresh ingress with no live row behind the count (invariant #16 broken; "
    "the replay route releases on its mirror-image store-side refusals)"
)


class _CancelBeforeRequeueStore:
    """Store wrapper landing an admin cancel inside the kicker's wake window.

    The kicker calls ``list_non_terminal`` (snapshot), then the token
    check and the gate admit, then ``record_attempt_result``. This
    wrapper delegates everything the kicker touches to the real store
    but runs the cancel ONCE immediately before the re-queue write, so
    the guarded UPDATE deterministically no-ops - the exact production
    interleaving of an operator cancelling a parked upload as a fresh
    token lands.
    """

    def __init__(self, real: SqliteUploadStore, on_requeue: Callable[[], Awaitable[None]]) -> None:
        self._real = real
        self._on_requeue = on_requeue
        self._fired = False

    async def list_non_terminal(self) -> list[UploadRow]:
        """Delegate the kicker's scan snapshot to the real store."""
        return await self._real.list_non_terminal()

    async def record_attempt_result(self, *args: Any, **kwargs: Any) -> int:
        """Land the racing cancel once, then delegate the kicker's write."""
        if not self._fired:
            self._fired = True
            await self._on_requeue()
        result: int = await self._real.record_attempt_result(*args, **kwargs)
        return result


async def _build_instance(tmp_path: Path) -> InstanceContext:
    """A real-store instance whose gate the kicker's wake must keep exact."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=fbs)
    tokens = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await store.start()
    await ram.start()
    await fbs.start()
    await body_store.start()
    await tokens.start()
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
        token_cache=tokens,
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
        current_settings=MagicMock(),
    )
    return track_instance(instance)


def _parked_row() -> UploadRow:
    """An auth_expired row parked with its slot released (the sender's park)."""
    now = datetime.now(tz=UTC)
    chain_id = uuid4()
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "emu",
            "group_id": chain_id,
            "multifile_id": None,
            "send_order": 0,
            "route_name": "files",
            "state": "auth_expired",
            "body_location": "ram",
            "body_size_bytes": _DECLARED_BYTES,
            "received_at": now,
            "updated_at": now,
            "endpoint": "files.example.com",
            "uid": "user-1",
            "chain_envelope_json": "{}",
            "idempotency_key": "k",
            "capture_reexecution_active": False,
        }
    )


async def test_noop_wake_returns_the_admitted_slot(tmp_path: Path) -> None:
    """A wake whose re-queue no-ops must leave the gate exactly idle.

    Attack: park an ``auth_expired`` row (no slot held - the sender
    released at park), land a fresh token so the kicker wakes it, and
    have an admin cancel claim the row between the kicker's list and
    its guarded write. The cancel releases nothing (parked rows hold no
    slot - correct), the kicker's UPDATE no-ops (state is now
    ``cancelled``), and the slot admitted one await earlier must be
    returned. Today it leaks forever.
    """
    instance = await _build_instance(tmp_path)
    row = _parked_row()
    await instance.store.insert(row)
    await instance.token_cache.set(
        "files.example.com",
        "user-1",
        "Bearer fresh-token",
        source="inbound_request",
    )
    real_store = instance.store

    async def admin_cancels_the_parked_row() -> None:
        outcome = await real_store.cancel(row.chain_id)
        # The cancel itself is accounting-correct: an auth_expired row
        # holds no slot, so no release fires here (row_holds_slot is
        # False for auth_expired) - the leak under test is the kicker's.
        assert outcome.previous_state == "auth_expired"

    instance.store = _CancelBeforeRequeueStore(real_store, admin_cancels_the_parked_row)
    kicker = Kicker(instance=instance, flavour=PHANTOM_BEARER_FLAVOUR)
    await kicker._rescan()

    final = await real_store.get(row.chain_id)
    assert final is not None and final.state == "cancelled", (
        "precondition: the racing admin cancel won the row before the "
        "kicker's guarded re-queue write"
    )
    assert instance.saturation.in_flight == 0, (
        "the kicker's no-op leg stranded the slot it admitted: in_flight is "
        f"{instance.saturation.in_flight} with zero rows in the in-flight "
        "set - the gate drifts up on every cancel-racing-wake and eventually "
        "503-refuses fresh ingress"
    )
    assert instance.saturation.in_flight_bytes == 0, (
        f"the kicker's no-op leg stranded {instance.saturation.in_flight_bytes} "
        "in-flight bytes that correspond to no live row"
    )
