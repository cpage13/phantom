"""Replay's re-admit decision must use in-transaction state, not a pre-fetch (R9-4).

The R8-4 cancel fix documents the discipline: "The release decision
uses the store's in-transaction ``previous_state`` (not the route's
pre-fetch, which can race a sender/kicker transition)" - and
``store.cancel`` returns ``CancelOutcome`` precisely so the route can
decide on authoritative state. The R8-6 replay fix did NOT get that
discipline: ``replay_upload`` computes ``needs_admit`` from the row
returned by its up-front ``_find_upload_with_ctx`` lookup
(``routes/admin.py``), then admits, then calls ``store.replay`` - and
``store.replay`` returns no accounting, so the route CANNOT reconcile
when the row's slot ownership changed inside that window.

The window spans two awaits and races the kicker's once-per-second
rescan (plus every cache-write wake) and the sender's transitions. Both
drift directions are reachable, breaking invariant #16:

* Double charge: the route pre-fetches an ``auth_expired`` row
  (released, ``needs_admit=True``); the kicker wakes it (admits +
  re-queues) inside the window; the route admits a SECOND slot and
  ``store.replay`` re-queues the already-queued row. One live row, two
  charges - the gate over-counts, capacity shrinks, and the row's
  eventual terminal release still leaves one phantom charge forever.
* Missed charge (mirror): the route pre-fetches a ``queued`` row
  (``needs_admit=False``); the sender drives it terminal (releasing)
  inside the window; ``store.replay`` re-queues it with NO charge - the
  gate under-counts and over-admits past the operator's caps, and the
  re-queued row's eventual release drains some other live row's slot.

MED severity, the R8-6 inverse-drift class on the exact surface the
loop fixed one round ago.

The test drives the REAL ``replay_upload`` route over a REAL
SqliteUploadStore, SqliteTokenCache, SaturationGate, and the REAL
AuthKicker as the concurrent actor. A store wrapper lands the kicker's
full wake (admit + guarded re-queue) deterministically inside the
route's window (the established R7-2/R8-3 hook technique). After the
dust settles exactly one live row is in flight, so the gate must hold
exactly one charge. Falsifiability proven both ways in scratch: the
real route ends at in_flight=2 (double charge); a route deciding on
in-transaction accounting ends at 1.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.models.upload import UploadRow
from phantom.routes import admin as admin_routes
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteTokenCache,
    SqliteUploadStore,
)
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.workers.auth_kicker import AuthKicker
from phantom.workers.saturation import SaturationGate

from .conftest import track_instance

pytestmark = pytest.mark.asyncio

# Parked row's declared bytes; doubled exactly when the route
# double-charges, so the byte assertion is unambiguous.
_DECLARED_BYTES: int = 500

# Generous caps so neither admit in the race is refused for capacity -
# the test is about the decision basis, not the admit threshold.
_GATE_ROW_CAP: int = 10
_GATE_BYTE_CAP: int = 10_000_000
_GATE_DISK_CAP: int = 10_000_000

_R9_4_REASON: str = (
    "R9-4: replay_upload computes needs_admit from its pre-fetch row, not "
    "in-transaction state (the discipline the R8-4 cancel fix documents and "
    "CancelOutcome exists for), and store.replay returns no accounting to "
    "reconcile with; a kicker wake landing inside the route's window makes "
    "the route charge a second slot for one live row (and the mirror "
    "interleaving re-queues a released row with no charge), so the ledger "
    "drifts in both directions - invariant #16 broken on the surface R8-6 "
    "fixed one round ago"
)


class _KickerWakesBeforeReplayStore:
    """Store wrapper landing the kicker's full wake inside replay's window.

    The route's sequence is pre-fetch (``get``), gate admit, then
    ``store.replay``. This wrapper delegates everything the route and
    the kicker touch to the real store but runs the REAL kicker rescan
    ONCE immediately before the replay write - placing the wake (gate
    admit + guarded auth_expired->queued re-queue) deterministically
    inside the window between the route's decision and its write.
    """

    def __init__(self, real: SqliteUploadStore, wake: Callable[[], Awaitable[None]]) -> None:
        self._real = real
        self._wake = wake
        self._fired = False

    async def get(self, chain_id: UUID) -> UploadRow | None:
        """Delegate the route's pre-fetch lookup to the real store."""
        return await self._real.get(chain_id)

    async def list_non_terminal(self) -> list[UploadRow]:
        """Delegate the kicker's scan snapshot to the real store."""
        return await self._real.list_non_terminal()

    async def record_attempt_result(self, *args: object, **kwargs: object) -> int:
        """Delegate the kicker's guarded re-queue write to the real store."""
        result: int = await self._real.record_attempt_result(*args, **kwargs)
        return result

    async def replay(self, chain_id: UUID) -> UploadRow | None:
        """Land the kicker's wake once, then delegate the replay write."""
        if not self._fired:
            self._fired = True
            await self._wake()
        return await self._real.replay(chain_id)


async def _build_instance(tmp_path: Path) -> InstanceContext:
    """A real-store instance whose gate the replay route must keep exact."""
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


async def test_replay_racing_a_kicker_wake_charges_exactly_one_slot(
    tmp_path: Path,
) -> None:
    """One live row must hold exactly one gate charge after the race.

    Attack: park an ``auth_expired`` row and land a fresh token. The
    operator replays it at the same moment the kicker wakes it: the
    route pre-fetches the parked state (``needs_admit=True``), the
    kicker's wake lands inside the window (admit + re-queue - the gate
    now correctly holds ONE charge for the queued row), then the route
    admits AGAIN and ``store.replay`` re-queues the already-queued row.
    End state today: one queued row, two charges - the gate over-counts
    forever. The ledger truth requires exactly one.
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
    kicker = AuthKicker(instance=instance)

    async def kicker_wakes_the_row() -> None:
        await kicker._rescan()
        woken = await real_store.get(row.chain_id)
        assert woken is not None and woken.state == "queued", (
            "precondition: the kicker's wake re-queued the parked row inside the route's window"
        )

    instance.store = _KickerWakesBeforeReplayStore(real_store, kicker_wakes_the_row)
    dispatcher = InstanceDispatcher([instance])

    await admin_routes.replay_upload(row.chain_id, dispatcher)

    final = await real_store.get(row.chain_id)
    assert final is not None and final.state == "queued", (
        "precondition: the replay left the row queued (one live in-flight row)"
    )
    assert instance.saturation.in_flight == 1, (
        "one live queued row must hold exactly one gate charge; the route's "
        f"pre-fetch needs_admit decision charged {instance.saturation.in_flight} "
        "- the surplus charge corresponds to no row and shrinks usable "
        "capacity forever"
    )
    assert instance.saturation.in_flight_bytes == _DECLARED_BYTES, (
        f"one live row of {_DECLARED_BYTES} bytes must account exactly "
        f"{_DECLARED_BYTES} in-flight bytes, not "
        f"{instance.saturation.in_flight_bytes}"
    )
