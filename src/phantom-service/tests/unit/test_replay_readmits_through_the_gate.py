"""Replay of a released terminal row must re-charge the saturation gate (R8-6).

Defender-found while fixing R8-4 (the admin cancel/delete slot leaks):
the saturation ledger's rule is one charge per row in the in-flight
set. Admission charges; the sender releases on terminal transitions
(succeeded / failed / corrupted / cancelled) and on the auth_expired
park (the bearer kicker re-admits through the gate on wake - the section
3.1 symmetry). ``POST /v1/admin/chains/{chain_id}/replay`` re-queues a
row from any of those RELEASED states without re-admitting:

* the gate undercounts while the replayed row is genuinely in flight
  (over-admission against the operator's caps), and
* when the sender finishes the replayed run it releases AGAIN, so the
  ledger drifts DOWN by one slot + body_size_bytes per replay - the
  inverse of the R8-4 leak, with the same end state: the gate's numbers
  stop describing reality.

``stored`` and ``queued`` replays are NOT affected (their slot is still
held; replay must not double-charge them), which is exactly the
bearer kicker's re-admit discriminator applied to replay's wider
pre-state set.

The repro replays a ``succeeded`` (released) row through the REAL route
against the REAL gate and asserts the gate is re-charged. The fix
mirrors the kicker: the route re-admits through
``SaturationGate.admit`` for released pre-states only, refusing the
replay with the canonical ``saturation_cap`` envelope when the gate is
full (re-queueing without a slot would just reintroduce the drift).
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from phantom.config.settings import InstanceCfg, RouteCfg
from phantom.instances.context import InstanceContext
from phantom.instances.dispatcher import InstanceDispatcher
from phantom.models.upload import UploadRow
from phantom.routes import admin as admin_routes
from phantom.storage import (
    FileBodyStore,
    RamBodyStore,
    SqliteUploadStore,
)
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies.fixed_intervals import FixedIntervalsStrategy
from phantom.workers.saturation import SaturationGate

from .conftest import track_instance

pytestmark = pytest.mark.asyncio

# Generous caps so the replay's re-admission is always grantable; the
# assertions are about ledger truth, not refusal behavior.
_GATE_ROW_CAP = 100
_GATE_BYTE_CAP = 10_000_000
_GATE_DISK_CAP = 1_000_000_000

# The declared size admission charged for the row.
_DECLARED_BYTES = 2_048


async def _build_instance(tmp_path: Path) -> InstanceContext:
    """A real-store instance whose saturation gate the route must keep exact."""
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
        current_settings=MagicMock(),
    )
    return track_instance(instance)


async def test_replay_of_succeeded_row_recharges_the_gate(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Replaying a succeeded upload must put its slot back on the ledger.

    The row's slot was released when the sender recorded ``succeeded``,
    so the gate idles at zero. The replay re-queues the row - it is in
    flight again and the sender will release it again on its next
    terminal transition - so the route must re-charge the gate exactly
    as the bearer kicker does when it wakes a parked row.
    """
    instance = await _build_instance(tmp_path)
    dispatcher = InstanceDispatcher([instance])
    row = make_upload_row(state="succeeded", route_name="files", body_size_bytes=_DECLARED_BYTES)
    await instance.store.insert(row)
    assert instance.saturation.in_flight == 0, "precondition: released row, idle gate"

    replayed = await admin_routes.replay_upload(row.chain_id, dispatcher)

    assert replayed.state == "queued", "precondition: the replay re-queued the row"
    assert instance.saturation.in_flight == 1, (
        "the replayed row is in flight but the gate was never re-charged: "
        "it undercounts now and double-frees when the sender releases"
    )
    assert instance.saturation.in_flight_bytes == _DECLARED_BYTES, (
        f"in_flight_bytes is {instance.saturation.in_flight_bytes}, not the "
        f"replayed row's {_DECLARED_BYTES}"
    )
