"""The sender's immediate-discard leg confirms-then-acts on live state (R10-1).

Invariant #17 (architecture-intent section 5): a worker acting on a row
re-reads the live truth at the decision instant instead of trusting the
state it acted on a moment ago. R9-5 inverted the REAPER's body-discard
leg to stamp-FIRST for exactly this reason; R10-1 (fixed this round)
applied the same inversion to the SENDER's immediate discard on the
chain-done success branch (``workers/sender.py`` ``_on_succeeded``,
fired when ``succeeded_body_seconds == 0`` - the default Pi posture).

Finding history (R10-1, pre-fix): the leg kept a files-FIRST order on an
"ownership" argument - it acts on the row it JUST transitioned to
``succeeded``, so the guarded ``discard_body_and_zero_accounting``
(state + stamp predicate) was assumed to match by construction. That
argument excluded a crash but NOT an admin replay. The uploads-table
write lock is RELEASED between the succeeded commit and the guarded
stamp, and a replay grabbing that gap is legal (the row is ``succeeded``
and unstamped, so BOTH of ``replay``'s refusal guards pass) and
re-queues the row to ``queued``. The pre-fix sender then destroyed the
LIVE queued row's bodies and its stamp no-op'd (state was now
``queued``), leaving a LIVE, ``queued``, UNSTAMPED row with no bodies -
``BodyMissingError`` -> ``corrupted`` on the next claim. The operator's
explicit replay, acknowledged with a 200 + ``UploadRow``, was destroyed
and laundered into a storage-fault diagnostic: the north-star violation,
in the same stale-identity family the loop fixed in R6-1 / R7-2 / R8-3 /
R9-5 - found in the ONE discard leg R9-5's inversion did not cover.

The fixed leg stamps FIRST (guarded on ``succeeded``) and deletes body
files only after a confirmed flip, so the race resolves cleanly in both
directions: the discard wins and the replay refuses with
``ReplayBodyDiscardedError`` (the row stays terminal + stamped), or the
replay wins and the discard guard mismatches so the files are preserved
for the live re-queued row. Either way property P holds: no live,
non-terminal row is ever left unstamped with no bodies.

The test drives the REAL ``Sender._on_succeeded`` over a REAL
SqliteUploadStore, SaturationGate, and HybridBodyStore, with the REAL
admin ``replay`` route as the concurrent actor. A body-store wrapper
lands the replay deterministically the instant the sender reaches its
first body-destroying effect (``body_store.delete``), the established
R7-2 / R8-3 / R9-5 hook technique; under the fixed stamp-first order
that effect sits after the confirmed flip, so the hook exercises the
discard-wins direction end to end. Falsifiability was proven both ways
in scratch while the finding was live (the pre-fix sender failed
property P at the body-present assert); committed as a strict xfail at
7000650 and flipped by the R10-1 fix in the same change that inverted
the leg.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from phantom.chain.executor import Succeeded
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
from phantom.models.upload import CapturedValues, UploadRow
from phantom.routes import admin as admin_routes
from phantom.storage import FileBodyStore, RamBodyStore, SqliteUploadStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.strategies import FixedIntervalsStrategy
from phantom.workers.saturation import SaturationGate, row_holds_slot
from phantom.workers.sender import Sender

from .conftest import track_instance

pytestmark = pytest.mark.asyncio

# The accepted upload's declared/stored bytes.
_BODY_BYTES: int = 700

# Default-posture retention: succeeded bodies drop immediately on success
# (succeeded_body_seconds == 0 fires the sender's IMMEDIATE discard leg);
# the metadata window stays open so the row survives the scenario.
_SUCCEEDED_METADATA_SECONDS: int = 3600

# Generous caps so no admit in the scenario is refused for capacity.
_GATE_ROW_CAP: int = 10
_GATE_BYTE_CAP: int = 10_000_000
_GATE_DISK_CAP: int = 10_000_000

# Live, non-terminal states: a row in any of these is still deliverable
# and MUST still have its bodies (otherwise the sender's next claim
# corrupts it).
_LIVE_NONTERMINAL: frozenset[str] = frozenset({"queued", "attempting", "auth_expired", "stored"})


def _snapshot() -> InstanceSettingsSnapshot:
    """A snapshot whose retention fires the sender's immediate discard."""
    return InstanceSettingsSnapshot(
        persist_trigger=PersistTriggerCfg(),
        body_store=BodyStoreCfg(),
        retention=RetentionCfg(
            succeeded_body_seconds=0,
            succeeded_metadata_seconds=_SUCCEEDED_METADATA_SECONDS,
        ),
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


class _BodyDeleteFiresAction:
    """Body-store wrapper firing an action before the FIRST ``delete``.

    ``body_store.delete`` is the sender's first body-destroying effect.
    Pre-R10-1 it ran BEFORE the guarded stamp (the racing replay then
    revived a row whose bodies the sender went on to destroy); the fixed
    leg reaches it only AFTER a confirmed flip, so the action lands
    against an already-stamped row. This wrapper fires the admin action
    once, the instant that delete is reached, then delegates to the real
    delete. Everything else proxies to the real body store.
    """

    def __init__(self, real: HybridBodyStore, action: Callable[[], Awaitable[None]]) -> None:
        self._real = real
        self._action = action
        self._fired = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)

    async def delete(self, chain_id: Any) -> None:
        """Land the admin action once, then perform the real delete."""
        if not self._fired:
            self._fired = True
            await self._action()
        await self._real.delete(chain_id)


async def _build_instance(tmp_path: Path) -> InstanceContext:
    """A real-store instance whose immediate-discard leg must stay safe."""
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
    snapshot = _snapshot()
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


def _make_attempting_row() -> UploadRow:
    """An ``attempting`` row with bodies present, holding one gate slot."""
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
            "state": "attempting",
            "body_location": "ram",
            "body_size_bytes": _BODY_BYTES,
            "received_at": now,
            "updated_at": now,
            "endpoint": "files.example.com",
            "uid": "user-1",
            "chain_envelope_json": "{}",
            "idempotency_key": str(uuid4()),
            "capture_reexecution_active": False,
            "body_hashes": {},
        }
    )


def _succeeded_chain_done() -> Succeeded:
    """A chain-done success result for the final step."""
    return Succeeded(
        captured=CapturedValues(),
        next_step_index=1,
        chain_done=True,
        step_name="put_s3",
        upstream_status=200,
        upstream_headers={},
    )


async def test_replayed_row_survives_the_immediate_discard_leg(tmp_path: Path) -> None:
    """A row replayed as the sender finalizes it must keep its bodies.

    Attack: an ``attempting`` row reaches chain-done success. The sender
    runs its immediate-discard leg (``succeeded_body_seconds == 0``):
    transitions to ``succeeded``, releases the gate, stamps via the
    guarded discard, and only then deletes the body files. An admin
    replay lands the instant the sender reaches its first
    body-destroying effect (pre-R10-1 that was BEFORE the guarded stamp;
    post-fix it is after the confirmed flip). The race must resolve with
    no live row left unstamped and bodiless: either the replay refuses
    against the stamp (row stays terminal) or the replay wins and the
    guard mismatch preserves the bodies. The gate must agree with
    ``row_holds_slot`` on the final row either way (invariant #16).
    """
    instance = await _build_instance(tmp_path)
    row = _make_attempting_row()
    await instance.store.insert(row)
    await instance.body_store.put(row.chain_id, {"body": b"accepted-upload-bytes"})
    granted = await instance.saturation.admit(_BODY_BYTES)
    assert granted.__class__.__name__ == "AdmissionGranted", granted

    dispatcher = InstanceDispatcher([instance])
    real_store = instance.store
    real_body_store = instance.body_store
    replay_observed = {"attempted": False}

    async def admin_replays_the_row() -> None:
        replay_observed["attempted"] = True
        # Under the fixed stamp-first leg the row is already stamped by
        # the time this hook fires (body_store.delete now runs only
        # after a confirmed flip), so the replay refuses with
        # ReplayBodyDiscardedError - the discard-wins direction. The
        # pre-fix files-first leg let the replay through here and then
        # destroyed the live row's bodies. Tolerating both outcomes
        # keeps the test pinning property P rather than one interleave.
        try:
            await admin_routes.replay_upload(row.chain_id, dispatcher)
        except admin_routes.ReplayBodyDiscardedError:
            return

    instance.body_store = _BodyDeleteFiresAction(  # type: ignore[assignment]
        real_body_store, admin_replays_the_row
    )
    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=1)
    await sender._on_succeeded(real_store, row, _succeeded_chain_done())

    assert replay_observed["attempted"], (
        "precondition: the admin replay was landed inside the immediate-discard "
        "window (the sender reached its first body-destroying effect)"
    )
    final = await real_store.get(row.chain_id)
    assert final is not None, "precondition: the row still exists after the leg"

    # Property P: the leg must NEVER leave a row that is still live and
    # non-terminal AND unstamped AND bodiless. The pre-fix files-first
    # sender produced exactly that (queued + unstamped + no bodies); the
    # stamp-first sender never does.
    body_present = await real_body_store.has_body_ref(row.chain_id, "body")
    left_live_and_bodiless = (
        final.state in _LIVE_NONTERMINAL and final.body_discarded_at is None and not body_present
    )
    assert not left_live_and_bodiless, (
        "the sender's immediate-discard leg left a LIVE "
        f"{final.state!r} row unstamped with no bodies: the operator's "
        "acknowledged replay is destroyed and the sender's next claim takes "
        "BodyMissingError to corrupted (north star; R10-1 inverted this leg "
        "to stamp-first exactly as R9-5 did the reaper leg)"
    )
    # The race must resolve into one of exactly two clean end states.
    if final.state == "queued":
        # Replay won: the live re-queued row is deliverable - bodies
        # intact, stamp absent (the sender's guard mismatched).
        assert body_present, "replay-won row must keep its bodies"
        assert final.body_discarded_at is None, "replay-won row must stay unstamped"
    else:
        # Discard won: the row stayed terminal, the stamp landed, and
        # the replay refused against it.
        assert final.state == "succeeded", f"unexpected end state {final.state!r}"
        assert final.body_discarded_at is not None, "discard-won row must carry the stamp"
        assert not body_present, "discard-won row's bodies are deleted after the flip"
    # Invariant #16: the gate agrees with row_holds_slot on the final
    # row. Discard-won: succeeded holds no slot (released at the
    # terminal transition; the replay's optimistic charge was returned
    # on its refusal). Replay-won: the re-queued row holds exactly the
    # one slot the replay charged.
    expected_in_flight = 1 if row_holds_slot(final.state, final.body_discarded_at) else 0
    assert instance.saturation.in_flight == expected_in_flight, (
        f"gate in_flight {instance.saturation.in_flight} disagrees with "
        f"row_holds_slot({final.state!r}, {final.body_discarded_at!r}) -> "
        f"{expected_in_flight} - the immediate-discard leg desynced the ledger"
    )
    assert instance.saturation.in_flight_bytes == expected_in_flight * _BODY_BYTES, (
        f"gate in_flight_bytes {instance.saturation.in_flight_bytes} does not "
        "match the final row's slot accounting"
    )
