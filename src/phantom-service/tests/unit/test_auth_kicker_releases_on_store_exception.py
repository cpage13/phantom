"""The AuthKicker releases its admitted slot when the wake write raises (R10-2).

Invariant #16 (the saturation ledger balances) requires exactly one gate
charge per row in the in-flight set, and a release on every path that
removes a row's claim to a slot. The kicker's wake is admit-then-write:
``saturation.admit`` (``workers/auth_kicker.py``) charges the gate, then
the guarded ``record_attempt_result(expected_state="auth_expired")``
re-queues the row. R9-3 closed the rowcount-0 leg of that window (the row
moved out of ``auth_expired`` before the write); R10-2 (fixed this round)
closed the window's SECOND failure mode: the write itself RAISING.

Finding history (R10-2, pre-fix): there was no try/except between the
admit and the write. A ``record_attempt_result`` raising a transient
``sqlite3.OperationalError`` - a ride-it-out ``SQLITE_BUSY`` past the
busy_timeout, or a genuine ``SQLITE_IOERR`` on flaky SD storage (the
Pi-class posture) - propagated straight out of ``_rescan`` with the slot
admitted one await earlier never released, while the replay route wrapped
its store call and released on ANY exception (``routes/admin.py``
replay_upload). The leak COMPOUNDED: ``AuthKicker.run`` catches the
exception and continues the loop, the failed write committed nothing (the
row stayed ``auth_expired`` with a fresh cache slot), so the next rescan
(every second, AND on every cache write) woke the SAME row, admitted
again, and stranded ANOTHER slot - until the gate saturated and fresh
ingress 503'd ``saturation_cap`` with ZERO live rows behind the count,
restart-only reset. MED severity, the R8-4 / R9-3 availability class,
reached via a store exception rather than a rowcount-0 race.

The fixed kicker wraps the wake write so that ANY exception releases the
just-admitted slot FIRST and then re-raises (run() still logs and
continues): both refusal legs of the admit->write window - rowcount 0 and
exception - now share one posture, the slot returning on every outcome
except a confirmed wake.

The test drives the REAL AuthKicker rescan over a REAL SqliteUploadStore,
SqliteTokenCache, and SaturationGate. A store wrapper raises a transient
``OperationalError`` on the kicker's FIRST re-queue write,
deterministically inside the admit->write window. After the rescan
iteration the gate must be idle (the admitted slot returned), and a
SECOND rescan - the wrapper's fault fires once, modeling the transient
clearing - must wake the row cleanly with exactly one slot held.
Falsifiability was proven both ways in scratch while the finding was live
(the pre-fix kicker stranded ``in_flight=1`` with the row's bytes);
committed as a strict xfail at 7a6c852 and flipped by the R10-2 fix.
"""

from __future__ import annotations

import sqlite3
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
from phantom.workers.auth_kicker import AuthKicker
from phantom.workers.saturation import SaturationGate

from .conftest import track_instance

pytestmark = pytest.mark.asyncio

# Parked row's declared bytes: a visibly non-zero residue when leaked.
_DECLARED_BYTES: int = 500

# Generous caps so the kicker's admit itself is never refused - the test
# is about the unreleased charge, not the admit threshold.
_GATE_ROW_CAP: int = 10
_GATE_BYTE_CAP: int = 10_000_000
_GATE_DISK_CAP: int = 10_000_000


class _RaiseOnRequeueStore:
    """Store wrapper raising a transient OperationalError on the kicker write.

    The kicker calls ``list_non_terminal`` (snapshot), then the token
    check and the gate admit, then ``record_attempt_result``. This
    wrapper delegates the scan to the real store but raises a transient
    ``sqlite3.OperationalError`` on the FIRST re-queue write - the exact
    Pi-class storage fault (busy past the timeout / I/O error) the kicker
    has no guard for. The flag makes it fire once so a corrected kicker's
    retry-on-next-rescan would proceed normally.
    """

    def __init__(self, real: SqliteUploadStore) -> None:
        self._real = real
        self._fired = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)

    async def list_non_terminal(self) -> list[UploadRow]:
        """Delegate the kicker's scan snapshot to the real store."""
        return await self._real.list_non_terminal()

    async def record_attempt_result(self, *args: Any, **kwargs: Any) -> int:
        """Raise a transient storage fault on the kicker's first write."""
        if not self._fired:
            self._fired = True
            raise sqlite3.OperationalError("disk I/O error")
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


async def test_wake_write_exception_returns_the_admitted_slot(tmp_path: Path) -> None:
    """A wake whose re-queue write RAISES must leave the gate exactly idle.

    Attack: park an ``auth_expired`` row (no slot held - the sender
    released at park), land a fresh token so the kicker wakes it, and
    have the guarded re-queue write raise a transient
    ``OperationalError`` (the Pi-class flaky-storage fault). The kicker
    admitted a slot one await before the write; that slot must be
    returned on the exception path, exactly as the replay route returns
    its slot on a store exception, and the fault must still propagate so
    run() logs it. A second rescan (the transient cleared) must then
    wake the row cleanly with exactly one slot held - the recovery the
    release-on-exception posture exists to keep exact.
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
    instance.store = _RaiseOnRequeueStore(real_store)  # type: ignore[assignment]
    kicker = AuthKicker(instance=instance)

    # The store write raises; AuthKicker.run() would catch this and
    # continue (auth_kicker.py:83-86). Model one rescan iteration by
    # catching the propagated fault here.
    raised = False
    try:
        await kicker._rescan()
    except sqlite3.OperationalError:
        raised = True
    assert raised, (
        "precondition: the kicker's re-queue write raised the transient "
        "storage fault inside the admit->write window"
    )

    final = await real_store.get(row.chain_id)
    assert final is not None and final.state == "auth_expired", (
        "precondition: the row stays auth_expired (the failed write never "
        "committed), so the next rescan would wake it again"
    )
    assert instance.saturation.in_flight == 0, (
        "the kicker stranded the slot it admitted when its re-queue write "
        f"raised: in_flight is {instance.saturation.in_flight} with zero rows "
        "in the in-flight set - the gate drifts up on every wake whose write "
        "faults, the row stays auth_expired so the next rescan strands another, "
        "and fresh ingress eventually 503s with no live row behind the count "
        "(R9-3 closed only the rowcount-0 leg; the replay route releases here)"
    )
    assert instance.saturation.in_flight_bytes == 0, (
        f"the kicker stranded {instance.saturation.in_flight_bytes} in-flight "
        "bytes that correspond to no live row"
    )

    # Recovery leg: the wrapper's fault fires once (the transient
    # cleared). The next rescan must wake the row cleanly - queued, with
    # exactly the one slot and byte charge the wake admitted.
    await kicker._rescan()
    woken = await real_store.get(row.chain_id)
    assert woken is not None and woken.state == "queued", (
        "after the transient cleared, the next rescan must re-queue the row"
    )
    assert instance.saturation.in_flight == 1, (
        "the clean wake must hold exactly one slot for the re-queued row, "
        f"not {instance.saturation.in_flight} - neither a stranded extra "
        "from the faulted attempt nor a missing charge"
    )
    assert instance.saturation.in_flight_bytes == _DECLARED_BYTES, (
        f"the clean wake must charge exactly the row's {_DECLARED_BYTES} "
        f"declared bytes, not {instance.saturation.in_flight_bytes}"
    )
