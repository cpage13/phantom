"""A re-POST never inherits a prior occupant's body refs (R11-1, fixed).

Finding R11-1 (a residual the R10-D1 fix did not close; extends the
R10-D1 / R8-3 stale-identity family). FIXED by the chain_id namespace
clear at admission: ``_persist_row_and_claim`` deletes the chain_id's
body namespace after the live-row refusal and before its put, so a
reused chain_id owns a virgin namespace by construction.

History (what the bug was): R10-D1 made the bulk-delete C1 cleanup loop
and the reaper ``max_rows`` eviction pass re-read the live table
immediately before each late body delete and STEP ASIDE when a
same-chain_id re-POST already landed (so the new owner's accepted bytes
are not wiped). The step-aside's original written justification - the
old row's leftover files are "removed with the new row's own body
lifecycle" - held ONLY when the re-POST declared the SAME body_ref
names as the removed row. When the OLD row declared a ref name the NEW
row OMITS (legal: the chain_id is the row identity, not a body-shape
contract), the stale file co-resided in the shared per-chain_id
directory because ``FileBodyStore.put`` is ADDITIVE (the BodyStore.put
contract, R11-a). The sender's ``_load_body_refs`` reads the directory
UNION via ``BodyStore.get_all`` and raises ``StorageCorruptionError``
for any file with no matching ``body_hashes`` entry - so the new
owner's first claim took the ACCEPTED (202'd) upload to the
``corrupted`` terminal state. North-star violation, deterministic in
``all_disk``; in hybrid it surfaced once the body migrated off RAM.
The same poison was reachable with NO race at all: a dead chain_id's
files linger between a crash inside the stamp-first discard legs
(R9-5/R10-1 posture) and the metadata pass, or while an orphan waits
out the janitor's two-sweep confirmation (R6-1), so a re-POST reusing
the chain_id in those windows inherited them sequentially.

Both legs below drive the REAL admission persist stage
(``_persist_row_and_claim`` - the function that owns the R11-1 clear),
a REAL SqliteUploadStore, SaturationGate, and FileBodyStore
(``all_disk`` mode), and the REAL sender with a real-shaped recording
executor:

* The RACE leg replays the R11-1 attack: a terminal row declaring
  ``{body, extra}`` is bulk deleted; a store wrapper lands the re-POST
  (declaring ONLY ``{body}``) through the real persist stage the
  instant the row DELETE returns - deterministically inside the R10-D1
  window, so the cleanup loop steps aside for the new owner.
* The SEQUENTIAL leg seeds the stale files in a DEAD namespace (no row
  at all - the crash / janitor-window posture) and re-POSTs through
  the same real persist stage. No race, no wrapper.

Property, both legs: the re-admitted accepted upload owns exactly its
declared namespace after admission and DELIVERS - the executor sees
precisely the new row's decoded bytes, the row ends ``succeeded``
(never ``corrupted``), and the gate idles at zero.

Falsifiability (proven both ways while the fix was authored): without
the namespace clear both legs corrupt on the stale undeclared ``extra``
ref; with it both deliver.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest
from phantom.chain.executor import Succeeded
from phantom.compression import build_codec_for_algorithm
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
from phantom.models.upload import CapturedValues, UploadRow
from phantom.routes import admin as admin_routes
from phantom.routes.admission import _persist_row_and_claim
from phantom.storage import FileBodyStore, RamBodyStore, SqliteUploadStore
from phantom.storage.interface import InsertClaimOutcome
from phantom.strategies import FixedIntervalsStrategy
from phantom.workers.saturation import AdmissionGranted, SaturationGate
from phantom.workers.sender import Sender

from .conftest import track_instance

pytestmark = pytest.mark.asyncio

# The re-admitted upload's accepted body, zstd-encoded so the real sender
# verify path (storage_hash then body_hash) is exercised end to end.
_CODEC = build_codec_for_algorithm("zstd")
_NEW_RAW: bytes = b"re-admitted-accepted-bytes-that-must-survive"
_NEW_ENCODED: bytes = _CODEC.encode(_NEW_RAW)
_NEW_STORAGE_HASH: str = hashlib.sha256(_NEW_ENCODED).hexdigest()
_NEW_BODY_HASH: str = hashlib.sha256(_NEW_RAW).hexdigest()

# The removed row declared TWO refs; the re-POST declares only the first.
_OLD_DECLARED_REFS: tuple[str, ...] = ("body", "extra")
_NEW_DECLARED_REF: str = "body"

_OLD_SIZE: int = 300

# Generous caps - nothing is refused for capacity in this scenario.
_GATE_ROW_CAP: int = 10
_GATE_BYTE_CAP: int = 10_000_000
_GATE_DISK_CAP: int = 10_000_000


def _snapshot() -> InstanceSettingsSnapshot:
    """A snapshot in ``all_disk`` mode whose retention is the default.

    ``all_disk`` is a first-class production mode (CONTEXT.md): the body
    store is the bare :class:`FileBodyStore`, so ``get_all`` reads the
    on-disk directory union directly - which is where a prior occupant's
    stale ``extra`` file lived pre-fix. In hybrid mode the same stale
    disk file is masked while the re-admitted body is still RAM-resident
    (RAM-first ``get_all``) and surfaces once the body migrates off RAM;
    ``all_disk`` makes the property deterministic and is the faithful
    minimal setting. The default retention's
    ``succeeded_body_seconds == 0`` also exercises the sender's
    immediate-discard leg after delivery.
    """
    return InstanceSettingsSnapshot(
        persist_trigger=PersistTriggerCfg(),
        body_store=BodyStoreCfg(mode="all_disk"),
        retention=RetentionCfg(),
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


def _old_row(chain_id: UUID) -> UploadRow:
    """The removed (terminal ``failed``) row - declares ``{body, extra}``."""
    now = datetime.now(tz=UTC) - timedelta(seconds=3600)
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "emu",
            "group_id": chain_id,
            "multifile_id": None,
            "send_order": 0,
            "route_name": "files",
            "state": "failed",
            "body_location": "file",
            "body_size_bytes": _OLD_SIZE,
            "received_at": now,
            "updated_at": now,
            "endpoint": "files.example.com",
            "uid": "user-1",
            "chain_envelope_json": "{}",
            "idempotency_key": str(uuid4()),
            "capture_reexecution_active": False,
            "storage_encoding": "zstd",
            "body_hashes": {
                name: {"body_hash": "a" * 64, "storage_hash": "b" * 64}
                for name in _OLD_DECLARED_REFS
            },
        }
    )


def _new_row(chain_id: UUID) -> UploadRow:
    """The re-admitted live row - declares ONLY ``{body}``, encoded zstd."""
    now = datetime.now(tz=UTC)
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "emu",
            "group_id": chain_id,
            "multifile_id": None,
            "send_order": 0,
            "route_name": "files",
            "state": "queued",
            "body_location": "file",
            "body_size_bytes": len(_NEW_ENCODED),
            "received_at": now,
            "updated_at": now,
            # Due now so claim_due (queued -> attempting) picks it up.
            "next_attempt_at": now - timedelta(seconds=1),
            "endpoint": "files.example.com",
            "uid": "user-1",
            "chain_envelope_json": "{}",
            "idempotency_key": str(uuid4()),
            "capture_reexecution_active": False,
            "storage_encoding": "zstd",
            "body_hashes": {
                _NEW_DECLARED_REF: {
                    "body_hash": _NEW_BODY_HASH,
                    "storage_hash": _NEW_STORAGE_HASH,
                }
            },
        }
    )


class _RecordingExecutor:
    """Real-shaped executor stub: records verified bytes, succeeds the chain.

    The sender hands ``execute_one_step`` the DECODED, hash-verified
    body_refs; recording them lets the test assert the delivered bytes
    are exactly the new row's declared payload and nothing else.
    """

    def __init__(self) -> None:
        self.seen_body_refs: list[dict[str, bytes]] = []

    async def execute_one_step(self, row: UploadRow, body_refs: dict[str, bytes]) -> Succeeded:
        """Record what would go upstream; report the chain done."""
        self.seen_body_refs.append(dict(body_refs))
        return Succeeded(
            captured=CapturedValues(),
            next_step_index=1,
            chain_done=True,
            step_name="files",
            upstream_status=200,
            upstream_headers={},
        )


async def _admit_readmission(instance: InstanceContext, chain_id: UUID) -> None:
    """Re-admit ``chain_id`` through the REAL admission persist stage.

    Mirrors ingress exactly where it matters: the gate charge precedes
    the persist stage (admit_chain stage 3 before stage 5), and
    :func:`_persist_row_and_claim` itself runs the live-row pre-check,
    the R11-1 namespace clear, the body put, and the atomic row + claim
    insert - so the production fix, not a hand-rolled imitation of
    admission, is what these tests exercise.
    """
    granted = await instance.saturation.admit(len(_NEW_ENCODED))
    assert isinstance(granted, AdmissionGranted), "precondition: the gate admits the re-POST"
    row = _new_row(chain_id)
    outcome = await _persist_row_and_claim(
        instance,
        row=row,
        idempotency_key=row.idempotency_key,
        stored_body_refs={_NEW_DECLARED_REF: _NEW_ENCODED},
    )
    assert outcome is InsertClaimOutcome.INSERTED, "precondition: the re-POST was admitted"


class _ReadmissionWithDifferentRefs:
    """Store wrapper landing a re-POST with a NARROWER ref set in the window.

    The row DELETE commits inside ``bulk_delete``; from that instant a
    re-POST of the removed chain_id is legal and the cleanup loop has
    not run yet. This wrapper fires the re-admission once, through the
    REAL admission persist stage (:func:`_admit_readmission`), declaring
    ONLY the ``{body}`` ref. Everything else, including the R10-D1
    guard's ``get`` re-read, proxies to the real store.
    """

    def __init__(self, instance: InstanceContext, real: SqliteUploadStore, target: UUID) -> None:
        self._instance = instance
        self._real = real
        self._target = target
        self._fired = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)

    async def bulk_delete(self, **kwargs: Any) -> Any:
        removed = await self._real.bulk_delete(**kwargs)
        if removed and not self._fired:
            self._fired = True
            await _admit_readmission(self._instance, self._target)
        return removed


async def _build_instance(tmp_path: Path) -> InstanceContext:
    """A real-store ``all_disk`` instance with a recording executor.

    The body store is the bare :class:`FileBodyStore` (the ``all_disk``
    composition), so ``get_all`` reads the disk directory union - the
    surface a stale undeclared file poisoned pre-fix.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    await store.start()
    await ram.start()
    await fbs.start()
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
        body_store=fbs,
        persist_controller=None,
        token_cache=MagicMock(),
        minter=None,
        retry_strategy=FixedIntervalsStrategy([1]),
        upstream_client=MagicMock(),
        executor=_RecordingExecutor(),  # type: ignore[arg-type]
        saturation=SaturationGate(
            max_in_flight=_GATE_ROW_CAP,
            max_in_flight_bytes=_GATE_BYTE_CAP,
            max_disk_bytes=_GATE_DISK_CAP,
        ),
        codec_factory=MagicMock(),
        current_settings=lambda: snapshot,
    )
    return track_instance(instance)


async def _claim_and_deliver(instance: InstanceContext, chain_id: UUID) -> None:
    """Claim the re-admitted row exactly as a sender worker does, then drive it.

    Asserts the delivery-side half of the R11-1 property: the executor
    sees exactly the new row's decoded declared bytes, the row ends
    ``succeeded`` with its delivery stamp, and the gate returns to zero
    (the re-admission's charge released at the terminal transition -
    invariant #16).
    """
    store = instance.store
    claimed = await store.claim_due(datetime.now(tz=UTC), limit=10)
    assert [c.chain_id for c in claimed] == [chain_id], (
        "precondition: the re-admitted row was claimed to attempting"
    )
    sender = Sender(
        instance=instance,
        worker_count=1,
        poll_interval_ms=10,
        metrics_registry=None,
    )
    await sender._drive_one(store, claimed[0])

    final = await store.get(chain_id)
    assert final is not None
    assert final.state == "succeeded", (
        "the re-admitted accepted upload must DELIVER. Pre-R11-1 it "
        "corrupted here: the prior occupant's undeclared 'extra' file "
        "survived admission's additive put into the shared chain_id "
        "namespace, and the sender's get_all union failed verification "
        f"on it. last_error={final.last_error!r}"
    )
    assert final.sent_at is not None, "delivery stamps sent_at exactly once"
    executor = instance.executor
    assert isinstance(executor, _RecordingExecutor)
    assert executor.seen_body_refs == [{_NEW_DECLARED_REF: _NEW_RAW}], (
        "upstream must receive exactly the NEW row's decoded declared "
        "bytes - nothing inherited, nothing dropped"
    )
    assert (instance.saturation.in_flight, instance.saturation.in_flight_bytes) == (0, 0), (
        "invariant #16: the re-admission's charge is released at the "
        "terminal transition; the gate idles at zero"
    )


async def _assert_namespace_is_exactly_declared(instance: InstanceContext, chain_id: UUID) -> None:
    """The admission-side half of the R11-1 property: a virgin namespace.

    Immediately after re-admission the chain_id's body namespace holds
    EXACTLY the declared ref set - the R11-1 clear removed every prior
    occupant's file before the put.
    """
    stored = await instance.body_store.get_all(chain_id)
    assert set(stored) == {_NEW_DECLARED_REF}, (
        f"the re-admitted namespace must hold exactly the declared refs; got {sorted(stored)}"
    )
    assert stored[_NEW_DECLARED_REF] == _NEW_ENCODED


async def test_readmission_with_narrower_refs_is_not_corrupted(tmp_path: Path) -> None:
    """RACE leg: bulk delete + in-window re-POST with fewer refs delivers.

    The R11-1 attack, post-fix: a terminal ``failed`` row declaring
    ``{body, extra}`` is bulk deleted (routine operator cleanup). The
    row DELETE commits, legalizing a same-chain_id re-POST; the wrapper
    re-admits with a NARROWER envelope (only ``{body}``) through the
    real persist stage the instant the DELETE returns, so the R10-D1
    cleanup loop finds a live new owner and steps aside. The namespace
    clear inside the re-admission is what makes the step-aside safe:
    the new owner never held the stale ``extra`` ref at all.
    """
    instance = await _build_instance(tmp_path)
    chain_id = uuid4()
    real_store = instance.store
    await real_store.insert(_old_row(chain_id))
    # The removed row's TWO files land on disk (it was failed/terminal).
    await instance.body_store.put(
        chain_id, {name: b"OLD-" + name.encode() for name in _OLD_DECLARED_REFS}
    )
    instance.store = _ReadmissionWithDifferentRefs(  # type: ignore[assignment]
        instance,
        real_store,  # type: ignore[arg-type]
        chain_id,
    )
    dispatcher = InstanceDispatcher([instance])

    response = await admin_routes.bulk_delete_uploads(DeleteFilter(state="failed"), dispatcher)
    assert response.deleted == 1, "precondition: the old failed row was bulk-deleted"

    # The re-admitted row is live, declares only {body}, and owns a
    # namespace holding exactly that - the old 'extra' file is gone.
    new_row = await instance.store.get(chain_id)
    assert new_row is not None and new_row.state == "queued", (
        "precondition: the re-admitted row exists and is live (queued)"
    )
    await _assert_namespace_is_exactly_declared(instance, chain_id)

    await _claim_and_deliver(instance, chain_id)


async def test_sequential_readmission_of_dead_namespace_is_not_poisoned(tmp_path: Path) -> None:
    """SEQUENTIAL leg: stale files in a dead namespace, no race at all.

    Models the raceless windows the R11-1 fix also closes: body files
    sit under a chain_id with NO live row (a crash inside the
    stamp-first discard legs before their file deletes - the R9-5/R10-1
    posture - or an orphan waiting out the janitor's two-sweep
    confirmation). A producer re-POSTs the same chain_id with a
    narrower ref set through the real persist stage. The pre-check
    passes (no live row), the R11-1 clear empties the namespace, and
    the upload delivers exactly its own bytes.
    """
    instance = await _build_instance(tmp_path)
    chain_id = uuid4()
    # Dead namespace: files on disk, no row anywhere.
    await instance.body_store.put(
        chain_id, {name: b"OLD-" + name.encode() for name in _OLD_DECLARED_REFS}
    )

    await _admit_readmission(instance, chain_id)
    await _assert_namespace_is_exactly_declared(instance, chain_id)

    await _claim_and_deliver(instance, chain_id)
