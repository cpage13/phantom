"""Q9: expire_row's slot release rides the CAS-guarded STATE write, not the discard.

Before ADR-036, ``expire_row`` released the saturation slot only when its
body discard FLIPPED. The discard's guard is
``state = 'expired' AND body_discarded_at IS NULL``, so it misses whenever
another actor stamped or removed the row between the two writes, and on every
one of those interleavings nobody released: the four removers each decide on
``row_holds_slot("expired", ...)``, which is False, and the reaper's body pass
owns only the row it stamped. The slot leaked for the process lifetime.

ADR-036 moves the decision inside the gate and rides it on the STATE write,
which is CAS-guarded on ``expected_state``: exactly one writer lands it, so
exactly one release follows, whoever wins the discard. This is the phase's one
pre-authorised behaviour delta.

Q31 stands and this test does NOT close it: the wrapper reproduces the OUTCOME
of the race, not the race. Building a seam that parks a real reaper between
``expire_row``'s two awaits remains recorded, not done.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from phantom.models.upload import BodyHash, BodyHashes, StorageHash, UploadRow, UploadState
from phantom.storage import RamBodyStore, SqliteUploadStore
from phantom.storage.interface import AttemptWriteOutcome, DiscardOutcome
from phantom.workers._expire import expire_row
from phantom.workers.saturation import SaturationGate

from .conftest import track_started

# asyncio_mode is "auto" repo-wide, so the async test needs no marker.

BODY_REF_NAME = "body"
BODY_BYTES = b"phantom-q9-the-slot-must-come-back-anyway"

DEADLINE_LAST_ERROR = "send_deadline:1s"

# Caps generous enough that the single admit below is never refused; the test
# is about what comes BACK, not about the admit threshold.
_GATE_ROW_CAP = 10
_GATE_BYTE_CAP = 10_000_000
_GATE_DISK_CAP = 10_000_000


class _AnotherOwnerWonTheDiscardStore:
    """Store wrapper reproducing the outcome of the Q9 race.

    Two overrides, both load-bearing:

    * ``discard_body_and_zero_accounting`` returns the exact value the real
      store returns when another actor already stamped or removed the row: a
      non-flip with no pre-image on either side. That is the race's outcome,
      substituted rather than raced for (Q31).
    * ``record_attempt_result`` delegates to the real store and STASHES the
      returned :class:`AttemptWriteOutcome`, because ``write`` is a local
      inside ``expire_row`` (which returns ``None``) and there is no other way
      to assert on the pre-image the gate settled from.

    Everything else is delegated, so the state write, its CAS guard and its
    in-transaction pre-image are all the real implementation's.
    """

    def __init__(self, real: SqliteUploadStore) -> None:
        self._real = real
        self.last_write: AttemptWriteOutcome | None = None

    async def get(self, chain_id: UUID) -> UploadRow | None:
        """Delegate row reads to the real store."""
        return await self._real.get(chain_id)

    async def record_attempt_result(self, *args: Any, **kwargs: Any) -> AttemptWriteOutcome:
        """Delegate the state write, keeping its outcome visible to the test."""
        result: AttemptWriteOutcome = await self._real.record_attempt_result(*args, **kwargs)
        self.last_write = result
        return result

    async def discard_body_and_zero_accounting(
        self, chain_id: UUID, *, expected_state: UploadState
    ) -> DiscardOutcome:
        """Answer as the real store does when someone else already stamped the row."""
        return DiscardOutcome(
            flipped=False,
            body_size_bytes=0,
            previous_state=None,
            discarded_at=None,
        )


def _attempting_row() -> UploadRow:
    """An ``attempting`` row holding one slot worth ``len(BODY_BYTES)`` bytes."""
    digest = hashlib.sha256(BODY_BYTES).hexdigest()
    now = datetime.now(tz=UTC)
    return UploadRow.model_validate(
        {
            "chain_id": uuid4(),
            "instance_id": "primary",
            "group_id": uuid4(),
            "multifile_id": uuid4(),
            "send_order": 0,
            "route_name": "files",
            "state": "attempting",
            "body_location": "ram",
            "received_at": now,
            "updated_at": now,
            "endpoint": "files.example.com",
            "uid": "user-1",
            "chain_envelope_json": "{}",
            "idempotency_key": "k",
            "capture_reexecution_active": False,
            "body_size_bytes": len(BODY_BYTES),
            "storage_encoding": "original",
            "body_hashes": {
                BODY_REF_NAME: BodyHashes(
                    body_hash=BodyHash(digest), storage_hash=StorageHash(digest)
                )
            },
        },
    )


async def test_expire_releases_the_slot_when_another_owner_won_the_discard(
    tmp_path: Path,
) -> None:
    """The slot AND its bytes come back even when this call did not stamp the body.

    Objective: prove the release depends on the CAS-guarded STATE write and
    not on which contender flipped the body, which is Q9's closure.

    Arrangement: a real gate holding one admitted charge of ``len(BODY_BYTES)``
    bytes, a real store with a persisted ``attempting`` row of that size, a
    real RAM body store, and the wrapper above answering the discard as a
    non-flip.

    Success, three assertions:

    1. ``gate.in_flight == 0`` - the count came back.
    2. ``gate.in_flight_bytes == 0`` - so did the bytes. This is what
       discriminates riding the basis on the STATE write from riding it on the
       discard outcome, whose size is 0 on a non-flip: that choice would return
       the COUNT and strand the BYTES, passing assertion 1 and failing this one.
    3. ``last_write.previous_state == "attempting"`` - pins the CAS-equality
       invariant, that a landed write's pre-image state IS its guard. It does
       NOT carry the argument for keeping the pre-image SELECT (an
       implementation deriving ``previous_state`` from the guard would pass
       it); the two reasons that protect the SELECT are that the stamp and the
       size are not derivable from the guard, and that a non-landed write has
       no other account of what happened.

    Pre-fix build: every ADR-036 edit landed EXCEPT the settle's insertion
    above ``expire_row``'s flip guard. On that build nothing is released,
    assertions 1 and 2 fail with ``in_flight == 1`` and
    ``in_flight_bytes == len(BODY_BYTES)``, and assertion 3 still passes. A
    build that keeps the old guarded release, and a build that puts the settle
    UNDER the flip guard, both fail 1 and 2 the same way.
    """
    real = track_started(SqliteUploadStore(str(tmp_path / "uploads.db")))
    ram = RamBodyStore()
    await real.start()
    await ram.start()
    gate = SaturationGate(
        max_in_flight=_GATE_ROW_CAP,
        max_in_flight_bytes=_GATE_BYTE_CAP,
        max_disk_bytes=_GATE_DISK_CAP,
    )
    row = _attempting_row()
    await real.insert(row)
    await ram.put(row.chain_id, {BODY_REF_NAME: BODY_BYTES})
    await gate.admit(len(BODY_BYTES))
    assert gate.in_flight == 1, "setup precondition: the row holds exactly one charge"

    wrapper = _AnotherOwnerWonTheDiscardStore(real)
    await expire_row(
        wrapper,  # type: ignore[arg-type]
        gate,
        ram,
        row,
        expected_state="attempting",
        last_error=DEADLINE_LAST_ERROR,
        upstream_status=None,
    )

    assert gate.in_flight == 0, (
        "Q9: the state write crossed out of SLOT_HOLDING_STATES, so the slot must come "
        "back whether or not this call won the discard"
    )
    assert gate.in_flight_bytes == 0, (
        "the release basis must ride the STATE write's in-transaction size; the discard "
        "outcome's size is 0 here, and using it would strand the bytes forever"
    )
    assert wrapper.last_write is not None
    assert wrapper.last_write.previous_state == "attempting", (
        "a landed write's pre-image state is its own CAS guard"
    )
