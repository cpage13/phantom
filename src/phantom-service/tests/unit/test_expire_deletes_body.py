"""F3: ``expire_row`` deletes the BYTES it says it discarded, not just the stamp.

``workers/_expire.py`` is the single writer of ``new_state="expired"`` (ADR-032).
It stamped ``body_discarded_at`` and zeroed the row's accounting through
``discard_body_and_zero_accounting``, then conditionally released the saturation
slot. It never called ``body_store.delete``, so the bytes stayed where they were,
and every reclaim path then excluded the stamped row: the reaper's body pass
filters ``body_discarded_at IS NULL``, its metadata pass never touches bodies,
``RamBodyStore.list_orphans`` returns ``[]`` unconditionally, and the
``PersistController`` skips stamped rows. Repeated send-deadline expiries grew
RAM without bound while the saturation gate reported free capacity.

F3 adds the byte-side delete, AFTER the stamp and only on a confirmed flip. The
ordering is the repo's existing posture on its other two discard legs (the
reaper's R9-5 leg and the sender's R10-1 leg): never delete bytes this call did
not stamp.

The tests cover both caller paths (the sender give-up on an ``attempting`` row
that still holds its slot, and the kicker sweep of a parked ``auth_expired`` row
whose slot went back at park), plus the confirm-then-act guard, plus a
documentation guard on the discard owner's two docstrings.
"""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from phantom.models.upload import BodyHash, BodyHashes, StorageHash, UploadRow
from phantom.storage import FileBodyStore, RamBodyStore, SqliteUploadStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.workers._expire import expire_row
from phantom.workers.saturation import SaturationGate

# asyncio_mode is "auto" repo-wide, so the async tests need no marker and the
# one synchronous documentation-guard test must not carry one.

# The body every test writes and then expects to be gone.
BODY_REF_NAME = "body"
BODY_BYTES = b"phantom-f3-expire-must-free-these-bytes"

# The deadline token ``expire_row`` stamps on ``last_error``.
DEADLINE_LAST_ERROR = "send_deadline:1s"


class _Harness:
    """The real collaborators ``expire_row`` takes, built over one tmp directory.

    ``expire_row`` takes its collaborators directly rather than an
    ``InstanceContext``, so no composition-side wiring is needed here.

    Attributes:
        store: A real :class:`SqliteUploadStore`.
        ram: The RAM half of the body store.
        files: The disk half.
        body_store: The hybrid over both, which is what callers pass.
        gate: A real :class:`SaturationGate`.
    """

    def __init__(
        self,
        *,
        store: SqliteUploadStore,
        ram: RamBodyStore,
        files: FileBodyStore,
        body_store: HybridBodyStore,
        gate: SaturationGate,
    ) -> None:
        self.store = store
        self.ram = ram
        self.files = files
        self.body_store = body_store
        self.gate = gate


async def _harness(tmp_path: Path) -> _Harness:
    """Build the real store, both body-store halves, the hybrid, and the gate.

    Args:
        tmp_path: pytest's per-test temporary directory.

    Returns:
        The assembled :class:`_Harness`.
    """
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    ram = RamBodyStore()
    files = FileBodyStore(tmp_path / "bodies")
    body_store = HybridBodyStore(ram=ram, disk=files)
    await store.start()
    await body_store.start()
    gate = SaturationGate(
        max_in_flight=10, max_in_flight_bytes=10_000_000, max_disk_bytes=10_000_000
    )
    return _Harness(store=store, ram=ram, files=files, body_store=body_store, gate=gate)


def _row(*, state: str, body_location: str) -> UploadRow:
    """Build a row declaring one body ref of ``BODY_BYTES``.

    Args:
        state: The row's pre-state, which is also ``expire_row``'s CAS guard.
        body_location: Where the bytes live, ``"ram"`` or ``"file"``.

    Returns:
        The persisted-shape :class:`UploadRow`.
    """
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
            "state": state,
            "body_location": body_location,
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


async def test_path_a_expire_frees_ram_bytes_and_the_slot(tmp_path: Path) -> None:
    """The sender give-up path frees both the accounting AND the actual RAM.

    Objective: F3's own defect. An ``attempting`` row that still holds its slot
    must, on expiry, end with the row stamped ``expired``, the RAM body gone,
    and the slot returned. Success: the re-read row is ``expired`` with
    ``body_discarded_at`` stamped, ``RamBodyStore.total_bytes()`` is zero, and
    the gate is back to zero rows and zero bytes.
    """
    h = await _harness(tmp_path)
    row = _row(state="attempting", body_location="ram")
    await h.store.insert(row)
    await h.ram.put(row.chain_id, {BODY_REF_NAME: BODY_BYTES})
    admitted = await h.gate.admit(len(BODY_BYTES))
    assert admitted.__class__.__name__ == "AdmissionGranted", admitted

    await expire_row(
        h.store,
        h.gate,
        h.body_store,
        row,
        expected_state="attempting",
        last_error=DEADLINE_LAST_ERROR,
        upstream_status=None,
    )

    fresh = await h.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "expired"
    assert fresh.body_discarded_at is not None
    assert await h.ram.total_bytes() == 0, "the expired transition must free the RAM bytes"
    assert h.gate.in_flight == 0
    assert h.gate.in_flight_bytes == 0


async def test_path_b_expire_frees_disk_bytes_and_leaves_the_slot_alone(tmp_path: Path) -> None:
    """The kicker sweep path frees the bytes without double-releasing a slot.

    Objective: a parked ``auth_expired`` row returned its slot at park, so the
    sweep must free the bytes and touch the gate not at all. Success: the row is
    ``expired``, the chain directory is gone (``get_all`` raises ``KeyError``),
    and the gate is unchanged at zero.
    """
    h = await _harness(tmp_path)
    row = _row(state="auth_expired", body_location="file")
    await h.store.insert(row)
    await h.files.put(row.chain_id, {BODY_REF_NAME: BODY_BYTES})

    await expire_row(
        h.store,
        h.gate,
        h.body_store,
        row,
        expected_state="auth_expired",
        last_error=DEADLINE_LAST_ERROR,
        upstream_status=None,
    )

    fresh = await h.store.get(row.chain_id)
    assert fresh is not None
    assert fresh.state == "expired"
    with pytest.raises(KeyError):
        await h.files.get_all(row.chain_id)
    assert h.gate.in_flight == 0
    assert h.gate.in_flight_bytes == 0


async def test_expire_touches_no_bytes_when_the_stamp_does_not_flip(tmp_path: Path) -> None:
    """Pin the R9-5 confirm-then-act posture: never delete bytes this call did not stamp.

    Objective: stop a future refactor making the delete unconditional, which
    would clobber a revived row's fresh bodies. Setup: the row is stamped
    directly through the discard owner while still in its pre-state, then fresh
    bytes are written for the same chain_id. ``record_attempt_result`` has no
    ``body_discarded_at`` guard so the state write still succeeds, but the
    discard's ``AND body_discarded_at IS NULL`` predicate then returns
    ``flipped=False``.

    Success: the fresh bytes are still readable.

    **The gate assertions changed with ADR-036 and the change is Q9's
    sanctioned delta, not a regression.** The slot no longer rides the
    DISCARD; it rides the CAS-guarded STATE write two lines above, which
    landed here. So the count comes back where it used to leak. The BYTES do
    not, and that is honest rather than a miss: the direct discard in this
    test's own setup zeroed ``body_size_bytes`` BEFORE the state write read
    its pre-image, so the basis riding that write is 0. In the interleaving
    Q9 is actually about (a racing stamper landing AFTER the state write) the
    pre-image still carries the true size and both counters return; see
    ``test_expire_releases_when_the_discard_did_not_flip.py``.
    """
    h = await _harness(tmp_path)
    row = _row(state="attempting", body_location="ram")
    await h.store.insert(row)
    admitted = await h.gate.admit(len(BODY_BYTES))
    assert admitted.__class__.__name__ == "AdmissionGranted", admitted

    already = await h.store.discard_body_and_zero_accounting(
        row.chain_id, expected_state="attempting"
    )
    assert already.flipped, "setup precondition: the direct discard must be the stamping call"
    revived = b"fresh-bytes-written-by-a-concurrent-owner"
    await h.ram.put(row.chain_id, {BODY_REF_NAME: revived})

    await expire_row(
        h.store,
        h.gate,
        h.body_store,
        row,
        expected_state="attempting",
        last_error=DEADLINE_LAST_ERROR,
        upstream_status=None,
    )

    assert await h.ram.get_all(row.chain_id) == {BODY_REF_NAME: revived}, (
        "a non-flipping discard means another owner holds these bytes; expire_row must not "
        "delete them"
    )
    assert h.gate.in_flight == 0, (
        "Q9/ADR-036: the release rides the CAS-guarded STATE write, which landed, so the "
        "count comes back whether or not this call won the discard"
    )
    assert h.gate.in_flight_bytes == len(BODY_BYTES), (
        "the basis is the STATE write's in-transaction size, which this test's own setup "
        "already zeroed, so the bytes stay charged on THIS interleaving"
    )


def test_discard_owner_docstrings_name_three_callers() -> None:
    """The discard owner's two docstrings must not still say "two callers".

    Objective: a documentation-as-test guard in the house style, matching
    ``test_discard_single_owner.py`` and ``test_transition_table.py``. F3 makes
    ``expire_row`` the third caller of ``discard_body_and_zero_accounting``, and
    the Protocol docstring plus the concrete implementation's docstring both
    described exactly two.

    **The scan whitespace-normalises before searching, and is case-insensitive.**
    The ``sqlite_store.py`` site wraps the phrase across a line break ("Exactly"
    ends one line, "TWO callers" starts the next) and upper-cases it, so a
    plain line-oriented, case-sensitive grep cannot see it.

    **The scan is scoped to those two files rather than the tree**, because six
    other files legitimately carry the phrase and a tree-wide ban would falsely
    trip on all six. They divide into three groups: ``expire_row``'s two caller
    SUBSYSTEMS (``workers/_expire.py`` twice, ``test_transition_table.py``, and
    ADR-032 three times), two unrelated uses (ADR-015's "No two callers share a
    transition" and ``payloads_routines.py``'s seed comment), and
    ``_record_stored``'s own pair in ``sender.py``, which F1 corrects for its
    own reason.
    """
    root = Path(__file__).resolve().parents[2] / "src" / "phantom" / "storage"
    for name in ("interface.py", "sqlite_store.py"):
        text = (root / name).read_text(encoding="utf-8")
        normalised = re.sub(r"\s+", " ", text).lower()
        assert "two callers" not in normalised, (
            f"{name} still describes the discard owner as having two callers; F3 made "
            f"expire_row the third"
        )
        assert "three callers" in normalised, (
            f"{name} must name all three discard callers (sender, reaper, expire_row)"
        )
