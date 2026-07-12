"""Unit tests for phantom.workers.recovery.

Slice 1.D rewrite (plan § 2.3.15). Recovery now operates on a single
:class:`UploadStore` + a :class:`BodyStore` (no more dual-store fixture).
The integrity guard is multipart-aware (strategy §1 — body is atomic):
a single missing body_ref in ``body_hashes`` quarantines the row as
``corrupted`` regardless of how many other refs are present.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from phantom.models.upload import BodyHash, BodyHashes, StorageHash, UploadRow
from phantom.storage import FileBodyStore, RamBodyStore, SqliteUploadStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.workers.recovery import reconcile_saturation, run_recovery
from phantom.workers.saturation import SaturationGate

from .conftest import track_started


async def _build_store(tmp_path: Path) -> SqliteUploadStore:
    """Construct a started single persistent store."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await store.start()
    return track_started(store)


async def _build_hybrid_body_store(
    tmp_path: Path,
) -> tuple[HybridBodyStore, RamBodyStore, FileBodyStore]:
    """Construct a HybridBodyStore over a fresh RAM + file pair."""
    ram = RamBodyStore()
    fbs = FileBodyStore(tmp_path / "bodies")
    hybrid = HybridBodyStore(ram=ram, disk=fbs)
    await hybrid.start()
    return track_started(hybrid), ram, fbs


def _hashes_for(body: bytes) -> dict[str, BodyHashes]:
    """Build a single-body_ref body_hashes map matching ``body``."""
    import hashlib

    digest = hashlib.sha256(body).hexdigest()
    return {
        "body": BodyHashes(
            body_hash=BodyHash(digest),
            storage_hash=StorageHash(digest),
        )
    }


def _row(
    chain_id,
    *,
    state: str = "queued",
    body_location: str = "ram",
    body_hashes: dict[str, BodyHashes] | None = None,
    body_discarded_at: datetime | None = None,
) -> UploadRow:
    """Build a minimal :class:`UploadRow` for the recovery tests."""
    now = datetime.now(tz=UTC)
    base: dict[str, object] = {
        "chain_id": chain_id,
        "instance_id": "primary",
        "group_id": chain_id,
        "multifile_id": chain_id,
        "send_order": 0,
        "route_name": "r",
        "state": state,
        "body_location": body_location,
        "received_at": now,
        "updated_at": now,
        "endpoint": "e",
        "uid": "u",
        "chain_envelope_json": "{}",
        "idempotency_key": "k",
        "capture_reexecution_active": False,
    }
    if body_hashes is not None:
        base["body_hashes"] = body_hashes
        base["body_size_bytes"] = sum(64 for _ in body_hashes)
    if body_discarded_at is not None:
        base["body_discarded_at"] = body_discarded_at
    return UploadRow.model_validate(base)


@pytest.mark.asyncio
async def test_attempting_reset_to_queued(tmp_path: Path) -> None:
    """Recovery resets every ``attempting`` row to ``queued`` (invariant #7)."""
    store = await _build_store(tmp_path)
    body_store, _, _ = await _build_hybrid_body_store(tmp_path)
    chain_id = uuid4()
    await store.insert(_row(chain_id, state="attempting"))
    await run_recovery(store, body_store)
    fresh = await store.get(chain_id)
    assert fresh is not None
    assert fresh.state == "queued"


@pytest.mark.asyncio
async def test_boot_reconstructs_only_slot_holding_saturation_rows(tmp_path: Path) -> None:
    """Queued/stored persisted rows charge a fresh gate; released states do not."""
    store = await _build_store(tmp_path)
    queued_id = uuid4()
    stored_id = uuid4()
    await store.insert(_row(queued_id, state="queued", body_hashes=_hashes_for(b"q")))
    await store.insert(_row(stored_id, state="stored", body_hashes=_hashes_for(b"s")))
    await store.insert(_row(uuid4(), state="auth_expired", body_hashes=_hashes_for(b"a")))
    await store.insert(_row(uuid4(), state="succeeded", body_hashes=_hashes_for(b"d")))
    saturation = SaturationGate(
        max_in_flight=10,
        max_in_flight_bytes=10_000,
        max_disk_bytes=0,
    )

    await reconcile_saturation(store, saturation)

    assert saturation.in_flight == 2
    assert saturation.in_flight_bytes == 128


@pytest.mark.asyncio
async def test_recovery_quarantines_ram_body_lost(tmp_path: Path) -> None:
    """``body_location='ram'`` row whose RAM bytes vanished marks ``corrupted``.

    The RAM tier is by-design ephemeral across restarts; a row that
    survived in the persistent store with body_location='ram' but no
    corresponding RAM entry is unrecoverable. Recovery quarantines.
    """
    store = await _build_store(tmp_path)
    body_store, _, _ = await _build_hybrid_body_store(tmp_path)
    chain_id = uuid4()
    # Row says body is in RAM but the RAM/file stores are empty.
    await store.insert(_row(chain_id, body_hashes=_hashes_for(b"x")))
    await run_recovery(store, body_store)
    fresh = await store.get(chain_id)
    assert fresh is not None
    assert fresh.state == "corrupted"
    assert fresh.last_error is not None
    assert fresh.last_error.startswith("ram_body_lost_on_restart:")


@pytest.mark.asyncio
async def test_recovery_quarantines_file_body_missing(tmp_path: Path) -> None:
    """``body_location='file'`` row whose disk bytes vanished marks ``corrupted``."""
    store = await _build_store(tmp_path)
    body_store, _, _ = await _build_hybrid_body_store(tmp_path)
    chain_id = uuid4()
    await store.insert(
        _row(chain_id, body_location="file", body_hashes=_hashes_for(b"y")),
    )
    await run_recovery(store, body_store)
    fresh = await store.get(chain_id)
    assert fresh is not None
    assert fresh.state == "corrupted"
    assert fresh.last_error is not None
    assert fresh.last_error.startswith("file_body_missing_on_recovery:")


@pytest.mark.asyncio
async def test_recovery_preserves_row_with_bodies_present(tmp_path: Path) -> None:
    """Body present in the body store → row stays untouched."""
    store = await _build_store(tmp_path)
    body_store, ram, _ = await _build_hybrid_body_store(tmp_path)
    chain_id = uuid4()
    body = b"hello"
    await store.insert(_row(chain_id, body_hashes=_hashes_for(body)))
    await ram.put(chain_id, {"body": body})
    await run_recovery(store, body_store)
    fresh = await store.get(chain_id)
    assert fresh is not None
    assert fresh.state == "queued"  # unchanged


@pytest.mark.asyncio
async def test_recovery_h4_carveout_discarded_body_not_quarantined(
    tmp_path: Path,
) -> None:
    """``body_discarded_at`` rows are intentionally body-less; do NOT quarantine.

    The reaper sets ``body_discarded_at`` when retention elapses for the
    body tier of a terminal-state row; the row's metadata survives
    longer for admin inspection. Recovery must not mistake that for
    corruption.
    """
    store = await _build_store(tmp_path)
    body_store, _, _ = await _build_hybrid_body_store(tmp_path)
    chain_id = uuid4()
    discarded_at = datetime.now(tz=UTC)
    await store.insert(
        _row(
            chain_id,
            state="succeeded",
            body_location="file",
            body_hashes=_hashes_for(b"z"),
            body_discarded_at=discarded_at,
        ),
    )
    await run_recovery(store, body_store)
    fresh = await store.get(chain_id)
    assert fresh is not None
    assert fresh.state == "succeeded"  # NOT corrupted


@pytest.mark.asyncio
async def test_recovery_does_not_requarantine_succeeded_row_with_deleted_body(
    tmp_path: Path,
) -> None:
    """A ``succeeded`` row whose body was deleted on delivery survives recovery.

    Finding R9-PM-3 (adopted aggressor seed). A delivered upload reaches
    ``state='succeeded'`` and the sender deletes its body on success
    (``succeeded_body_seconds=0`` default) WITHOUT stamping
    ``body_discarded_at`` — so the H4 carve-out
    (:func:`test_recovery_h4_carveout_discarded_body_not_quarantined`) does
    NOT fire. Pre-fix, the next restart's body-existence walk found the body
    missing and re-quarantined the row to ``corrupted``, destroying the
    durable success record (confirmed in all three storage modes). The
    terminal-state skip exempts it: a finished row's missing body is expected,
    not a corruption signal.
    """
    store = await _build_store(tmp_path)
    body_store, _, _ = await _build_hybrid_body_store(tmp_path)
    chain_id = uuid4()
    # Delivered: state='succeeded', body already gone (stores empty),
    # body_discarded_at NOT stamped (matches sender delete-on-success).
    await store.insert(_row(chain_id, state="succeeded", body_hashes=_hashes_for(b"delivered")))

    await run_recovery(store, body_store)

    fresh = await store.get(chain_id)
    assert fresh is not None
    assert fresh.state == "succeeded", (
        f"recovery re-quarantined a delivered succeeded row to {fresh.state!r} — "
        "a terminal row's missing body is expected (body deleted on success), "
        "not a corruption signal"
    )


@pytest.mark.asyncio
async def test_recovery_still_quarantines_deliverable_ram_lost_row(tmp_path: Path) -> None:
    """Scope guard for R9-PM-3: a QUEUED RAM-lost row must STILL quarantine.

    The terminal-skip must exempt ONLY terminal rows. A still-deliverable
    (``queued``) row whose RAM body vanished is genuinely unrecoverable and
    must still quarantine (pins the fix's scope so it does not over-correct
    into skipping every missing-body row).
    """
    store = await _build_store(tmp_path)
    body_store, _, _ = await _build_hybrid_body_store(tmp_path)
    chain_id = uuid4()
    await store.insert(_row(chain_id, state="queued", body_hashes=_hashes_for(b"deliverable")))

    await run_recovery(store, body_store)

    fresh = await store.get(chain_id)
    assert fresh is not None
    assert fresh.state == "corrupted", (
        "a deliverable (queued) RAM-lost row must still quarantine — the fix must "
        "exempt only terminal-state rows, not all missing-body rows"
    )
