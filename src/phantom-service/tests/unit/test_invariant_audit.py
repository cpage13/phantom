"""Unit tests for :class:`InvariantAuditor` (plan § 4.2.3).

Tests cover:

* Auditor registers ``invariant_violation_total`` and
  ``invariant_audit_runs_total`` on the registry.
* Happy-path row walk leaves all counter labels at zero.
* Row with ``body_location='file'`` but no body on disk bumps
  ``missing_body_file``.
* Row with ``body_location='ram'`` but no body in RAM bumps
  ``missing_body_in_ram``.
* H4 carve-out: row with ``body_discarded_at`` set is SKIPPED — no
  invariant bump.
* ConfigInvariantError raised by ``check_retention_floor`` when retention
  body_seconds exceeds metadata_seconds.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path
from uuid import UUID

import pytest
from phantom.config.settings import RetentionCfg, Settings
from phantom.models.upload import UploadRow
from phantom.observability.metrics import MetricsRegistry
from phantom.runtime.startup_checks import ConfigInvariantError, check_retention_floor
from phantom.storage.file_body_store import FileBodyStore
from phantom.storage.ram_body_store import RamBodyStore
from phantom.storage.sqlite_store import SqliteUploadStore
from phantom.workers.invariant_audit import InvariantAuditor

from .conftest import make_snapshot, snapshot_thunk, track_started


@pytest.fixture
async def auditor_stack(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> dict[str, object]:
    registry = MetricsRegistry()
    store = track_started(
        SqliteUploadStore(str(tmp_path / "uploads.db"), metrics_registry=registry)
    )
    await store.start()
    ram = track_started(RamBodyStore())
    await ram.start()
    file_bs = track_started(FileBodyStore(tmp_path / "bodies", shard_prefix_chars=2))
    await file_bs.start()
    auditor = InvariantAuditor(
        store=store,
        body_store=ram,  # default RAM-only for happy-path test cases
        current_settings=snapshot_thunk(make_snapshot()),
        metrics_registry=registry,
    )
    return {
        "store": store,
        "ram": ram,
        "file": file_bs,
        "registry": registry,
        "auditor": auditor,
        "make_row": make_upload_row,
    }


def _violations(registry: MetricsRegistry) -> dict[str, int]:
    return dict(registry.counters["invariant_violation_total"].snapshot())


@pytest.mark.asyncio
async def test_auditor_registers_canonical_metrics(auditor_stack: dict[str, object]) -> None:
    registry = auditor_stack["registry"]
    assert isinstance(registry, MetricsRegistry)
    assert "invariant_violation_total" in registry.counters
    assert "invariant_audit_runs_total" in registry.counters


@pytest.mark.asyncio
async def test_auditor_happy_path_no_violations(auditor_stack: dict[str, object]) -> None:
    """A row whose body bytes are present in the body store yields no violation."""
    store = auditor_stack["store"]
    ram = auditor_stack["ram"]
    registry = auditor_stack["registry"]
    auditor = auditor_stack["auditor"]
    make_row = auditor_stack["make_row"]
    assert isinstance(store, SqliteUploadStore)
    assert isinstance(ram, RamBodyStore)
    assert isinstance(registry, MetricsRegistry)
    assert isinstance(auditor, InvariantAuditor)
    assert callable(make_row)

    row = make_row(body_location="ram")
    await store.insert(row)
    # Body stored in RAM for every declared body_hash.
    body_refs = dict.fromkeys(row.body_hashes, b"data")
    await ram.put(row.chain_id, body_refs)

    await auditor._sweep_once()  # type: ignore[reportPrivateUsage]
    assert _violations(registry) == {"": 0}


@pytest.mark.asyncio
async def test_auditor_missing_body_in_ram_bumps_counter(
    auditor_stack: dict[str, object],
) -> None:
    from phantom.models.upload import BodyHash, BodyHashes, StorageHash

    store = auditor_stack["store"]
    registry = auditor_stack["registry"]
    auditor = auditor_stack["auditor"]
    make_row = auditor_stack["make_row"]
    assert isinstance(store, SqliteUploadStore)
    assert isinstance(registry, MetricsRegistry)
    assert isinstance(auditor, InvariantAuditor)
    assert callable(make_row)

    row = make_row(
        body_location="ram",
        body_hashes={
            "a": BodyHashes(body_hash=BodyHash("bh"), storage_hash=StorageHash("sh")),
        },
    )
    await store.insert(row)
    # Do NOT put body in RAM — invariant should fire.
    await auditor._sweep_once()  # type: ignore[reportPrivateUsage]
    snap = _violations(registry)
    assert snap.get("missing_body_in_ram", 0) >= 1


@pytest.mark.asyncio
async def test_auditor_missing_body_file_bumps_counter(
    auditor_stack: dict[str, object],
) -> None:
    """Row claiming body_location='file' but with no file on disk fires missing_body_file."""
    store = auditor_stack["store"]
    file_bs = auditor_stack["file"]
    registry = auditor_stack["registry"]
    make_row = auditor_stack["make_row"]
    assert isinstance(store, SqliteUploadStore)
    assert isinstance(file_bs, FileBodyStore)
    assert isinstance(registry, MetricsRegistry)
    assert callable(make_row)

    # Build a fresh auditor pointing at the file store (so file
    # absence is detectable).
    auditor = InvariantAuditor(
        store=store,
        body_store=file_bs,
        current_settings=snapshot_thunk(make_snapshot()),
        metrics_registry=registry,
    )

    from phantom.models.upload import BodyHash, BodyHashes, StorageHash

    row = make_row(
        body_location="file",
        body_hashes={
            "a": BodyHashes(body_hash=BodyHash("bh"), storage_hash=StorageHash("sh")),
        },
    )
    await store.insert(row)
    # No file on disk — invariant #1 violation.
    await auditor._sweep_once()  # type: ignore[reportPrivateUsage]
    snap = _violations(registry)
    assert snap.get("missing_body_file", 0) >= 1


@pytest.mark.asyncio
async def test_auditor_h4_carve_out_skips_discarded_rows(
    auditor_stack: dict[str, object],
) -> None:
    """A row with body_discarded_at set is SKIPPED — no invariant bump."""
    from datetime import UTC, datetime

    store = auditor_stack["store"]
    registry = auditor_stack["registry"]
    auditor = auditor_stack["auditor"]
    make_row = auditor_stack["make_row"]
    assert isinstance(store, SqliteUploadStore)
    assert isinstance(registry, MetricsRegistry)
    assert isinstance(auditor, InvariantAuditor)
    assert callable(make_row)

    row = make_row(body_location="ram", body_discarded_at=datetime.now(tz=UTC))
    await store.insert(row)
    # No body in RAM — without the H4 carve-out the auditor would bump.
    await auditor._sweep_once()  # type: ignore[reportPrivateUsage]
    snap = _violations(registry)
    # Counter still at zero (the "" bucket is the default).
    assert snap == {"": 0}


@pytest.mark.asyncio
async def test_auditor_terminal_carve_out_skips_bodyless_succeeded_row(
    auditor_stack: dict[str, object],
) -> None:
    """A bodyless ``succeeded`` row is SKIPPED — no spurious violation (R9-PM-3).

    A delivered upload reaches ``succeeded`` and its body is deleted on
    success WITHOUT stamping ``body_discarded_at`` (so the H4 carve-out does
    not fire). Without the terminal carve-out the auditor would bump
    ``missing_body_in_ram`` + ``body_hash_set_mismatch`` on every sweep —
    operational noise that masks real corruption. The terminal-state skip
    keeps the auditor quiet on finished rows.
    """
    from phantom.models.upload import BodyHash, BodyHashes, StorageHash

    store = auditor_stack["store"]
    registry = auditor_stack["registry"]
    auditor = auditor_stack["auditor"]
    make_row = auditor_stack["make_row"]
    assert isinstance(store, SqliteUploadStore)
    assert isinstance(registry, MetricsRegistry)
    assert isinstance(auditor, InvariantAuditor)
    assert callable(make_row)

    # Delivered + reaped: succeeded, RAM body gone, body_discarded_at unset.
    row = make_row(
        state="succeeded",
        body_location="ram",
        body_hashes={
            "a": BodyHashes(body_hash=BodyHash("bh"), storage_hash=StorageHash("sh")),
        },
    )
    await store.insert(row)
    await auditor._sweep_once()  # type: ignore[reportPrivateUsage]
    snap = _violations(registry)
    assert snap == {"": 0}, f"auditor fired spurious violations on a finished succeeded row: {snap}"


# --- ConfigInvariantError (plan § 4.2.4) --------------------------------


def test_check_invariants_passes_when_body_le_metadata() -> None:
    settings = Settings()
    # Default retention has body <= metadata for every state — passes.
    check_retention_floor(settings)


def test_check_invariants_raises_when_body_exceeds_metadata() -> None:
    """Bodies-outlive-rows configuration triggers ConfigInvariantError."""
    settings = Settings()
    # Carve the retention block: succeeded body 10s, metadata 5s.
    settings = settings.model_copy(
        update={
            "retention": RetentionCfg(
                succeeded_metadata_seconds=5,
                succeeded_body_seconds=10,
                failed_metadata_seconds=10,
                failed_body_seconds=10,
                auth_expired_metadata_seconds=-1,
                auth_expired_body_seconds=-1,
                stored_metadata_seconds=-1,
                stored_body_seconds=-1,
            )
        }
    )
    with pytest.raises(ConfigInvariantError, match="succeeded"):
        check_retention_floor(settings)


def test_check_invariants_rejects_forever_body_with_finite_metadata() -> None:
    """body_seconds=-1 (forever) with finite metadata fails the check."""
    settings = Settings()
    settings = settings.model_copy(
        update={
            "retention": RetentionCfg(
                succeeded_metadata_seconds=60,
                succeeded_body_seconds=-1,  # forever — invariant violation
                failed_metadata_seconds=10,
                failed_body_seconds=10,
                auth_expired_metadata_seconds=-1,
                auth_expired_body_seconds=-1,
                stored_metadata_seconds=-1,
                stored_body_seconds=-1,
            )
        }
    )
    with pytest.raises(ConfigInvariantError, match="forever"):
        check_retention_floor(settings)


# How many rows the concurrency witness below drives through one sweep. Any
# number above one proves the fan-out; four keeps the barrier small and stays
# well inside the auditor's own per-batch ceiling.
_CONCURRENT_ROWS = 4

# The witness's own guard. A sweep that checks rows one at a time never
# satisfies the barrier, and a hung test is a wedged suite rather than a
# failure, so the guard turns the hang into a visible TimeoutError.
_SWEEP_GUARD_SECONDS = 5.0


class _BarrieredRamBodyStore(RamBodyStore):
    """A RAM body store whose presence check waits for company.

    Every ``has_body_ref`` blocks on a barrier sized to the number of rows in
    the sweep, so the sweep can only finish if that many checks are in flight
    at once. It stands in for the real cost: the file store's check is an
    ``aiofiles`` hop measured at 135 us against 4.7 us synchronous, so the
    expense is round-trip latency, which overlapping removes and serialising
    multiplies by the row count.
    """

    def __init__(self, parties: int) -> None:
        """Build a store whose checks release only once ``parties`` are waiting."""
        super().__init__()
        self._barrier = asyncio.Barrier(parties)

    async def has_body_ref(self, chain_id: UUID, name: str) -> bool:
        """Wait for the other in-flight checks, then answer normally."""
        await self._barrier.wait()
        return await super().has_body_ref(chain_id, name)


@pytest.mark.asyncio
async def test_a_sweep_checks_body_refs_concurrently(
    tmp_path: Path,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """S8-7: one sweep overlaps its body-store round trips instead of serialising.

    Objective: the walk covers a population unbounded BY DESIGN, because
    ``auth_expired`` is deliberately not terminal and nothing trims it. One
    round trip per declared body_ref per row, taken strictly in turn, is about
    two minutes of event-loop time every 300 s at 864k parked rows, all of it
    on the loop every other worker shares.

    Expected outcome: the sweep completes. Each row's presence check blocks
    until every other row's check has also started, which a sweep that waits
    for each answer before issuing the next can never satisfy.
    """
    registry = MetricsRegistry()
    store = track_started(
        SqliteUploadStore(str(tmp_path / "uploads.db"), metrics_registry=registry)
    )
    await store.start()
    body_store = track_started(_BarrieredRamBodyStore(_CONCURRENT_ROWS))
    await body_store.start()
    auditor = InvariantAuditor(
        store=store,
        body_store=body_store,
        current_settings=snapshot_thunk(make_snapshot()),
        metrics_registry=registry,
    )
    from phantom.models.upload import BodyHash, BodyHashes, StorageHash

    for _ in range(_CONCURRENT_ROWS):
        row = make_upload_row(
            body_location="ram",
            body_hashes={
                "a": BodyHashes(body_hash=BodyHash("bh"), storage_hash=StorageHash("sh")),
            },
        )
        await store.insert(row)
        await body_store.put(row.chain_id, dict.fromkeys(row.body_hashes, b"data"))

    async with asyncio.timeout(_SWEEP_GUARD_SECONDS):
        await auditor._sweep_once()  # type: ignore[reportPrivateUsage]
    assert _violations(registry) == {"": 0}
