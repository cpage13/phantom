"""Adopted regression test for aggressor Round-5 finding V3 + the defender's cap.

V3 observed that retention was purely TIME-based: ``max_in_flight`` bounds only
in-flight rows, so terminal rows could grow the ``uploads`` table without bound
between time-based reaps (notably the forever-retained ``stored``/``auth_expired``
metadata). The defender's contract decision (R6) added a row-count backstop the
reaper enforces (oldest-DONE-first eviction over the cap, never dropping an in-flight
or still-deliverable ``auth_expired`` row). The default was later raised off the
historical unbounded value to ``retention.max_rows = 100_000`` so every instance is
row-bounded out of the box; set ``-1`` to opt back into time-only (unbounded).

This test pins both halves of that contract:

* ``test_default_retention_has_a_generous_row_backstop``: with the DEFAULT config
  the table is bounded by a generous ``max_rows`` backstop (100_000); terminal rows
  still accumulate past ``max_in_flight`` at normal scale. This is the canary: it
  documents the default row cap and guards against an accidental flip.
* ``test_max_rows_cap_evicts_oldest_terminal_first`` — with ``max_rows`` set the
  store's backstop evicts the OLDEST terminal rows until at or below the cap,
  newest terminal rows survive, and the returned chain_ids match the evicted set.
* ``test_max_rows_cap_never_evicts_in_flight_or_auth_expired`` — DURABILITY: a
  queued (in-flight) row and an ``auth_expired`` (still-deliverable) row are NEVER
  evicted even when the table is over the cap; only fully-terminal rows go. The
  cap is left unmet rather than dropping an undelivered upload (invariant #1).
* ``test_reaper_store_call_deletes_overdue_terminal_rows`` — EXPLORATORY: the
  time-based store primitive the reaper depends on. (The full Reaper worker loop
  is covered by tests/e2e/test_e2e_17_reaper_retention.)
* ``test_reap_cleans_all_associated_state`` — EXPLORATORY: reaping a body-bearing
  terminal row cleans uploads row + body bytes + idempotency_index + RAM
  accounting — no leak in any of the four.

Provenance: these tests used the deleted ``compose_and_run`` (R-2) only as a
convenient store + body-store factory — the assertions are pure
:class:`SqliteUploadStore` / :class:`HybridBodyStore` primitive behavior, not
composition wiring. They now construct the store + body-store halves DIRECTLY
(the established store-test pattern — see ``test_sqlite_store.py`` /
``test_invariant_audit.py``), NOT the full production lifespan: the live
lifespan also spawns the Reaper on its own interval, which would reap the
overdue terminal rows these tests insert out from under them. Driving a single
unit-under-test is not a "parallel composition path"; it is the correct home for
store-primitive assertions. Composition wiring is tested in
``test_composition.py`` / ``test_composition_root.py``.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import aiosqlite
import pytest
from phantom.config.settings import SaturationCfg, Settings
from phantom.models.upload import UploadRow
from phantom.observability.metrics import MetricsRegistry
from phantom.storage.file_body_store import FileBodyStore
from phantom.storage.hybrid_body_store import HybridBodyStore
from phantom.storage.ram_body_store import RamBodyStore
from phantom.storage.sqlite_store import SqliteUploadStore

pytestmark = [pytest.mark.asyncio]

_BODY = b"z" * 2048
_MAX_IN_FLIGHT = 8


def _row(
    chain_id: UUID,
    *,
    updated_at: datetime,
    state: str = "failed",
    body_location: str = "ram",
) -> UploadRow:
    """Build an UploadRow in ``state`` with body_hashes for ``_BODY``."""
    digest = hashlib.sha256(_BODY).hexdigest()
    return UploadRow.model_validate(
        {
            "chain_id": chain_id,
            "instance_id": "primary",
            "group_id": chain_id,
            "multifile_id": chain_id,
            "send_order": 0,
            "route_name": "r",
            "state": state,
            "body_location": body_location,
            "received_at": updated_at,
            "updated_at": updated_at,
            "endpoint": "e",
            "uid": "u",
            "chain_envelope_json": "{}",
            "idempotency_key": f"idem-{chain_id}",
            "chain_id_at_ingress": str(chain_id),
            "capture_reexecution_active": False,
            "body_hashes": {"body": {"body_hash": digest, "storage_hash": digest}},
            "body_size_bytes": len(_BODY),
        },
    )


async def _count_rows(store: SqliteUploadStore) -> int:
    n = 0
    async for _ in store.iter_rows():
        n += 1
    return n


async def _states(store: SqliteUploadStore) -> dict[str, int]:
    counts: dict[str, int] = {}
    async for row in store.iter_rows():
        counts[row.state] = counts.get(row.state, 0) + 1
    return counts


async def _count_idx(db_path: Path) -> int:
    async with (
        aiosqlite.connect(str(db_path)) as conn,
        conn.execute("SELECT COUNT(*) FROM idempotency_index") as cur,
    ):
        row = await cur.fetchone()
    return int(row[0]) if row else 0


@dataclass
class _Stack:
    """The store + body-store under test, built directly (no lifespan)."""

    store: SqliteUploadStore
    body_store: HybridBodyStore
    db_path: Path


@asynccontextmanager
async def _hybrid_stack(data_root: Path) -> AsyncIterator[_Stack]:
    """Construct + start a persistent store + a hybrid body store directly.

    Mirrors how every other store-primitive unit test instantiates the
    unit under test (``SqliteUploadStore`` + body halves) — without the
    production lifespan's competing Reaper / auditor workers, which would
    race the overdue terminal rows these tests insert.
    """
    data_root.mkdir(parents=True, exist_ok=True)
    db_path = data_root / "uploads.db"
    registry = MetricsRegistry()
    store = SqliteUploadStore(str(db_path), metrics_registry=registry)
    ram = RamBodyStore()
    file_bs = FileBodyStore(data_root / "bodies", shard_prefix_chars=2)
    await store.start()
    await ram.start()
    await file_bs.start()
    body_store = HybridBodyStore(ram=ram, disk=file_bs)
    await body_store.start()
    try:
        yield _Stack(store=store, body_store=body_store, db_path=db_path)
    finally:
        await body_store.stop()
        await store.stop()


def _capped_settings() -> Settings:
    """Top-level Settings with the saturation cap the canary asserts against.

    The canary reads the retention DEFAULT (``max_rows == 100_000``) and the
    ``max_in_flight`` cap; the eviction/reap tests drive the store
    primitive directly and never touch Settings, so a single saturation
    overlay is all this helper needs.
    """
    return Settings(
        saturation=SaturationCfg(  # type: ignore[call-arg]
            max_in_flight=_MAX_IN_FLIGHT,
            max_in_flight_bytes=10_000_000,
            max_disk_bytes=10_000_000_000,
            large_body_threshold_bytes=1_000_000,
            max_large_in_flight=4,
        )
    )


async def test_default_retention_has_a_generous_row_backstop(tmp_path: Path) -> None:
    """CANARY: the DEFAULT config bounds the table with a generous max_rows backstop.

    The default ``max_rows`` is 100_000: an instance is row-bounded out of the box,
    yet the cap is generous enough that terminal rows still accumulate past
    max_in_flight at normal scale (the cap only bites past 100_000). If this value
    flips, the default cap changed (deliberate decision required).
    """
    settings = _capped_settings()
    assert settings.retention.max_rows == 100_000, (
        "default row backstop changed (deliberate decision required)"
    )
    now = datetime.now(tz=UTC)
    rows = 120
    async with _hybrid_stack(tmp_path / "d") as st:
        for _ in range(rows):
            await st.store.insert(_row(uuid4(), updated_at=now))
        count = await _count_rows(st.store)
    assert count == rows
    assert count > _MAX_IN_FLIGHT * 10, (
        f"the {rows} terminal rows (<< the 100_000 default cap) all persist and "
        f"accumulate past max_in_flight={_MAX_IN_FLIGHT}; got {count}."
    )


async def test_max_rows_cap_evicts_oldest_terminal_first(tmp_path: Path) -> None:
    """The opt-in backstop evicts oldest-terminal-first down to the cap."""
    cap = 10
    base = datetime.now(tz=UTC) - timedelta(hours=1)
    # 30 terminal rows with strictly increasing updated_at (oldest first).
    ordered: list[UUID] = []
    async with _hybrid_stack(tmp_path / "d") as st:
        for i in range(30):
            cid = uuid4()
            ordered.append(cid)
            await st.store.insert(_row(cid, updated_at=base + timedelta(seconds=i)))
        assert await _count_rows(st.store) == 30

        evicted = await st.store.evict_terminal_over_limit(cap)

        # 30 - 10 = 20 evicted, and they are the 20 OLDEST.
        assert len(evicted) == 20
        assert {entry.chain_id for entry in evicted} == set(ordered[:20])
        assert await _count_rows(st.store) == cap
        # The 10 newest survive.
        for cid in ordered[20:]:
            assert await st.store.get(cid) is not None
        # Idempotent once at/below the cap.
        assert await st.store.evict_terminal_over_limit(cap) == []


async def test_max_rows_cap_never_evicts_in_flight_or_auth_expired(tmp_path: Path) -> None:
    """DURABILITY: in-flight + auth_expired rows are never evicted by the cap.

    The cap is intentionally tighter than the number of non-evictable rows, so
    honoring it fully would require dropping an undelivered upload. The backstop
    must refuse — it deletes only the eligible terminal rows and leaves the cap
    unmet rather than losing buffered data (invariant #1).
    """
    cap = 1
    base = datetime.now(tz=UTC) - timedelta(hours=1)
    queued_id = uuid4()
    attempting_id = uuid4()
    auth_expired_id = uuid4()
    terminal_ids = [uuid4() for _ in range(3)]
    async with _hybrid_stack(tmp_path / "d") as st:
        # 3 non-evictable rows (the OLDEST, so a naive oldest-first eviction
        # would target them first) + 3 evictable terminal rows.
        await st.store.insert(_row(queued_id, updated_at=base, state="queued"))
        await st.store.insert(
            _row(attempting_id, updated_at=base + timedelta(seconds=1), state="attempting")
        )
        await st.store.insert(
            _row(auth_expired_id, updated_at=base + timedelta(seconds=2), state="auth_expired")
        )
        for i, cid in enumerate(terminal_ids):
            await st.store.insert(
                _row(cid, updated_at=base + timedelta(seconds=10 + i), state="failed")
            )
        assert await _count_rows(st.store) == 6

        evicted = await st.store.evict_terminal_over_limit(cap)

        # Overage is 5, but only 3 rows are evictable → exactly 3 evicted.
        assert len(evicted) == 3
        assert {entry.chain_id for entry in evicted} == set(terminal_ids)
        # The three non-evictable rows ALL survive — durability over the cap.
        assert await st.store.get(queued_id) is not None
        assert await st.store.get(attempting_id) is not None
        assert await st.store.get(auth_expired_id) is not None
        # Cap left unmet (3 rows > cap of 1) — the safe direction.
        assert await _count_rows(st.store) == 3
        assert await _states(st.store) == {
            "queued": 1,
            "attempting": 1,
            "auth_expired": 1,
        }


async def test_reaper_store_call_deletes_overdue_terminal_rows(tmp_path: Path) -> None:
    """EXPLORATORY: the reaper's time-based store primitive deletes overdue rows."""
    past = datetime.now(tz=UTC) - timedelta(seconds=10)
    async with _hybrid_stack(tmp_path / "d") as st:
        for _ in range(20):
            await st.store.insert(_row(uuid4(), updated_at=past))
        assert await _count_rows(st.store) == 20
        deleted = await st.store.delete_terminal_older_than("failed", datetime.now(tz=UTC))
        assert len(deleted) == 20
        # R8-4: per-row accounting rides along for the reaper's release.
        assert all(entry.state == "failed" for entry in deleted)
        assert await _count_rows(st.store) == 0


async def test_reap_cleans_all_associated_state(tmp_path: Path) -> None:
    """EXPLORATORY: reaping a row cleans row + body + idempotency_index + RAM accounting."""
    async with _hybrid_stack(tmp_path / "d") as st:
        cid = uuid4()
        past = datetime.now(tz=UTC) - timedelta(seconds=10)
        await st.store.insert(_row(cid, updated_at=past))
        async with aiosqlite.connect(str(st.db_path)) as conn:
            await conn.execute(
                "INSERT OR IGNORE INTO idempotency_index "
                "(chain_id_at_ingress, chain_id) VALUES (?, ?)",
                (str(cid), str(cid)),
            )
            await conn.commit()
        await st.body_store.put(cid, {"body": _BODY})

        assert await st.body_store._ram.total_bytes() == len(_BODY)  # type: ignore[attr-defined]
        assert await _count_idx(st.db_path) == 1

        # Reap exactly as Reaper._sweep_instance does.
        cutoff = datetime.now(tz=UTC)
        for row in await st.store.list_terminal_older_than("failed", cutoff):
            await st.body_store.delete(row.chain_id)
            await st.store.discard_body_and_zero_accounting(row.chain_id, expected_state="failed")
        await st.store.delete_terminal_older_than("failed", cutoff)
        await st.store.cleanup_idempotency_index()

        assert await st.store.get(cid) is None
        assert await st.body_store.has_body_ref(cid, "body") is False
        assert await _count_idx(st.db_path) == 0
        assert await st.body_store._ram.total_bytes() == 0  # type: ignore[attr-defined]
