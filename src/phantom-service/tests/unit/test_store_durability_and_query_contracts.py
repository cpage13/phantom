"""Store-level durability, lock-cost and query contracts.

One file for the SQLite-store findings that are not about read-snapshot
isolation (which lives in ``test_store_read_snapshot_isolation.py``):

* S2-4: ``PRAGMA journal_mode=WAL`` reports the RESULTING mode instead of
  raising, so a data_dir whose filesystem cannot support WAL left the
  store booting healthy on a rollback journal.
* S2-5: the retention count cap ran a full-table ``COUNT(*)`` inside the
  single write lock on every reaper sweep, stalling admission.
* SP-6: ``vacuum`` was the one writer that skipped the rollback-on-error
  wrapper, so a full-disk VACUUM could wedge every writer.
* S2-9: ``list_uploads`` indexed the last row of a page it had truncated
  to ``limit``, so ``limit=0`` raised ``IndexError``.
* D4: JSON lookups compared a JSON leaf to a bound TEXT parameter, so a
  numeric identifier never matched.
* D6: the capture-name and local-uuid lookups built escaped quoted
  JSON-path labels, which SQLite below 3.50 cannot parse.
* SP-7: the paginated listing sorted on an unindexed column.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import AsyncIterator, Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import aiosqlite
import pytest
from phantom.models.upload import CapturedStepValues, CapturedValues, UploadRow
from phantom.storage.interface import InsertClaimOutcome
from phantom.storage.sqlite_store import SqliteUploadStore

# Bound on a call that must NOT wait for the store's write lock. Generous
# against CI jitter; the failure mode it guards (the call queueing behind a
# lock the test is holding) never completes at all.
_LOCK_FREE_TIMEOUT_SECONDS = 2.0

# A retention cap far above anything these tests seed, so the count cap is
# exercised on its UNDER-CAP arm, which is the arm the reaper takes on every
# sweep in a healthy deployment.
_CAP_WELL_ABOVE_SEEDED_ROWS = 10_000

# The escape sequence at the heart of the version-skew class: a quoted JSON
# path label carrying an escaped double quote. SQLite below ~3.50 cannot parse
# it, and no query this store issues may contain one.
_ESCAPED_QUOTE_IN_A_JSON_PATH_LABEL = '\\"'


@pytest.fixture
async def file_store(tmp_path: Path) -> AsyncIterator[SqliteUploadStore]:
    """A started FILE-backed store, the production shape."""
    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await store.start()
    yield store
    await store.stop()


def _record_statements(
    monkeypatch: pytest.MonkeyPatch, issued: list[tuple[str, list[Any]]]
) -> None:
    """Record every statement and bound parameter list issued by any connection."""
    real_execute = aiosqlite.Connection.execute

    def recording(
        self: aiosqlite.Connection, sql: str, parameters: Iterable[Any] | None = None
    ) -> aiosqlite.Cursor:
        issued.append((sql, list(parameters) if parameters is not None else []))
        return real_execute(self, sql, parameters)

    monkeypatch.setattr(aiosqlite.Connection, "execute", recording)


# ---------------------------------------------------------------------
# S2-4 - the journal_mode pragma's result is the assertion.
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_start_refuses_a_store_whose_journal_mode_did_not_become_wal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Objective: a non-WAL store must fail to start, not boot healthy (S2-4).

    Simulates the filesystem that cannot support WAL (NFS, a 9p container
    volume) the only way the defect actually presents: the pragma does not
    raise, it answers with the mode the database ended up in.

    Expected outcome: ``start()`` raises ``RuntimeError`` naming
    ``journal_mode``. Before the fix this pragma was the one whose result was
    discarded, so the store applied its schema, passed its other four pragma
    assertions and reported healthy while running on a rollback journal, where
    the default ``synchronous=NORMAL`` is no longer corruption-safe and the
    reader/writer split no longer holds.
    """
    real_execute = aiosqlite.Connection.execute

    def journal_mode_never_switches(
        self: aiosqlite.Connection, sql: str, parameters: Iterable[Any] | None = None
    ) -> aiosqlite.Cursor:
        if "journal_mode" in sql.lower():
            return real_execute(self, "SELECT 'delete' AS journal_mode", None)
        return real_execute(self, sql, parameters)

    monkeypatch.setattr(aiosqlite.Connection, "execute", journal_mode_never_switches)

    store = SqliteUploadStore(str(tmp_path / "uploads.db"))
    with pytest.raises(RuntimeError, match="journal_mode"):
        await store.start()
    await store.stop()


@pytest.mark.asyncio
async def test_in_memory_store_starts_despite_reporting_journal_mode_memory(
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Objective: the WAL assertion must exempt ``:memory:`` (S2-4).

    An in-memory database has no journal file, so SQLite answers ``memory``
    to the WAL pragma and there is nothing to assert.

    Expected outcome: the store starts and round-trips a row. This pins the
    exemption so a future tightening of the assertion cannot take the whole
    unit suite down with it.
    """
    store = SqliteUploadStore(":memory:")
    await store.start()
    try:
        row = make_upload_row()
        await store.insert(row)
        assert await store.get(row.chain_id) is not None
    finally:
        await store.stop()


# ---------------------------------------------------------------------
# S2-5 - the count cap must not price a read as a write.
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_under_cap_eviction_does_not_wait_for_the_write_lock(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Objective: deciding there is nothing to evict must not block writers (S2-5).

    Holds the store's write lock, which is what admission, the sender pool,
    the persist controller and the reaper all contend for, and asks the count
    cap for its answer on a table far below the cap.

    Expected outcome: an empty list, returned promptly. Before the fix the
    whole sequence including the full-table ``COUNT(*)`` ran inside that lock,
    so this call could not even begin; in production it meant every writer
    stalled behind a six-figure-row scan once per reaper interval, which
    ``busy_timeout`` cannot absorb because the contention is Phantom's own
    asyncio lock rather than SQLITE_BUSY.
    """
    for _ in range(5):
        await file_store.insert(make_upload_row(state="succeeded"))

    async with file_store._write_lock:
        evicted = await asyncio.wait_for(
            file_store.evict_terminal_over_limit(_CAP_WELL_ABOVE_SEEDED_ROWS),
            timeout=_LOCK_FREE_TIMEOUT_SECONDS,
        )

    assert evicted == []


@pytest.mark.asyncio
async def test_over_cap_eviction_still_deletes_oldest_terminal_rows(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Objective: taking the count off the lock must not change what is evicted (S2-5).

    Seeds four terminal rows with distinct ``updated_at`` stamps plus one
    queued row, and caps the table below its size.

    Expected outcome: the oldest terminal rows are evicted down to the cap and
    the queued row is never touched, exactly as before the change.
    """
    base = datetime.now(tz=UTC)
    terminal = [
        make_upload_row(state="succeeded", updated_at=base.replace(microsecond=index))
        for index in range(4)
    ]
    for row in terminal:
        await file_store.insert(row)
    in_flight = make_upload_row(state="queued")
    await file_store.insert(in_flight)

    evicted = await file_store.evict_terminal_over_limit(3)

    assert {entry.chain_id for entry in evicted} == {terminal[0].chain_id, terminal[1].chain_id}
    assert await file_store.get(in_flight.chain_id) is not None


# ---------------------------------------------------------------------
# SP-6 - a failed VACUUM must not wedge the writer.
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_failed_vacuum_leaves_the_writer_usable(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Objective: a full-disk VACUUM must roll back, not wedge every writer (SP-6).

    VACUUM needs free space of roughly the database's own size, so it is the
    writer most likely to fail on the nearly-full SD card that motivated
    ``auto_vacuum=NONE``. This drives that failure with a transaction already
    open on the shared writer connection, which is the state a genuine
    mid-flight failure leaves, and then runs the admission write.

    Expected outcome: ``vacuum`` propagates the disk-full error, and the next
    admission commits. Admission is the writer that proves it, because it is
    the one that issues an explicit ``BEGIN``: before the fix ``vacuum`` took
    the raw write lock instead of the rollback-on-error wrapper every other
    writer uses, so the open transaction survived the failure and admission's
    ``BEGIN`` raised "cannot start a transaction within a transaction". Every
    subsequent admission then did the same, so the service kept 500-ing new
    uploads until restart.
    """
    seeded = make_upload_row()
    await file_store.insert(seeded)

    real_execute = aiosqlite.Connection.execute

    def vacuum_fails_with_a_transaction_open(
        self: aiosqlite.Connection, sql: str, parameters: Iterable[Any] | None = None
    ) -> Any:
        if sql.strip().upper().startswith("VACUUM"):

            async def _fail() -> None:
                # A real DML statement first: Python's sqlite3 opens the
                # implicit transaction at the first DML, so this reproduces
                # the open-transaction state a failure mid-VACUUM leaves.
                await real_execute(self, "UPDATE uploads SET attempts = attempts WHERE 0", None)
                raise sqlite3.OperationalError("database or disk is full")

            return _fail()
        return real_execute(self, sql, parameters)

    monkeypatch.setattr(aiosqlite.Connection, "execute", vacuum_fails_with_a_transaction_open)
    with pytest.raises(sqlite3.OperationalError, match="disk is full"):
        await file_store.vacuum()
    monkeypatch.undo()

    admitted = make_upload_row()
    outcome = await file_store.insert_with_idempotency_claim(admitted, "ingress-key-after-vacuum")
    assert outcome is InsertClaimOutcome.INSERTED
    assert await file_store.get(admitted.chain_id) is not None


# ---------------------------------------------------------------------
# S2-9 - a page of zero rows is a caller error, not an IndexError.
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_uploads_rejects_a_limit_below_one(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Objective: ``limit=0`` must raise a stated precondition, not IndexError (S2-9).

    Seeds enough rows that the page would have been truncated, then asks for
    a page of zero.

    Expected outcome: ``ValueError`` naming the limit. Before the fix the
    method built its resume cursor from ``rows[-1]`` on a list it had just
    truncated to ``limit``, so a zero limit raised ``IndexError`` out of the
    store, which reads as a store bug rather than as the caller precondition
    the Protocol never stated.
    """
    for _ in range(3):
        await file_store.insert(make_upload_row())

    with pytest.raises(ValueError, match="limit must be at least"):
        await file_store.list_uploads(limit=0)


# ---------------------------------------------------------------------
# D4 - a JSON leaf is matched by its text rendering, not its storage class.
# ---------------------------------------------------------------------


def _captured_with_file_id(file_id: object) -> CapturedValues:
    """Captured values whose identifier is whatever JSON type ``file_id`` is."""
    now = datetime.now(tz=UTC)
    return CapturedValues(
        steps={
            "create_file": CapturedStepValues(
                values={"file_information": {"id": file_id}},
                captured_at=now,
                expires_at={"file_information": None},
            )
        }
    )


@pytest.mark.asyncio
async def test_captured_value_lookup_matches_a_numeric_identifier(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Objective: a JSON number identifier must be findable by its digits (D4).

    Seeds a row whose upstream returned ``{"id": 918273}`` as a JSON number,
    which is what an upstream that does not quote its ids returns, and looks
    it up the only way the admin route can: with the path segment as a string.

    Expected outcome: the row is found. Before the fix the comparison was a
    JSON leaf against a bound TEXT parameter, and SQLite compares INTEGER
    storage to TEXT WITHOUT conversion, so the endpoint reported the chain
    absent for the entire deployment, permanently and silently.
    """
    numeric = make_upload_row(captured_values=_captured_with_file_id(918273))
    textual = make_upload_row(captured_values=_captured_with_file_id("918273"))
    for row in (numeric, textual):
        await file_store.insert(row)

    found = await file_store.find_by_captured_value("create_file", "file_information.id", "918273")

    assert {entry.chain_id for entry in found} == {numeric.chain_id, textual.chain_id}


@pytest.mark.asyncio
async def test_key_value_lookup_matches_a_numeric_metadata_value(
    file_store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """Objective: a JSON number in the metadata KVS must match its digits (D4).

    The key-value store is unconstrained producer JSON, so a producer is free
    to stamp a number. Seeds one and searches for its text form.

    Expected outcome: the row is found. Before the fix ``json_each.value``
    carried INTEGER storage and never equalled the bound TEXT parameter.
    """
    envelope = json.dumps(
        {
            "steps": [
                {
                    "name": "create_file",
                    "body": {"value": {"metadata": {"keyValueStore": {"batch": 7}}}},
                }
            ]
        }
    )
    row = make_upload_row(chain_envelope_json=envelope)
    await file_store.insert(row)

    assert [m.chain_id for m in await file_store.list_by_key_value("batch", "7")] == [row.chain_id]


# ---------------------------------------------------------------------
# D6 - no lookup may build an escaped quoted JSON-path label.
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_json_lookup_builds_an_escaped_quoted_path_label(
    file_store: SqliteUploadStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Objective: the version-skew shape must be unreachable from every lookup (D6).

    Runs all three JSON lookups with a quote-bearing label and inspects the
    statements and bound parameters they actually issue.

    Expected outcome: no statement text and no bound parameter contains an
    escaped quote inside a JSON path. Before the fix
    ``find_by_captured_value`` bound exactly that path for a quote-bearing
    capture name, and SQLite below ~3.50, the CI and deploy version, cannot
    parse it: ``json_extract`` yields NULL and the route reports the chain
    absent with no error. This is a SHAPE assertion on purpose. The
    behavioural consequence is invisible on a modern SQLite, where the buggy
    form happens to work, so the parametrised
    ``special_character`` cases in ``test_sqlite_store.py`` cover the
    behaviour on the old-SQLite lane and this covers it on every runner.
    """
    issued: list[tuple[str, list[Any]]] = []
    _record_statements(monkeypatch, issued)

    await file_store.list_by_key_value('q"uote', "value")
    await file_store.find_by_captured_value('q"uote', "file_information.id", "value")
    await file_store.find_by_local_uuid(uuid4())

    monkeypatch.undo()
    assert issued, "the lookups issued no statements to inspect"
    for sql, parameters in issued:
        assert _ESCAPED_QUOTE_IN_A_JSON_PATH_LABEL not in sql, sql
        for parameter in parameters:
            assert _ESCAPED_QUOTE_IN_A_JSON_PATH_LABEL not in str(parameter), parameter


# ---------------------------------------------------------------------
# SP-7 - the listing's sort key carries an index.
# ---------------------------------------------------------------------


async def _query_plan(store: SqliteUploadStore, sql: str, parameters: list[Any]) -> str:
    """Return the joined EXPLAIN QUERY PLAN detail lines for ``sql``."""
    reader = store._read_conn
    assert reader is not None
    async with reader.execute(f"EXPLAIN QUERY PLAN {sql}", parameters) as cursor:
        rows = await cursor.fetchall()
    return " | ".join(str(row["detail"]) for row in rows)


@pytest.mark.asyncio
async def test_paginated_listing_sorts_and_resumes_through_an_index(
    file_store: SqliteUploadStore,
) -> None:
    """Objective: the listing must not sort the whole table in a temp B-tree (SP-7).

    Asks SQLite how it would run the two statements ``list_uploads`` issues:
    the first page, and a keyset resume.

    Expected outcome: both plans use ``idx_uploads_received_at``, and neither
    builds a temp B-tree. Before the index existed ``received_at`` was the
    sort key of the admin UI's default view and the export tar's row source
    with no index on it at all, so the first page scanned and sorted the whole
    table and every resume scanned to its cursor instead of seeking: paging N
    pages cost O(N x rows).
    """
    first_page = await _query_plan(
        file_store,
        "SELECT * FROM uploads ORDER BY received_at ASC, chain_id ASC LIMIT ?",
        [101],
    )
    resumed = await _query_plan(
        file_store,
        "SELECT * FROM uploads WHERE (received_at, chain_id) > (?, ?) "
        "ORDER BY received_at ASC, chain_id ASC LIMIT ?",
        ["2026-09-08T00:00:00+00:00", str(uuid4()), 101],
    )

    assert "idx_uploads_received_at" in first_page, first_page
    assert "TEMP B-TREE" not in first_page.upper(), first_page
    assert "idx_uploads_received_at" in resumed, resumed
    assert "TEMP B-TREE" not in resumed.upper(), resumed
