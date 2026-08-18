"""Unit tests for the boot-time schema gate (plan § 4S.2 / § 4S.3 / § 4S.7).

The full always-boots-over-an-old-DB E2E lives in § 5 (the production-path
lifespan driver). This module carries focused UNIT tests for the new pure and
near-pure building blocks so the phase is independently verifiable:

* :func:`decide_schema_action` (pure, the core logic): BOOT_AS_IS for a fresh
  DB (empty columns) and a stamped-current DB; DISCARD_AND_BOOT_FRESH for an
  unstamped DB EVEN with the current columns (pre-version, population of zero)
  and for a missing-required-column DB REGARDLESS of version (the never-crash
  net); with ``SCHEMA_MIGRATIONS`` empty every non-current schema discards.
* :data:`EXPECTED_UPLOADS_COLUMNS` drift guard: derived from ``schema.sql`` at
  import, non-empty, carries the load-bearing columns, and a fresh ``start()``
  DB's ``uploads`` columns are a SUPERSET of it (the subset predicate the gate
  uses).
* The :data:`SCHEMA_VERSION` stamp: a fresh ``start()`` leaves ``PRAGMA
  user_version == SCHEMA_VERSION``; a re-``start()`` keeps it.
* :func:`run_schema_gate` (near-pure, real SQLite temp files): a
  fresh/non-existent path -> BOOT_AS_IS, nothing deleted; a current+stamped DB
  -> BOOT_AS_IS, nothing deleted; a hand-built OLD DB -> the live DB is GONE,
  ``discarded_path`` set, and NO backup pair anywhere on disk; a DB file
  present but WITHOUT an ``uploads`` table -> BOOT_AS_IS.
* The cycle-7 revision gate legs (plan 06_09 phase 1): the derived column set
  carries ``group_id`` / ``multifile_id`` / ``send_order`` / ``sent_at`` and
  neither old name; ``SCHEMA_VERSION`` is pinned at 3 (D2/F6 added
  ``auth_blocked_host`` on top of the cycle-7 revision); a version-1-stamped
  old-shape DB discards and boots fresh at the current stamp; a
  current-stamped DB missing a required column discards and boots fresh.

There are NO schema-backup reconciliation or marker tests in this phase: the
discard is a self-contained, marker-less unlink (nothing is preserved).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import aiosqlite
from phantom.runtime.startup_checks import (
    EXPECTED_UPLOADS_COLUMNS,
    SCHEMA_MIGRATIONS,
    SchemaAction,
    decide_schema_action,
    run_schema_gate,
)
from phantom.storage.sqlite_store import SCHEMA_VERSION, SqliteUploadStore

# The load-bearing columns the gate's drift guard asserts are present (plan
# § 4S.7). A subset of the derived EXPECTED_UPLOADS_COLUMNS that names the
# columns whose absence is the documented schema-mismatch crash (the Phase-1
# committed/tier -> body_location/chain_id_at_ingress rename plus the
# non-indexed body_size_bytes).
_LOAD_BEARING_COLUMNS: frozenset[str] = frozenset(
    {"chain_id", "state", "body_location", "chain_id_at_ingress", "body_size_bytes"}
)


# ---------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------


def _make_old_uploads_db(db_path: Path) -> None:
    """Hand-build an OLD-schema DB: an ``uploads`` table missing required cols.

    Models the Phase-1 rename: the table predates ``body_location`` /
    ``chain_id_at_ingress``, and ``user_version`` is never stamped (stays 0).
    This is the exact shape that crashes ``start()``'s ``executescript`` on the
    ``idx_uploads_chain_id_at_ingress`` index today.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "CREATE TABLE uploads (chain_id TEXT PRIMARY KEY, state TEXT, instance_id TEXT)"
        )
        conn.commit()
    finally:
        conn.close()


def _make_db_without_uploads_table(db_path: Path) -> None:
    """Create a real SQLite file that has NO ``uploads`` table."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE unrelated (x INTEGER PRIMARY KEY)")
        conn.commit()
    finally:
        conn.close()


# The version stamp the cycle-6 (pre-revision) schema carried. The cycle-7
# revision bumped SCHEMA_VERSION to 2 and D2/F6 bumped it again to 3; a DB
# stamped 1 is the old batch_id / order_in_batch shape and must discard at
# the gate, exactly as any other stale stamp does.
_CYCLE6_SCHEMA_VERSION = 1


def _make_version1_old_shape_db(db_path: Path) -> None:
    """Hand-build a version-1-STAMPED old-shape DB (the cycle-6 schema).

    Models the pre-cycle-7 ``uploads`` shape: ``batch_id`` /
    ``order_in_batch`` present, no ``group_id`` / ``multifile_id`` /
    ``send_order`` / ``sent_at``, and ``user_version`` stamped 1 (the
    cycle-6 SCHEMA_VERSION). The gate must discard on the version
    mismatch alone; the missing columns are the second, independent net.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE uploads (
                chain_id        TEXT PRIMARY KEY,
                instance_id     TEXT NOT NULL,
                batch_id        TEXT NOT NULL,
                order_in_batch  INTEGER NOT NULL DEFAULT 0,
                state           TEXT NOT NULL,
                body_location   TEXT NOT NULL
            )
            """
        )
        conn.execute(f"PRAGMA user_version = {_CYCLE6_SCHEMA_VERSION}")
        conn.commit()
    finally:
        conn.close()


def _drop_uploads_column(db_path: Path, column: str, *, index_to_drop: str | None) -> None:
    """Drop one ``uploads`` column off an existing DB, keeping its stamp.

    ``PRAGMA user_version`` survives the ALTER, so applying this to a
    freshly ``start()``-ed DB yields the stamp-right-shape-wrong case
    the gate's column-subset net exists for. SQLite refuses to drop an
    indexed column, so the covering index (when one exists) is dropped
    first.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        if index_to_drop is not None:
            conn.execute(f"DROP INDEX IF EXISTS {index_to_drop}")
        conn.execute(f"ALTER TABLE uploads DROP COLUMN {column}")
        conn.commit()
    finally:
        conn.close()


def _no_backup_pair_exists(data_root: Path) -> bool:
    """Return True iff NO quarantine/backup sibling exists under ``data_root``.

    The gate DELETES — it must never write a backup. Asserts the absence of
    both reasons' artifacts (``.mode_switch.`` and ``.corrupted.``) and of any
    timestamped DB/body backup, so a regression that smuggled in § 1's mover
    would be caught.
    """
    if not data_root.is_dir():
        return True
    for child in data_root.iterdir():
        name = child.name
        if ".mode_switch." in name or ".corrupted." in name or ".quarantine." in name:
            return False
    return True


# ---------------------------------------------------------------------
# 1. decide_schema_action — pure core logic.
# ---------------------------------------------------------------------


def test_decide_fresh_db_boots_as_is() -> None:
    """An empty column set (no uploads table) -> BOOT_AS_IS (nothing to discard)."""
    action = decide_schema_action(observed_version=0, observed_columns=frozenset())
    assert action is SchemaAction.BOOT_AS_IS


def test_decide_stamped_current_boots_as_is() -> None:
    """Stamped-current version + the current columns -> BOOT_AS_IS."""
    action = decide_schema_action(
        observed_version=SCHEMA_VERSION, observed_columns=EXPECTED_UPLOADS_COLUMNS
    )
    assert action is SchemaAction.BOOT_AS_IS


def test_decide_stamped_current_with_extra_column_boots_as_is() -> None:
    """An EXTRA/future column with all required present is tolerated -> BOOT_AS_IS."""
    action = decide_schema_action(
        observed_version=SCHEMA_VERSION,
        observed_columns=EXPECTED_UPLOADS_COLUMNS | {"future_column"},
    )
    assert action is SchemaAction.BOOT_AS_IS


def test_decide_unstamped_with_current_columns_discards() -> None:
    """user_version=0 with the CURRENT columns -> DISCARD (pre-version, pop. zero).

    The crux of the design: both the stamp AND the columns must hold for
    BOOT_AS_IS. An unstamped DB is pre-version and cannot be a live field
    buffer, so it discards even though every column matches.
    """
    action = decide_schema_action(observed_version=0, observed_columns=EXPECTED_UPLOADS_COLUMNS)
    assert action is SchemaAction.DISCARD_AND_BOOT_FRESH


def test_decide_missing_required_column_discards_even_when_stamped_current() -> None:
    """A missing required column -> DISCARD regardless of version (never-crash net)."""
    deficient = EXPECTED_UPLOADS_COLUMNS - {"body_location"}
    action = decide_schema_action(observed_version=SCHEMA_VERSION, observed_columns=deficient)
    assert action is SchemaAction.DISCARD_AND_BOOT_FRESH


def test_decide_future_version_discards() -> None:
    """A version != SCHEMA_VERSION discards (with the registry empty)."""
    action = decide_schema_action(
        observed_version=SCHEMA_VERSION + 1, observed_columns=EXPECTED_UPLOADS_COLUMNS
    )
    assert action is SchemaAction.DISCARD_AND_BOOT_FRESH


def test_schema_migrations_registry_is_empty_today() -> None:
    """The migration seam is defined but EMPTY, so every mismatch discards."""
    assert SCHEMA_MIGRATIONS == ()


# ---------------------------------------------------------------------
# 2. EXPECTED_UPLOADS_COLUMNS drift guard (derived from schema.sql).
# ---------------------------------------------------------------------


def test_expected_columns_non_empty_and_carry_load_bearing_columns() -> None:
    """The derived set is non-empty and contains the load-bearing columns."""
    assert EXPECTED_UPLOADS_COLUMNS
    assert _LOAD_BEARING_COLUMNS <= EXPECTED_UPLOADS_COLUMNS


def test_expected_columns_reflect_cycle7_revision() -> None:
    """The derived set carries the cycle-7 columns and neither old name.

    Plan 06_09 task 1.2: ``EXPECTED_UPLOADS_COLUMNS`` derives from
    ``schema.sql`` at import (never hand-edited), so this pins that the
    schema revision actually landed in the derived gate set:
    ``group_id`` / ``multifile_id`` / ``send_order`` / ``sent_at`` in,
    ``batch_id`` / ``order_in_batch`` out.
    """
    revision_columns = frozenset({"group_id", "multifile_id", "send_order", "sent_at"})
    assert revision_columns <= EXPECTED_UPLOADS_COLUMNS
    assert "batch_id" not in EXPECTED_UPLOADS_COLUMNS
    assert "order_in_batch" not in EXPECTED_UPLOADS_COLUMNS


def test_schema_version_tracks_every_schema_shape_change() -> None:
    """``SCHEMA_VERSION`` is 3: cycle-7's revision plus D2/F6's new column.

    Deliberately pinned: each clean break REQUIRES the bump, because no
    migration is registered and the gate discards a DB whose stamp does not
    match. Version 2 was the cycle-7 uploads revision (a version-1 DB carries
    the old batch_id shape); version 3 added ``auth_blocked_host`` (D2/F6).
    When the schema next changes shape, bump the constant and update this pin
    in the same change.
    """
    expected_current_version = 3
    assert expected_current_version == SCHEMA_VERSION


async def test_fresh_start_db_columns_superset_of_expected(tmp_path: Path) -> None:
    """A fresh ``start()`` DB's ``uploads`` columns are a SUPERSET of the expected set.

    This is the subset predicate the gate uses: a real, freshly-created store
    must satisfy ``EXPECTED_UPLOADS_COLUMNS <= observed``. Derived-vs-runtime
    drift would turn this RED.
    """
    db_path = tmp_path / "uploads.db"
    store = SqliteUploadStore(str(db_path))
    await store.start()
    await store.stop()
    async with aiosqlite.connect(str(db_path)) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("PRAGMA table_info(uploads)") as cur:
            rows = await cur.fetchall()
    observed = frozenset(row["name"] for row in rows)
    assert observed >= EXPECTED_UPLOADS_COLUMNS


# ---------------------------------------------------------------------
# 3. SCHEMA_VERSION stamp (§ 4S.1) — verified via start().
# ---------------------------------------------------------------------


async def test_fresh_start_stamps_schema_version(tmp_path: Path) -> None:
    """A fresh ``start()`` leaves ``PRAGMA user_version == SCHEMA_VERSION``."""
    db_path = tmp_path / "uploads.db"
    store = SqliteUploadStore(str(db_path))
    await store.start()
    await store.stop()
    async with (
        aiosqlite.connect(str(db_path)) as conn,
        conn.execute("PRAGMA user_version") as cur,
    ):
        row = await cur.fetchone()
    assert row is not None
    assert row[0] == SCHEMA_VERSION


async def test_restart_keeps_schema_version_stamp(tmp_path: Path) -> None:
    """Re-``start()`` over the same DB keeps ``user_version == SCHEMA_VERSION``."""
    db_path = tmp_path / "uploads.db"
    first = SqliteUploadStore(str(db_path))
    await first.start()
    await first.stop()
    second = SqliteUploadStore(str(db_path))
    await second.start()
    await second.stop()
    async with (
        aiosqlite.connect(str(db_path)) as conn,
        conn.execute("PRAGMA user_version") as cur,
    ):
        row = await cur.fetchone()
    assert row is not None
    assert row[0] == SCHEMA_VERSION


# ---------------------------------------------------------------------
# 4. run_schema_gate — near-pure, real SQLite temp files.
# ---------------------------------------------------------------------


async def test_gate_nonexistent_path_boots_as_is(tmp_path: Path) -> None:
    """A non-existent DB path -> BOOT_AS_IS, nothing created or deleted."""
    db_path = tmp_path / "uploads.db"
    result = await run_schema_gate(db_path=db_path, bodies_root=tmp_path / "bodies")
    assert result.action is SchemaAction.BOOT_AS_IS
    assert result.discarded_path is None
    assert result.observed_version == 0
    assert result.observed_columns == frozenset()
    assert not db_path.exists()


async def test_gate_current_stamped_db_boots_as_is(tmp_path: Path) -> None:
    """A DB with the current schema + stamp -> BOOT_AS_IS, not deleted."""
    db_path = tmp_path / "uploads.db"
    store = SqliteUploadStore(str(db_path))
    await store.start()
    await store.stop()
    result = await run_schema_gate(db_path=db_path, bodies_root=tmp_path / "bodies")
    assert result.action is SchemaAction.BOOT_AS_IS
    assert result.discarded_path is None
    assert result.observed_version == SCHEMA_VERSION
    assert db_path.exists()


async def test_gate_old_db_is_deleted_with_no_backup_pair(tmp_path: Path) -> None:
    """A hand-built OLD DB is DELETED; ``discarded_path`` set; NO backup anywhere.

    The crux: the gate deletes (population of zero), it never backs up. Asserts
    the live DB plus its WAL/SHM siblings are gone and that no quarantine
    sibling of either reason was created.
    """
    db_path = tmp_path / "uploads.db"
    _make_old_uploads_db(db_path)
    # Lay down WAL/SHM siblings to prove the idempotent multi-unlink.
    (tmp_path / "uploads.db-wal").write_bytes(b"stale-wal")
    (tmp_path / "uploads.db-shm").write_bytes(b"stale-shm")

    result = await run_schema_gate(db_path=db_path, bodies_root=tmp_path / "bodies")

    assert result.action is SchemaAction.DISCARD_AND_BOOT_FRESH
    assert result.discarded_path == db_path
    assert result.observed_version == 0
    # The live DB and both siblings are gone.
    assert not db_path.exists()
    assert not (tmp_path / "uploads.db-wal").exists()
    assert not (tmp_path / "uploads.db-shm").exists()
    # No backup pair of EITHER reason exists anywhere in the data root.
    assert _no_backup_pair_exists(tmp_path)
    # No leftover DB or body backup of any timestamped form.
    assert not list(tmp_path.glob("*.db"))


async def test_gate_db_without_uploads_table_boots_as_is(tmp_path: Path) -> None:
    """A DB file present but WITHOUT an ``uploads`` table -> BOOT_AS_IS, kept.

    The CREATE TABLE IF NOT EXISTS in ``start()`` will build the table; the gate
    treats an absent table as fresh-enough.
    """
    db_path = tmp_path / "uploads.db"
    _make_db_without_uploads_table(db_path)
    result = await run_schema_gate(db_path=db_path, bodies_root=tmp_path / "bodies")
    assert result.action is SchemaAction.BOOT_AS_IS
    assert result.discarded_path is None
    assert result.observed_columns == frozenset()
    assert db_path.exists()


async def test_gate_version1_old_shape_db_discards_and_boots_fresh(tmp_path: Path) -> None:
    """A version-1-stamped old-shape (batch_id-era) DB is DELETED; fresh boot re-stamps.

    The cycle-7 clean break: no migration is registered, so the gate
    routes the cycle-6 schema (stamped 1, batch_id / order_in_batch
    columns) to DISCARD_AND_BOOT_FRESH, and a subsequent ``start()``
    recreates the revised schema stamped with the current version.
    """
    db_path = tmp_path / "uploads.db"
    _make_version1_old_shape_db(db_path)

    result = await run_schema_gate(db_path=db_path, bodies_root=tmp_path / "bodies")

    assert result.action is SchemaAction.DISCARD_AND_BOOT_FRESH
    assert result.discarded_path == db_path
    assert result.observed_version == _CYCLE6_SCHEMA_VERSION
    assert "batch_id" in result.observed_columns
    assert not db_path.exists()

    # The fresh boot over the discarded path carries the revised shape.
    store = SqliteUploadStore(str(db_path))
    await store.start()
    await store.stop()
    conn = sqlite3.connect(str(db_path))
    try:
        stamped = int(conn.execute("PRAGMA user_version").fetchone()[0])
        columns = frozenset(row[1] for row in conn.execute("PRAGMA table_info(uploads)"))
    finally:
        conn.close()
    assert stamped == SCHEMA_VERSION
    assert {"group_id", "multifile_id", "send_order", "sent_at"} <= columns


async def test_gate_current_stamped_db_missing_required_column_discards(tmp_path: Path) -> None:
    """A CURRENT-stamped DB missing a required column is DELETED; fresh boot follows.

    The never-crash net is independent of the version stamp: the stamp
    is right, the shape is not (``group_id`` dropped), so the gate
    discards and the next ``start()`` boots fresh.
    """
    db_path = tmp_path / "uploads.db"
    seeded = SqliteUploadStore(str(db_path))
    await seeded.start()
    await seeded.stop()
    _drop_uploads_column(db_path, "group_id", index_to_drop="idx_uploads_group_id")

    result = await run_schema_gate(db_path=db_path, bodies_root=tmp_path / "bodies")

    assert result.action is SchemaAction.DISCARD_AND_BOOT_FRESH
    assert result.discarded_path == db_path
    assert result.observed_version == SCHEMA_VERSION
    assert "group_id" not in result.observed_columns
    assert not db_path.exists()

    # A fresh start() over the same path boots the full revised shape.
    fresh = SqliteUploadStore(str(db_path))
    await fresh.start()
    await fresh.stop()
    conn = sqlite3.connect(str(db_path))
    try:
        columns = frozenset(row[1] for row in conn.execute("PRAGMA table_info(uploads)"))
    finally:
        conn.close()
    assert "group_id" in columns
