"""Aiosqlite-backed UploadStore.

Phase 1: collapsed from the pre-refactor two-tier split (separate
``:memory:`` and disk DBs with identical schemas) to ONE persistent
SQLite holding all ``uploads`` + ``idempotency_index`` + ``token_cache``
rows. The ``body_location`` column ('ram' | 'file') replaces the old
``committed`` boolean + ``tier`` column as the source of truth for
which BodyStore is holding the body files.

Plan § 2.3.4 + § 2.3.5 + § 2.3.7:

* Dropped the ``tier`` constructor parameter; the store no longer
  carries a per-tier identity.
* Dropped legacy ``mark_committed`` / ``list_uncommitted`` /
  ``list_memory_rows_older_than`` methods. The persist-handoff commit
  point is :meth:`mark_persisted` (sole writer of body_location
  ram→file per plan § 0.5 invariant #6).
* Added :meth:`mark_persisted`, :meth:`mark_corrupted`,
  :meth:`iter_rows`, :meth:`list_oldest_ram_bodies`,
  :meth:`list_chain_ids`, and :meth:`insert_with_idempotency_claim`
  (atomic admission transaction — closes H7 structurally; uses
  explicit BEGIN/commit/rollback per Round 3 B2).
* Pragma block parameterized from Settings: ``synchronous`` (default
  ``NORMAL`` for SD-card wear), ``journal_size_limit`` (default 16
  MiB). ``auto_vacuum=NONE`` stays HARDCODED per § 0.3 — never
  configurable.
* Startup pragma assertions verify each pragma stuck after open.

The downstream slices (1.C composition root + PersistController;
1.D worker refactors; 1.E admission + admin rewrites) consume these
new methods.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import sqlite3
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final, get_args
from urllib.parse import quote
from uuid import UUID

import aiosqlite

from phantom.config.settings import SqliteCfg
from phantom.models.upload import (
    BodyHash,
    BodyHashes,
    CapturedValues,
    StorageHash,
    UploadRow,
    UploadState,
)
from phantom.observability.metrics import MetricsRegistry
from phantom.storage.errors import ReplayBodyDiscardedError, ReplayRefusedAttemptingError
from phantom.storage.interface import (
    TERMINAL_STATES,
    CancelOutcome,
    DeletedRowAccounting,
    DiscardOutcome,
    InsertClaimOutcome,
    ReplayOutcome,
    StateTally,
)

logger = logging.getLogger(__name__)

# Number of attempts to fetch per claim_due call. Plan §4.10 leaves it caller-supplied.
DEFAULT_CLAIM_LIMIT = 1

# The full, valid ``UploadState`` vocabulary, sourced from the single
# ``Literal`` definition so it can never drift. Used by
# :meth:`SqliteUploadStore.counts_by_state` to narrow the ``str`` state
# column read back from SQLite to a typed :data:`UploadState` key and to
# skip (loudly) any unrecognized value rather than crash the stats read.
_VALID_UPLOAD_STATES: Final[frozenset[UploadState]] = frozenset(get_args(UploadState))

# Default SQLite pragma values used when no Settings are passed to the store
# (unit tests primarily). Production code threads `Settings.storage.sqlite`
# through. The defaults mirror :class:`SqliteCfg` so a store constructed
# without overrides matches the production default posture.
_DEFAULT_SYNCHRONOUS = "NORMAL"
_DEFAULT_JOURNAL_SIZE_LIMIT_BYTES = 16_777_216

# Default SQLite ``busy_timeout`` in milliseconds for the no-Settings
# construction path (unit tests). SQLite busy-WAITS in its connection worker
# thread for up to this long when a write contends for a held lock before
# raising "database is locked" (SQLITE_BUSY). Mirrors :class:`SqliteCfg`'s
# ``busy_timeout_ms`` default so a store/cache built without overrides matches
# the production default posture; production threads ``cfg.busy_timeout_ms``.
#
# WHY 1 s, not the former 5 s (finding R9-V6-1 — the lock-amplification fix).
# Phantom's store serializes EVERY writer (admission + the sender pool + reaper
# + persist-controller + admin) through ONE ``asyncio.Lock`` (``_write_lock``)
# on a single aiosqlite connection, so there is NEVER more than one Phantom
# write in flight at the SQLite level — Phantom-internal write-vs-write
# contention is impossible by construction. The busy_timeout therefore does
# NOT exist to give "concurrent workers headroom"; its ONLY effect is under
# EXTERNAL cross-process contention — a sibling connection holding the WAL
# write lock (a stray ``sqlite3 uploads.db`` admin session, a backup/snapshot
# tool, a second instance mis-sharing the data_dir). Under such a hold, a LARGE
# busy_timeout is actively harmful: each contended writer monopolizes the
# single ``_write_lock`` + connection-thread slot for the full window, so a
# burst of admissions queues serially behind multiple 5 s busy-waits and the
# producer's HTTP read times out BEFORE admission can return its clean
# ``storage_unavailable`` 503 — the burst surfaced as bare
# ``PhantomTimeoutError``s instead of clean retryables (R9-V6-1; an 8-deep
# burst under a 9 s hold took ~93 s at 5 s vs ~13 s at 1 s, all clean 503s).
# 1 s comfortably rides out sub-second external blips while failing FAST under
# a sustained external hold so the contended write returns a clean retryable
# signal quickly (admission → 503 + Retry-After; a sender's ``claim_due`` →
# retry on its next poll) rather than blocking the single writer slot.
# Durability is unaffected — a failed contended write commits no row
# (R9-V6-3 confirms the data layer never corrupts under the lock). Boot-time
# recovery rides out a lock for far longer than this via its own bounded
# retry-with-backoff (``workers.recovery``), independent of this value. See
# :class:`phantom.config.settings.SqliteCfg.busy_timeout_ms` for the
# operator-facing knob (default stays 1000).
_DEFAULT_BUSY_TIMEOUT_MS = 1000

# Numeric encodings PRAGMA returns for ``synchronous``. SQLite reports the
# normalized numeric value (0 OFF, 1 NORMAL, 2 FULL, 3 EXTRA) rather than
# the string name we pushed in.
_SYNCHRONOUS_TO_PRAGMA_INT: dict[str, int] = {
    "OFF": 0,
    "NORMAL": 1,
    "FULL": 2,
    "EXTRA": 3,
}

_SCHEMA_PATH = Path(__file__).with_name("schema.sql")

# The metadata key the producer-side adapter stamps into every submission's
# metadata key-value store. The by-local-uuid admin lookup is PINNED to this
# key (cycle-7 task 4.2): :meth:`SqliteUploadStore.find_by_local_uuid` builds
# its JSON1 extract path from it via :func:`_metadata_kvs_json_path`, so
# callers never spell a JSON path. Route code that surfaces the value reads
# THIS constant rather than re-spelling the key.
PHANTOM_LOCAL_UUID_METADATA_KEY: Final[str] = "phantom_local_uuid"


def _quote_json_path_label(label: str) -> str:
    """Quote one object label for a SQLite JSON1 path.

    SQLite's JSON path grammar accepts double-quoted object labels with
    backslash escapes for the quote and the backslash itself, so every
    label is emitted quoted with ``\\`` and ``"`` escaped. This makes
    labels containing ``.``, ``:``, ``"``, ``[``, or ``\\`` addressable
    exactly; the pre-fix unquoted interpolation silently re-segmented
    the path at a ``.`` and could not address such keys at all (round 2
    defender fix R2-3). The path string itself is always BOUND as an
    SQL parameter by the callers, never interpolated into SQL text.
    """
    escaped = label.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _metadata_kvs_json_path(key: str) -> str:
    """Build the JSON1 path to a metadata key-value-store entry.

    The metadata is carried in step 1's JSON body and serialized in
    camelCase (the upstream convention: the producing client's camelCase
    alias generator emits ``keyValueStore`` for the snake-cased Python
    attribute ``key_value_store``). The ONE place the extract path is
    spelled; :meth:`SqliteUploadStore.list_by_key_value` (caller-supplied
    key) and :meth:`SqliteUploadStore.find_by_local_uuid` (pinned key)
    both build their paths here so they can never drift. The key segment
    is quoted via :func:`_quote_json_path_label`, so KVS keys are
    user-defined dynamic keys in the fullest sense: dots, colons,
    quotes, and backslashes are all addressable (round 2 defender fix
    R2-3).
    """
    return f"$.steps[0].body.value.metadata.keyValueStore.{_quote_json_path_label(key)}"


# The on-disk schema contract version for the ``uploads`` /
# ``idempotency_index`` / ``token_cache`` tables defined in ``schema.sql``.
# Stamped into ``PRAGMA user_version`` by :meth:`SqliteUploadStore.start`
# (§ 4S.1) and read at boot by
# :func:`phantom.runtime.startup_checks.run_schema_gate` (§ 4S.2): a DB whose
# stamp does not equal this value is treated as pre-version / wrong-schema and
# deleted, then the instance boots fresh (population of zero — no field DB can
# hold real undelivered uploads yet; see ADR-025's scoped exception and
# § 4S.0). Version 2 is the cycle-7 uploads revision (group_id, multifile_id,
# send_order, sent_at replacing batch_id / order_in_batch), bumped WITHOUT a
# registered migration: the discard-and-boot-fresh path IS the clean break
# while the population is zero. When ``schema.sql`` next changes shape, bump
# this again and, once field DBs hold real data, register a forward
# :class:`phantom.runtime.startup_checks.SchemaMigration` in the § 4S.3 seam.
# ``startup_checks.py`` and the tests import THIS constant, never a
# re-declared literal, so the version lives in exactly one place.
SCHEMA_VERSION: int = 2


def is_chain_id_collision(exc: sqlite3.IntegrityError) -> bool:
    """Return True if ``exc`` is a ``uploads.chain_id`` PRIMARY KEY collision.

    SQLite phrases the PK-collision message as
    ``UNIQUE constraint failed: uploads.chain_id``. Match on the column
    token so the classifier is robust to message wording across SQLite
    versions while staying specific to the chain_id PK (not, e.g., a
    future UNIQUE index on another uploads column).
    """
    return "uploads.chain_id" in str(exc)


# Lower-cased SQLITE_BUSY / SQLITE_LOCKED message fragments. SQLite reports
# cross-process write contention (a sibling connection holding the WAL write
# lock past our ``busy_timeout``) and same-connection cursor-vs-checkpoint
# collisions as ``sqlite3.OperationalError`` with one of these phrasings. Match
# on the fragment (not an exact string) so the classifier is robust across
# SQLite versions while staying specific to the TRANSIENT contention class — a
# schema/syntax/type ``OperationalError`` carries none of these fragments and is
# correctly left un-classified (it is a genuine, non-retryable fault).
_TRANSIENT_LOCK_FRAGMENTS: frozenset[str] = frozenset(
    {
        "database is locked",  # SQLITE_BUSY — the cross-process write-lock timeout.
        "database is busy",  # alternate SQLITE_BUSY phrasing across versions.
        "database table is locked",  # SQLITE_LOCKED — table-level contention.
    }
)


def is_transient_lock_error(exc: BaseException) -> bool:
    """Return True if ``exc`` is a TRANSIENT SQLite lock/contention error.

    The single, shared definition of "this ``sqlite3.OperationalError`` is a
    ride-it-out lock contention, not a permanent fault" — used by admission
    (→ a clean ``storage_unavailable`` 503) and by recovery boot (→ a bounded
    retry-with-backoff) so both paths classify the SAME way (findings R9-V6-1 /
    R9-V6-2; the R7-1-D uncaught-``OperationalError`` class).

    A cross-process ``SQLITE_BUSY`` that outlasts ``busy_timeout`` raises
    ``sqlite3.OperationalError: database is locked``; a same-connection
    cursor-vs-checkpoint collision raises ``SQLITE_LOCKED`` ("database is
    locked" / "database table is locked"). Both are transient: the holder
    releases, the checkpoint completes, and the next attempt succeeds — Phantom
    must NOT surface them as a naked 5xx or crash startup over them.

    Deliberately NARROW: only an :class:`sqlite3.OperationalError` whose message
    carries a known SQLITE_BUSY/SQLITE_LOCKED fragment qualifies. A genuine
    ``OperationalError`` (malformed schema, ``no such table``, a type error) is
    NOT transient and must NOT be misclassified as retryable — it would mask a
    real bug behind an infinite retry / a misleading 503. Non-OperationalError
    exceptions (``IntegrityError``, ``OSError``) are out of scope and return
    False so their existing dedicated handling is untouched.

    Args:
        exc: The exception to classify (any ``BaseException`` so call sites can
            pass a caught error without a prior isinstance narrowing).

    Returns:
        ``True`` only for a transient SQLite lock/contention
        ``OperationalError``; ``False`` for every other exception.
    """
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    message = str(exc).lower()
    return any(fragment in message for fragment in _TRANSIENT_LOCK_FRAGMENTS)


def _bool_to_int(value: bool) -> int:
    """Encode bool → 0/1 for SQLite storage."""
    return 1 if value else 0


def _int_to_bool(value: int | None) -> bool:
    """Decode 0/1 → bool from SQLite storage."""
    return bool(value)


def _encode_body_hashes(body_hashes: dict[str, BodyHashes]) -> str:
    """Serialize body_hashes for the ``body_hashes_json`` column."""
    return json.dumps(
        {
            name: {"body_hash": h.body_hash, "storage_hash": h.storage_hash}
            for name, h in body_hashes.items()
        }
    )


def _decode_body_hashes(raw: str | None) -> dict[str, BodyHashes]:
    """Deserialize the ``body_hashes_json`` column into typed hashes."""
    if not raw:
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        return {}
    result: dict[str, BodyHashes] = {}
    for name, value in parsed.items():
        if not isinstance(value, dict):
            continue
        body_hash_raw = value.get("body_hash")
        storage_hash_raw = value.get("storage_hash")
        if not isinstance(body_hash_raw, str) or not isinstance(storage_hash_raw, str):
            continue
        result[name] = BodyHashes(
            body_hash=BodyHash(body_hash_raw),
            storage_hash=StorageHash(storage_hash_raw),
        )
    return result


def _accounting_from_sql_row(row: aiosqlite.Row) -> DeletedRowAccounting:
    """Decode the saturation-accounting columns of one deleted row (R8-4).

    Expects a SELECT carrying ``chain_id``, ``state``,
    ``body_size_bytes``, and ``body_discarded_at``, captured inside the
    same write transaction as the DELETE that removes the row.
    """
    discarded_raw = row["body_discarded_at"]
    return DeletedRowAccounting(
        chain_id=UUID(row["chain_id"]),
        state=row["state"],
        body_size_bytes=int(row["body_size_bytes"]),
        body_discarded_at=(
            datetime.fromisoformat(discarded_raw) if discarded_raw is not None else None
        ),
    )


def _row_to_upload(row: aiosqlite.Row) -> UploadRow:
    """Decode one SQLite row into an :class:`UploadRow`."""
    return UploadRow(
        chain_id=UUID(row["chain_id"]),
        instance_id=row["instance_id"],
        group_id=UUID(row["group_id"]),
        multifile_id=(UUID(row["multifile_id"]) if row["multifile_id"] is not None else None),
        send_order=int(row["send_order"]),
        route_name=row["route_name"],
        state=row["state"],
        body_location=row["body_location"],
        attempts=int(row["attempts"]),
        next_attempt_at=(
            datetime.fromisoformat(row["next_attempt_at"]) if row["next_attempt_at"] else None
        ),
        received_at=datetime.fromisoformat(row["received_at"]),
        sent_at=(datetime.fromisoformat(row["sent_at"]) if row["sent_at"] else None),
        updated_at=datetime.fromisoformat(row["updated_at"]),
        last_error=row["last_error"],
        endpoint=row["endpoint"],
        uid=row["uid"],
        chain_envelope_json=row["chain_envelope_json"],
        captured_values=CapturedValues.model_validate_json(row["captured_values_json"]),
        current_step_index=int(row["current_step_index"]),
        idempotency_key=row["idempotency_key"],
        capture_reexecution_active=_int_to_bool(row["capture_reexecution_active"]),
        storage_encoding=row["storage_encoding"],
        body_size_bytes=int(row["body_size_bytes"]),
        body_discarded_at=(
            datetime.fromisoformat(row["body_discarded_at"]) if row["body_discarded_at"] else None
        ),
        upstream_status_code=(
            int(row["upstream_status_code"]) if row["upstream_status_code"] is not None else None
        ),
        upstream_response_headers_json=row["upstream_response_headers_json"],
        last_step_completed=row["last_step_completed"],
        body_hashes=_decode_body_hashes(row["body_hashes_json"]),
        chain_id_at_ingress=_optional_row_value(row, "chain_id_at_ingress"),
    )


# Exception group intentionally bound to a module-level constant. ruff 0.15.x
# strips the parentheses from a parenthesized ``except (A, B):`` under Python
# 3.14 (producing the bare 3.14-only form), so the constant binding is the
# stable, portable, consistent form across interpreters.
_ROW_COLUMN_ABSENT_ERRORS: Final[tuple[type[BaseException], ...]] = (
    IndexError,
    KeyError,
)
"""Raised by ``aiosqlite.Row`` indexing when the column is absent from the row."""


def _optional_row_value(row: aiosqlite.Row, column: str) -> str | None:
    """Read an optional TEXT column tolerant of column-absent rows.

    Returns the string when present, ``None`` when NULL or when the
    column itself is absent from ``row``. Defensive against test
    fixtures + sibling code paths that hand-build rows without every
    column.
    """
    try:
        value = row[column]
    except _ROW_COLUMN_ABSENT_ERRORS:
        return None
    return value if value is not None else None


def _params_for_insert(row: UploadRow) -> dict[str, Any]:
    """Encode :class:`UploadRow` for INSERT/UPDATE param binding."""
    return {
        "chain_id": str(row.chain_id),
        "instance_id": row.instance_id,
        "group_id": str(row.group_id),
        "multifile_id": (str(row.multifile_id) if row.multifile_id is not None else None),
        "send_order": row.send_order,
        "route_name": row.route_name,
        "state": row.state,
        "body_location": row.body_location,
        "attempts": row.attempts,
        "next_attempt_at": (row.next_attempt_at.isoformat() if row.next_attempt_at else None),
        "received_at": row.received_at.isoformat(),
        "sent_at": (row.sent_at.isoformat() if row.sent_at else None),
        "updated_at": row.updated_at.isoformat(),
        "last_error": row.last_error,
        "endpoint": row.endpoint,
        "uid": row.uid,
        "chain_envelope_json": row.chain_envelope_json,
        "captured_values_json": row.captured_values.model_dump_json(),
        "current_step_index": row.current_step_index,
        "idempotency_key": row.idempotency_key,
        "capture_reexecution_active": _bool_to_int(row.capture_reexecution_active),
        "storage_encoding": row.storage_encoding,
        "body_size_bytes": row.body_size_bytes,
        "body_discarded_at": (row.body_discarded_at.isoformat() if row.body_discarded_at else None),
        "upstream_status_code": row.upstream_status_code,
        "upstream_response_headers_json": row.upstream_response_headers_json,
        "last_step_completed": row.last_step_completed,
        "body_hashes_json": _encode_body_hashes(row.body_hashes),
        "chain_id_at_ingress": row.chain_id_at_ingress,
    }


class SqliteUploadStore:
    """SQLite-backed :class:`UploadStore` implementation.

    The ``tier`` constructor parameter is
    gone — every store instance is just "the SQLite at this path." The
    composition root (plan § 2.3.10) constructs exactly
    one store per process; the historical ``:memory:`` + ``disk`` split
    is dead.

    Optional ``sqlite_cfg`` carries the parameterized pragma values
    (``synchronous``, ``journal_size_limit_bytes``). When omitted the
    defaults match :class:`SqliteCfg` defaults so unit tests need no
    explicit Settings to exercise the store.
    """

    def __init__(
        self,
        db_path: str,
        *,
        sqlite_cfg: SqliteCfg | None = None,
        metrics_registry: MetricsRegistry | None = None,
    ) -> None:
        """Construct a store rooted at ``db_path``.

        Args:
            db_path: SQLite path. ``":memory:"`` is supported for
                in-process unit tests but is no longer a production
                deployment mode.
            sqlite_cfg: Pragma configuration. When ``None`` the
                :class:`SqliteCfg` defaults apply.
            metrics_registry: Optional :class:`MetricsRegistry` for
                emit-site wiring (plan § 4.2.2). When ``None`` a
                throwaway registry is constructed so emission is a
                no-op. The store registers gauges + counters but does
                not push values per write (per § 4.2.2, the admin
                endpoint computes ``body_location_distribution`` on
                demand via a single SQL grouping); see plan note.
        """
        self._db_path = db_path
        self._cfg = sqlite_cfg
        self._conn: aiosqlite.Connection | None = None
        # Dedicated read-only connection (cycle-7 task 4.1, strategy A6).
        # Opened by start() AFTER the writer is fully set up; every
        # read-only UploadStore method runs on it so admin/SDK reads
        # never queue behind in-flight writes at the connection level
        # (WAL lets the one reader proceed beside the single writer).
        # ``None`` until start(), and stays ``None`` for ``:memory:``
        # stores (see ``_read_connection``).
        self._read_conn: aiosqlite.Connection | None = None
        # Metrics surface (plan § 4.2.2). Store-level metrics are
        # registered eagerly so the admin endpoint surfaces a
        # zero-valued bucket before any row is written. The store does
        # NOT bump these per write — per plan § 4.2.2, the admin
        # endpoint computes the body-location-distribution gauge on
        # demand to avoid drift from invariant-coupled per-write
        # updates.
        self._metrics = metrics_registry if metrics_registry is not None else MetricsRegistry()
        # body_location_distribution + uploads_total: documentation
        # registrations only. Admin endpoint (plan § 4.2.5) computes
        # these on demand via SQL grouping and uses the registry
        # entries as the description surface.
        self._metrics.register_gauge(
            "body_location_distribution",
            "Current row count grouped by body_location (computed on demand by admin route).",
        )
        # All WRITE coroutines in the same Phantom process share one
        # aiosqlite connection. aiosqlite serializes individual
        # ``execute`` / ``commit`` calls on its background thread, but a
        # writer that ``execute``-then-``commit``s can be interleaved with
        # another coroutine's ``execute`` between the two awaits, surfacing
        # as ``sqlite3.OperationalError: cannot commit transaction - SQL
        # statements in progress``. Serialize every write path through
        # this lock so each ``execute`` / ``commit`` pair runs as one
        # atomic unit on the connection. Reads are exempt AND (cycle-7
        # task 4.1) no longer even share the connection: every read-only
        # method runs on the dedicated ``mode=ro`` reader, so a read can
        # neither race the writer's transaction state nor queue behind an
        # in-flight write at the connection level.
        self._write_lock = asyncio.Lock()

    def _synchronous(self) -> str:
        """Resolve the ``synchronous`` PRAGMA name from cfg or default."""
        if self._cfg is None:
            return _DEFAULT_SYNCHRONOUS
        return self._cfg.synchronous

    def _journal_size_limit_bytes(self) -> int:
        """Resolve the ``journal_size_limit`` PRAGMA value (bytes)."""
        if self._cfg is None:
            return _DEFAULT_JOURNAL_SIZE_LIMIT_BYTES
        return self._cfg.journal_size_limit_bytes

    def _busy_timeout_ms(self) -> int:
        """Resolve the ``busy_timeout`` PRAGMA value (milliseconds)."""
        if self._cfg is None:
            return _DEFAULT_BUSY_TIMEOUT_MS
        return self._cfg.busy_timeout_ms

    async def start(self) -> None:
        """Open the connections, set pragmas, apply schema, and stamp the version.

        After applying ``schema.sql`` this stamps ``PRAGMA user_version =
        SCHEMA_VERSION`` (§ 4S.1) inside the same transaction, before the
        commit, so every DB this build creates or keeps carries the current
        schema-contract version. The boot-time schema gate
        (:func:`phantom.runtime.startup_checks.run_schema_gate`) guarantees
        ``start`` only ever runs on a matching-or-fresh DB (a pre-version or
        wrong-schema DB is deleted before this point), so the stamp is always
        correct for the schema actually on disk.

        Last, ``start`` opens the dedicated READ-ONLY connection (cycle-7
        task 4.1). Because the composition root runs ``start`` only after
        the instance's integrity gate and mode guard have finished moving
        files, the reader's lifecycle ordering is inherited for free; a
        transient lock on either open rides the same boot-open retry
        (``retry_on_transient_lock``, judged by the one shared
        :func:`is_transient_lock_error` classifier per ADR-023).
        """
        self._conn = await aiosqlite.connect(self._db_path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        synchronous = self._synchronous()
        await self._conn.execute(f"PRAGMA synchronous={synchronous};")
        journal_limit = self._journal_size_limit_bytes()
        await self._conn.execute(f"PRAGMA journal_size_limit={journal_limit};")
        await self._conn.execute("PRAGMA foreign_keys=ON;")
        # Autovacuum locked OFF — SD-card-death risk on flash (plan § 0.3 hard rule;
        # NEVER configurable via Settings). VacuumScheduler reclaims space at
        # most once a day, gated on the in-flight queue being empty.
        await self._conn.execute("PRAGMA auto_vacuum=NONE;")
        # busy_timeout — see SqliteCfg.busy_timeout_ms / _DEFAULT_BUSY_TIMEOUT_MS
        # for the value rationale (R9-V6-1).
        busy_timeout_ms = self._busy_timeout_ms()
        await self._conn.execute(f"PRAGMA busy_timeout={busy_timeout_ms};")
        schema = _SCHEMA_PATH.read_text(encoding="utf-8")
        await self._conn.executescript(schema)
        # Stamp the schema-contract version (§ 4S.1). PRAGMA user_version does
        # NOT accept a bound ``?`` parameter, so the value is interpolated;
        # SCHEMA_VERSION is a typed int module constant (never user input), so
        # the f-string is injection-safe. Inside the same transaction as the
        # schema application, before the commit below.
        await self._conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION};")
        await self._conn.commit()

        # Defensive: confirm pragmas actually took effect. Catches
        # builds with PRAGMA support disabled or upstream regressions
        # in SQLite. The "missing pragma" assertion message is unique
        # per pragma so an operator log search lands on the right line.
        expected_sync = _SYNCHRONOUS_TO_PRAGMA_INT[synchronous]
        observed_sync = await self._read_pragma_int("synchronous")
        if observed_sync != expected_sync:
            raise RuntimeError(
                f"synchronous pragma did not stick: expected {expected_sync} "
                f"({synchronous}), got {observed_sync}"
            )
        observed_journal_limit = await self._read_pragma_int("journal_size_limit")
        if observed_journal_limit != journal_limit:
            raise RuntimeError(
                f"journal_size_limit pragma did not stick: expected "
                f"{journal_limit}, got {observed_journal_limit}"
            )
        observed_busy_timeout = await self._read_pragma_int("busy_timeout")
        if observed_busy_timeout != busy_timeout_ms:
            raise RuntimeError(
                f"busy_timeout pragma did not stick: expected "
                f"{busy_timeout_ms}, got {observed_busy_timeout}"
            )
        observed_auto_vacuum = await self._read_pragma_int("auto_vacuum")
        if observed_auto_vacuum != 0:
            raise RuntimeError(
                f"auto_vacuum pragma did not stick: expected 0 (NONE), got {observed_auto_vacuum}"
            )

        # Belt-and-suspenders for V1/V2 (defense in depth; the root-cause fix
        # is the cursor-drain discipline in run_recovery). A genuine SIGKILL
        # (power loss / OOM-kill) leaves a HOT ``uploads.db-wal`` — the killed
        # process never checkpointed it, so it can be many MB, well past
        # SQLite's ``wal_autocheckpoint`` threshold. The very next write would
        # otherwise trigger an in-line autocheckpoint; if any read cursor is
        # open on this single connection at that moment, the checkpoint and the
        # cursor collide → ``SQLITE_LOCKED`` ("database is locked"). Checkpoint
        # + TRUNCATE the WAL HERE, at start() — before recovery's sweep and
        # before any worker opens a cursor, with no cursor of our own open — so
        # the WAL is COLD when recovery (and steady-state traffic) runs. This is
        # a no-op on a clean WAL and a normal maintenance op on a hot one.
        await self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        await self._conn.commit()

        # Dedicated read-only connection (cycle-7 task 4.1). Opened LAST so
        # the schema exists and the version stamp is committed before the
        # reader ever sees the file. ``:memory:`` stores get NO reader: a
        # second connection to ``:memory:`` would open a DIFFERENT empty
        # database, so unit-test stores keep reading off the writer
        # connection (``_read_connection`` falls back).
        if self._db_path != ":memory:":
            self._read_conn = await aiosqlite.connect(self._read_only_uri(), uri=True)
            self._read_conn.row_factory = aiosqlite.Row
            # Same busy_timeout posture as the writer (R9-V6-1 rationale at
            # _DEFAULT_BUSY_TIMEOUT_MS): WAL readers rarely block, but a
            # read can hit SQLITE_BUSY around an exclusive checkpoint
            # phase; ride out sub-second blips, fail fast past that.
            await self._read_conn.execute(f"PRAGMA busy_timeout={busy_timeout_ms};")
            # Split-brain guard: confirm the reader resolved to the SAME
            # database the writer just stamped. A URI-resolution slip
            # (an unexpected path interpretation) would otherwise surface
            # later as silently-empty reads.
            async with self._read_conn.execute("PRAGMA user_version") as cursor:
                version_row = await cursor.fetchone()
            observed_version = int(version_row[0]) if version_row is not None else -1
            if observed_version != SCHEMA_VERSION:
                raise RuntimeError(
                    f"read-only connection opened against the wrong database: "
                    f"expected user_version {SCHEMA_VERSION}, got {observed_version}"
                )

    def _read_only_uri(self) -> str:
        """Build the ``file:`` URI that opens ``db_path`` read-only.

        ``mode=ro`` makes SQLite refuse every write on the connection, so
        the reader cannot perturb writer state even by accident. The path
        is percent-encoded per the SQLite URI rules (``?`` and ``#`` in a
        plain path would otherwise be parsed as URI syntax).
        """
        encoded_path = quote(str(Path(self._db_path).absolute()), safe="/")
        return f"file:{encoded_path}?mode=ro"

    async def _read_pragma_int(self, name: str) -> int:
        """Fetch a single-int PRAGMA value off the writer connection."""
        conn = self._require_conn()
        async with conn.execute(f"PRAGMA {name}") as cursor:
            row = await cursor.fetchone()
        if row is None:
            raise RuntimeError(f"PRAGMA {name} returned no row")
        return int(row[0])

    async def stop(self) -> None:
        """Close the read-only connection, then the writer."""
        if self._read_conn is not None:
            await self._read_conn.close()
            self._read_conn = None
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    def _require_conn(self) -> aiosqlite.Connection:
        """Return the open WRITER connection; raise if not started."""
        if self._conn is None:
            raise RuntimeError("SqliteUploadStore is not started")
        return self._conn

    def _read_connection(self) -> aiosqlite.Connection:
        """Return the connection every read-only method executes on.

        File-backed stores get the dedicated ``mode=ro`` reader opened by
        :meth:`start`, so reads never queue behind in-flight writes at the
        connection level and never observe the writer's uncommitted
        transaction state. Every read returns a CONSISTENT COMMITTED WAL
        snapshot; when reads overlap on this one connection, SQLite pins
        the connection's read transaction to the oldest still-active
        statement, so an overlapped read may serve a snapshot as-of that
        older read's start (staleness bounded by read duration; admin
        reads are short, and a quiescent read always sees the latest
        commit). ``:memory:`` stores (unit tests) fall back to the writer
        connection: a second connection to ``:memory:`` would be a
        different, empty database.

        Lifecycle posture (cycle-7 task 4.1): during a staged quarantine
        restore the reader, exactly like the writer, keeps its old file
        descriptor until the required process restart; there is NO
        reopen/refresh path. Error classification on this connection uses
        the one shared :func:`is_transient_lock_error` (ADR-023); reads
        add no second classifier and no swallow-and-retry of their own.
        """
        if self._read_conn is not None:
            return self._read_conn
        return self._require_conn()

    @asynccontextmanager
    async def _write_txn(self, conn: aiosqlite.Connection) -> AsyncIterator[None]:
        """Hold the write lock and ROLL BACK the transaction on ANY failure.

        Findings R7-1-D / R7-2-B (Medium-High — service-wide wedge). Every
        write path on the single shared aiosqlite connection issues
        ``execute(...)`` (an implicit ``BEGIN`` under aiosqlite's
        autocommit-off default) and ends with ``await conn.commit()``. If
        EITHER the DML ``execute`` OR the ``commit`` raises — a
        ``sqlite3.OperationalError`` for SQLITE_IOERR (fsync EIO on the WAL)
        or SQLITE_FULL (disk full), the exact B-13 / C-01/C-02 storage-fault
        classes — the implicit transaction is left OPEN on the connection.
        Because admission + sender + reaper + persist-controller all funnel
        through this ONE connection (``self._conn``), the next writer's
        ``BEGIN`` then raises "cannot start a transaction within a
        transaction" and EVERY subsequent writer wedges until restart. That
        is the PostgreSQL fsyncgate cascade realized: a transient I/O error
        on one commit poisons the whole store.

        This context manager makes the transaction self-healing: it acquires
        ``self._write_lock`` (the existing per-connection serialization), and
        on ANY exception leaving the body it issues ``await conn.rollback()``
        before re-raising, clearing the open transaction so the connection
        stays usable. A clean exit does NOT commit — each writer commits
        explicitly as its last in-lock statement (so post-commit reads inside
        the lock are preserved); the CM only guarantees rollback-on-error.

        POSTURE (the maintainer's fsyncgate question). We do NOT
        PANIC-and-restart on SQLITE_IOERR. R7-1/R7-2 PROVED Phantom's
        durability holds under these faults — a failed commit leaves NO
        durable half-commit (a fresh reader sees nothing; ``integrity_check``
        = ``ok``). Unlike PostgreSQL's fsyncgate (where the kernel marked the
        page clean and the half-write was unrecoverable), Phantom can fully
        recover the connection with a rollback, and the fault is transient
        (disk-full self-heals as the reaper/operator frees space; a transient
        EIO may not recur). A hard process exit would needlessly abort
        in-flight durable deliveries. Rollback-and-continue is the correct,
        less-destructive posture given the proven no-half-commit property.

        If the rollback ITSELF raises (e.g. the connection is truly wedged at
        the driver level), we log it and re-raise the ORIGINAL exception so
        the caller sees the real cause, not the rollback's secondary error.
        """
        async with self._write_lock:
            try:
                yield
            except BaseException:
                try:
                    await conn.rollback()
                except Exception:
                    logger.exception(
                        "rollback failed after a write error; the shared "
                        "connection may be wedged (re-raising the original error)"
                    )
                raise

    async def insert(self, row: UploadRow) -> None:
        """Insert a new row."""
        conn = self._require_conn()
        async with self._write_txn(conn):
            await conn.execute(
                """
                INSERT INTO uploads (
                    chain_id, instance_id, group_id, multifile_id, send_order, route_name,
                    state, body_location, attempts, next_attempt_at, received_at,
                    sent_at, updated_at, last_error, endpoint, uid,
                    chain_envelope_json, captured_values_json, current_step_index,
                    idempotency_key, capture_reexecution_active,
                    storage_encoding,
                    body_size_bytes, body_discarded_at,
                    upstream_status_code, upstream_response_headers_json,
                    last_step_completed, body_hashes_json,
                    chain_id_at_ingress
                )
                VALUES (
                    :chain_id, :instance_id, :group_id, :multifile_id, :send_order, :route_name,
                    :state, :body_location, :attempts, :next_attempt_at, :received_at,
                    :sent_at, :updated_at, :last_error, :endpoint, :uid,
                    :chain_envelope_json, :captured_values_json, :current_step_index,
                    :idempotency_key, :capture_reexecution_active,
                    :storage_encoding,
                    :body_size_bytes, :body_discarded_at,
                    :upstream_status_code, :upstream_response_headers_json,
                    :last_step_completed, :body_hashes_json,
                    :chain_id_at_ingress
                )
                """,
                _params_for_insert(row),
            )
            await conn.commit()

    async def insert_with_idempotency_claim(
        self,
        row: UploadRow,
        idempotency_key: str,
    ) -> InsertClaimOutcome:
        """Atomically INSERT upload row AND idempotency claim.

        Returns an :class:`InsertClaimOutcome`:

        * :attr:`~InsertClaimOutcome.INSERTED` — both rows committed.
        * :attr:`~InsertClaimOutcome.IDEMPOTENCY_COLLISION` — the
          ``idempotency_index`` claim already exists for this ingress key
          (admission resolves the existing row → replay or conflict).
        * :attr:`~InsertClaimOutcome.CHAIN_ID_COLLISION` — the
          ``uploads.chain_id`` PRIMARY KEY already exists (finding D-1).

        Either both INSERTs commit or neither does (single SQLite
        transaction). Closes H7 structurally per plan § 2.3.17. The
        ``CHAIN_ID_COLLISION`` arm replaces a previous bare ``raise`` that
        let the IntegrityError escape admission as a naked HTTP 500.

        Uses explicit ``BEGIN`` / ``commit`` / ``rollback`` (Round 3
        B2 — ``async with self._conn:`` is not safe for aiosqlite
        because aiosqlite's context manager protocol does not map to
        SQLite-level transactions).
        """
        conn = self._require_conn()
        async with self._write_lock:
            try:
                await conn.execute("BEGIN")
                # Finding R3-2 root-cause closure: an ``idempotency_index``
                # entry can outlive its ``uploads`` row — the reaper deletes
                # the row (``delete_terminal_older_than``) in one transaction
                # and cleans the index (``cleanup_idempotency_index``) in a
                # later one; admin / bulk deletes never touch the index at
                # all. A claim whose ``chain_id`` is no longer in ``uploads``
                # is ORPHANED: its prior owner is gone, so this submission
                # may take the key. Drop the orphan IN THIS TRANSACTION
                # (single ``_write_lock`` holder ⇒ no concurrent writer ⇒ the
                # read-then-delete is atomic), so the claim INSERT below
                # succeeds and the row is admitted (202) instead of the
                # admission helper crashing on a vanished row. A LIVE claim
                # (its chain_id present in ``uploads``) is left intact, so the
                # claim INSERT collides and IDEMPOTENCY_COLLISION fires for a
                # genuine duplicate only.
                await conn.execute(
                    """
                    DELETE FROM idempotency_index
                     WHERE chain_id_at_ingress = ?
                       AND chain_id NOT IN (SELECT chain_id FROM uploads)
                    """,
                    (idempotency_key,),
                )
                await conn.execute(
                    """
                    INSERT INTO uploads (
                        chain_id, instance_id, group_id, multifile_id, send_order, route_name,
                        state, body_location, attempts, next_attempt_at, received_at,
                        sent_at, updated_at, last_error, endpoint, uid,
                        chain_envelope_json, captured_values_json, current_step_index,
                        idempotency_key, capture_reexecution_active,
                        storage_encoding,
                        body_size_bytes, body_discarded_at,
                        upstream_status_code, upstream_response_headers_json,
                        last_step_completed, body_hashes_json,
                        chain_id_at_ingress
                    )
                    VALUES (
                        :chain_id, :instance_id, :group_id, :multifile_id, :send_order, :route_name,
                        :state, :body_location, :attempts, :next_attempt_at, :received_at,
                        :sent_at, :updated_at, :last_error, :endpoint, :uid,
                        :chain_envelope_json, :captured_values_json, :current_step_index,
                        :idempotency_key, :capture_reexecution_active,
                        :storage_encoding,
                        :body_size_bytes, :body_discarded_at,
                        :upstream_status_code, :upstream_response_headers_json,
                        :last_step_completed, :body_hashes_json,
                        :chain_id_at_ingress
                    )
                    """,
                    _params_for_insert(row),
                )
                await conn.execute(
                    "INSERT INTO idempotency_index (chain_id_at_ingress, chain_id) VALUES (?, ?)",
                    (idempotency_key, str(row.chain_id)),
                )
                await conn.commit()
                return InsertClaimOutcome.INSERTED
            except sqlite3.IntegrityError as exc:
                await conn.rollback()
                msg = str(exc)
                # chain_id PK collision is checked FIRST: the uploads
                # INSERT runs before the idempotency INSERT, so a
                # chain_id collision aborts the transaction with the
                # ``uploads.chain_id`` message and the idempotency row is
                # never attempted (finding D-1).
                if is_chain_id_collision(exc):
                    return InsertClaimOutcome.CHAIN_ID_COLLISION
                if "idempotency_index" in msg:
                    return InsertClaimOutcome.IDEMPOTENCY_COLLISION
                # Foreign-key / check / other unexpected integrity failure
                # — re-raise so the caller's catch-all surfaces it
                # (genuinely unexpected → internal_error is correct here).
                raise
            except BaseException:
                # Findings R7-1-D / R7-2-B: the explicit ``BEGIN`` above means
                # a NON-IntegrityError failure — a ``sqlite3.OperationalError``
                # for SQLITE_IOERR (fsync EIO on the WAL) or SQLITE_FULL (disk
                # full) raised by either the DML ``execute`` OR ``commit`` —
                # must ALSO roll back. The pre-R8 code caught ONLY
                # ``IntegrityError``, so such an OperationalError propagated
                # with the transaction left OPEN, wedging every subsequent
                # writer on the single shared connection ("cannot start a
                # transaction within a transaction"). Roll back to clear the
                # open transaction, then re-raise (admission's catch-all maps
                # it / releases the slot). If the rollback itself fails, log
                # and re-raise the ORIGINAL error. See ``_write_txn`` for the
                # full posture rationale (rollback-and-continue, not PANIC —
                # R7 proved durability holds with no half-commit).
                try:
                    await conn.rollback()
                except Exception:
                    logger.exception(
                        "rollback failed after insert_with_idempotency_claim "
                        "error; the shared connection may be wedged "
                        "(re-raising the original error)"
                    )
                raise

    async def get(self, chain_id: UUID) -> UploadRow | None:
        """Fetch one row by chain_id (read-only connection)."""
        conn = self._read_connection()
        async with conn.execute(
            "SELECT * FROM uploads WHERE chain_id = ?",
            (str(chain_id),),
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            return _row_to_upload(row)

    async def update_state(
        self,
        chain_id: UUID,
        *,
        new_state: UploadState,
        expected_state: UploadState | None = None,
    ) -> bool:
        """Atomic state transition; returns False on contention."""
        conn = self._require_conn()
        now_iso = datetime.now(tz=UTC).isoformat()
        async with self._write_txn(conn):
            if expected_state is None:
                cursor = await conn.execute(
                    "UPDATE uploads SET state = ?, updated_at = ? WHERE chain_id = ?",
                    (new_state, now_iso, str(chain_id)),
                )
            else:
                cursor = await conn.execute(
                    "UPDATE uploads SET state = ?, updated_at = ? WHERE chain_id = ? AND state = ?",
                    (new_state, now_iso, str(chain_id), expected_state),
                )
            await conn.commit()
            return cursor.rowcount > 0

    async def claim_due(self, now: datetime, limit: int) -> list[UploadRow]:
        """Atomic queued → attempting claim for due rows.

        Implemented as a single ``UPDATE ... RETURNING *`` against a
        scoped sub-SELECT. SQLite (≥3.35) executes the UPDATE
        atomically: each row is either flipped to ``attempting`` and
        returned, or not. Concurrent callers see disjoint result sets.
        """
        conn = self._require_conn()
        now_iso = now.isoformat()
        async with self._write_txn(conn):
            async with conn.execute(
                """
                UPDATE uploads
                   SET state = 'attempting', updated_at = ?
                 WHERE chain_id IN (
                     SELECT chain_id FROM uploads
                      WHERE state = 'queued'
                        AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                      ORDER BY next_attempt_at ASC
                      LIMIT ?
                 )
                 RETURNING *
                """,
                (now_iso, now_iso, limit),
            ) as cursor:
                fetched = await cursor.fetchall()
            await conn.commit()
        return [_row_to_upload(r) for r in fetched]

    async def record_attempt_result(
        self,
        chain_id: UUID,
        *,
        new_state: UploadState,
        attempts: int,
        next_attempt_at: datetime | None,
        last_error: str | None,
        upstream_status: int | None,
        upstream_headers_json: str | None,
        captured_values: CapturedValues | None,
        current_step_index: int | None,
        last_step_completed: str | None,
        expected_state: UploadState = "attempting",
        stamp_sent_at: bool = False,
    ) -> int:
        """Persist one attempt's result.

        M-W4-F7 audit closure (Phase 2 § 3.2.8): the UPDATE is guarded
        by ``WHERE state = :expected_state`` — default ``'attempting'``.
        If a concurrent admin ``cancel`` or ``replay`` moved the row
        out of ``attempting`` between the sender's ``claim_due`` and
        the terminal UPDATE here, the UPDATE finds no rows. The caller
        observes rowcount=0 and logs without overwriting.

        ``stamp_sent_at`` (cycle-7 task 2.5): when True, the write-once
        CASE guard stamps ``sent_at = updated_at`` ONLY when the column
        is still NULL. The ONLY caller passing True is the sender's
        chain-done success branch, so ``sent_at`` permanently records
        the moment of first confirmed upstream delivery. The
        ``sent_at IS NULL`` clause is what makes it never-moved: an
        operator ``replay`` resets a succeeded row to queued WITHOUT
        clearing ``sent_at``, so a replayed-then-resucceeded row keeps
        its ORIGINAL stamp.

        Returns the number of rows updated (0 or 1).
        """
        conn = self._require_conn()
        now_iso = datetime.now(tz=UTC).isoformat()
        async with self._write_txn(conn):
            cursor = await conn.execute(
                """
                UPDATE uploads SET
                  state = :state,
                  attempts = :attempts,
                  next_attempt_at = :next_attempt_at,
                  updated_at = :updated_at,
                  sent_at = CASE WHEN :stamp_sent_at AND sent_at IS NULL
                                 THEN :updated_at ELSE sent_at END,
                  last_error = :last_error,
                  upstream_status_code = :upstream_status,
                  upstream_response_headers_json = :upstream_headers_json,
                  captured_values_json = COALESCE(:captured_values_json, captured_values_json),
                  current_step_index = COALESCE(:current_step_index, current_step_index),
                  last_step_completed = COALESCE(:last_step_completed, last_step_completed)
                WHERE chain_id = :chain_id
                  AND state = :expected_state
                """,
                {
                    "chain_id": str(chain_id),
                    "state": new_state,
                    "attempts": attempts,
                    "next_attempt_at": (next_attempt_at.isoformat() if next_attempt_at else None),
                    "updated_at": now_iso,
                    "stamp_sent_at": stamp_sent_at,
                    "last_error": last_error,
                    "upstream_status": upstream_status,
                    "upstream_headers_json": upstream_headers_json,
                    "captured_values_json": (
                        captured_values.model_dump_json() if captured_values is not None else None
                    ),
                    "current_step_index": current_step_index,
                    "last_step_completed": last_step_completed,
                    "expected_state": expected_state,
                },
            )
            await conn.commit()
        return cursor.rowcount

    async def list_uploads(
        self,
        *,
        state: UploadState | None = None,
        route: str | None = None,
        multifile_id: UUID | None = None,
        group_id: UUID | None = None,
        since: datetime | None = None,
        limit: int = 100,
        cursor: str | None = None,
        instance: str | None = None,
    ) -> tuple[list[UploadRow], str | None]:
        """Filter + cursor-paginate rows.

        The ``multifile_id`` filter is special-cased: its results are
        ordered ``send_order ASC`` (recorded position; chain_id as a
        deterministic tiebreak) instead of the receipt-time order, it
        REJECTS the keyset cursor (the two orderings are incompatible
        and a multi-file set is producer-scale small, so pagination has
        no job there), and it never emits a ``next_cursor``. Every other
        filter combination, ``group_id`` included, paginates exactly as
        before.

        Raises:
            ValueError: When ``cursor`` is combined with the
                ``multifile_id`` filter.
        """
        conn = self._read_connection()
        wheres: list[str] = []
        params: list[Any] = []
        if multifile_id is not None and cursor is not None:
            raise ValueError(
                "cursor pagination is not supported with the multifile_id "
                "filter (results are ordered by send_order, not the "
                "cursor's receipt-time keyset)"
            )
        if state is not None:
            wheres.append("state = ?")
            params.append(state)
        if route is not None:
            wheres.append("route_name = ?")
            params.append(route)
        if multifile_id is not None:
            wheres.append("multifile_id = ?")
            params.append(str(multifile_id))
        if group_id is not None:
            wheres.append("group_id = ?")
            params.append(str(group_id))
        if since is not None:
            wheres.append("received_at >= ?")
            params.append(since.isoformat())
        if instance is not None:
            wheres.append("instance_id = ?")
            params.append(instance)
        if cursor is not None:
            decoded = self.decode_resume_cursor(cursor)
            wheres.append("(received_at, chain_id) > (?, ?)")
            params.extend([decoded["received_at"], decoded["chain_id"]])
        where_clause = (" WHERE " + " AND ".join(wheres)) if wheres else ""
        # Default sort: ``received_at`` (always set; ``datetime``) and
        # ``chain_id`` (primary key; ``UUID`` text). ``next_attempt_at`` is
        # nullable (terminal-state rows carry NULL there), so using it as
        # a cursor axis breaks the strict-inequality comparison (SQL
        # ``NULL > x`` is NULL, which silently drops the rest of the page).
        # Receipt time is a stable, non-null total order that's also the
        # natural "show me everything in the buffer" display order. With
        # the multifile filter the recorded position IS the display order.
        order_by = (
            "send_order ASC, chain_id ASC"
            if multifile_id is not None
            else "received_at ASC, chain_id ASC"
        )
        sql = f"SELECT * FROM uploads{where_clause} ORDER BY {order_by} LIMIT ?"
        params.append(limit + 1)
        async with conn.execute(sql, params) as cur:
            fetched = list(await cur.fetchall())
        rows = [_row_to_upload(r) for r in fetched[:limit]]
        next_cursor: str | None = None
        if len(fetched) > limit and multifile_id is None:
            next_cursor = self.build_resume_cursor_for(rows[-1])
        return rows, next_cursor

    @staticmethod
    def build_resume_cursor_for(row: UploadRow) -> str:
        """Build a list_uploads-compatible resume cursor for ``row``.

        The cursor format is a base64-urlsafe-encoded JSON object with
        ``received_at`` (ISO 8601) and ``chain_id`` (str) keys. The
        WHERE clause in :meth:`list_uploads` consumes this cursor via
        ``(received_at, chain_id) > (?, ?)`` so the next query resumes
        precisely after ``row`` in the
        ``ORDER BY received_at ASC, chain_id ASC`` order.

        Co-located with :meth:`list_uploads` because the format is
        load-bearing for that method's WHERE/ORDER pair. Admin
        pagination (``routes/admin.py``) consumes this cursor
        opaquely — it carries the cursor through to the next request
        without inspecting its contents.
        """
        payload = {
            "received_at": row.received_at.isoformat(),
            "chain_id": str(row.chain_id),
        }
        return base64.urlsafe_b64encode(
            json.dumps(payload).encode("utf-8"),
        ).decode("ascii")

    @staticmethod
    def decode_resume_cursor(cursor: str) -> dict[str, str]:
        """Decode a list_uploads-format cursor into the inner key dict.

        Inverse of :meth:`build_resume_cursor_for`. The result has
        ``received_at`` and ``chain_id`` keys as ISO/UUID strings. The
        admin handler passes the decoded inner cursor verbatim back
        into :meth:`list_uploads` via the ``cursor`` parameter — this
        method is the explicit decode point so the format coupling
        lives on the store rather than spreading across callers.
        """
        decoded_bytes = base64.urlsafe_b64decode(cursor.encode("ascii"))
        parsed = json.loads(decoded_bytes)
        if not isinstance(parsed, dict):
            raise ValueError("cursor payload must be an object")
        result: dict[str, str] = {}
        for key in ("received_at", "chain_id"):
            value = parsed.get(key)
            if not isinstance(value, str):
                raise ValueError(f"cursor payload missing required string field {key!r}")
            result[key] = value
        return result

    async def list_by_key_value(
        self,
        key: str,
        value: str,
        *,
        instance: str | None = None,
        limit: int = 100,
    ) -> list[UploadRow]:
        """Find rows by ``metadata.keyValueStore.<key> == value``.

        Uses SQLite JSON1 against ``chain_envelope_json``. The metadata
        is carried in step 1's JSON body and serialized in camelCase
        (the upstream convention - the upstream client's camelCase
        alias generator produces ``keyValueStore`` for the snake-cased
        Python attribute ``key_value_store``). The JSON path therefore queries
        ``$.steps[0].body.value.metadata.keyValueStore.<key>`` (built by
        :func:`_metadata_kvs_json_path`, shared with
        :meth:`find_by_local_uuid`).
        """
        conn = self._read_connection()
        json_path = _metadata_kvs_json_path(key)
        params: list[Any] = [json_path, value]
        sql = "SELECT * FROM uploads WHERE json_extract(chain_envelope_json, ?) = ? "
        if instance is not None:
            sql += "AND instance_id = ? "
            params.append(instance)
        sql += "LIMIT ?"
        params.append(limit)
        async with conn.execute(sql, params) as cur:
            fetched = await cur.fetchall()
        return [_row_to_upload(r) for r in fetched]

    async def list_by_group_id(
        self,
        group_id: UUID,
        *,
        instance: str | None = None,
    ) -> list[UploadRow]:
        """Every row in one query group (indexed equality scan, un-paginated).

        The group-rollup read (cycle-7 task 4.2): a single
        ``WHERE group_id = ?`` served by ``idx_uploads_group_id``. A query
        group is producer-scale bound (a handful to at most a few hundred
        related uploads, the same bound the export per-instance cap
        assumes), so the scan is deliberately UN-paginated; callers that
        need paginated raw rows use :meth:`list_uploads` with its
        ``group_id`` filter instead. Results are ordered
        ``received_at ASC, chain_id ASC`` (the listing display order) so
        the rollup's member list is deterministic.

        Args:
            group_id: The query-grouping handle (NOT NULL on every row;
                admission defaults it to chain_id).
            instance: Optional ``instance_id`` scope within this store.

        Returns:
            All matching rows; empty list when no row carries the id.
        """
        conn = self._read_connection()
        params: list[Any] = [str(group_id)]
        sql = "SELECT * FROM uploads WHERE group_id = ?"
        if instance is not None:
            sql += " AND instance_id = ?"
            params.append(instance)
        sql += " ORDER BY received_at ASC, chain_id ASC"
        async with conn.execute(sql, params) as cur:
            fetched = await cur.fetchall()
        return [_row_to_upload(r) for r in fetched]

    async def find_by_captured_value(
        self,
        capture_name: str,
        subpath: str,
        value: str,
    ) -> list[UploadRow]:
        """Find rows whose captured values carry ``value`` at a bound path.

        The by-captured-id admin lookup (cycle-7 task 4.2): a JSON1
        ``json_extract`` over ``captured_values_json`` following the
        :meth:`list_by_key_value` pattern. The extract path is
        ``$.steps.<capture_name>.values.<subpath>``: ``capture_name`` is
        the capturing step's key under the row's captured-values ``steps``
        map, and ``subpath`` is the dotted path within that step's
        ``values`` map down to the identifier. Both binding values come
        from per-instance deployment configuration
        (``InstanceCfg.admin_lookup``), never from query params and never
        from code, so the service stays upstream-ignorant.

        Un-indexed by design: O(rows-per-instance) at producer scale,
        exactly how :meth:`list_by_key_value` already runs. The named
        escalation, if volume ever demands it, is a generated column plus
        index, deliberately not built now.

        Args:
            capture_name: Key under ``$.steps`` in the captured-values
                JSON (the capturing step's name). A SINGLE label, quoted
                via :func:`_quote_json_path_label` so step names with
                path-special characters cannot corrupt the path.
            subpath: Dotted path under that step's ``values`` map to the
                identifier field. A multi-segment PATH by contract
                (``AdminLookupCfg.json_path``), interpolated as-is.
            value: The identifier value to match (TEXT equality).

        Returns:
            All matching rows; empty list on a miss (the route maps a
            miss to ``found=false``, not 404).
        """
        conn = self._read_connection()
        json_path = f"$.steps.{_quote_json_path_label(capture_name)}.values.{subpath}"
        sql = "SELECT * FROM uploads WHERE json_extract(captured_values_json, ?) = ?"
        async with conn.execute(sql, [json_path, value]) as cur:
            fetched = await cur.fetchall()
        return [_row_to_upload(r) for r in fetched]

    async def find_by_local_uuid(self, local_uuid: UUID) -> list[UploadRow]:
        """Find rows stamped with ``local_uuid`` in their metadata KVS.

        The by-local-uuid admin lookup (cycle-7 task 4.2): the SAME JSON1
        extract :meth:`list_by_key_value` runs, with the key PINNED to
        :data:`PHANTOM_LOCAL_UUID_METADATA_KEY` (the key the producer-side
        adapter writes). Callers never spell a path; the path is built by
        the shared :func:`_metadata_kvs_json_path` so this lookup and the
        generic key-value match can never drift.

        Un-indexed by design, same posture and escalation path as
        :meth:`find_by_captured_value`; this pinned key is the
        better-behaved candidate if either lookup is ever promoted to an
        indexed expression column.

        Args:
            local_uuid: The producer-minted correlation uuid. Matched as
                its canonical string form (TEXT equality).

        Returns:
            All matching rows; empty list on a miss. A list because
            Phantom enforces no global uniqueness on the key.
        """
        conn = self._read_connection()
        json_path = _metadata_kvs_json_path(PHANTOM_LOCAL_UUID_METADATA_KEY)
        sql = "SELECT * FROM uploads WHERE json_extract(chain_envelope_json, ?) = ?"
        async with conn.execute(sql, [json_path, str(local_uuid)]) as cur:
            fetched = await cur.fetchall()
        return [_row_to_upload(r) for r in fetched]

    async def list_non_terminal(self) -> list[UploadRow]:
        """Every non-terminal row (read-only connection)."""
        conn = self._read_connection()
        placeholders = ",".join("?" * len(TERMINAL_STATES))
        sql = f"SELECT * FROM uploads WHERE state NOT IN ({placeholders})"
        async with conn.execute(sql, tuple(TERMINAL_STATES)) as cur:
            fetched = await cur.fetchall()
        return [_row_to_upload(r) for r in fetched]

    async def counts_by_state(self) -> dict[UploadState, StateTally]:
        """Row count and summed body bytes per state, in one read.

        A read-only ``GROUP BY state`` over the whole ``uploads`` table
        (no ``_write_txn``; mirrors the :meth:`list_non_terminal` read
        idiom). States with zero rows do not appear in the result, so the
        returned mapping omits them; callers default a missing state to
        ``StateTally(0, 0)``.

        A state value read back from SQLite that is not in the known
        :data:`UploadState` vocabulary would mean a corrupt/foreign row;
        it is logged at WARNING and skipped rather than crashing the
        stats read.
        """
        conn = self._read_connection()
        sql = (
            "SELECT state, COUNT(*) AS cnt, COALESCE(SUM(body_size_bytes), 0) AS total_bytes "
            "FROM uploads GROUP BY state"
        )
        async with conn.execute(sql) as cur:
            fetched = await cur.fetchall()
        tallies: dict[UploadState, StateTally] = {}
        for row in fetched:
            state_value = row["state"]
            if state_value not in _VALID_UPLOAD_STATES:
                logger.warning(
                    "counts_by_state: skipping unrecognized state %r (%d rows)",
                    state_value,
                    int(row["cnt"]),
                )
                continue
            tallies[state_value] = StateTally(
                count=int(row["cnt"]),
                bytes=int(row["total_bytes"]),
            )
        return tallies

    async def list_all_chain_ids(self) -> list[UUID]:
        """Return every chain_id in this store regardless of state.

        Returns the chain_id population the reaper folds into
        ``cleanup_idempotency_index``'s preserve set (plan § 2.3.16).
        """
        conn = self._read_connection()
        async with conn.execute("SELECT chain_id FROM uploads") as cur:
            fetched = await cur.fetchall()
        return [UUID(row["chain_id"]) for row in fetched]

    async def list_chain_ids(self) -> list[UUID]:
        """Alias of :meth:`list_all_chain_ids` named for the orphan janitor.

        The body-orphan janitor (plan § 2.3.14) reads this
        to build the "known chain_ids" set passed to
        ``BodyStore.list_orphans``. Same query as
        :meth:`list_all_chain_ids`; the rename clarifies intent at the
        janitor call site.
        """
        return await self.list_all_chain_ids()

    async def reset_attempting_to_queued(self) -> int:
        """Recovery sweep — reset stuck ``attempting`` rows."""
        conn = self._require_conn()
        now_iso = datetime.now(tz=UTC).isoformat()
        async with self._write_txn(conn):
            cursor = await conn.execute(
                "UPDATE uploads SET state = 'queued', updated_at = ? WHERE state = 'attempting'",
                (now_iso,),
            )
            await conn.commit()
        return cursor.rowcount

    async def mark_persisted(self, chain_id: UUID) -> int:
        """Flip body_location from 'ram' to 'file'.

        SOLE writer of this transition (plan § 0.5 single-writer
        manifest invariant #6). Called by the PersistController
        (plan § 2.3.11) after fsync of the body file(s)
        and their parent directory completes. Two ``WHERE`` guards:

        * ``body_location = 'ram'`` is defensive — a duplicate call
          after an unrelated race is a no-op rather than a silent
          over-write.
        * ``body_discarded_at IS NULL`` is the H4 carve-out (R7-2): a
          migration that raced the reaper's body-discard must NOT flip
          the row to ``'file'`` and resurrect policy-discarded bytes
          (every other H4 consumer — recovery, the InvariantAuditor,
          replay, the AuthKicker — already guards the stamp).

        Returns:
            The UPDATE rowcount: 1 when the flip committed, 0 when a
            guard refused it. The PersistController uses 0 to undo a
            disk write that raced the discard.
        """
        conn = self._require_conn()
        now_iso = datetime.now(tz=UTC).isoformat()
        async with self._write_txn(conn):
            cursor = await conn.execute(
                "UPDATE uploads SET body_location = 'file', updated_at = ? "
                "WHERE chain_id = ? AND body_location = 'ram' "
                "AND body_discarded_at IS NULL",
                (now_iso, str(chain_id)),
            )
            await conn.commit()
        return cursor.rowcount

    async def mark_corrupted(self, chain_id: UUID, reason: str) -> None:
        """Quarantine a NON-terminal row in the terminal ``corrupted`` state.

        Sole caller is recovery's body-integrity guard
        (plan § 2.3.15), invoked when a still-deliverable row's body
        files vanished between persist and process restart. (The
        sender's ADR-014 body-hash-mismatch path quarantines via
        :meth:`record_attempt_result` with ``new_state='corrupted'``,
        not this method.) Sets ``state='corrupted'``,
        ``last_error=reason``, clears ``next_attempt_at`` (terminal
        rows never advance on their own), and updates ``updated_at``.

        TERMINAL-STATE GUARD (finding R9-PM-3): the ``WHERE`` clause
        excludes :data:`TERMINAL_STATES`, so a quarantine attempt on an
        already-terminal row (e.g. a ``succeeded`` row whose body was
        deleted on delivery) is a safe no-op (rowcount=0) rather than
        silently overwriting the finished row's state. Recovery's own
        terminal-skip is the primary defense; this is belt-and-suspenders
        against any future caller (the unconditional overwrite was the
        footgun that let R9-PM-3 destroy delivered ``succeeded`` records).
        """
        conn = self._require_conn()
        now_iso = datetime.now(tz=UTC).isoformat()
        # Mirrors the ``list_non_terminal`` idiom above: static '?' tokens,
        # one per terminal state, with the states bound as parameters.
        placeholders = ",".join("?" * len(TERMINAL_STATES))
        async with self._write_txn(conn):
            await conn.execute(
                f"""
                UPDATE uploads
                   SET state = 'corrupted',
                       last_error = ?,
                       next_attempt_at = NULL,
                       updated_at = ?
                 WHERE chain_id = ?
                   AND state NOT IN ({placeholders})
                """,
                (reason, now_iso, str(chain_id), *tuple(TERMINAL_STATES)),
            )
            await conn.commit()

    async def iter_rows(self) -> AsyncIterator[UploadRow]:
        """Stream every row via a cursor on the read-only connection.

        Used by recovery and the invariant-audit
        coroutine (Phase 3). The cursor stays open for the duration of
        iteration; callers MUST consume promptly.

        WRITE-DURING-WALK posture (V1/V2 history, revised by cycle-7
        task 4.1): the walk used to hold its ``SELECT`` cursor on the
        single shared connection, where a concurrent write over a
        SIGKILL-hot WAL could trigger an in-line checkpoint that
        collided with the open cursor (``SQLITE_LOCKED``). The walk now
        runs on the DEDICATED read-only connection, so a concurrent
        write cannot collide with the cursor; the open read snapshot
        merely pins the WAL from being checkpointed past it until the
        walk finishes (benign). The collect-targets-then-write
        discipline in :func:`phantom.workers.recovery.run_recovery` is
        retained as good hygiene: the walk sees ONE consistent snapshot
        and the writes land after it, which keeps recovery's sweep
        semantics easy to reason about.
        """
        conn = self._read_connection()
        async with conn.execute("SELECT * FROM uploads") as cursor:
            async for row in cursor:
                yield _row_to_upload(row)

    async def list_oldest_ram_bodies(self, limit: int) -> list[UUID]:
        """Return the oldest ``body_location='ram'`` chain_ids.

        Used by the RAM-pressure watcher (plan § 2.3.12)
        to pick migration candidates when RAM pressure breaches the
        ceiling. Ordered by ``received_at`` ASC; capped at ``limit``.
        Returns an empty list when no RAM-tier rows remain.
        """
        conn = self._read_connection()
        async with conn.execute(
            """
            SELECT chain_id FROM uploads
             WHERE body_location = 'ram'
             ORDER BY received_at ASC
             LIMIT ?
            """,
            (limit,),
        ) as cur:
            fetched = await cur.fetchall()
        return [UUID(row["chain_id"]) for row in fetched]

    async def find_by_chain_id_at_ingress(self, chain_id_at_ingress: str) -> UUID | None:
        """Return the chain_id of any row matching the admission ingress key.

        Admission-side dedup fallback for the case where the
        ``idempotency_index`` row was reaped (or a buggy cleanup
        pruned it mid-retention) but the upload row is still live.
        Matches against the row's ``chain_id_at_ingress`` column —
        the producer-supplied ``X-Phantom-Idempotency-Key`` captured at
        admission. Distinct from ``idempotency_key`` (the envelope
        field that Phantom forwards to upstream).

        Returns the first matching chain_id or ``None`` if no row has
        that ingress key. With the Phase-1 single-store collapse the
        old "cross-tier fallback" phrasing is gone — there is just one
        store to scan.
        """
        conn = self._read_connection()
        async with conn.execute(
            "SELECT chain_id FROM uploads WHERE chain_id_at_ingress = ? LIMIT 1",
            (chain_id_at_ingress,),
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return None
        return UUID(row["chain_id"])

    async def claim_idempotency(
        self,
        chain_id_at_ingress: str,
        chain_id: UUID,
    ) -> UUID:
        """INSERT-OR-IGNORE; returns the surviving chain_id for the key.

        The non-atomic admission path; admission uses
        :meth:`insert_with_idempotency_claim`
        for the H7 structural closure.
        """
        conn = self._require_conn()
        async with self._write_txn(conn):
            await conn.execute(
                """
                INSERT OR IGNORE INTO idempotency_index (chain_id_at_ingress, chain_id)
                VALUES (?, ?)
                """,
                (chain_id_at_ingress, str(chain_id)),
            )
            await conn.commit()
        async with conn.execute(
            "SELECT chain_id FROM idempotency_index WHERE chain_id_at_ingress = ?",
            (chain_id_at_ingress,),
        ) as cur:
            row = await cur.fetchone()
        if row is None:  # pragma: no cover — INSERT OR IGNORE just ran
            raise RuntimeError(
                f"idempotency_index missing row for {chain_id_at_ingress}",
            )
        return UUID(row["chain_id"])

    async def cleanup_idempotency_index(
        self,
        *,
        preserve_chain_ids: Iterable[UUID] = (),
    ) -> int:
        """Drop idempotency rows whose linked upload no longer exists.

        With one store the ``preserve_chain_ids`` parameter carves
        out chain_ids the caller knows are still live but absent
        from ``uploads`` (plan § 2.3.16).

        Args:
            preserve_chain_ids: Chain ids the caller knows are still
                live but absent from this store's ``uploads`` table.

        Returns:
            The count of idempotency rows deleted.
        """
        conn = self._require_conn()
        preserve_strs = tuple({str(cid) for cid in preserve_chain_ids})
        async with self._write_txn(conn):
            if preserve_strs:
                placeholders = ",".join("?" * len(preserve_strs))
                sql = (
                    "DELETE FROM idempotency_index "
                    " WHERE chain_id NOT IN (SELECT chain_id FROM uploads) "
                    f"  AND chain_id NOT IN ({placeholders})"
                )
                cursor = await conn.execute(sql, preserve_strs)
            else:
                cursor = await conn.execute(
                    """
                    DELETE FROM idempotency_index
                     WHERE chain_id NOT IN (SELECT chain_id FROM uploads)
                    """,
                )
            await conn.commit()
        return cursor.rowcount

    async def delete(self, chain_id: UUID) -> DeletedRowAccounting | None:
        """Hard delete one row; return its accounting view (R8-4).

        The accounting columns are captured in the same write
        transaction as the DELETE so the caller's gate release cannot
        race a concurrent transition.
        """
        conn = self._require_conn()
        async with self._write_txn(conn):
            async with conn.execute(
                "SELECT chain_id, state, body_size_bytes, body_discarded_at "
                "FROM uploads WHERE chain_id = ?",
                (str(chain_id),),
            ) as cur:
                fetched = await cur.fetchone()
            await conn.execute("DELETE FROM uploads WHERE chain_id = ?", (str(chain_id),))
            await conn.commit()
        if fetched is None:
            return None
        return _accounting_from_sql_row(fetched)

    async def replay(self, chain_id: UUID) -> ReplayOutcome | None:
        """Reset the row to a from-step-0 restart.

        Replay is restart-from-step-0: the chain re-runs every step
        from the beginning. Resetting only ``state`` / ``attempts``
        leaves ``current_step_index`` and ``captured_values_json``
        pointing at the previous run, so the sender resumes against
        an expired capture and re-routes the row right back to
        ``stored``. The fix is to wipe every per-attempt field so the
        next ``claim_due`` sees a fresh chain.

        M-W4-F7 audit closure (Phase 2 § 3.2.8): the UPDATE is guarded
        by ``state IN ('succeeded','failed','corrupted','cancelled',
        'queued','auth_expired','stored')`` — every state EXCEPT
        ``attempting``. A sender is actively driving an ``attempting``
        row; replay must refuse rather than clobber the sender's
        in-flight work. Round 1 defender fix (R1-1): the refusal is now
        the typed :class:`ReplayRefusedAttemptingError`, raised from the
        same in-write-lock precheck as the body-accounting refusal so
        the state read is authoritative; the app-registered handler
        converts it into the canonical 409 ``replay_refused_attempting``
        envelope. ``None`` now means exactly one thing: the row does not
        exist (deleted between the route's lookup and this lock), and
        the route answers 404.

        Body-accounting refusal (cycle-7 phase 7 pre-round defender
        fix): a row whose ``body_discarded_at`` is stamped has no bytes
        left to send (the one discard owner,
        :meth:`discard_body_and_zero_accounting`, deleted the body
        files and zeroed ``body_size_bytes`` on the same write, on
        either the sender's immediate leg or the reaper's scheduled
        leg). Re-queuing it would land the row in ``corrupted`` on the
        sender's next claim, so replay refuses UP FRONT by raising
        :class:`ReplayBodyDiscardedError` and leaves the row untouched.
        The check runs inside the write transaction, transactionally
        coupled to the UPDATE (the ``bulk_delete`` in-lock SELECT
        pattern), so a reaper sweep cannot stamp the row between the
        check and the re-queue. Rows still holding bodies (stamp NULL)
        replay exactly as before.

        Raises:
            ReplayBodyDiscardedError: When ``body_discarded_at`` is
                stamped on the row. Carries the chain_id, the row's
                instance_id, and the discard timestamp; the admin
                handler converts it into the canonical 409
                ``replay_body_discarded`` envelope. Checked FIRST: a
                stamped row refuses in every state, including
                ``attempting`` (the pinned precedence).
            ReplayRefusedAttemptingError: When the row is in
                ``attempting``. Carries the chain_id and the row's
                instance_id; the admin handler converts it into the
                canonical 409 ``replay_refused_attempting`` envelope
                (round 1 defender fix, R1-1).
        """
        conn = self._require_conn()
        now_iso = datetime.now(tz=UTC).isoformat()
        async with self._write_txn(conn):
            async with conn.execute(
                "SELECT instance_id, state, body_discarded_at FROM uploads WHERE chain_id = ?",
                (str(chain_id),),
            ) as precheck:
                accounting = await precheck.fetchone()
            if accounting is None:
                # The row vanished (deleted) before this lock was taken;
                # the route answers the same 404 as its up-front lookup
                # miss. None means exactly "row missing".
                return None
            if accounting["body_discarded_at"]:
                raise ReplayBodyDiscardedError(
                    chain_id=chain_id,
                    instance_id=str(accounting["instance_id"]),
                    body_discarded_at=datetime.fromisoformat(accounting["body_discarded_at"]),
                )
            if accounting["state"] == "attempting":
                raise ReplayRefusedAttemptingError(
                    chain_id=chain_id,
                    instance_id=str(accounting["instance_id"]),
                )
            await conn.execute(
                """
                UPDATE uploads
                   SET state = 'queued',
                       attempts = 0,
                       next_attempt_at = ?,
                       updated_at = ?,
                       last_error = NULL,
                       current_step_index = 0,
                       captured_values_json = ?,
                       last_step_completed = NULL,
                       upstream_status_code = NULL,
                       upstream_response_headers_json = NULL
                 WHERE chain_id = ?
                   AND state IN (
                       'succeeded', 'failed', 'corrupted', 'cancelled',
                       'queued', 'auth_expired', 'stored'
                   )
                """,
                (now_iso, now_iso, CapturedValues().model_dump_json(), str(chain_id)),
            )
            await conn.commit()
        # The in-lock precheck guarantees the row exists and is in one of
        # the seven non-attempting states, all of which the UPDATE's state
        # predicate covers, so the write cannot have missed.
        row = await self.get(chain_id)
        if row is None:
            raise KeyError(f"No upload row for chain_id={chain_id}")
        # R9-4: the in-transaction pre-state is the route's gate
        # reconciliation input; a route-side pre-fetch races the kicker
        # wake and the sender's terminal transitions in both directions.
        return ReplayOutcome(row=row, previous_state=accounting["state"])

    async def cancel(self, chain_id: UUID) -> CancelOutcome:
        """Transition to ``cancelled`` if non-terminal.

        The pre-UPDATE state is captured inside the same write
        transaction (R8-4): it is the state the row was actually
        cancelled FROM, so the route's gate-release decision cannot
        race a concurrent sender/kicker transition. ``previous_state``
        is ``None`` when the row was already terminal (no transition).
        """
        conn = self._require_conn()
        now_iso = datetime.now(tz=UTC).isoformat()
        async with self._write_txn(conn):
            async with conn.execute(
                "SELECT state FROM uploads WHERE chain_id = ?",
                (str(chain_id),),
            ) as cur:
                fetched = await cur.fetchone()
            cursor = await conn.execute(
                """
                UPDATE uploads
                   SET state = 'cancelled', updated_at = ?
                 WHERE chain_id = ?
                   AND state IN ('queued','attempting','auth_expired','stored')
                """,
                (now_iso, str(chain_id)),
            )
            await conn.commit()
        previous_state = fetched["state"] if fetched is not None and cursor.rowcount == 1 else None
        row = await self.get(chain_id)
        if row is None:
            raise KeyError(f"No upload row for chain_id={chain_id}")
        return CancelOutcome(row=row, previous_state=previous_state)

    async def bulk_delete(
        self,
        *,
        state: UploadState | None = None,
        route: str | None = None,
        since: datetime | None = None,
        instance: str | None = None,
    ) -> list[DeletedRowAccounting]:
        """Bulk delete by filter. ADR-004 — refuses empty filter.

        Returns one :class:`DeletedRowAccounting` per deleted row. The
        admin route iterates these to delete the corresponding bodies in
        the instance's body store (C1 audit closure — bodies were
        previously leaked until the orphan janitor's next sweep) and to
        release the saturation gate for rows that still held a slot
        (R8-4).

        The accounting rows are collected inside the write lock
        alongside the DELETE so the read-then-delete pair is atomic
        against concurrent admission of a row whose key would have
        matched the filter.
        """
        if state is None and route is None and since is None and instance is None:
            raise ValueError("Bulk delete requires at least one filter field")
        conn = self._require_conn()
        wheres: list[str] = []
        params: list[Any] = []
        if state is not None:
            wheres.append("state = ?")
            params.append(state)
        if route is not None:
            wheres.append("route_name = ?")
            params.append(route)
        if since is not None:
            wheres.append("received_at >= ?")
            params.append(since.isoformat())
        if instance is not None:
            wheres.append("instance_id = ?")
            params.append(instance)
        where_clause = " AND ".join(wheres)
        select_sql = (
            "SELECT chain_id, state, body_size_bytes, body_discarded_at "
            f"FROM uploads WHERE {where_clause}"
        )
        delete_sql = f"DELETE FROM uploads WHERE {where_clause}"
        async with self._write_txn(conn):
            async with conn.execute(select_sql, params) as cur:
                fetched = await cur.fetchall()
            deleted = [_accounting_from_sql_row(row) for row in fetched]
            await conn.execute(delete_sql, params)
            await conn.commit()
        return deleted

    async def discard_body_and_zero_accounting(
        self, chain_id: UUID, *, expected_state: UploadState
    ) -> DiscardOutcome:
        """Record a body discard: stamp the time AND zero the size accounting.

        The ONE owner of the row-side body-discard effect (cycle-7 task
        4.7, findings D9/D10), with BOTH effects in the name because both
        are load-bearing:

        * stamps ``body_discarded_at`` (recovery's integrity guard skips
          stamped rows instead of quarantining them, and the reaper's
          body-discard pass uses the stamp to never re-process a row);
        * zeroes ``body_size_bytes`` (load-bearing for saturation
          accounting: stats and any size aggregate over rows must not
          count bytes that no longer exist on disk or in RAM).

        R9-5 confirm-then-act: the UPDATE is guarded in-transaction on
        ``state = expected_state AND body_discarded_at IS NULL``, and
        the pre-zero size is captured in the same transaction. A
        cross-owner sweep acting on its snapshot therefore cannot
        discard a row that a replay or kicker wake revived mid-sweep
        (the guard mismatches; nothing is touched), a row another path
        already stamped or removed is a no-op, and the returned size is
        the exact release basis for a ``stored`` row's slot. Exactly
        TWO callers, split by configuration - the sender's immediate
        discard on the chain-done success branch
        (``succeeded_body_seconds == 0``) and the reaper's scheduled
        discard (non-zero window elapsed) - and BOTH stamp first,
        deleting body files only after a confirmed flip (R9-5 for the
        reaper, R10-1 for the sender); a crash in between leaves a
        stamped row whose files the metadata pass and the orphan
        janitor converge.
        """
        conn = self._require_conn()
        now_iso = datetime.now(tz=UTC).isoformat()
        async with self._write_txn(conn):
            async with conn.execute(
                "SELECT body_size_bytes FROM uploads "
                "WHERE chain_id = ? AND state = ? AND body_discarded_at IS NULL",
                (str(chain_id), expected_state),
            ) as cur:
                fetched = await cur.fetchone()
            if fetched is None:
                await conn.commit()
                return DiscardOutcome(flipped=False, body_size_bytes=0)
            await conn.execute(
                """
                UPDATE uploads
                   SET body_discarded_at = ?,
                       body_size_bytes = 0
                 WHERE chain_id = ? AND state = ? AND body_discarded_at IS NULL
                """,
                (now_iso, str(chain_id), expected_state),
            )
            await conn.commit()
        return DiscardOutcome(flipped=True, body_size_bytes=int(fetched["body_size_bytes"]))

    async def vacuum(self) -> None:
        """Run ``VACUUM`` on the store.

        Reclaims free pages and rebuilds the database file. Acquired
        under the write lock so concurrent writes wait until the
        VACUUM completes (SQLite VACUUM itself requires exclusive
        access; the lock just keeps Phantom's own writers honest).

        ``:memory:`` stores are exempt — VACUUM on an in-memory
        database is a no-op in SQLite but holding the write lock
        through it serves no purpose. The check on ``db_path`` is the
        right shape now that the ``tier`` attribute is gone.
        """
        if self._db_path == ":memory:":
            return
        conn = self._require_conn()
        async with self._write_lock:
            await conn.execute("VACUUM;")
            await conn.commit()

    async def delete_terminal_older_than(
        self,
        state: UploadState,
        cutoff: datetime,
    ) -> list[DeletedRowAccounting]:
        """Reaper helper — delete terminal rows older than ``cutoff``.

        Returns per-row accounting captured in the same transaction so
        the reaper can release the gate for ``stored`` rows whose body
        was never separately discarded (R8-4).
        """
        if state not in TERMINAL_STATES and state != "auth_expired":
            raise ValueError(f"Cannot bulk-delete non-terminal state {state!r}")
        conn = self._require_conn()
        async with self._write_txn(conn):
            async with conn.execute(
                "SELECT chain_id, state, body_size_bytes, body_discarded_at "
                "FROM uploads WHERE state = ? AND updated_at < ?",
                (state, cutoff.isoformat()),
            ) as cur:
                fetched = await cur.fetchall()
            deleted = [_accounting_from_sql_row(row) for row in fetched]
            await conn.execute(
                "DELETE FROM uploads WHERE state = ? AND updated_at < ?",
                (state, cutoff.isoformat()),
            )
            await conn.commit()
        return deleted

    async def list_terminal_older_than(
        self,
        state: UploadState,
        cutoff: datetime,
    ) -> list[UploadRow]:
        """Reaper helper — list rows whose body should be discarded."""
        conn = self._read_connection()
        async with conn.execute(
            """
            SELECT * FROM uploads
             WHERE state = ?
               AND body_discarded_at IS NULL
               AND updated_at < ?
            """,
            (state, cutoff.isoformat()),
        ) as cur:
            fetched = await cur.fetchall()
        return [_row_to_upload(r) for r in fetched]

    async def evict_terminal_over_limit(self, max_rows: int) -> list[DeletedRowAccounting]:
        """Reaper helper (V3) — count-cap backstop on the ``uploads`` table.

        Enforces ``retention.max_rows`` as a hard cap AFTER the reaper's
        time-based passes. When the table holds more than ``max_rows`` rows,
        deletes the oldest-DONE-first (ordered by ``updated_at`` ASC) until the
        total row count is at or below the cap, and returns the deleted
        chain_ids so the caller can delete their bodies + trim the idempotency
        index. ``max_rows < 0`` is "unbounded" — a no-op (returns ``[]``),
        preserving the historical time-only retention contract.

        DURABILITY (invariant #1 — "no upload lost while running normally"):
        ONLY fully-terminal rows (:data:`TERMINAL_STATES` —
        succeeded/failed/cancelled/stored/corrupted) are eligible for eviction.
        In-flight rows (queued/attempting) and still-deliverable ``auth_expired``
        rows (the auth_kicker re-queues those on token refresh) are NEVER
        evicted, so the backstop cannot drop an undelivered upload. If the table
        is over the cap but the overage is all ineligible rows, the cap is left
        unmet (fewer than the full overage are deleted) — durability wins over
        the count cap; the caller logs the shortfall.

        The count → select-oldest → delete sequence runs inside a single
        ``_write_lock`` hold so it is atomic against concurrent admission (a row
        admitted between the count and the delete cannot be mis-evicted, and the
        eligible-set membership is consistent).
        """
        if max_rows < 0:
            return []
        conn = self._require_conn()
        placeholders = ",".join("?" * len(TERMINAL_STATES))
        async with self._write_txn(conn):
            async with conn.execute("SELECT COUNT(*) FROM uploads") as cur:
                count_row = await cur.fetchone()
            total = int(count_row[0]) if count_row is not None else 0
            overage = total - max_rows
            if overage <= 0:
                return []
            # Oldest-DONE-first among the terminal (evictable) states only.
            select_sql = (
                "SELECT chain_id, state, body_size_bytes, body_discarded_at "
                f"FROM uploads WHERE state IN ({placeholders}) "
                "ORDER BY updated_at ASC LIMIT ?"
            )
            async with conn.execute(select_sql, (*TERMINAL_STATES, overage)) as cur:
                fetched = await cur.fetchall()
            deleted = [_accounting_from_sql_row(row) for row in fetched]
            if not deleted:
                return []
            id_placeholders = ",".join("?" * len(deleted))
            await conn.execute(
                f"DELETE FROM uploads WHERE chain_id IN ({id_placeholders})",
                tuple(str(entry.chain_id) for entry in deleted),
            )
            await conn.commit()
        return deleted
