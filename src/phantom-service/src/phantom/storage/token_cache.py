"""SQLite-backed token cache (ADR-002 / ADR-003).

The cache is keyed by ``(endpoint, uid)``; bearer values persist to disk
so they survive Phantom restart. Bad tokens stay in the cache with
``status='bad'`` rather than being deleted (ADR-003).

The cache lives in its OWN database file (production wires
``<instance data_root>/token_cache.db``, see ``app.py``), deliberately
separate from ``uploads.db``: SQLite serializes writers per database
file, so the split keeps token reads and writes off the hot uploads
writer lock, and a token is shared across many uploads anyway.

Admin reads use :class:`TokenSlot` which has no bearer field (ADR-004).

**``set``'s return value is a dead contract** (finding S9-7). All seven call
sites across both auth stores discard it. It survives only because the
``TokenCache`` Protocol in :mod:`phantom.storage.interface` declares it, and
the two must change together; until then the value it returns describes the
write it committed rather than a re-read that could report someone else's.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Final

import aiosqlite

from phantom.config.settings import SqliteCfg
from phantom.models.admin import TokenSlot
from phantom.models.token import TokenCacheRow, TokenSource, TokenStatus
from phantom.storage._connection import open_store_connection
from phantom.storage.interface import WakeHandler

logger = logging.getLogger(__name__)

# The status every :meth:`SqliteTokenCache.set` forces. Named once because it
# appears twice - in the UPSERT and in the row that call returns - and a
# refreshed bearer un-badding the slot is what wakes parked rows, so the two
# must not be able to drift apart.
_FRESH_STATUS: Final[TokenStatus] = "fresh"


def _row_to_cache_row(row: aiosqlite.Row) -> TokenCacheRow:
    """Decode one SQLite row into a :class:`TokenCacheRow`."""
    return TokenCacheRow(
        endpoint=row["endpoint"],
        uid=row["uid"],
        bearer=row["bearer"],
        observed_at=datetime.fromisoformat(row["observed_at"]),
        source=row["source"],
        status=row["status"],
    )


def _row_to_slot(row: aiosqlite.Row) -> TokenSlot:
    """Decode one SQLite row into a :class:`TokenSlot` (no bearer)."""
    return TokenSlot(
        endpoint=row["endpoint"],
        uid=row["uid"],
        last_updated=datetime.fromisoformat(row["observed_at"]),
        status=row["status"],
    )


class SqliteTokenCache:
    """Disk-tier SQLite token cache.

    Optional ``sqlite_cfg`` carries the parameterized ``busy_timeout_ms``
    pragma value (shared with :class:`SqliteUploadStore`). When omitted the
    default matches :class:`SqliteCfg` so unit tests need no explicit Settings
    to exercise the cache; production threads ``settings.storage.sqlite``.
    """

    def __init__(self, db_path: str, *, sqlite_cfg: SqliteCfg | None = None) -> None:
        """Construct a cache rooted at ``db_path`` (a SQLite file path).

        Args:
            db_path: SQLite path for the token-cache table.
            sqlite_cfg: Pragma configuration. When ``None`` the
                :class:`SqliteCfg` defaults apply (the ``busy_timeout_ms``
                default in particular).
        """
        self._db_path = db_path
        self._cfg = sqlite_cfg
        self._conn: aiosqlite.Connection | None = None
        self._wake_handlers: list[WakeHandler] = []
        # Every write path on a shared aiosqlite connection must atomicize
        # its ``execute`` / ``commit`` pair so concurrent coroutines don't
        # race the transaction state. The lock stays per store because it
        # guards THIS store's connection.
        self._write_lock = asyncio.Lock()

    async def start(self) -> None:
        """Open the cache's own database file and apply its DDL.

        The connection and its four durability pragmas come from the shared
        opener (U1); the DDL below stays here, because ADR-030 makes each
        store's own schema deliberate.
        """
        self._conn = await open_store_connection(self._db_path, self._cfg)
        # The cache owns this DDL: production wires the cache at its own
        # token_cache.db (two databases per instance by design; see the
        # module docstring). schema.sql declares the same table inside
        # uploads.db; that copy sits empty in production, and the
        # duplication is deliberate. A change here must land there too.
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS token_cache (
                endpoint        TEXT NOT NULL,
                uid             TEXT NOT NULL,
                bearer          TEXT NOT NULL,
                observed_at     TEXT NOT NULL,
                source          TEXT NOT NULL,
                status          TEXT NOT NULL,
                PRIMARY KEY (endpoint, uid)
            )
            """,
        )
        await self._conn.commit()

    async def stop(self) -> None:
        """Close the connection."""
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    def _require_conn(self) -> aiosqlite.Connection:
        """Return the open connection or raise."""
        if self._conn is None:
            raise RuntimeError("SqliteTokenCache is not started")
        return self._conn

    @asynccontextmanager
    async def _write_txn(self, conn: aiosqlite.Connection) -> AsyncIterator[None]:
        """Hold the write lock and ROLL BACK on ANY failure.

        Findings R7-1-D / R7-2-B applied to the token cache's OWN aiosqlite
        connection. Same hazard as ``SqliteUploadStore._write_txn``: a
        SQLITE_IOERR / SQLITE_FULL from a write ``execute`` or ``commit``
        would otherwise leave the transaction open and wedge every
        subsequent token write (``set`` / ``mark_bad`` / ``delete``). The
        token cache is on the auth-refresh hot path (the auth-kicker writes
        refreshed bearers here), so a wedge here strands ``auth_expired``
        rows. Roll back on error to keep the connection self-healing; see the
        store's ``_write_txn`` for the full rollback-not-PANIC rationale.
        """
        async with self._write_lock:
            try:
                yield
            except BaseException:
                try:
                    await conn.rollback()
                except Exception:
                    logger.exception(
                        "token-cache rollback failed after a write error; the "
                        "connection may be wedged (re-raising the original error)"
                    )
                raise

    async def get(self, endpoint: str, uid: str) -> TokenCacheRow | None:
        """Return the cached row for ``(endpoint, uid)`` or ``None``."""
        conn = self._require_conn()
        async with conn.execute(
            "SELECT * FROM token_cache WHERE endpoint = ? AND uid = ?",
            (endpoint, uid),
        ) as cur:
            row = await cur.fetchone()
        return _row_to_cache_row(row) if row else None

    async def set(
        self,
        endpoint: str,
        uid: str,
        bearer: str,
        *,
        source: TokenSource,
    ) -> None:
        """Write ``bearer`` for ``(endpoint, uid)`` and fire wake handlers.

        UPSERT forcing :data:`_FRESH_STATUS`, then fire the registered wake
        handlers so parked rows for this slot are re-queued.

        RETURNS NOTHING (finding S9-7). This used to hand back the slot it had
        written, and none of the seven call sites ever read it. Producing it
        also cost a second query that could contradict the write: the re-read
        ran AFTER ``_write_txn`` released the write lock, so a ``mark_bad``
        landing in that window made this method report ``status='bad'`` for a
        write it had just forced to ``fresh``, a state its own transaction
        never saw. Dropping the return removes the race, the query and an
        unreachable error arm, on the auth-refresh hot path.

        Args:
            endpoint: The upstream host axis of the cache key (ADR-002).
            uid: The opaque caller-supplied credential identifier.
            bearer: The Authorization-header value to cache.
            source: Where this bearer came from.
        """
        conn = self._require_conn()
        now = datetime.now(tz=UTC)
        async with self._write_txn(conn):
            await conn.execute(
                f"""
                INSERT INTO token_cache (endpoint, uid, bearer, observed_at, source, status)
                VALUES (?, ?, ?, ?, ?, '{_FRESH_STATUS}')
                ON CONFLICT(endpoint, uid) DO UPDATE SET
                  bearer = excluded.bearer,
                  observed_at = excluded.observed_at,
                  source = excluded.source,
                  status = '{_FRESH_STATUS}'
                """,
                (endpoint, uid, bearer, now.isoformat(), source),
            )
            await conn.commit()

        # Fire wake handlers. Exceptions in handlers are logged, not propagated.
        for handler in self._wake_handlers:
            try:
                await handler(endpoint, uid)
            except Exception:
                logger.exception(
                    "Token cache wake handler raised for endpoint=%s uid=%s",
                    endpoint,
                    uid,
                )

    async def mark_bad(
        self, endpoint: str, uid: str, *, observed_at: datetime | None = None
    ) -> None:
        """ADR-003: bad tokens stay in cache, status flips to ``bad``.

        Args:
            endpoint: The upstream host axis of the cache key (ADR-002).
            uid: The opaque caller-supplied credential identifier.
            observed_at: The FENCE. When given, the flip applies only if the
                slot still holds the token whose ``observed_at`` this is, so a
                request rejected with an old bearer cannot mark bad a newer one
                pushed while that request was in flight. ``None`` flips
                unconditionally, which is right for an operator's explicit
                invalidation and wrong for a post-response rejection.
        """
        conn = self._require_conn()
        sql = "UPDATE token_cache SET status = 'bad' WHERE endpoint = ? AND uid = ?"
        params: tuple[str, ...] = (endpoint, uid)
        if observed_at is not None:
            sql += " AND observed_at = ?"
            params = (*params, observed_at.isoformat())
        async with self._write_txn(conn):
            await conn.execute(sql, params)
            await conn.commit()

    async def mark_all_bad(self) -> int:
        """ADR-003: flip every slot to ``status='bad'`` (preserve, don't delete).

        The bulk analogue of :meth:`mark_bad`, used by the admin
        invalidate-all surface. Returns the number of slots affected.
        """
        conn = self._require_conn()
        async with self._write_txn(conn):
            cursor = await conn.execute("UPDATE token_cache SET status = 'bad'")
            await conn.commit()
        return cursor.rowcount

    async def list_slots(
        self,
        *,
        endpoint: str | None = None,
    ) -> list[TokenSlot]:
        """Return slot metadata - bearer is NEVER included (ADR-004)."""
        conn = self._require_conn()
        if endpoint is not None:
            async with conn.execute(
                "SELECT * FROM token_cache WHERE endpoint = ?", (endpoint,)
            ) as cur:
                fetched = await cur.fetchall()
        else:
            async with conn.execute("SELECT * FROM token_cache") as cur:
                fetched = await cur.fetchall()
        return [_row_to_slot(r) for r in fetched]

    def register_wake_handler(self, handler: WakeHandler) -> None:
        """Register a callback invoked on every ``set()``."""
        self._wake_handlers.append(handler)
