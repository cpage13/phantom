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
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import aiosqlite

from phantom.config.settings import SqliteCfg
from phantom.models.admin import TokenSlot
from phantom.models.token import TokenCacheRow, TokenSource
from phantom.storage.interface import WakeHandler
from phantom.storage.sqlite_store import _DEFAULT_BUSY_TIMEOUT_MS

logger = logging.getLogger(__name__)


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
        # See SqliteUploadStore._write_lock — every write path on a
        # shared aiosqlite connection must atomicize its ``execute`` /
        # ``commit`` pair so concurrent coroutines don't race the
        # transaction state.
        self._write_lock = asyncio.Lock()

    def _busy_timeout_ms(self) -> int:
        """Resolve the ``busy_timeout`` PRAGMA value (milliseconds)."""
        if self._cfg is None:
            return _DEFAULT_BUSY_TIMEOUT_MS
        return self._cfg.busy_timeout_ms

    async def start(self) -> None:
        """Open the cache's own database file and apply its DDL."""
        self._conn = await aiosqlite.connect(self._db_path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute("PRAGMA synchronous=FULL;")
        await self._conn.execute("PRAGMA auto_vacuum=NONE;")
        # busy_timeout — see SqliteCfg.busy_timeout_ms / _DEFAULT_BUSY_TIMEOUT_MS
        # for the value rationale (R9-V6-1).
        await self._conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms()};")
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
    ) -> TokenCacheRow:
        """Write ``bearer`` for ``(endpoint, uid)`` and fire wake handlers."""
        conn = self._require_conn()
        now_iso = datetime.now(tz=UTC).isoformat()
        async with self._write_txn(conn):
            await conn.execute(
                """
                INSERT INTO token_cache (endpoint, uid, bearer, observed_at, source, status)
                VALUES (?, ?, ?, ?, ?, 'fresh')
                ON CONFLICT(endpoint, uid) DO UPDATE SET
                  bearer = excluded.bearer,
                  observed_at = excluded.observed_at,
                  source = excluded.source,
                  status = 'fresh'
                """,
                (endpoint, uid, bearer, now_iso, source),
            )
            await conn.commit()

        # Re-read to return the full row.
        fetched = await self.get(endpoint, uid)
        if fetched is None:  # pragma: no cover — write just completed
            raise RuntimeError("Token cache row missing after set")

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
        return fetched

    async def mark_bad(self, endpoint: str, uid: str) -> None:
        """ADR-003: bad tokens stay in cache, status flips to ``bad``."""
        conn = self._require_conn()
        async with self._write_txn(conn):
            await conn.execute(
                "UPDATE token_cache SET status = 'bad' WHERE endpoint = ? AND uid = ?",
                (endpoint, uid),
            )
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
        """Return slot metadata — bearer is NEVER included (ADR-004)."""
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

    async def delete(self, endpoint: str, uid: str) -> None:
        """Hard delete one slot."""
        conn = self._require_conn()
        async with self._write_txn(conn):
            await conn.execute(
                "DELETE FROM token_cache WHERE endpoint = ? AND uid = ?",
                (endpoint, uid),
            )
            await conn.commit()

    async def delete_all(self) -> int:
        """Hard delete every slot. Returns the count."""
        conn = self._require_conn()
        async with self._write_txn(conn):
            cursor = await conn.execute("DELETE FROM token_cache")
            await conn.commit()
        return cursor.rowcount

    def register_wake_handler(self, handler: WakeHandler) -> None:
        """Register a callback invoked on every ``set()``."""
        self._wake_handlers.append(handler)
