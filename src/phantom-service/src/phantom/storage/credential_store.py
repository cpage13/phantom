"""SQLite-backed destination-credential store (ADR-003 / ADR-004).

A FAITHFUL copy of :class:`phantom.storage.token_cache.SqliteTokenCache` (the
2026-06-23 owner directive — copy the token implementation, differ only where
forced). The store is keyed by the resolved destination **host alone**; the
structured credential value persists to disk so it survives Phantom restart.
Bad credentials stay in the store with ``status='bad'`` rather than being
deleted (ADR-003).

The store lives in its OWN database file (production wires
``<instance data_root>/credential_store.db``), deliberately separate from
``uploads.db`` and ``token_cache.db``: SQLite serializes writers per database
file, so the split keeps credential reads and writes off the hot uploads /
token-cache writer locks, and a credential is shared across many uploads anyway.

Admin reads use :class:`phantom.models.credential.CredentialSlot`, which carries
no secret material (ADR-004); the store itself never exposes a secret read-back
endpoint — the credential value is read internally only (the signer retrieves it
at sign time inside the executor).

The forced differences from the token cache (and nothing else):

* the key is the destination host alone — the PK drops the token cache's
  ``uid`` axis;
* the value is the structured :data:`~phantom.models.credential.DestinationCredential`
  serialized to a ``cred_json`` column, not a bare ``bearer`` string;
* the wake handler takes one argument ``(dest_host)``, not two
  ``(endpoint, uid)`` — see :data:`CredentialWakeHandler`.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import UTC, datetime

import aiosqlite

from phantom.config.settings import SqliteCfg
from phantom.models.credential import (
    CredCacheRow,
    CredentialSource,
    DestinationCredential,
    HostCredKey,
    ProfileRefCred,
    SigningService,
    SigV4StaticCreds,
)
from phantom.storage.interface import CredentialWakeHandler
from phantom.storage.sqlite_store import _DEFAULT_BUSY_TIMEOUT_MS

logger = logging.getLogger(__name__)


def _credential_from_json(kind: str, cred_json: str) -> DestinationCredential:
    """Rebuild a frozen credential variant from its ``kind`` + serialized JSON.

    The inverse of the ``set`` write path's ``json.dumps(asdict(credential))``;
    dispatches on the discriminator ``kind`` to the matching frozen variant.

    The write side serialized ``service`` to a plain string (the ``StrEnum``
    member serializes to ``"s3"`` via ``asdict``); this ``**payload`` splat path
    has NO pydantic, so the body's before-validator cannot run here. ``service``
    is re-coerced to :class:`SigningService` explicitly before construction. An
    unknown service string (a corrupt DB row) raises ``ValueError`` — fail loud,
    correct.
    """
    payload = json.loads(cred_json)
    payload["service"] = SigningService(payload["service"])
    if kind == "sigv4_static":
        return SigV4StaticCreds(**payload)
    if kind == "profile_ref":
        return ProfileRefCred(**payload)
    raise ValueError(f"Unknown credential kind {kind!r} in credential_store row")


def _row_to_cache_row(row: aiosqlite.Row) -> CredCacheRow:
    """Decode one SQLite row into a :class:`CredCacheRow`."""
    return CredCacheRow(
        dest_host=HostCredKey(row["dest_host"]),
        credential=_credential_from_json(row["kind"], row["cred_json"]),
        observed_at=datetime.fromisoformat(row["observed_at"]),
        source=row["source"],
        status=row["status"],
    )


class SqliteCredentialStore:
    """Disk-tier SQLite destination-credential store.

    A COPY of :class:`SqliteTokenCache`. Optional ``sqlite_cfg`` carries the
    parameterized ``busy_timeout_ms`` pragma value (shared with
    :class:`SqliteUploadStore`). When omitted the default matches
    :class:`SqliteCfg` so unit tests need no explicit Settings to exercise the
    store; production threads ``settings.storage.sqlite``.
    """

    def __init__(self, db_path: str, *, sqlite_cfg: SqliteCfg | None = None) -> None:
        """Construct a store rooted at ``db_path`` (a SQLite file path).

        Args:
            db_path: SQLite path for the credential-store table.
            sqlite_cfg: Pragma configuration. When ``None`` the
                :class:`SqliteCfg` defaults apply (the ``busy_timeout_ms``
                default in particular).
        """
        self._db_path = db_path
        self._cfg = sqlite_cfg
        self._conn: aiosqlite.Connection | None = None
        self._wake_handlers: list[CredentialWakeHandler] = []
        # See SqliteTokenCache._write_lock / SqliteUploadStore._write_lock —
        # every write path on a shared aiosqlite connection must atomicize its
        # ``execute`` / ``commit`` pair so concurrent coroutines don't race the
        # transaction state.
        self._write_lock = asyncio.Lock()

    def _busy_timeout_ms(self) -> int:
        """Resolve the ``busy_timeout`` PRAGMA value (milliseconds)."""
        if self._cfg is None:
            return _DEFAULT_BUSY_TIMEOUT_MS
        return self._cfg.busy_timeout_ms

    async def start(self) -> None:
        """Open the store's own database file and apply its DDL."""
        self._conn = await aiosqlite.connect(self._db_path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL;")
        await self._conn.execute("PRAGMA synchronous=FULL;")
        await self._conn.execute("PRAGMA auto_vacuum=NONE;")
        # busy_timeout — see SqliteCfg.busy_timeout_ms / _DEFAULT_BUSY_TIMEOUT_MS
        # for the value rationale (R9-V6-1).
        await self._conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms()};")
        # The store owns this DDL in its own credential_store.db (one more
        # database per instance by design; see the module docstring). A COPY of
        # the token_cache DDL with the value + status columns swapped: the PK is
        # the destination host alone (drops the token cache's uid axis), and the
        # structured credential serializes into cred_json.
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS credential_store (
                dest_host       TEXT NOT NULL PRIMARY KEY,
                kind            TEXT NOT NULL,
                cred_json       TEXT NOT NULL,
                observed_at     TEXT NOT NULL,
                source          TEXT NOT NULL,
                status          TEXT NOT NULL
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
            raise RuntimeError("SqliteCredentialStore is not started")
        return self._conn

    @asynccontextmanager
    async def _write_txn(self, conn: aiosqlite.Connection) -> AsyncIterator[None]:
        """Hold the write lock and ROLL BACK on ANY failure.

        Findings R7-1-D / R7-2-B applied to the credential store's OWN aiosqlite
        connection (a COPY of ``SqliteTokenCache._write_txn``). A SQLITE_IOERR /
        SQLITE_FULL from a write ``execute`` or ``commit`` would otherwise leave
        the transaction open and wedge every subsequent credential write
        (``set`` / ``mark_bad``). The credential store is on the SigV4
        auth-refresh hot path, so a wedge here strands ``auth_expired`` rows.
        Roll back on error to keep the connection self-healing; see the token
        cache's ``_write_txn`` for the full rollback-not-PANIC rationale.
        """
        async with self._write_lock:
            try:
                yield
            except BaseException:
                try:
                    await conn.rollback()
                except Exception:
                    logger.exception(
                        "credential-store rollback failed after a write error; the "
                        "connection may be wedged (re-raising the original error)"
                    )
                raise

    async def get(self, dest_host: HostCredKey) -> CredCacheRow | None:
        """Return the cached row for ``dest_host`` or ``None``.

        The row carries ``status``; consumers enforce "bad == unusable" (the
        executor arm treats ``row is None or row.status == 'bad'`` as no-creds,
        the kicker treats ``row is None or row.status != 'fresh'`` as don't-wake).
        """
        conn = self._require_conn()
        async with conn.execute(
            "SELECT * FROM credential_store WHERE dest_host = ?",
            (dest_host,),
        ) as cur:
            row = await cur.fetchone()
        return _row_to_cache_row(row) if row else None

    async def set(
        self,
        dest_host: HostCredKey,
        credential: DestinationCredential,
        *,
        source: CredentialSource,
    ) -> CredCacheRow:
        """Write ``credential`` for ``dest_host`` and fire wake handlers.

        UPSERT forcing ``status='fresh'`` (so a re-push un-bads the slot — the
        recovery loop relies on this), re-read to return the full row, then fire
        the registered wake handlers. The ``secret_access_key`` of a
        :class:`SigV4StaticCreds` persists at rest (the ADR-003 posture the
        owner's persist-on-restart decision accepts, matching the token
        precedent).
        """
        conn = self._require_conn()
        now_iso = datetime.now(tz=UTC).isoformat()
        cred_json = json.dumps(asdict(credential))
        async with self._write_txn(conn):
            await conn.execute(
                """
                INSERT INTO credential_store
                    (dest_host, kind, cred_json, observed_at, source, status)
                VALUES (?, ?, ?, ?, ?, 'fresh')
                ON CONFLICT(dest_host) DO UPDATE SET
                  kind = excluded.kind,
                  cred_json = excluded.cred_json,
                  observed_at = excluded.observed_at,
                  source = excluded.source,
                  status = 'fresh'
                """,
                (dest_host, credential.kind, cred_json, now_iso, source),
            )
            await conn.commit()

        # Re-read to return the full row.
        fetched = await self.get(dest_host)
        if fetched is None:  # pragma: no cover — write just completed
            raise RuntimeError("Credential store row missing after set")

        # Fire wake handlers. Exceptions in handlers are logged, not propagated.
        for handler in self._wake_handlers:
            try:
                await handler(dest_host)
            except Exception:
                logger.exception(
                    "Credential store wake handler raised for dest_host=%s",
                    dest_host,
                )
        return fetched

    async def mark_bad(self, dest_host: HostCredKey) -> None:
        """ADR-003: bad credentials stay in the store, status flips to ``bad``."""
        conn = self._require_conn()
        async with self._write_txn(conn):
            await conn.execute(
                "UPDATE credential_store SET status = 'bad' WHERE dest_host = ?",
                (dest_host,),
            )
            await conn.commit()

    def register_wake_handler(self, handler: CredentialWakeHandler) -> None:
        """Register a callback invoked on every ``set()``."""
        self._wake_handlers.append(handler)
