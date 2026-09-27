"""SQLite-backed destination-credential store (ADR-003 / ADR-004).

A FAITHFUL copy of :class:`phantom.storage.token_cache.SqliteTokenCache` (the
2026-06-23 owner directive - copy the token implementation, differ only where
forced). The store is keyed by the resolved destination **host alone**; the
structured credential value persists to disk so it survives Phantom restart.
Bad credentials stay in the store with ``status='bad'`` rather than being
deleted (ADR-003).

The store lives in its OWN database file (production wires
``<instance data_root>/credential_store.db``), deliberately separate from
``uploads.db`` and ``token_cache.db``: SQLite serializes writers per database
file, so the split keeps credential reads and writes off the hot uploads /
token-cache writer locks, and a credential is shared across many uploads anyway.

ADR-004 holds here by ABSENCE, not by a redaction model: this store has no list
method and no read-back endpoint, so no admin response can carry a credential
at all. The value is read internally only, by the signer at sign time inside
the executor. A ``CredentialSlot`` model this docstring used to point at as the
guarantee had no consumer anywhere and has been deleted (finding S9-8); a
future GET-list brings its own no-secret response model.

The forced differences from the token cache (and nothing else):

* the key is the destination host alone - the PK drops the token cache's
  ``uid`` axis;
* the value is the structured :data:`~phantom.models.credential.DestinationCredential`
  serialized to a ``cred_json`` column, not a bare ``bearer`` string;
* the wake handler takes one argument ``(dest_host)``, not two
  ``(endpoint, uid)`` - see :data:`CredentialWakeHandler`.

**``set``'s return value is a dead contract** (finding S9-7). All seven call
sites across both auth stores discard it. It survives only because the
``CredentialStore`` Protocol in :mod:`phantom.storage.interface` declares it,
and the two must change together; until then the value it returns describes
the write it committed rather than a re-read that could report someone else's.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Annotated, Final

import aiosqlite
from pydantic import Field, TypeAdapter, ValidationError

from phantom.config.settings import SqliteCfg
from phantom.models.credential import (
    CredCacheRow,
    CredentialSource,
    CredentialStatus,
    DestinationCredential,
    HostCredKey,
)
from phantom.storage._connection import open_store_connection
from phantom.storage.interface import CredentialWakeHandler

logger = logging.getLogger(__name__)

# The status every :meth:`SqliteCredentialStore.set` forces. Named once
# because it appears twice - in the UPSERT and in the row that call returns -
# and a re-push un-badding the slot is what the credential-recovery loop
# relies on, so the two must not be able to drift apart.
_FRESH_STATUS: Final[CredentialStatus] = "fresh"


# The ``cred_json`` decoder. A discriminated union on ``kind``, so one adapter
# validates EVERY field of either variant rather than splatting raw JSON into a
# frozen dataclass that checks nothing (finding S9-6). It also re-coerces the
# wire ``service`` string back to :class:`SigningService`, which the write
# side's ``asdict`` + ``json.dumps`` flattened, so the round trip is exact.
_CREDENTIAL_ADAPTER: Final = TypeAdapter[DestinationCredential](
    Annotated[DestinationCredential, Field(discriminator="kind")]
)


def _redacted_decode_reason(exc: ValueError) -> str:
    """Describe a row-decode failure without echoing the row.

    A ``pydantic.ValidationError`` renders the offending INPUT in its message,
    and on this model the offending input can be the resolved secret access
    key. The operator needs to know which field of which variant failed and
    why, which is exactly the part that carries no value.

    Args:
        exc: The decode failure. Anything that is not a pydantic
            ``ValidationError`` is one of this module's own messages, which
            quote only structural values (a ``kind``, a column name).

    Returns:
        A one-line, value-free description.
    """
    if not isinstance(exc, ValidationError):
        return str(exc)
    return "; ".join(
        f"{'.'.join(str(part) for part in error['loc']) or '<root>'}: {error['type']}"
        for error in exc.errors(include_url=False)
    )


def _credential_from_json(kind: str, cred_json: str) -> DestinationCredential:
    """Rebuild a frozen credential variant from its ``kind`` + serialized JSON.

    The inverse of the ``set`` write path's ``json.dumps(asdict(credential))``.
    Validation runs through :data:`_CREDENTIAL_ADAPTER`, so a corrupt row fails
    on ANY malformed field (a missing ``service``, an unknown service string, a
    non-string key id, a field the variant does not declare), not only on the
    two the hand-written splat happened to touch.

    ``kind`` arrives twice, as its own column and inside the blob. They are
    cross-checked rather than one being believed: a row where they disagree is
    corrupt, and resolving it silently in favour of whichever the code read
    first is how a credential of one type gets served as another.

    Args:
        kind: The row's ``kind`` column.
        cred_json: The row's ``cred_json`` column.

    Returns:
        The frozen :data:`DestinationCredential` the row holds.

    Raises:
        ValueError: When the blob is not valid JSON, does not validate against
            either variant, or carries a ``kind`` other than the column's.
            ``pydantic.ValidationError`` IS a ``ValueError``, so callers catch
            the one type. :meth:`SqliteCredentialStore.get` is the caller that
            does, and it is where the policy for a corrupt row lives.
    """
    credential = _CREDENTIAL_ADAPTER.validate_json(cred_json)
    if credential.kind != kind:
        raise ValueError(
            f"credential_store row declares kind {kind!r} but its cred_json "
            f"carries {credential.kind!r}"
        )
    return credential


def _row_to_cache_row(row: aiosqlite.Row) -> CredCacheRow:
    """Decode one SQLite row into a :class:`CredCacheRow`.

    Args:
        row: One ``credential_store`` row.

    Returns:
        The validated row.

    Raises:
        ValueError: When any column is outside what the row type accepts.
            The row model is strict (S9-6), so this covers the ``status`` and
            ``source`` Literals and the timestamp as well as the credential
            blob; ``pydantic.ValidationError`` is a ``ValueError``.
    """
    return CredCacheRow(
        dest_host=HostCredKey(row["dest_host"]),
        credential=_credential_from_json(row["kind"], row["cred_json"]),
        observed_at=datetime.fromisoformat(row["observed_at"]),
        source=row["source"],
        status=row["status"],
    )


class SqliteCredentialStore:
    """Disk-tier SQLite destination-credential store.

    Its connection plumbing is the SHARED one
    (:func:`phantom.storage._connection.open_store_connection`), so the "A COPY
    of :class:`SqliteTokenCache`" this docstring used to open with is true of
    the DDL only, which ADR-030 sanctions: this store's primary key is the
    destination host alone, dropping the token cache's uid axis. Optional
    ``sqlite_cfg`` carries the parameterized ``busy_timeout_ms`` pragma value
    (shared with :class:`SqliteUploadStore`). When omitted the default matches
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
        # Every write path on a shared aiosqlite connection must atomicize
        # its ``execute`` / ``commit`` pair so concurrent coroutines don't
        # race the transaction state. The lock stays per store because it
        # guards THIS store's connection; the rationale it used to
        # cross-reference two siblings for now lives on the shared opener.
        self._write_lock = asyncio.Lock()

    async def start(self) -> None:
        """Open the store's own database file and apply its DDL.

        The connection and its four durability pragmas come from the shared
        opener (U1); the DDL below stays here, because ADR-030 makes each
        store's own schema deliberate.
        """
        self._conn = await open_store_connection(self._db_path, self._cfg)
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

        A row that does not decode answers ``None`` and logs at ERROR, rather
        than raising (finding S9-6). Both halves of that are deliberate.
        ``None`` is the answer both consumers already handle and both handle
        CONSERVATIVELY: the executor parks the upload and the kicker leaves it
        parked, which is what an unusable credential means. Raising instead
        would cross into the credential kicker's scan loop, whose one raising
        call is documented to be the route resolve, so a single corrupt row
        would abort a whole rescan pass and strand every row behind it. The
        ERROR log is what makes the defect loud without making it fatal; it
        names the host and which field failed, never a stored value, because
        a validation message renders the input it rejected and on this model
        that input can be the resolved secret.

        Args:
            dest_host: The resolved destination host to look up.

        Returns:
            The validated row, or ``None`` when there is no slot for this host
            or the slot's stored row is corrupt.
        """
        conn = self._require_conn()
        async with conn.execute(
            "SELECT * FROM credential_store WHERE dest_host = ?",
            (dest_host,),
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return None
        try:
            return _row_to_cache_row(row)
        except ValueError as exc:
            logger.error(
                "credential_store row for dest_host=%s does not decode and is "
                "being treated as absent (the upload parks); reason: %s",
                dest_host,
                _redacted_decode_reason(exc),
            )
            return None

    async def set(
        self,
        dest_host: HostCredKey,
        credential: DestinationCredential,
        *,
        source: CredentialSource,
    ) -> None:
        """Write ``credential`` for ``dest_host`` and fire wake handlers.

        UPSERT forcing :data:`_FRESH_STATUS` (so a re-push un-bads the slot -
        the recovery loop relies on this), then fire the registered wake
        handlers. The ``secret_access_key`` of a
        :class:`~phantom.models.credential.SigV4StaticCreds` persists at rest
        (the ADR-003 posture the owner's persist-on-restart decision accepts,
        matching the token precedent).

        RETURNS NOTHING (finding S9-7). This used to hand back the slot it had
        written, and none of the seven call sites ever read it. Producing it
        also cost a second query that could contradict the write: the re-read
        ran AFTER ``_write_txn`` released the write lock, so a ``mark_bad``
        landing in that window made this method report ``status='bad'`` for a
        write it had just forced to ``fresh``, a state its own transaction
        never saw.

        Args:
            dest_host: The resolved destination host to key the slot on.
            credential: The structured credential to persist.
            source: How this credential was supplied.
        """
        conn = self._require_conn()
        now = datetime.now(tz=UTC)
        cred_json = json.dumps(asdict(credential))
        async with self._write_txn(conn):
            await conn.execute(
                f"""
                INSERT INTO credential_store
                    (dest_host, kind, cred_json, observed_at, source, status)
                VALUES (?, ?, ?, ?, ?, '{_FRESH_STATUS}')
                ON CONFLICT(dest_host) DO UPDATE SET
                  kind = excluded.kind,
                  cred_json = excluded.cred_json,
                  observed_at = excluded.observed_at,
                  source = excluded.source,
                  status = '{_FRESH_STATUS}'
                """,
                (dest_host, credential.kind, cred_json, now.isoformat(), source),
            )
            await conn.commit()

        # Fire wake handlers. Exceptions in handlers are logged, not propagated.
        for handler in self._wake_handlers:
            try:
                await handler(dest_host)
            except Exception:
                logger.exception(
                    "Credential store wake handler raised for dest_host=%s",
                    dest_host,
                )

    async def mark_bad(
        self, dest_host: HostCredKey, *, observed_at: datetime | None = None
    ) -> None:
        """ADR-003: bad credentials stay in the store, status flips to ``bad``.

        Args:
            dest_host: The resolved destination host whose slot to flip.
            observed_at: The FENCE. When given, the flip applies only if the
                slot still holds the credential whose ``observed_at`` this is,
                so a request rejected with an old credential cannot mark bad a
                newer one pushed while that request was in flight. ``None``
                flips unconditionally, which is right for an operator's
                explicit invalidation and wrong for a post-response rejection.
        """
        conn = self._require_conn()
        sql = "UPDATE credential_store SET status = 'bad' WHERE dest_host = ?"
        params: tuple[str, ...] = (dest_host,)
        if observed_at is not None:
            sql += " AND observed_at = ?"
            params = (*params, observed_at.isoformat())
        async with self._write_txn(conn):
            await conn.execute(sql, params)
            await conn.commit()

    def register_wake_handler(self, handler: CredentialWakeHandler) -> None:
        """Register a callback invoked on every ``set()``."""
        self._wake_handlers.append(handler)
