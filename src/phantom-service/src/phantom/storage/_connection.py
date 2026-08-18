"""The shared SQLite connection opener for Phantom's two small stores.

``SqliteCredentialStore`` opened its connection with the same five statements
``SqliteTokenCache`` did, and said so: its class docstring opened "A COPY of
:class:`SqliteTokenCache`" and its write-lock comment pointed the reader at two
sibling classes for the rationale. U1 moves the plumbing here so the two say it
once.

**The plumbing only. The DDL stays with its store,** because ADR-030 makes the
duplication deliberate: each store owns its own database file and its own
schema, and the credential store's primary key differs from the token cache's
(the destination host alone, dropping the uid axis). Each ``start()`` therefore
calls this opener and then applies its OWN table definition.

**``SqliteUploadStore`` is NOT a caller, on evidence rather than on taste.**
Its ``synchronous`` pragma is configurable and defaults to ``NORMAL``, while
both stores here hardcode ``FULL``; it applies two pragmas these do not
(``journal_size_limit``, ``foreign_keys``) and verifies four stuck afterwards.
Pointing it at a fixed-``FULL`` opener would change durability and write cost
on the hot path, which is a behaviour change rather than a deduplication.
"""

from __future__ import annotations

import aiosqlite

from phantom.config.settings import SqliteCfg

# Default SQLite ``busy_timeout`` in milliseconds for the no-Settings
# construction path (unit tests). SQLite busy-WAITS in its connection worker
# thread for up to this long when a write contends for a held lock before
# raising "database is locked" (SQLITE_BUSY). Mirrors :class:`SqliteCfg`'s
# ``busy_timeout_ms`` default so a store/cache built without overrides matches
# the production default posture; production threads ``cfg.busy_timeout_ms``.
#
# WHY 1 s, not the former 5 s (finding R9-V6-1, the lock-amplification fix).
# Phantom's store serializes EVERY writer (admission + the sender pool + reaper
# + persist-controller + admin) through ONE ``asyncio.Lock`` (``_write_lock``)
# on a single aiosqlite connection, so there is NEVER more than one Phantom
# write in flight at the SQLite level, so Phantom-internal write-vs-write
# contention is impossible by construction. The busy_timeout therefore does
# NOT exist to give "concurrent workers headroom"; its ONLY effect is under
# EXTERNAL cross-process contention: a sibling connection holding the WAL
# write lock (a stray ``sqlite3 uploads.db`` admin session, a backup/snapshot
# tool, a second instance mis-sharing the data_dir). Under such a hold, a LARGE
# busy_timeout is actively harmful: each contended writer monopolizes the
# single ``_write_lock`` + connection-thread slot for the full window, so a
# burst of admissions queues serially behind multiple 5 s busy-waits and the
# producer's HTTP read times out BEFORE admission can return its clean
# ``storage_unavailable`` 503, so the burst surfaced as bare
# ``PhantomTimeoutError``s instead of clean retryables (R9-V6-1; an 8-deep
# burst under a 9 s hold took ~93 s at 5 s vs ~13 s at 1 s, all clean 503s).
# 1 s comfortably rides out sub-second external blips while failing FAST under
# a sustained external hold so the contended write returns a clean retryable
# signal quickly (admission returns 503 + Retry-After; a sender's
# ``claim_due`` retries on its next poll) rather than blocking the single writer slot.
# Durability is unaffected: a failed contended write commits no row
# (R9-V6-3 confirms the data layer never corrupts under the lock). Boot-time
# recovery rides out a lock for far longer than this via its own bounded
# retry-with-backoff (``workers.recovery``), independent of this value. See
# :class:`phantom.config.settings.SqliteCfg.busy_timeout_ms` for the
# operator-facing knob (default stays 1000).
_DEFAULT_BUSY_TIMEOUT_MS = 1000


def resolve_busy_timeout_ms(cfg: SqliteCfg | None) -> int:
    """Return the ``busy_timeout`` PRAGMA value (milliseconds) for ``cfg``.

    Args:
        cfg: The store's pragma configuration, or ``None`` when the caller
            was constructed without Settings (unit tests, mostly).

    Returns:
        The configured value, or :data:`_DEFAULT_BUSY_TIMEOUT_MS`.
    """
    if cfg is None:
        return _DEFAULT_BUSY_TIMEOUT_MS
    return cfg.busy_timeout_ms


async def open_store_connection(db_path: str, cfg: SqliteCfg | None) -> aiosqlite.Connection:
    """Open a Phantom SQLite store connection with the standard durability pragmas.

    Connect, set the row factory, then apply the four pragmas both small
    stores share: WAL journalling, ``synchronous=FULL`` (these stores hold
    auth material whose loss strands undelivered uploads, so neither trades
    durability for write cost), ``auto_vacuum=NONE``, and the resolved
    busy timeout.

    The caller applies its own DDL afterwards and commits; see the module
    docstring for why the DDL does not move here.

    Args:
        db_path: The store's own SQLite file path.
        cfg: Pragma configuration, or ``None`` for the defaults.

    Returns:
        The open connection, pragma-applied and uncommitted.
    """
    conn = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=FULL;")
    await conn.execute("PRAGMA auto_vacuum=NONE;")
    await conn.execute(f"PRAGMA busy_timeout={resolve_busy_timeout_ms(cfg)};")
    return conn
