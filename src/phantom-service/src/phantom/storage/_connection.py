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
(``journal_size_limit``, ``foreign_keys``) and verifies the rest stuck
afterwards. Pointing it at a fixed-``FULL`` opener would change durability and
write cost on the hot path, which is a behaviour change rather than a
deduplication. The one thing the two DO share is the journal-mode assertion,
which each makes for itself: see :func:`open_store_connection` for why the
pragma's own answer is the only way to know it took (finding S2-4).
"""

from __future__ import annotations

from typing import Final

import aiosqlite

from phantom.config.settings import SqliteCfg

# The journal mode a store gets on the no-Settings construction path (unit
# tests). Read off the field's OWN declaration rather than restated here, so
# the opener and the settings schema cannot disagree about the default, which
# is the drift C8 is about.
_DEFAULT_JOURNAL_MODE: Final[str] = SqliteCfg.model_fields["journal_mode"].default

# What SQLite reports for an in-memory database, which has no journal file for
# a journal mode to mean anything about. ``:memory:`` is a unit-test-only
# shape, so it is EXEMPT from the assertion below rather than failing it.
# Matches :class:`SqliteUploadStore`, which carries the same exemption for the
# same reason.
_IN_MEMORY_JOURNAL_MODE: Final[str] = "memory"

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


def resolve_journal_mode(cfg: SqliteCfg | None) -> str:
    """Return the ``journal_mode`` PRAGMA value for ``cfg``.

    The knob exists, is described as load-bearing, is validated at load and is
    exported to the settings contract; until finding C8 nothing read it and
    both PRAGMA sites hardcoded the string, so an implementation built from
    the schema would wire it through and track the config where the reference
    did not. Its type is ``Literal["WAL"]``, so this returns ``"WAL"`` for
    every configuration that can be loaded; what changes is that the value now
    comes FROM the declaration.

    Args:
        cfg: The store's pragma configuration, or ``None`` when the caller
            was constructed without Settings (unit tests, mostly).

    Returns:
        The configured journal mode.
    """
    if cfg is None:
        return _DEFAULT_JOURNAL_MODE
    return cfg.journal_mode


async def open_store_connection(db_path: str, cfg: SqliteCfg | None) -> aiosqlite.Connection:
    """Open a Phantom SQLite store connection with the standard durability pragmas.

    Connect, set the row factory, then apply the four pragmas both small
    stores share: the configured journal mode (C8; ``WAL`` for every loadable
    configuration), ``synchronous=FULL`` (these stores hold auth material
    whose loss strands undelivered uploads, so neither trades durability for
    write cost), ``auto_vacuum=NONE``, and the resolved busy timeout.

    The journal mode is the one pragma whose ANSWER is the assertion. It does
    NOT raise when the switch cannot happen: it returns the RESULTING mode as
    a row, so a data_dir whose VFS lacks the shared-memory support WAL needs
    (NFS, a 9p container volume) left these stores in ``delete`` and they
    opened reporting healthy (finding S2-4, fixed in the upload store first
    and carried here). Under a rollback journal the ``synchronous=FULL`` these
    stores pin is doing less than the caller believes, and readers contend
    with the writer instead of riding a WAL snapshot. An in-memory database
    is exempt: see :data:`_IN_MEMORY_JOURNAL_MODE`.

    The caller applies its own DDL afterwards and commits; see the module
    docstring for why the DDL does not move here.

    Args:
        db_path: The store's own SQLite file path.
        cfg: Pragma configuration, or ``None`` for the defaults.

    Returns:
        The open connection, pragma-applied and uncommitted.

    Raises:
        RuntimeError: When the journal mode did not take effect. The
            connection is closed first, so a refused open leaks no handle.
    """
    conn = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row
    # A PRAGMA takes no bound parameter, so the mode is interpolated. It comes
    # from a ``Literal``-typed pydantic field, so the only value that reaches
    # here is one the settings schema declares.
    journal_mode = resolve_journal_mode(cfg)
    async with conn.execute(f"PRAGMA journal_mode={journal_mode};") as journal_cursor:
        journal_row = await journal_cursor.fetchone()
    observed_journal_mode = str(journal_row[0]).lower() if journal_row is not None else ""
    if observed_journal_mode not in (journal_mode.lower(), _IN_MEMORY_JOURNAL_MODE):
        await conn.close()
        raise RuntimeError(
            f"journal_mode pragma did not stick: expected {journal_mode.lower()!r}, "
            f"got {observed_journal_mode!r}; the filesystem holding {db_path!r} "
            f"cannot support it, and this store's synchronous=FULL durability "
            f"argument depends on it"
        )
    await conn.execute("PRAGMA synchronous=FULL;")
    await conn.execute("PRAGMA auto_vacuum=NONE;")
    await conn.execute(f"PRAGMA busy_timeout={resolve_busy_timeout_ms(cfg)};")
    return conn
