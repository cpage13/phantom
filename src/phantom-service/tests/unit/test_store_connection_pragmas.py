"""The shared small-store connection opener's pragma contract.

:func:`phantom.storage._connection.open_store_connection` is the one opener
behind :class:`SqliteTokenCache` and :class:`SqliteCredentialStore`. Two
findings land on it:

* the carried-over half of S2-4. ``PRAGMA journal_mode=WAL`` does NOT raise
  when the switch cannot happen; it returns the RESULTING mode as a row. The
  upload store verifies that row; this opener discarded it, so a token cache
  or credential store on a filesystem that cannot support WAL booted healthy
  in ``delete`` mode, where ``synchronous`` no longer means what these stores
  assume it means.
* C8. ``SqliteCfg.journal_mode`` was declared, described as load-bearing,
  validated and exported to the settings contract while no code path read it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import aiosqlite
import pytest
from phantom.config.settings import SqliteCfg
from phantom.storage._connection import open_store_connection
from phantom.storage.credential_store import SqliteCredentialStore
from phantom.storage.token_cache import SqliteTokenCache

pytestmark = pytest.mark.asyncio


def _pin_journal_mode_answer(monkeypatch: pytest.MonkeyPatch, answer: str) -> None:
    """Make every ``journal_mode`` pragma report ``answer`` however it is set.

    Stands in for the filesystem the finding is about: a data_dir on NFS or a
    9p container volume, where SQLite accepts the statement and simply does
    not switch. Every other statement runs for real.
    """
    real_execute = aiosqlite.Connection.execute

    def journal_mode_never_switches(
        self: aiosqlite.Connection, sql: str, parameters: Any = None
    ) -> Any:
        if "journal_mode" in sql.lower():
            return real_execute(self, f"SELECT '{answer}' AS journal_mode", None)
        return real_execute(self, sql, parameters)

    monkeypatch.setattr(aiosqlite.Connection, "execute", journal_mode_never_switches)


async def test_opener_refuses_a_connection_whose_journal_mode_did_not_stick(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S2-4 (carried over): a journal mode that did not take is a boot failure.

    Objective: the pragma reports its result instead of raising, so a discarded
    result means the store applies its schema and declares itself healthy in a
    mode where ``synchronous=FULL`` is doing less than the caller believes and
    readers contend with the writer.

    Expected outcome: ``open_store_connection`` raises ``RuntimeError`` naming
    ``journal_mode`` and the mode it actually observed.
    """
    _pin_journal_mode_answer(monkeypatch, "delete")
    with pytest.raises(RuntimeError, match="journal_mode"):
        await open_store_connection(str(tmp_path / "token_cache.db"), None)


async def test_token_cache_start_refuses_a_non_wal_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S2-4 (carried over): the refusal reaches the token cache's ``start``.

    Objective: the opener is only useful if the store's boot path propagates
    its refusal rather than swallowing it.

    Expected outcome: ``SqliteTokenCache.start`` raises ``RuntimeError``.
    """
    _pin_journal_mode_answer(monkeypatch, "delete")
    cache = SqliteTokenCache(str(tmp_path / "token_cache.db"))
    with pytest.raises(RuntimeError, match="journal_mode"):
        await cache.start()


async def test_credential_store_start_refuses_a_non_wal_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S2-4 (carried over): and the credential store's ``start`` too.

    Objective: both stores share the opener, so both must inherit the check;
    the credential store is the one holding signing material.

    Expected outcome: ``SqliteCredentialStore.start`` raises ``RuntimeError``.
    """
    _pin_journal_mode_answer(monkeypatch, "delete")
    store = SqliteCredentialStore(str(tmp_path / "credential_store.db"))
    with pytest.raises(RuntimeError, match="journal_mode"):
        await store.start()


async def test_an_in_memory_store_is_exempt_from_the_wal_assertion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S2-4 (carried over): ``:memory:`` reports ``memory`` and is not a failure.

    Objective: an in-memory database has no journal file for WAL to mean
    anything about, and SQLite answers ``memory``. The upload store exempts
    that shape rather than failing it, and this opener must match, or the
    assertion turns a unit-test-only shape into a boot error.

    Expected outcome: the connection opens and reports ``memory``.
    """
    del tmp_path, monkeypatch
    conn = await open_store_connection(":memory:", None)
    try:
        async with conn.execute("PRAGMA journal_mode;") as cursor:
            row = await cursor.fetchone()
        assert row is not None
        assert str(row[0]).lower() == "memory"
    finally:
        await conn.close()


async def test_the_opener_applies_the_configured_journal_mode(tmp_path: Path) -> None:
    """C8: the opener reads ``SqliteCfg.journal_mode`` instead of hardcoding it.

    Objective: the knob was declared, validated and exported to the contract
    while both PRAGMA sites hardcoded the string, so a port built from the
    schema would wire it through and behave differently from the reference.
    The field's ``Literal["WAL"]`` means production can only ever ask for WAL,
    so the wiring is proved with ``model_construct``, which is the only way to
    put a second value in the field at all.

    Expected outcome: the opened connection reports the CONFIGURED mode.
    """
    cfg = SqliteCfg.model_construct(journal_mode="DELETE")
    conn = await open_store_connection(str(tmp_path / "token_cache.db"), cfg)
    try:
        async with conn.execute("PRAGMA journal_mode;") as cursor:
            row = await cursor.fetchone()
        assert row is not None
        assert str(row[0]).lower() == "delete"
    finally:
        await conn.close()


async def test_the_default_journal_mode_is_still_wal(tmp_path: Path) -> None:
    """C8: reading the knob must not change the shipped posture.

    Objective: the no-Settings construction path (unit tests) and the default
    config must both still land on WAL, which every durability argument these
    two stores make depends on.

    Expected outcome: a connection opened with ``None`` and one opened with a
    default :class:`SqliteCfg` both report ``wal``.
    """
    for label, cfg in (("no-settings", None), ("defaults", SqliteCfg())):
        conn = await open_store_connection(str(tmp_path / f"{label}.db"), cfg)
        try:
            async with conn.execute("PRAGMA journal_mode;") as cursor:
                row = await cursor.fetchone()
            assert row is not None
            assert str(row[0]).lower() == "wal", label
        finally:
            await conn.close()
