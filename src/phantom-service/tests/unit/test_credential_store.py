"""Unit tests for phantom.storage.credential_store.

A COPY of ``tests/unit/test_token_cache.py``'s structure (the persistence
round-trip + wake-handler + mark_bad patterns), adapted to the credential
store's forced differences: keyed by host alone, a structured tagged-union
value, and a one-argument wake handler.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from phantom.models.credential import (
    HostCredKey,
    ProfileRefCred,
    SigningService,
    SigV4StaticCreds,
)
from phantom.storage.credential_store import SqliteCredentialStore

_HOST = HostCredKey("s3.us-east-1.amazonaws.com")
_OTHER_HOST = HostCredKey("s3.eu-west-1.amazonaws.com")


def _static_creds() -> SigV4StaticCreds:
    """A resolved static SigV4 key-pair fixture value."""
    return SigV4StaticCreds(
        access_key_id="AKIAEXAMPLE",
        secret_access_key="wJalrXUtnFEMI/K7MDENG/EXAMPLEKEY",
        region="us-east-1",
        service=SigningService.S3,
        session_token="FQoGZXItoken",
    )


@pytest.fixture
async def store(tmp_path: Path) -> AsyncIterator[SqliteCredentialStore]:
    """Started credential store backed by a tmp SQLite file."""
    s = SqliteCredentialStore(str(tmp_path / "credential_store.db"))
    await s.start()
    yield s
    await s.stop()


@pytest.mark.asyncio
async def test_set_get_roundtrip_static(store: SqliteCredentialStore) -> None:
    """Set + get round-trips a static SigV4 credential and freshens it."""
    creds = _static_creds()
    await store.set(_HOST, creds, source="admin_push")
    row = await store.get(_HOST)
    assert row is not None
    assert row.credential == creds
    assert row.dest_host == _HOST
    assert row.source == "admin_push"
    assert row.status == "fresh"


@pytest.mark.asyncio
async def test_set_get_roundtrip_profile(store: SqliteCredentialStore) -> None:
    """Set + get round-trips a profile-reference credential."""
    creds = ProfileRefCred(service=SigningService.S3, profile="prod", region="us-west-2")
    await store.set(_HOST, creds, source="config")
    row = await store.get(_HOST)
    assert row is not None
    assert row.credential == creds
    assert row.status == "fresh"


@pytest.mark.asyncio
async def test_get_absent_host_is_none(store: SqliteCredentialStore) -> None:
    """``get`` of an unknown host returns ``None``."""
    assert await store.get(HostCredKey("never.seen.example.com")) is None


@pytest.mark.asyncio
async def test_host_keying(store: SqliteCredentialStore) -> None:
    """Distinct hosts are distinct slots; a host returns only its own creds."""
    east = _static_creds()
    west = SigV4StaticCreds(
        access_key_id="AKIAWEST",
        secret_access_key="westsecret",
        region="eu-west-1",
        service=SigningService.S3,
    )
    await store.set(_HOST, east, source="admin_push")
    await store.set(_OTHER_HOST, west, source="admin_push")

    east_row = await store.get(_HOST)
    west_row = await store.get(_OTHER_HOST)
    assert east_row is not None
    assert west_row is not None
    assert east_row.credential == east
    assert west_row.credential == west


@pytest.mark.asyncio
async def test_persist_reopen_resolvable(tmp_path: Path) -> None:
    """A credential survives stop()/start() — the file IS the store (ADR-003)."""
    db = str(tmp_path / "credential_store.db")
    creds = _static_creds()

    s1 = SqliteCredentialStore(db)
    await s1.start()
    await s1.set(_HOST, creds, source="admin_push")
    await s1.stop()

    s2 = SqliteCredentialStore(db)
    await s2.start()
    try:
        row = await s2.get(_HOST)
        assert row is not None
        assert row.credential == creds
        assert row.credential.secret_access_key == creds.secret_access_key
        assert row.status == "fresh"
    finally:
        await s2.stop()


@pytest.mark.asyncio
async def test_mark_bad_preserves_credential(store: SqliteCredentialStore) -> None:
    """``mark_bad`` flips status but keeps the credential (ADR-003)."""
    creds = _static_creds()
    await store.set(_HOST, creds, source="admin_push")
    await store.mark_bad(_HOST)
    row = await store.get(_HOST)
    assert row is not None
    assert row.status == "bad"
    assert row.credential == creds


@pytest.mark.asyncio
async def test_set_unbads_slot(store: SqliteCredentialStore) -> None:
    """A re-push freshens a previously-``mark_bad``'d slot (the loop contract)."""
    await store.set(_HOST, _static_creds(), source="admin_push")
    await store.mark_bad(_HOST)
    bad = await store.get(_HOST)
    assert bad is not None
    assert bad.status == "bad"

    await store.set(_HOST, _static_creds(), source="admin_push")
    fresh = await store.get(_HOST)
    assert fresh is not None
    assert fresh.status == "fresh"


@pytest.mark.asyncio
async def test_set_fires_wake_handler(store: SqliteCredentialStore) -> None:
    """Registered wake handlers run on every set(), with the dest_host."""
    fired: list[HostCredKey] = []

    async def handler(dest_host: HostCredKey) -> None:
        fired.append(dest_host)

    store.register_wake_handler(handler)
    await store.set(_HOST, _static_creds(), source="admin_push")
    assert fired == [_HOST]


@pytest.mark.asyncio
async def test_mark_bad_does_not_fire_wake_handler(store: SqliteCredentialStore) -> None:
    """``mark_bad`` does NOT fire wake handlers — only ``set`` is the trigger."""
    fired: list[HostCredKey] = []

    async def handler(dest_host: HostCredKey) -> None:
        fired.append(dest_host)

    await store.set(_HOST, _static_creds(), source="admin_push")
    store.register_wake_handler(handler)
    await store.mark_bad(_HOST)
    assert fired == []


@pytest.mark.asyncio
async def test_busy_timeout_pragma_applied(store: SqliteCredentialStore, tmp_path: Path) -> None:
    """The store applies the configured busy_timeout PRAGMA (COPY of the token-cache test).

    Asserts both the no-Settings module default AND a cfg-threaded non-default
    value, proving the ``cfg.busy_timeout_ms`` -> PRAGMA wiring.
    """
    from phantom.config.settings import SqliteCfg
    from phantom.storage.sqlite_store import _DEFAULT_BUSY_TIMEOUT_MS

    assert _DEFAULT_BUSY_TIMEOUT_MS == 1000

    conn = store._conn
    assert conn is not None
    cursor = await conn.execute("PRAGMA busy_timeout;")
    try:
        row = await cursor.fetchone()
    finally:
        await cursor.close()
    assert row is not None
    assert row[0] == _DEFAULT_BUSY_TIMEOUT_MS == 1000

    cfg = SqliteCfg(busy_timeout_ms=2500)
    s2 = SqliteCredentialStore(str(tmp_path / "credential_store_cfg.db"), sqlite_cfg=cfg)
    await s2.start()
    try:
        conn2 = s2._conn
        assert conn2 is not None
        cursor2 = await conn2.execute("PRAGMA busy_timeout;")
        try:
            row2 = await cursor2.fetchone()
        finally:
            await cursor2.close()
        assert row2 is not None
        assert row2[0] == cfg.busy_timeout_ms == 2500
    finally:
        await s2.stop()
