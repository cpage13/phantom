"""Unit tests for phantom.storage.credential_store.

A COPY of ``tests/unit/test_token_cache.py``'s structure (the persistence
round-trip + wake-handler + mark_bad patterns), adapted to the credential
store's forced differences: keyed by host alone, a structured tagged-union
value, and a one-argument wake handler.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
from phantom.models.credential import (
    CredCacheRow,
    HostCredKey,
    ProfileRefCred,
    SigningService,
    SigV4StaticCreds,
)
from phantom.storage.credential_store import SqliteCredentialStore
from pydantic import ValidationError

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


async def _write_raw_row(
    store: SqliteCredentialStore,
    *,
    dest_host: str,
    kind: str,
    cred_json: str,
    status: str,
) -> None:
    """Write a credential row straight through SQLite, bypassing ``set``.

    ``set`` can only write shapes the model already accepts, so a test about
    what the DECODE does with a row it did not write has to put the row there
    itself. This is the on-disk state a schema change, a hand-edit or a
    partially-migrated database leaves behind.
    """
    conn = store._conn
    assert conn is not None
    await conn.execute(
        """
        INSERT INTO credential_store
            (dest_host, kind, cred_json, observed_at, source, status)
        VALUES (?, ?, ?, ?, 'config', ?)
        """,
        (dest_host, kind, cred_json, "2026-09-08T00:00:00+00:00", status),
    )
    await conn.commit()


@pytest.mark.asyncio
async def test_a_row_whose_status_is_outside_the_literal_is_not_served(
    store: SqliteCredentialStore, caplog: pytest.LogCaptureFixture
) -> None:
    """S9-6: a status outside ``CredentialStatus`` is refused, not handed out.

    Objective: the row decode used to splat raw column strings into a bare
    frozen dataclass, so ANY value passed. A ``status`` of neither ``fresh``
    nor ``bad`` then read as USABLE to the executor arm (``status == 'bad'``
    is False) and as UNWAKEABLE to the kicker (``status != 'fresh'``), and the
    two consumers silently disagreed about the same row.

    Expected outcome: ``get`` answers ``None`` (the same answer it gives for a
    host with no credential, which parks the row) and logs the defect at ERROR.
    """
    await _write_raw_row(
        store,
        dest_host=_HOST,
        kind="profile_ref",
        cred_json='{"kind":"profile_ref","service":"s3","profile":null,"region":null}',
        status="weird",
    )
    with caplog.at_level("ERROR"):
        assert await store.get(_HOST) is None
    assert any("credential_store" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_a_corrupt_credential_blob_does_not_raise_into_the_caller(
    store: SqliteCredentialStore,
) -> None:
    """S9-6: an undecodable ``cred_json`` answers ``None`` instead of raising.

    Objective: the decode raised ``KeyError`` on a blob missing ``service``,
    which is neither the ``ValueError`` its own docstring promises nor
    something any caller expects. It escaped ``get`` into the credential
    kicker's scan loop, whose comment states that the route resolve is the
    only raising call in it, so one corrupt row aborted the whole rescan pass.

    Expected outcome: ``get`` answers ``None``, no exception reaches the caller.
    """
    await _write_raw_row(
        store,
        dest_host=_HOST,
        kind="sigv4_static",
        cred_json='{"kind":"sigv4_static","access_key_id":"a","secret_access_key":"b"}',
        status="fresh",
    )
    assert await store.get(_HOST) is None


@pytest.mark.asyncio
async def test_a_kind_column_disagreeing_with_its_blob_is_refused(
    store: SqliteCredentialStore,
) -> None:
    """S9-6: the ``kind`` column and the blob's own discriminator must agree.

    Objective: the row carries the discriminator twice. Decoding from one
    while ignoring the other means a disagreement is resolved silently in
    favour of whichever the code happened to read.

    Expected outcome: ``get`` answers ``None`` rather than serving a
    credential whose stored type is not the type the row claims.
    """
    await _write_raw_row(
        store,
        dest_host=_HOST,
        kind="sigv4_static",
        cred_json='{"kind":"profile_ref","service":"s3","profile":null,"region":null}',
        status="fresh",
    )
    assert await store.get(_HOST) is None


def test_the_credential_row_refuses_a_status_outside_the_literal() -> None:
    """S9-6: ``CredCacheRow`` validates like the token row it copies.

    Objective: ``CredCacheRow`` documents itself as a verbatim copy of
    ``TokenCacheRow``, which is a strict pydantic model. As a bare frozen
    dataclass it validated nothing, so the twin that is supposed to behave
    identically at the boundary behaved oppositely.

    Expected outcome: constructing a row with a status outside
    ``CredentialStatus`` raises, the way the token row does.
    """
    with pytest.raises(ValidationError):
        CredCacheRow(
            dest_host=_HOST,
            credential=_static_creds(),
            observed_at=datetime(2026, 9, 8, tzinfo=UTC),
            source="config",
            status="weird",  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_set_does_not_read_the_slot_back_after_committing(
    store: SqliteCredentialStore,
) -> None:
    """S9-7: ``set`` describes its own write, not whatever landed afterwards.

    Objective: ``set`` re-read the row to return it, and the re-read ran AFTER
    the write transaction released the write lock. A ``mark_bad`` landing in
    that window made ``set`` report ``status='bad'`` for a write it had just
    forced to ``fresh``, which is not a state its own transaction ever saw.

    The interleaving is forced by making the lookup itself flip the slot,
    which is exactly the window the finding describes: after the commit,
    before the read that used to supply the answer.

    The whole return contract is now gone, because no call site ever read it,
    so the property worth pinning is the ABSENCE of the read-back.

    Expected outcome: ``set`` consults ``get`` zero times, answers ``None``,
    and the slot really is ``fresh`` afterwards.
    """
    creds = _static_creds()
    real_get = store.get
    lookups: list[str] = []

    async def recording_get(dest_host: HostCredKey) -> object:
        """Record every read-back so the test can prove there were none."""
        lookups.append(dest_host)
        return await real_get(dest_host)

    store.get = recording_get  # type: ignore[method-assign]
    result = await store.set(_HOST, creds, source="admin_push")

    assert result is None, "set still hands back a row that nothing reads"
    assert lookups == [], (
        f"set re-read the slot after committing ({lookups}); that read runs "
        "outside the write lock, so a concurrent mark_bad can make it "
        "contradict the write it just made"
    )

    store.get = real_get  # type: ignore[method-assign]
    row = await store.get(_HOST)
    assert row is not None
    assert row.status == "fresh"
    assert row.credential == creds
