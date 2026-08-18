"""Unit tests for phantom.storage.token_cache."""

from __future__ import annotations

from pathlib import Path

import pytest
from phantom.storage.token_cache import SqliteTokenCache


@pytest.fixture
async def cache(tmp_path: Path):
    """Started token cache backed by a tmp SQLite file."""
    c = SqliteTokenCache(str(tmp_path / "tokens.db"))
    await c.start()
    yield c
    await c.stop()


@pytest.mark.asyncio
async def test_set_get_roundtrip(cache: SqliteTokenCache) -> None:
    """Set + get round-trips a single slot."""
    await cache.set("upstream.example.com", "user-1", "Bearer abc", source="inbound_request")
    row = await cache.get("upstream.example.com", "user-1")
    assert row is not None
    assert row.bearer == "Bearer abc"
    assert row.status == "fresh"


@pytest.mark.asyncio
async def test_set_fires_wake_event(cache: SqliteTokenCache) -> None:
    """Registered wake handlers run on every set()."""
    fired: list[tuple[str, str]] = []

    async def handler(endpoint: str, uid: str) -> None:
        fired.append((endpoint, uid))

    cache.register_wake_handler(handler)
    await cache.set("e", "u", "B", source="admin_push")
    assert fired == [("e", "u")]


@pytest.mark.asyncio
async def test_list_no_bearer(cache: SqliteTokenCache) -> None:
    """``list_slots`` never returns the bearer field."""
    await cache.set("e", "u", "BEARER VALUE", source="inbound_request")
    slots = await cache.list_slots()
    assert len(slots) == 1
    blob = slots[0].model_dump_json()
    assert "BEARER VALUE" not in blob


@pytest.mark.asyncio
async def test_mark_bad_preserves_bearer(cache: SqliteTokenCache) -> None:
    """``mark_bad`` flips status but keeps the bearer (ADR-003)."""
    await cache.set("e", "u", "Bearer abc", source="inbound_request")
    await cache.mark_bad("e", "u")
    row = await cache.get("e", "u")
    assert row is not None
    assert row.status == "bad"
    assert row.bearer == "Bearer abc"


@pytest.mark.asyncio
async def test_mark_all_bad_preserves_every_slot(cache: SqliteTokenCache) -> None:
    """``mark_all_bad`` flips every slot to ``bad`` and keeps it (ADR-003)."""
    await cache.set("e1", "u", "Bearer one", source="inbound_request")
    await cache.set("e2", "u", "Bearer two", source="inbound_request")
    affected = await cache.mark_all_bad()
    assert affected == 2
    slots = await cache.list_slots()
    assert len(slots) == 2
    assert {s.status for s in slots} == {"bad"}


@pytest.mark.asyncio
async def test_list_slots_filter_by_endpoint(cache: SqliteTokenCache) -> None:
    """``list_slots(endpoint=...)`` scopes results."""
    await cache.set("e1", "u", "x", source="inbound_request")
    await cache.set("e2", "u", "x", source="inbound_request")
    slots = await cache.list_slots(endpoint="e2")
    assert len(slots) == 1
    assert slots[0].endpoint == "e2"


@pytest.mark.asyncio
async def test_busy_timeout_pragma_applied(cache: SqliteTokenCache, tmp_path: Path) -> None:
    """Token cache also applies the configured busy_timeout PRAGMA.

    The token cache shares the ``busy_timeout`` default with the upload store
    (``_DEFAULT_BUSY_TIMEOUT_MS`` / ``SqliteCfg.busy_timeout_ms``) and likewise
    serializes every writer through its own ``_write_lock``, so internal
    write-vs-write contention is impossible; the busy_timeout only governs
    EXTERNAL cross-process contention. busy_timeout=0 would fail-fast on any
    contention; 1 s rides out sub-second external blips while failing fast
    under a sustained external hold (finding R9-V6-1 — the former 5 s
    amplified an external-lock burst into HTTP read timeouts).

    Asserts both paths: the no-Settings ``cache`` fixture (module default) AND
    a cfg-threaded non-default value (so the ``cfg.busy_timeout_ms`` → PRAGMA
    wiring is proven, not just the module default).
    """
    from phantom.config.settings import SqliteCfg
    from phantom.storage.sqlite_store import _DEFAULT_BUSY_TIMEOUT_MS

    assert _DEFAULT_BUSY_TIMEOUT_MS == 1000

    # No-Settings path (the ``cache`` fixture): the module default applies.
    conn = cache._conn
    assert conn is not None
    cursor = await conn.execute("PRAGMA busy_timeout;")
    try:
        row = await cursor.fetchone()
    finally:
        await cursor.close()
    assert row is not None
    assert row[0] == _DEFAULT_BUSY_TIMEOUT_MS == 1000

    # Config-threaded path: an explicit non-default value reaches the PRAGMA.
    cfg = SqliteCfg(busy_timeout_ms=2500)
    c2 = SqliteTokenCache(str(tmp_path / "tokens_cfg.db"), sqlite_cfg=cfg)
    await c2.start()
    try:
        conn2 = c2._conn
        assert conn2 is not None
        cursor2 = await conn2.execute("PRAGMA busy_timeout;")
        try:
            row2 = await cursor2.fetchone()
        finally:
            await cursor2.close()
        assert row2 is not None
        assert row2[0] == cfg.busy_timeout_ms == 2500
    finally:
        await c2.stop()
