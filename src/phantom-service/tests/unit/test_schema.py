"""Unit tests for storage/schema.sql application."""

from __future__ import annotations

import pytest
from phantom.storage.sqlite_store import SqliteUploadStore


@pytest.mark.asyncio
async def test_schema_applies_idempotently() -> None:
    """Applying the schema twice against ``:memory:`` is a no-op."""
    store = SqliteUploadStore(":memory:")
    await store.start()
    # Re-running the schema must not raise (CREATE TABLE IF NOT EXISTS).
    await store.start()
    await store.stop()
