"""A caller-supplied ``since`` bound must be compared as an instant, not as text.

Objective: pin the UTC normalisation on every admin ``since`` filter.

THE DEFECT THIS CLOSES. Every timestamp column is written from
``datetime.now(tz=UTC).isoformat()``, so every stored value ends in ``+00:00``,
and SQLite compares these columns as TEXT. ``bulk_delete`` and ``list_uploads``
bound the caller's value with a bare ``.isoformat()``, keeping whatever offset
it arrived with, so the comparison was lexicographic across two different
offsets.

``DeleteFilter.since`` is a plain datetime with ``strict=False`` and no UTC
validator, so an operator on a UTC-7 host running ``date -Iseconds`` submits
``2026-09-07T13:00:00-07:00`` meaning "everything at or after 20:00Z". The raw
text sorts as 13:00Z, so the HARD DELETE took seven extra hours of rows.
``bulk_delete`` places no state restriction, so those extra rows included
``queued`` and ``attempting`` uploads that had never been delivered. An
east-of-UTC offset fails the other way, silently skipping rows the operator did
ask to remove.

This is the same class of defect :mod:`phantom.storage.timestamps` exists to
close for filesystem artifact names, one column family over.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta, timezone

import pytest
from phantom.models.upload import UploadRow
from phantom.storage.sqlite_store import SqliteUploadStore

# The operator's local zone in the worked example. West of UTC, which is the
# direction that OVER-deletes.
_WEST = timezone(timedelta(hours=-7))
# East of UTC, the direction that silently UNDER-deletes.
_EAST = timezone(timedelta(hours=+7))

_NOON_UTC = datetime(2026, 9, 7, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
async def store():
    """A started in-memory store; stops after the test."""
    s = SqliteUploadStore(":memory:")
    await s.start()
    yield s
    await s.stop()


async def _seed_hourly(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> list[UploadRow]:
    """Insert one queued row per hour from 12:00Z through 20:00Z inclusive."""
    rows: list[UploadRow] = []
    for hour in range(9):
        row = make_upload_row(
            state="queued",
            received_at=_NOON_UTC + timedelta(hours=hour),
            updated_at=_NOON_UTC + timedelta(hours=hour),
        )
        await store.insert(row)
        rows.append(row)
    return rows


@pytest.mark.asyncio
async def test_west_of_utc_since_does_not_over_delete(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: a UTC-7 bound deletes from the instant it names, not from its text.

    Expected: exactly the one seeded row at 20:00Z. Before normalisation the
    text sorted as 13:00Z, so this deleted seven extra hours' worth, including
    undelivered queued uploads.
    """
    await _seed_hourly(store, make_upload_row)
    # 13:00 at UTC-7 IS 20:00Z. Rows at 20:00Z are the only ones at or after it.
    since = datetime(2026, 9, 7, 13, 0, 0, tzinfo=_WEST)
    assert since == datetime(2026, 9, 7, 20, 0, 0, tzinfo=UTC)

    removed = await store.bulk_delete(state=None, route=None, since=since)

    assert len(removed) == 1, (
        f"expected only the 20:00Z row, deleted {len(removed)}; a west-of-UTC "
        "offset compared as text selects hours of extra rows"
    )
    survivors = await _all_rows(store)
    assert len(survivors) == 8


@pytest.mark.asyncio
async def test_east_of_utc_since_does_not_under_delete(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the mirror direction, where text comparison silently skips rows.

    Expected: the four rows at or after 17:00Z. An east-of-UTC bound sorts
    LATER than its true instant, so before normalisation the operator was
    quietly left with rows they had asked to remove.
    """
    await _seed_hourly(store, make_upload_row)
    # 24:00 at UTC+7 is 17:00Z on the same day.
    since = datetime(2026, 9, 8, 0, 0, 0, tzinfo=_EAST)
    assert since == datetime(2026, 9, 7, 17, 0, 0, tzinfo=UTC)

    removed = await store.bulk_delete(state=None, route=None, since=since)

    assert len(removed) == 4, (
        f"expected the 17:00Z through 20:00Z rows, deleted {len(removed)}; an "
        "east-of-UTC offset compared as text skips rows the operator asked for"
    )


@pytest.mark.asyncio
async def test_naive_since_is_read_as_utc(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: a naive bound is interpreted the way every writer means it.

    Expected: the same result as the equivalent aware UTC bound. The admin
    models accept a naive datetime off the wire, and every writer of these
    columns means UTC, so a naive value is normalised rather than rejected.
    """
    await _seed_hourly(store, make_upload_row)
    naive = datetime(2026, 9, 7, 18, 0, 0)

    removed = await store.bulk_delete(state=None, route=None, since=naive)

    assert len(removed) == 3


@pytest.mark.asyncio
async def test_list_uploads_since_is_normalised_too(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the read path carries the same normalisation as the delete path.

    Expected: the one seeded row at 20:00Z, matching what the delete path
    takes for the same bound. The read is less dangerous than the hard delete
    but shares the bug, and an operator who lists before deleting would
    otherwise be shown a different set than the delete would remove.
    """
    await _seed_hourly(store, make_upload_row)
    since = datetime(2026, 9, 7, 13, 0, 0, tzinfo=_WEST)

    rows, _ = await store.list_uploads(since=since, limit=100)

    assert len(rows) == 1


async def _all_rows(store: SqliteUploadStore) -> list[UploadRow]:
    """Every row currently in the table, for survivor assertions."""
    return [row async for row in store.iter_rows()]
