"""Claim fairness under a steady admission flood (round 2 adversary).

``SqliteUploadStore.claim_due`` orders claims by ``next_attempt_at``
alone (``ORDER BY next_attempt_at ASC LIMIT n``, NULL stamps first).
Round 1 reasoned this coherent without pinning it; this module makes
the fairness property executable: a steady flood of freshly admitted
rows (each stamped at its admission instant) cannot starve parked
retries whose due stamps are earlier, because eligibility and ordering
both read the same column. The drain order is the stamp order, batch
boundaries and mid-drain arrivals notwithstanding.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from phantom.models.upload import UploadRow
from phantom.storage import SqliteUploadStore

from .conftest import track_started

# Parked retries: due earliest (their backoff windows expired first).
_N_RETRY = 10

# Initial flood: fresh admissions stamped after every retry's due time.
_N_FLOOD_INITIAL = 20

# Mid-drain flood: arrives between claim batches, stamped later still.
_N_FLOOD_MID_DRAIN = 10

# Claim batch size; small so the drain spans several batches and the
# mid-drain arrivals land between them.
_CLAIM_BATCH = 5

# Stamp spacing; any positive spacing works (ordering is what matters).
_STAMP_SPACING = timedelta(milliseconds=50)

# How far in the past the earliest stamp sits; everything is due at
# claim time so eligibility never filters, only ordering decides.
_PAST_OFFSET = timedelta(minutes=5)


@pytest.fixture
async def store(tmp_path: Path) -> SqliteUploadStore:
    """Live single-store fixture."""
    s = SqliteUploadStore(str(tmp_path / "uploads.db"))
    await s.start()
    return track_started(s)


@pytest.mark.asyncio
async def test_flood_cannot_starve_due_retries(
    store: SqliteUploadStore,
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """The drain order is exactly the ``next_attempt_at`` order.

    One NULL-stamped row pins the NULLs-first edge; the parked retries
    (earliest stamps) must all surface before any flood row, and the
    mid-drain arrivals (latest stamps) must wait their turn even though
    they are inserted while the drain is already running.
    """
    now = datetime.now(tz=UTC)
    base = now - _PAST_OFFSET

    null_row = make_upload_row(state="queued", next_attempt_at=None)
    await store.insert(null_row)

    expected_after_null: list[UUID] = []
    retry_ids: list[UUID] = []
    for index in range(_N_RETRY):
        row = make_upload_row(
            state="queued",
            next_attempt_at=base + _STAMP_SPACING * index,
            attempts=1,
        )
        await store.insert(row)
        expected_after_null.append(row.chain_id)
        retry_ids.append(row.chain_id)

    flood_base = base + _STAMP_SPACING * _N_RETRY
    for index in range(_N_FLOOD_INITIAL):
        row = make_upload_row(
            state="queued",
            next_attempt_at=flood_base + _STAMP_SPACING * index,
        )
        await store.insert(row)
        expected_after_null.append(row.chain_id)

    claimed_ids: list[UUID] = []
    first_batch = await store.claim_due(now, _CLAIM_BATCH)
    claimed_ids.extend(row.chain_id for row in first_batch)
    assert claimed_ids[0] == null_row.chain_id, "a NULL stamp claims first (SQL NULLs-first)"

    # The flood keeps arriving mid-drain, stamped later than everything
    # already parked; it must queue up behind, not preempt.
    mid_drain_base = flood_base + _STAMP_SPACING * _N_FLOOD_INITIAL
    for index in range(_N_FLOOD_MID_DRAIN):
        row = make_upload_row(
            state="queued",
            next_attempt_at=mid_drain_base + _STAMP_SPACING * index,
        )
        await store.insert(row)
        expected_after_null.append(row.chain_id)

    total_rows = 1 + _N_RETRY + _N_FLOOD_INITIAL + _N_FLOOD_MID_DRAIN
    while len(claimed_ids) < total_rows:
        batch = await store.claim_due(now, _CLAIM_BATCH)
        assert batch, "drain stalled with eligible rows remaining"
        claimed_ids.extend(row.chain_id for row in batch)

    assert claimed_ids == [null_row.chain_id, *expected_after_null], (
        "claims must drain in next_attempt_at order, batch boundaries and "
        "mid-drain arrivals notwithstanding"
    )
    retry_positions = [claimed_ids.index(chain_id) for chain_id in retry_ids]
    assert max(retry_positions) < 1 + _N_RETRY, (
        f"every parked retry must surface before any flood row; positions={retry_positions!r}"
    )
    assert await store.claim_due(now, _CLAIM_BATCH) == [], "nothing left to claim"
