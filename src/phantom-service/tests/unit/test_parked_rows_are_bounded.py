"""A parked row with no payload left must not be immortal.

Objective: pin that the row-count cap can reach an upload which can never be
delivered again, and that it still cannot reach one that can.

THE DEFECT THIS CLOSES (N1). ``auth_expired`` is deliberately excluded from
``TERMINAL_STATES`` so the kickers keep sweeping it, and
``auth_expired_metadata_seconds`` defaults to -1, so the reaper's time-based
pass never runs for it either. Those two facts are individually correct and
deliberate. Together they meant that once the reaper aged out such a row's
BODY, nothing in the service could ever remove the row:

* the kicker skips it, because ``list_parked_candidates`` filters on the same
  ``body_discarded_at`` stamp;
* replay refuses it, for the same reason;
* the reaper's metadata pass is disabled by the -1 default;
* and the row-count cap could not see it, because it is not terminal.

Measured before the fix: fifty aged parked rows survived
``evict_terminal_over_limit(0)``, the hardest cap expressible, untouched. Each
one holds a ``max_rows`` slot for the life of the deployment while being, by
definition, undeliverable: there are no bytes left to send.

The rule is now "terminal OR undeliverable", so the only rows the cap refuses
to take are those that are BOTH non-terminal AND still holding their payload.
That is the durability guarantee stated exactly.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from phantom.models.upload import UploadRow
from phantom.storage.sqlite_store import SqliteUploadStore

_PARKED = "auth_expired"


@pytest.fixture
async def store():
    """A started in-memory store; stops after the test."""
    s = SqliteUploadStore(":memory:")
    await s.start()
    yield s
    await s.stop()


async def test_a_parked_row_whose_body_is_gone_can_be_evicted(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the cap reaches a payload-less parked row.

    Expected: it is evicted. It can never be delivered, so holding a
    ``max_rows`` slot for it forever buys nothing and costs the slot.
    """
    row = make_upload_row(state=_PARKED)
    await store.insert(row)
    flip = await store.discard_body_and_zero_accounting(row.chain_id, expected_state=_PARKED)
    assert flip.flipped is True

    evicted = await store.evict_terminal_over_limit(0)

    assert [e.chain_id for e in evicted] == [row.chain_id], (
        "a parked row with no body left survived the hardest possible row cap; "
        "nothing else in the service can remove it either, so it holds a "
        "max_rows slot for the life of the deployment"
    )
    assert await store.get(row.chain_id) is None


async def test_a_parked_row_that_still_has_its_body_is_never_evicted(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: the durability guarantee is intact.

    Expected: untouched at the hardest cap. This row is waiting on nothing but
    a credential push, and its bytes are intact, so the cap must not take it.
    This is the assertion that makes the fix safe rather than merely effective.
    """
    row = make_upload_row(state=_PARKED)
    await store.insert(row)

    assert await store.evict_terminal_over_limit(0) == []
    assert await store.get(row.chain_id) is not None


async def test_an_undelivered_in_flight_row_is_never_evicted(
    store: SqliteUploadStore, make_upload_row: Callable[..., UploadRow]
) -> None:
    """Objective: widening the predicate did not expose in-flight work.

    Expected: untouched. ``queued`` is neither terminal nor stamped, so it must
    remain outside the cap's reach; this pins that the new arm keys on the
    payload stamp and not on something broader.
    """
    row = make_upload_row(state="queued")
    await store.insert(row)

    assert await store.evict_terminal_over_limit(0) == []
    assert await store.get(row.chain_id) is not None
