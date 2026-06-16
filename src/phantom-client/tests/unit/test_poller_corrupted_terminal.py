"""poll_until must stop on ``corrupted`` - a service terminal state (round 6, R6-5).

The service's terminal set (``phantom.storage.interface.TERMINAL_STATES``)
is ``{succeeded, failed, stored, cancelled, corrupted}``: ``corrupted`` is
terminal - the sender reaches it when body verification fails at send
time (storage_hash mismatch, body_hash / codec round-trip drift, or a
missing body file), and no retry will ever move the row off it.

The SDK's ``poll_until`` default stop-set
(``phantom_client.models.status.TERMINAL_STATES``) is
``{succeeded, failed, cancelled, stored, auth_expired}``. It deliberately
ADDS ``auth_expired`` (a revivable state) so the caller is not left
hanging on a token wait, and the companion ``poll_group_until_finished``
docstring states that ``corrupted`` members "count as finished". But the
single-chain stop-set OMITS ``corrupted``. So a chain that reaches the
most-terminal state of all never satisfies
``response.state in terminal_states``: ``poll_until`` loops until the
deadline and raises ``PollDeadlineExceeded`` (or loops forever when no
deadline is set).

Why it matters: a caller using ``poll_until`` to wait for an upload that
corrupts cannot tell "permanently corrupted" from "still slowly
progressing" - both surface as a poll timeout. On Pi-class hardware with
flaky storage, ``corrupted`` is a realistic outcome, and the caller burns
its whole deadline budget before learning the upload is dead, instead of
getting a prompt ``ChainAdminDetail(state="corrupted")`` it can act on.
The fix is to add ``corrupted`` to the SDK default stop-set (it is more
terminal than ``auth_expired``/``stored``, both already in the set),
aligning the single-chain poller with the service terminal set and with
the group poller's own "corrupted counts as finished" rule.

The repro returns a ``corrupted`` row on every poll and asserts
``poll_until`` returns it promptly. Before R6-5 it never stopped and
the bounded deadline raised ``PollDeadlineExceeded``; the fix added
``corrupted`` to ``TERMINAL_STATES``.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from phantom_client.config import ClientConfig, RetryPolicy
from phantom_client.poller import poll_until
from phantom_client.transport import Transport

# A short, bounded deadline so the test cannot hang on the defect: under
# the bug poll_until loops past it and raises PollDeadlineExceeded within
# this budget; under the fix it returns on the first poll, well inside it.
_DEADLINE_BUDGET_SECONDS = 1.0
# Tiny inter-poll delay so the loop iterates quickly toward the deadline.
_POLL_DELAY_SECONDS = 0.01


def _corrupted_row_response(chain_id: str) -> httpx.Response:
    """Build a ``GET /v1/admin/chains/{id}`` response with state=corrupted.

    Mirrors the ChainAdminDetail shape the test_poller fixture builds; the
    state is ``corrupted`` with a storage-corruption ``last_error`` exactly
    as the sender stamps it on a body-verification failure.
    """
    now = "2026-06-10T00:00:00+00:00"
    return httpx.Response(
        200,
        content=json.dumps(
            {
                "chain_id": chain_id,
                "state": "corrupted",
                "received_at": now,
                "updated_at": now,
                "next_attempt_at": None,
                "sent_at": None,
                "group_id": chain_id,
                "multifile_id": None,
                "send_order": 0,
                "body_location": "ram",
                "last_step_completed": None,
                "captured": [],
                "attempts": 1,
                "last_error": "storage_corruption:bodies_missing",
            }
        ),
        headers={
            "Content-Type": "application/json",
            "X-Phantom-Suggested-Poll-After": "0",
        },
    )


def _make_transport(handler: object) -> Transport:
    cfg = ClientConfig(
        phantom_url="http://test",
        retry_policy=RetryPolicy(max_attempts=1, backoff_jitter=False),
    )
    return Transport(cfg, transport=httpx.MockTransport(handler))  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_poll_until_stops_on_corrupted() -> None:
    """A ``corrupted`` upload must end the poll promptly, not time out.

    ``corrupted`` is a permanent service terminal state; ``poll_until``
    must return the row as soon as it observes it. Before R6-5 the
    state was missing from the default stop-set, so the loop ran to the
    bounded deadline and raised ``PollDeadlineExceeded``; the fix adds
    ``corrupted`` to ``TERMINAL_STATES``.
    """
    chain_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        return _corrupted_row_response(str(chain_id))

    transport = _make_transport(handler)
    await transport.start()
    deadline = datetime.now(tz=UTC) + timedelta(seconds=_DEADLINE_BUDGET_SECONDS)
    try:
        response = await poll_until(
            transport,
            chain_id,
            initial_delay_seconds=_POLL_DELAY_SECONDS,
            deadline=deadline,
        )
    finally:
        await transport.aclose()

    assert response.state == "corrupted", (
        "poll_until must stop on the terminal 'corrupted' state; instead it "
        "looped to the deadline (a caller cannot distinguish a permanently "
        "corrupted upload from one that is merely slow)"
    )
