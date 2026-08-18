"""Q2: an out-of-range persisted step index is classified, not raised.

``ChainExecutor.execute_one_step`` raises when ``row.current_step_index``
points past the end of the row's persisted envelope. Nothing used to catch it:
``_drive_one``'s ``try`` wrapped the body load only, and ``_worker_loop``
re-raises anything that is not a classified lock error, so the exception
escaped the worker TaskGroup and killed the process. Recovery then reset the
row to ``queued`` and the next claim crashed again, which is F1's crash-loop
shape.

The state is unreachable through every writer of that column, so this test
constructs it directly. What it proves is the CLASSIFICATION, not that the
state occurs.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock

from phantom.chain.executor import ChainExecutor, default_clock
from phantom.instances.context import InstanceContext
from phantom.models.upload import UploadRow
from phantom.storage.interface import UploadStore
from phantom.workers.sender import Sender

# A one-step chain. The row below claims to be on step 1 of it, which is one
# past the end: the executor's own bound is ``step_index >= len(steps)``.
_ONE_STEP_ENVELOPE = json.dumps(
    {
        "chain_id": "00000000-0000-4000-8000-000000000001",
        "idempotency_key": "k",
        "steps": [
            {
                "name": "only_step",
                "method": "POST",
                "url": "https://files.example.com/v2/files",
            }
        ],
    }
)


async def test_step_index_past_the_end_routes_the_row_to_corrupted(
    make_upload_row: Callable[..., UploadRow],
) -> None:
    """A row whose index outruns its envelope lands in ``corrupted``, not a crash.

    Objective: the REAL executor raises on the out-of-range index, and
    ``_drive_one`` catches that one type and routes the row through the same
    ``_on_corrupted`` path the two hash failures use, with a
    ``step_index_out_of_range`` detail token an operator can grep. Success is
    that the call RETURNS having transitioned the row, rather than propagating
    the exception out of the worker.

    Both halves are exercised together on purpose: mocking the executor would
    prove the arm without proving the raise site still reaches it. Pre-fix the
    executor raised a bare ``ValueError``, ``_drive_one``'s ``try`` did not
    span the executor call at all, and the exception escaped, so no assertion
    below ran. The catch is deliberately bound to the named type: a bare
    ``except ValueError`` would also swallow ``resolve_route``'s, and F1 gives
    that its own classification.
    """
    row = make_upload_row(
        state="attempting",
        chain_envelope_json=_ONE_STEP_ENVELOPE,
        current_step_index=1,
        body_hashes={},
    )
    instance = MagicMock(spec=InstanceContext)
    instance.executor = ChainExecutor(
        token_cache=MagicMock(),
        upstream_client=MagicMock(),
        resolve_route=MagicMock(),
        clock=default_clock,
        instance=MagicMock(),
    )
    sender = Sender(instance=instance, worker_count=1, poll_interval_ms=1)
    corrupted = AsyncMock()
    sender._on_corrupted = corrupted  # type: ignore[method-assign]
    store = MagicMock(spec=UploadStore)

    await sender._drive_one(store, row)

    corrupted.assert_awaited_once()
    kwargs = corrupted.await_args.kwargs
    assert kwargs["error_code"] == "storage_corruption"
    assert kwargs["detail"].startswith("step_index_out_of_range:")
    assert "past end of chain" in kwargs["detail"]
