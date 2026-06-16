"""Regression test for aggressor Round-7 finding R7-5 scenario B (adopted Defender R8).

Catalog-ref: D-05 (truncated/short response) + D-13 (garbled/incomplete success
body). Source: Toxiproxy truncation; Kafka truncation analogue; PAR
"detect-don't-trust". Surfaced by R7-5's RST-on-create scenario.

A multi-step chain step (e.g. create-file) declares captures the NEXT step
substitutes (``{{create_file.upload_url}}``). On the pre-R8 code, Phantom marked a
step ``succeeded`` purely on the 2xx status and then extracted captures from the
body. A 2xx with an INCOMPLETE body (truncation / buggy proxy / CGNAT half-close —
the emulator's approximate-RST reproduces it: 2xx + empty body) left the declared
captures unextracted, the chain ADVANCED to the upload step, and the upload's URL
template resolved to ``None`` → the row WEDGED in ``attempting`` forever (every
attempt failed substitution, ``attempts`` stayed 0, a saturation slot stayed
HELD), never delivering.

Defender R8 fix (executor.py): a 2xx is no longer advanced on status alone. The
executor validates that every capture this step DECLARES and a LATER step
REFERENCES was actually extracted; a missing required capture is a RETRYABLE
``CaptureIncomplete`` outcome (the same step re-runs, bounded by max-attempts →
``stored``), so a complete body on a later attempt produces the capture and the
chain reaches a clean terminal.
"""

from __future__ import annotations

import asyncio
import hashlib
from uuid import uuid4

import pytest
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope
from tests.e2e.helpers.payloads import build_create_file_request
from tests.e2e.helpers.stack import E2EStack
from tests.e2e.helpers.timing import await_until

BODY = b"R7-5-2xx-missing-capture-" + b"W" * 4096
TERMINAL = {"succeeded", "failed", "corrupted", "stored"}


@pytest.mark.asyncio
async def test_2xx_with_missing_required_capture_does_not_wedge(stack: E2EStack) -> None:
    """A 2xx create response missing a required capture must not wedge the chain."""
    emulator = stack.emulator
    emulator.clear_received()
    emulator.clear_failures()
    bearer = stack.fake_security_token()
    chain_id = uuid4()
    # GLOBAL approximate-RST: the create POST (first upstream call) gets a 2xx with
    # an EMPTY body → the upload_url capture is unextractable.
    emulator.inject_failure(
        FailurePolicy(scope=FailureScope.GLOBAL, tcp_rst_on_request=True)  # type: ignore[call-arg]
    )

    request = build_create_file_request(file_name="r7-5-wedge")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request, files_api_base=stack.emulator_url, local_uuid=chain_id
    )
    await stack.phantom_client.submit_chain(
        envelope,
        body_refs={"body": BODY},
        uid="00000000-0000-0000-0000-000000000001",
        auth_token=f"Bearer {bearer}",
    )
    instance = stack.get_instance("primary")

    # Let a few attempts run under the toxic, then clear so a healthy create can
    # produce the captures. (No event to await on for "N attempts ran under the
    # toxic" — the sleep widens the under-toxic window deterministically.)
    await asyncio.sleep(1.0)  # pre-commit-allow: sleep — widen the under-toxic attempt window
    emulator.clear_failures()

    async def _terminal() -> bool:
        r = await instance.store.get(chain_id)
        return r is not None and r.state in TERMINAL

    # The chain must reach a clean terminal — NOT stay wedged in 'attempting'.
    reached_terminal = True
    try:
        await await_until(
            _terminal,
            timeout_seconds=25.0,
            poll_interval_seconds=0.25,
            message="row never reached a terminal state",
        )
    except Exception:
        reached_terminal = False

    final = await instance.store.get(chain_id)
    assert final is not None
    assert reached_terminal and final.state in TERMINAL, (
        f"chain WEDGED in state={final.state} step={final.current_step_index} "
        f"attempts={final.attempts}: a 2xx create response with a missing required "
        f"capture (upload_url) advanced the chain and stranded the upload step, which "
        f"can never resolve its URL template — the row is stuck forever holding a "
        f"saturation slot"
    )
    # If it succeeded, the body must have delivered exactly once.
    if final.state == "succeeded":
        body_hash = hashlib.sha256(BODY).hexdigest()
        assert sum(1 for e in emulator.received() if e.body_hash == body_hash) == 1
