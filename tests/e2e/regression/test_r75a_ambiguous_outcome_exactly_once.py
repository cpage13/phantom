"""Adopted EXPLORATORY regression test for aggressor Round-7 finding R7-5-A.

Adopted by Defender R8 as permanent coverage of a GREAT durability result (the
property held on current code; this pins it).

Catalog-ref: D-09 (connection reset mid-stream) + D-12 (ambiguous outcome — the
object was stored but the ack was lost) + D-05 (truncated/short response).
Source: Toxiproxy RST/truncation toxics; Jepsen at-least-once with dedup.

The emulator's ``tcp_rst_on_request`` / ``body_cutoff_at_bytes`` mangle the
RESPONSE *after* the handler ran — the upload body WAS received and the
server-side op SUCCEEDED; only the ack to Phantom is reset/truncated. This is the
realistic LTE/CGNAT ambiguous-outcome shape. The correct behavior is
AT-LEAST-ONCE WITH DEDUP: the body must be delivered EXACTLY ONCE (no
double-delivery just because the ack was lost; no loss). Asserted on the TERMINAL
upload step (the last step), where a lost ack must not trigger a redundant
re-upload.
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

BODY = b"R7-5-A-ambiguous-outcome-" + b"Q" * 4096


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "toxic_kwargs",
    [
        {"body_cutoff_at_bytes": 0},  # truncated upload ack (D-05)
        {"tcp_rst_on_request": True},  # RST on the upload ack (D-09)
    ],
    ids=["truncated_upload_ack", "rst_upload_ack"],
)
async def test_ambiguous_outcome_on_upload_delivers_exactly_once(
    stack: E2EStack,
    toxic_kwargs: dict[str, object],
) -> None:
    """A lost/garbled ack on the terminal upload step → body delivered exactly once."""
    emulator = stack.emulator
    emulator.clear_received()
    emulator.clear_failures()
    bearer = stack.fake_security_token()
    chain_id = uuid4()
    # The toxic hits ONLY the upload step (the terminal step). The create step
    # completes cleanly and produces its capture; the upload's server-side write
    # succeeds but its ack is reset/truncated → ambiguous outcome.
    emulator.inject_failure(
        FailurePolicy(scope=FailureScope.UPSTREAM_FILES_UPLOAD, **toxic_kwargs)  # type: ignore[arg-type]
    )

    request = build_create_file_request(file_name="r7-5a")
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

    # Let attempts run under the toxic (the dedup / double-send window), then
    # clear so the ambiguous attempt can resolve. (No event for "the ambiguous
    # attempt happened" — the sleep widens that window deterministically.)
    await asyncio.sleep(1.0)  # pre-commit-allow: sleep — widen the ambiguous-outcome window
    emulator.clear_failures()

    async def _terminal() -> bool:
        r = await instance.store.get(chain_id)
        return r is not None and r.state in ("succeeded", "failed", "corrupted", "stored")

    await await_until(
        _terminal,
        timeout_seconds=30.0,
        poll_interval_seconds=0.25,
        message="row never reached a terminal state under ambiguous outcome",
    )

    final = await instance.store.get(chain_id)
    assert final is not None
    assert final.state == "succeeded", (
        f"ambiguous outcome ended {final.state} (last_error={final.last_error!r}) "
        f"instead of succeeding — the lost ack was mishandled"
    )
    body_hash = hashlib.sha256(BODY).hexdigest()
    deliveries = sum(1 for e in emulator.received() if e.body_hash == body_hash)
    assert deliveries == 1, (
        f"body delivered {deliveries} times (want exactly 1) — a lost ack on the "
        f"terminal upload step caused a double-delivery (at-least-once dedup failed) "
        f"or a loss"
    )
