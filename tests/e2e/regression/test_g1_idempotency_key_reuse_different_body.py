"""Regression test for aggressor probe G-1 (adopted Round 2).

Asserts that reusing an ``X-Phantom-Idempotency-Key`` with a DIFFERENT
body does NOT silently discard the second body behind a success-shaped
200. Round 1 found admission's idempotency claim was keyed on the header
alone (no body binding), so the second submit replayed the FIRST chain
and the second body never reached the upstream — with no signal.

This is distinct from ``tests/e2e/test_aggressor_idempotency_dedup.py``,
which covers same-key + different-chain_id with the SAME body (a
legitimate replay). This test covers the data-loss angle: same key,
DIFFERENT body.

Defender Round 2 decision: REJECT. On an idempotency-key collision,
admission compares the incoming body's raw ``body_hash`` set against the
existing row's (``_body_hashes_diverge``). A divergent body raises a
deterministic 422 ``idempotency_key_conflict`` (registered in ADR-017,
``phantom.models.errors``, ``phantom_client.errors`` as
``PhantomUnprocessableError``); an identical body still replays at 200.
An idempotency key MUST be a function of the body (see ADR-017 +
operator-playbook). No schema change — the binding reuses the existing
per-row ``body_hashes``.
"""

from __future__ import annotations

import hashlib
from uuid import uuid4

import pytest
from phantom_client import PhantomClient, SubmitOptions

from tests.e2e._driver import build_in_memory_upload_envelope
from tests.e2e.helpers.payloads import build_create_file_request
from tests.e2e.helpers.stack import E2EStack

pytestmark = pytest.mark.e2e

SHARED_KEY = "g1-regression-shared-idempotency-key"
BODY_A = b"G1-regression-BODY-A-forwarded"
BODY_B = b"G1-regression-BODY-B-different-bytes"


async def test_idempotency_key_reuse_with_different_body_is_not_silently_dropped(
    stack: E2EStack,
) -> None:
    """Same key + different body must NOT silently drop the second body.

    Asserts the reject posture (the Defender R2 decision): the second
    submit returns a 4xx idempotency-conflict rather than a
    success-shaped 200 replay that swallows BODY_B.
    """
    import httpx

    pc: PhantomClient = stack.phantom_client
    stack.emulator.clear_received()
    bearer = stack.fake_security_token()
    hash_b = hashlib.sha256(BODY_B).hexdigest()

    async def _submit(chain_id, body: bytes):
        req = build_create_file_request(file_name=f"g1-{chain_id.hex[:8]}")
        req.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
        envelope, _ = build_in_memory_upload_envelope(
            request=req,
            files_api_base=stack.emulator_url,
            local_uuid=chain_id,
        )
        return await pc.submit_chain(
            envelope,
            body_refs={"body": body},
            uid="00000000-0000-0000-0000-000000000001",
            auth_token=f"Bearer {bearer}",
            options=SubmitOptions(idempotency_key=SHARED_KEY),  # type: ignore[call-arg]
        )

    chain_a = uuid4()
    chain_b = uuid4()

    # First submit (BODY_A) — admitted, drains upstream.
    await _submit(chain_a, BODY_A)

    # Second submit, SAME key, DIFFERENT body. The reject posture means
    # this raises a 4xx client error rather than returning a 200 replay
    # that swallows BODY_B.
    from phantom_client.errors import PhantomClientError

    with pytest.raises(PhantomClientError) as exc_info:
        await _submit(chain_b, BODY_B)

    # The error must describe an idempotency-key conflict (the exact code
    # is the fix author's choice; the bright line is "not a silent 200").
    msg = str(exc_info.value).lower()
    assert "idempotency" in msg or "conflict" in msg, (
        f"second submit raised {exc_info.value!r}; expected an "
        "idempotency-key-conflict 4xx. BODY_B must not be silently dropped."
    )

    # Belt-and-suspenders: BODY_B must never have been forwarded under a
    # silent replay (it should also not be forwarded under reject — the
    # point is the producer gets a signal, not a success).
    async with httpx.AsyncClient() as http:
        recv = (await http.get(f"{stack.emulator_url}/control/received")).json()
    received_hashes = [r["body_hash"] for r in recv.get("received", [])]
    # Under the reject posture BODY_B is not forwarded AND the producer was
    # told. Under the current (buggy) posture BODY_B is not forwarded but
    # the producer got a 200 — which is what the pytest.raises above catches.
    assert hash_b not in received_hashes, (
        "BODY_B reached the upstream unexpectedly; the conflict path should not forward it."
    )
