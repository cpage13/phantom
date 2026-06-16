"""Regression test for aggressor Round-7 finding R7-4b (adopted Defender R8).

Surfaced by R7-4 (SIGSTOP/SIGCONT freeze, catalog A-09) but NOT freeze-specific —
the defect class is D-1's rejection cleanup + an ambiguous-outcome client retry
(catalog D-12 / H-01).

A client that retries an in-flight upload (same chain_id — e.g. its 202 response
was lost) must NOT destroy the original. On the pre-R8 code the 409
``chain_id_in_use`` rejection path ran ``body_store.delete(chain_id)`` to roll
back ITS OWN body-store put — but the body store is keyed by chain_id, which is
SHARED with the original live upload. So the rollback deleted the ORIGINAL's body
→ the original went ``corrupted`` (``body_missing_in_sender``) and was never
delivered. An acknowledged (202) upload was silently lost (invariant #1).

Defender R8 fix (admission.py): a cheap chain_id-collision PRE-CHECK runs BEFORE
``body_store.put``, so a duplicate submit of an already-live chain_id is rejected
409 WITHOUT ever touching the body store — the original's body is never written
over and never deleted. (The atomic insert's CHAIN_ID_COLLISION arm additionally
no longer deletes the shared body, covering the residual tight-race window.)
"""

from __future__ import annotations

import hashlib
from uuid import uuid4

import pytest
from phantom_client.errors import PhantomClientError

from tests.e2e._driver import build_in_memory_upload_envelope
from tests.e2e.helpers.payloads import build_create_file_request
from tests.e2e.helpers.stack import E2EStack
from tests.e2e.helpers.timing import await_until

BODY = b"R7-4b-inflight-body-" + b"Z" * 8192


@pytest.mark.asyncio
async def test_duplicate_chainid_submit_does_not_destroy_inflight_upload(
    stack: E2EStack,
) -> None:
    """A duplicate submit of an in-flight chain_id must not lose the original."""
    emulator = stack.emulator
    emulator.clear_received()
    emulator.clear_failures()
    emulator.pause()  # hold the upstream so the first upload stays in-flight
    bearer = stack.fake_security_token()
    chain_id = uuid4()
    request = build_create_file_request(file_name="r74b")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request, files_api_base=stack.emulator_url, local_uuid=chain_id
    )

    # 1. First submit — accepted (202), buffered, queued.
    await stack.phantom_client.submit_chain(
        envelope,
        body_refs={"body": BODY},
        uid="00000000-0000-0000-0000-000000000001",
        auth_token=f"Bearer {bearer}",
    )
    instance = stack.get_instance("primary")
    assert await instance.body_store.has_body_ref(chain_id, "body"), "first body not buffered"

    # 2. Duplicate submit of the SAME chain_id (a client retry). A 409 rejection
    # is acceptable; DESTROYING the original is not.
    with pytest.raises(PhantomClientError):
        await stack.phantom_client.submit_chain(
            envelope,
            body_refs={"body": BODY},
            uid="00000000-0000-0000-0000-000000000001",
            auth_token=f"Bearer {bearer}",
        )

    # The ORIGINAL upload's body must survive the duplicate's rejection cleanup.
    assert await instance.body_store.has_body_ref(chain_id, "body"), (
        "the duplicate submit's 409 cleanup deleted the ORIGINAL live upload's "
        "body (shared chain_id key) — an acknowledged upload was destroyed"
    )

    # 3. Lift the pause — the original must deliver successfully, not corrupt.
    # ``resume()`` clears the global 503 pause; ``clear_failures()`` only drops
    # injected policies and would leave the upstream paused (so the original
    # could never deliver regardless of the fix).
    emulator.resume()
    emulator.clear_failures()

    async def _terminal() -> bool:
        r = await instance.store.get(chain_id)
        return r is not None and r.state in ("succeeded", "failed", "corrupted", "stored")

    await await_until(
        _terminal,
        timeout_seconds=30.0,
        poll_interval_seconds=0.2,
        message="original row never reached terminal",
    )

    final = await instance.store.get(chain_id)
    assert final is not None
    assert final.state == "succeeded", (
        f"original upload ended {final.state} (last_error={final.last_error!r}) "
        f"instead of delivering — destroyed by the duplicate submit"
    )
    body_hash = hashlib.sha256(BODY).hexdigest()
    assert any(e.body_hash == body_hash for e in emulator.received()), (
        "original body never delivered upstream"
    )
