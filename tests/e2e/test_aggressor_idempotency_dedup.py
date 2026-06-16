"""Aggressor — idempotency dedup via ``X-Phantom-Idempotency-Key``.

The SDK sends ``X-Phantom-Idempotency-Key`` on every submission (default
value = ``str(envelope.chain_id)``). Phantom's admission path consults
``claim_idempotency`` against the disk store: if the same key has
already been claimed by another chain, the new submission must return
the existing row (status 200, idempotency replay) rather than create a
new row.

This test covers the normal-user idempotency scenario: two submissions
with the *same* explicit ``Idempotency-Key`` (but distinct chain_ids)
should result in ONE buffered upload, not two. The second response
must reference the first chain_id.

Contract pinned:

1. Submit chain A with ``Idempotency-Key=K`` → row A created.
2. Submit chain B with ``Idempotency-Key=K`` (different chain_id) →
   admission's ``claim_idempotency`` finds K already claimed, returns
   row A.
3. ``list_uploads`` returns one row (A), not two.

If the dedup path fails (both rows persist), this catches it. If the
dedup path's 200-vs-202 distinction is missing, the per-row count is
still the load-bearing assertion.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient, SubmitOptions

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"
SHARED_KEY: str = "phantom-aggressor-idempotency-key-shared-001"
BODY: bytes = b"phantom-aggressor-idempotency-body"
TERMINAL_BUDGET_SECONDS: float = 15.0

pytestmark = pytest.mark.e2e


async def _submit_with_key(
    pc: PhantomClient,
    *,
    emulator_url: str,
    bearer: str,
    chain_id: UUID,
    idempotency_key: str,
) -> UUID:
    """Submit one chain with an explicit Idempotency-Key.

    Returns the chain_id Phantom assigned (either the submitted one or
    the existing one on dedup).
    """
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    response = await pc.submit_chain(
        envelope,
        body_refs={"body": BODY},
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
        options=SubmitOptions(idempotency_key=idempotency_key),  # type: ignore[call-arg]
    )
    return response.chain_id


async def test_aggressor_idempotency_dedup_same_key(tmp_path: Path) -> None:
    """Two submissions with same Idempotency-Key produce ONE row."""
    stack: E2EStack = await boot_stack(tmp_path=tmp_path)
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()
        bearer = stack.fake_security_token()

        chain_a = uuid4()
        chain_b = uuid4()
        assert chain_a != chain_b

        returned_a = await _submit_with_key(
            pc,
            emulator_url=stack.emulator_url,
            bearer=bearer,
            chain_id=chain_a,
            idempotency_key=SHARED_KEY,
        )
        # The first submission is admitted; the returned chain_id
        # equals the submitted chain_id.
        assert returned_a == chain_a, (
            f"first submission returned chain_id={returned_a}, expected {chain_a}"
        )

        # Wait for chain A to reach succeeded — establishes the row's
        # persistence before the dedup attempt.
        await assert_chain_reaches_state(
            pc,
            chain_a,
            state="succeeded",
            timeout_seconds=TERMINAL_BUDGET_SECONDS,
        )

        # Second submission with the SAME Idempotency-Key but a
        # DIFFERENT chain_id. The dedup path must claim the existing
        # row and return chain_a, not chain_b.
        returned_b = await _submit_with_key(
            pc,
            emulator_url=stack.emulator_url,
            bearer=bearer,
            chain_id=chain_b,
            idempotency_key=SHARED_KEY,
        )
        assert returned_b == chain_a, (
            f"second submission with same Idempotency-Key should return chain_a={chain_a}, "
            f"got {returned_b}. Expected idempotency dedup; got new row."
        )

        # No row for chain_b should exist (it was deduped).
        rows, _ = await pc.list_uploads(limit=200)
        row_ids = {r.chain_id for r in rows}
        assert chain_a in row_ids, f"chain_a {chain_a} missing from list_uploads"
        assert chain_b not in row_ids, (
            f"chain_b {chain_b} should have been deduped; "
            f"observed in list_uploads alongside chain_a — admission's "
            f"claim_idempotency did not dedup"
        )

        # Only one row total for the shared idempotency key — the
        # row count among our two submitted ids should be exactly 1.
        relevant = [r for r in rows if r.chain_id in {chain_a, chain_b}]
        assert len(relevant) == 1, (
            f"expected 1 row across (chain_a, chain_b), got {len(relevant)}: "
            f"{[(r.chain_id, r.state) for r in relevant]}"
        )
    finally:
        await stack.tear_down()
