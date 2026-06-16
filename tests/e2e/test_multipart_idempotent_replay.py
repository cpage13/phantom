"""Multipart idempotent-replay E2E (plan § 6.2.6).

Submits the same multipart envelope twice with the same chain_id +
idempotency key; asserts:

1. The second submit returns the existing row's chain_id (Phantom's
   admission idempotency cache hits per H7 closure).
2. Only ONE row exists in the store after both submits.
3. Every upstream PUT corresponds to a single body delivery (no
   duplicate upstream traffic from the second submit).

This tests the admission-side idempotency path for multipart
envelopes specifically — the H7 closure must work uniformly across
single-body and multi-body chains.
"""

from __future__ import annotations

import logging
import secrets
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient, SubmitOptions

from .helpers.assertions import assert_chain_reaches_state
from .helpers.stack import E2EStack
from .test_multipart_happy_path import _build_multipart_envelope

logger = logging.getLogger(__name__)

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

MULTIPART_FILE_COUNT: int = 3
PER_FILE_BODY_BYTES: int = 4096

PER_CHAIN_BUDGET_SECONDS: float = 60.0

pytestmark = [pytest.mark.e2e]


async def test_multipart_replay_idempotent(
    stack: E2EStack,
    phantom_client: PhantomClient,
) -> None:
    """Two submits with the same idempotency header → second is a no-op.

    SDK-style replay: same ``X-Phantom-Idempotency-Key`` (shared
    across the two submits) but a fresh ``envelope.chain_id`` on the
    second attempt (mirroring real SDK retry: each network-level
    retry rotates the chain_id but keeps the SDK idempotency key
    constant per ``phantom-client/headers.py``).

    Phantom's H7 atomic admission rejects the second insert via the
    idempotency_index unique constraint; the existing chain_id is
    returned. No new upstream traffic results from the second
    submit.
    """
    emulator = stack.emulator
    emulator.clear_received()
    emulator.clear_failures()
    bearer = stack.fake_security_token()

    # Shared idempotency key across both submits — the SDK-replay axis.
    shared_idempotency_key = f"multipart-replay-{uuid4()}"

    # First submit — assign a chain_id, build envelope, submit.
    chain_id_first: UUID = uuid4()
    envelope_first, names = _build_multipart_envelope(
        chain_id=chain_id_first,
        emulator_url=stack.emulator_url,
        body_count=MULTIPART_FILE_COUNT,
    )
    bodies: dict[str, bytes] = {n: secrets.token_bytes(PER_FILE_BODY_BYTES) for n in names}

    response_1 = await phantom_client.submit_chain(
        envelope_first,
        body_refs=bodies,
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
        options=SubmitOptions(idempotency_key=shared_idempotency_key),
    )
    assert response_1.chain_id == chain_id_first
    await assert_chain_reaches_state(
        phantom_client,
        chain_id_first,
        state="succeeded",
        timeout_seconds=PER_CHAIN_BUDGET_SECONDS,
    )

    received_after_first = sum(
        1
        for entry in emulator.received()
        if entry.metadata_kvs.get("phantom_local_uuid") == str(chain_id_first)
    )

    # Second submit — fresh chain_id, SAME idempotency key. The
    # admission transaction's idempotency_index unique constraint
    # collides; admission returns the first chain_id.
    chain_id_second: UUID = uuid4()
    envelope_second, _ = _build_multipart_envelope(
        chain_id=chain_id_second,
        emulator_url=stack.emulator_url,
        body_count=MULTIPART_FILE_COUNT,
    )

    response_2 = await phantom_client.submit_chain(
        envelope_second,
        body_refs=bodies,
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
        options=SubmitOptions(idempotency_key=shared_idempotency_key),
    )
    assert response_2.chain_id == chain_id_first, (
        f"replay must return the FIRST submit's chain_id (idempotency hit); "
        f"got {response_2.chain_id} vs first {chain_id_first}"
    )

    # The second envelope's chain_id was NOT admitted.
    from phantom_client.errors import PhantomNotFoundError

    try:
        await phantom_client.get_upload(chain_id_second)
        pytest.fail(
            f"second envelope's chain_id={chain_id_second} should not have been "
            f"admitted (idempotency replay), but a row exists"
        )
    except PhantomNotFoundError:
        pass  # Expected.

    # No additional upstream traffic from the second submit.
    received_after_second = sum(
        1
        for entry in emulator.received()
        if entry.metadata_kvs.get("phantom_local_uuid") == str(chain_id_first)
    )
    assert received_after_second == received_after_first, (
        f"replay caused {received_after_second - received_after_first} extra "
        f"upstream PUTs; idempotency cache failed for multipart"
    )
