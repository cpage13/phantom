"""Idempotency-claim race stress test (plan § 6.2.3).

Fires N concurrent submits with the same idempotency key. Per Phase 1
§ 2.3.17 H7 closure, the atomic admission transaction guarantees
exactly one INSERT pair lands; the rest see the idempotency-collision
response.

Marked ``@pytest.mark.stress``.
"""

from __future__ import annotations

import asyncio
import logging
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient, SubmitOptions

from tests.e2e._driver import build_in_memory_upload_envelope

from ..helpers.payloads import build_create_file_request
from ..helpers.stack import boot_stack

logger = logging.getLogger(__name__)

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"

RACE_CONCURRENCY: int = 32
BODY_BYTES: bytes = b"phantom-idempotency-race-body"

pytestmark = [pytest.mark.e2e, pytest.mark.stress]


async def _submit_with_key(
    pc: PhantomClient,
    *,
    emulator_url: str,
    bearer: str,
    chain_id: UUID,
    idempotency_key: str,
) -> str:
    """Submit one chain forcing a specific idempotency key.

    Returns the server-side chain_id observed in the response. If the
    idempotency key has already been claimed by a different chain_id,
    the server returns the existing chain_id (the H7 collision
    response).
    """
    request = build_create_file_request(file_name=f"race-{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    response = await pc.submit_chain(
        envelope,
        body_refs={"body": BODY_BYTES},
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
        options=SubmitOptions(idempotency_key=idempotency_key),
    )
    return str(response.chain_id)


async def test_idempotency_claim_race_exactly_one_wins() -> None:
    """N concurrent submits with the same key: every response refers to ONE chain_id.

    The atomic admission transaction (plan § 2.3.17 / H7) guarantees
    only one INSERT pair commits. Every concurrent caller — winner
    plus N-1 losers — must see the same final chain_id in its
    response (either it admitted, or it observed the existing
    admitted row's chain_id via the idempotency-collision path).
    """
    stack = await boot_stack(
        config_overrides={
            "saturation": {
                "max_in_flight": RACE_CONCURRENCY * 2,
                "max_in_flight_bytes": RACE_CONCURRENCY * len(BODY_BYTES) * 4,
            },
        },
    )
    shared_key = "race-test-key-shared"
    chain_ids: list[UUID] = [uuid4() for _ in range(RACE_CONCURRENCY)]

    try:
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        bearer = stack.fake_security_token()

        # Fire all concurrently.
        results = await asyncio.gather(
            *(
                _submit_with_key(
                    stack.phantom_client,
                    emulator_url=stack.emulator_url,
                    bearer=bearer,
                    chain_id=chain_ids[i],
                    idempotency_key=shared_key,
                )
                for i in range(RACE_CONCURRENCY)
            )
        )

        # Every response converges to ONE chain_id.
        distinct = set(results)
        assert len(distinct) == 1, (
            f"idempotency race produced {len(distinct)} distinct chain_ids "
            f"under shared key {shared_key!r}; expected exactly 1. Got: {distinct}"
        )

        # That ONE chain_id is the surviving row.
        surviving_chain_id = next(iter(distinct))
        admin = await stack.phantom_client.get_upload(UUID(surviving_chain_id))
        assert admin.chain_id == UUID(surviving_chain_id)
    finally:
        await stack.tear_down()
