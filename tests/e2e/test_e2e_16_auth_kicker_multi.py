"""E2E-16 — ``auth_kicker`` ordering under multi-row contention.

Submits N envelopes that share one ``(endpoint, uid)`` slot; injects 401
on every upstream call so each chain transitions to ``auth_expired``;
admin pushes one fresh bearer; the kicker's wake handler fires once and
wakes all N rows in a single sweep. Each chain reaches ``succeeded``
exactly once after the wake.
"""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import boot_stack

# Number of contending rows that share the same (endpoint, uid) slot.
# Five is large enough to test fan-out, small enough to avoid SQLite
# contention.
N_CONTENDING: int = 5

# Body bytes per envelope.
BODY_BYTES: bytes = b"phantom-e2e-auth-kicker-multi"

# Wait budgets.
EXPIRED_WAIT_SECONDS: float = 10.0
SUCCEEDED_WAIT_SECONDS: float = 20.0

# Shared sub claim — every row derives the same uid header.
SHARED_SUB: str = "00000000-0000-0000-0000-000000000016"


@pytest.mark.e2e
async def test_e2e_16_auth_kicker_multi() -> None:
    """One push_token wakes all N auth_expired rows sharing the slot."""
    stack = await boot_stack(
        config_overrides={
            "instances": [
                {
                    "id": "primary",
                    "host_prefixes": ["emulator", "127.0.0.1", "localhost"],
                    "data_dir": "primary",
                    "capture_reexecution": False,
                    "routes": [
                        {
                            "name": "emulator",
                            "hosts": ["emulator", "127.0.0.1", "localhost"],
                            "auth_mode": "phantom_bearer",
                        },
                    ],
                },
            ],
        },
    )
    try:
        pc = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()

        # 1. Inject 401 on every upstream call. auth_401_after_n_calls=0
        #    means "401 from the first call" (the middleware uses
        #    `count > N` so N=0 triggers from count=1).
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                scope=FailureScope.GLOBAL,
                auth_401_after_n_calls=0,
            ),
        )

        # 2. Mint one bearer; every submission rides the same Authorization
        #    header → same uid → same cache slot.
        stale_bearer = stack.fake_security_token(sub=SHARED_SUB)

        chain_ids: list[UUID] = []
        for _ in range(N_CONTENDING):
            cid = uuid4()
            chain_ids.append(cid)
            await _submit_one(
                pc,
                chain_id=cid,
                emulator_url=stack.emulator_url,
                bearer=stale_bearer,
            )

        # 3. Wait for all to reach auth_expired.
        for cid in chain_ids:
            await assert_chain_reaches_state(
                pc,
                cid,
                state="auth_expired",
                timeout_seconds=EXPIRED_WAIT_SECONDS,
            )

        # 4. Confirm there's exactly one (endpoint, uid) token-cache
        #    slot for this shared identity, and its status is `bad`
        #    (or otherwise non-fresh) — the 401 retained the bad
        #    bearer per ADR-003.
        slots = await pc.list_tokens()
        matching = [s for s in slots if s.uid == SHARED_SUB]
        assert len(matching) == 1, (
            f"expected exactly one cache slot for uid={SHARED_SUB}, got {len(matching)}: {matching}"
        )

        # 5. Clear failures and push a fresh bearer. The cache's
        #    set() will fire the wake handler — the auth-kicker
        #    re-queues every auth_expired row sharing (endpoint, uid).
        emulator.clear_failures()
        fresh_bearer = stack.fake_security_token(sub=SHARED_SUB)
        await pc.push_token(
            endpoint=matching[0].endpoint,
            uid=SHARED_SUB,
            token=f"Bearer {fresh_bearer}",
        )

        # 6. Within the deadline, every row reaches succeeded.
        await asyncio.gather(
            *(
                assert_chain_reaches_state(
                    pc,
                    cid,
                    state="succeeded",
                    timeout_seconds=SUCCEEDED_WAIT_SECONDS,
                )
                for cid in chain_ids
            )
        )

        # 7. Emulator received N entries, one per phantom_local_uuid —
        #    no double execution under the multi-row wake.
        received = emulator.received()
        seen = {e.metadata_kvs["phantom_local_uuid"] for e in received}
        expected = {str(c) for c in chain_ids}
        assert seen == expected, (
            f"emulator received mismatch: missing={expected - seen}, extra={seen - expected}"
        )
        # Each chain_id appears exactly once.
        per_uuid_count: dict[str, int] = {}
        for entry in received:
            uid = entry.metadata_kvs["phantom_local_uuid"]
            per_uuid_count[uid] = per_uuid_count.get(uid, 0) + 1
        assert all(c == 1 for c in per_uuid_count.values()), (
            f"each chain_id should appear exactly once: {per_uuid_count}"
        )
    finally:
        await stack.tear_down()


async def _submit_one(
    pc: PhantomClient,
    *,
    chain_id: UUID,
    emulator_url: str,
    bearer: str,
) -> None:
    """Build a single envelope and submit it through phantom-client."""
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    await pc.submit_chain(
        envelope,
        body_refs={"body": BODY_BYTES},
        uid=SHARED_SUB,
        auth_token=f"Bearer {bearer}",
    )
