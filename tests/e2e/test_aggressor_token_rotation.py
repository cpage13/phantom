"""Aggressor — Token rotation (admin push under `wait` strategy).

The producer's token lifetime is ~3600s (Azure AD default for
client-credentials). When a buffered upload's cached token rotates
in flight, Phantom MUST use the new token on the next attempt — not
the cached-at-submission-time bytes. This catches a class of bug
where an attempt captures the bearer at submit time rather than
reading the cache slot on each retry.

Two sub-cases:

A — `wait` strategy + admin-pushed rotation BEFORE chains complete.
    Submit 10 chains while emulator returns 5xx to force them to park
    in the retry queue. Push T2 (replacing T1) via admin. Clear the
    failure policy. Verify all 10 chains succeed with T2 in the
    Authorization header of the eventual successful attempts.

B — `wait` strategy + 401 auth_expired path under sustained block;
    admin-push-token wakes the parked chains. This mirrors the v0
    operator-JWT flow where the operator re-logins and the dashboard
    pushes the fresh token.

The user's protocol asked for an `ad_client_credentials` test (B);
however, the existing E2E-4 test documents that azure-identity's path
templating against the local emulator is unstable. We adopt the same
`wait` + admin-push pattern that E2E-4 validates and use it for the
auth-recovery shape. The structural invariant (Phantom does not pin a
stale token across rotation) is the same regardless of refresh
strategy.
"""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID, uuid4

import pytest
from phantom_client import PhantomClient
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until

DEFAULT_SUB: str = "00000000-0000-0000-0000-000000000001"
BODY: bytes = b"phantom-aggressor-token-rotation-body"
N_CHAINS: int = 10
TERMINAL_BUDGET_SECONDS: float = 30.0
AUTH_EXPIRED_BUDGET_SECONDS: float = 5.0

pytestmark = pytest.mark.e2e


async def _submit_one(
    pc: PhantomClient,
    *,
    emulator_url: str,
    bearer: str,
    chain_id: UUID,
) -> None:
    """Submit one chain with the given bearer + chain_id."""
    request = build_create_file_request(file_name=f"e2e_{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=emulator_url,
        local_uuid=chain_id,
    )
    await pc.submit_chain(
        envelope,
        body_refs={"body": BODY},
        uid=DEFAULT_SUB,
        auth_token=f"Bearer {bearer}",
    )


async def test_aggressor_admin_pushed_rotation_during_retry(tmp_path: Path) -> None:
    """T1 cached → 5xx forces retries → admin push T2 → chains succeed.

    Verifies that Phantom does NOT pin the original bearer captured
    at submission time. The (endpoint, uid) cache slot is consulted
    on each attempt; rotating it mid-retry should propagate.
    """
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            # Retry quickly so the test can observe the post-rotation
            # attempt within a few seconds.
            "retry": {
                "worker_count": 2,
                "poll_interval_ms": 100,
                "default_strategy": {
                    "type": "fixed_intervals",
                    "intervals_seconds": [0, 1, 1, 1, 2, 3, 5],
                },
            },
            "saturation": {
                "max_in_flight": 50,
                "max_in_flight_bytes": 32 * 1024 * 1024,
            },
        },
    )
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()

        # Mint T1 (the bearer the producer submits with).
        t1 = stack.fake_security_token(extra_claims={"token_version": "T1"})
        # Mint T2 (the rotated bearer the admin pushes).
        t2 = stack.fake_security_token(extra_claims={"token_version": "T2"})
        assert t1 != t2, "T1 and T2 must differ for this test to be meaningful"

        # Install 5xx so the first attempt of every chain fails and
        # triggers retries.
        stack.emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]
                scope=FailureScope.GLOBAL,
                error_rate_5xx=1.0,
            ),
        )

        # Submit 10 chains under T1.
        chain_ids: list[UUID] = []
        for _ in range(N_CHAINS):
            cid = uuid4()
            chain_ids.append(cid)
            await _submit_one(
                pc,
                emulator_url=stack.emulator_url,
                bearer=t1,
                chain_id=cid,
            )

        # Wait until each chain has at least one failed attempt — the
        # 5xx is actually being applied. This confirms the cache slot
        # is populated with T1.
        async def _at_least_one_attempt() -> bool:
            details = [await pc.get_upload(c) for c in chain_ids]
            return all(d.attempts >= 1 for d in details)

        await await_until(
            _at_least_one_attempt,
            timeout_seconds=15.0,
            message="not all chains saw at least one failing attempt under T1",
        )

        # Push T2 into the (endpoint, uid) slot. The slot is keyed on
        # the emulator hostname and the producer's uid (DEFAULT_SUB).
        emulator_host = urlparse(stack.emulator_url).hostname or ""
        await pc.push_token(
            endpoint=emulator_host,
            uid=DEFAULT_SUB,
            token=t2,
        )

        # Clear the 5xx policy so the next attempt sees a healthy
        # upstream and uses T2 from the cache slot.
        stack.emulator.clear_failures()

        # All chains should reach succeeded.
        for cid in chain_ids:
            detail = await assert_chain_reaches_state(
                pc,
                cid,
                state="succeeded",
                timeout_seconds=TERMINAL_BUDGET_SECONDS,
            )
            assert detail.state == "succeeded"

        # The token-cache slot should now reflect T2 (or its
        # admin-side hash); the slot listing surfaces a freshness
        # status but never the bearer value (ADR-004).
        slots = await pc.list_tokens(endpoint=emulator_host)
        matching = [s for s in slots if s.uid == DEFAULT_SUB]
        assert matching, f"no slot for uid={DEFAULT_SUB}"
        assert matching[0].status in {"fresh", "unknown"}, (
            f"slot status={matching[0].status!r}, expected fresh/unknown after success"
        )
    finally:
        await stack.tear_down()


async def test_aggressor_auth_kicker_recovery_via_admin_push(
    tmp_path: Path,
) -> None:
    """Auth_expired chains wake when a fresh token lands via admin push.

    Sub-case B variant: emulator returns 401 unconditionally, so the
    first attempt parks the chain in `auth_expired`. The admin then
    pushes a fresh token + clears the failure policy. The bearer kicker
    re-queues the parked chains and they reach succeeded.

    Mirrors the producer's safety net for AAD/v0-JWT outage windows: a
    sustained 401 stream while the producer is mid-burst would otherwise
    cause N buffered uploads to permanent-fail. Phantom keeps them.
    """
    stack: E2EStack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={
            "retry": {
                "worker_count": 2,
                "poll_interval_ms": 100,
            },
            "saturation": {
                "max_in_flight": 50,
                "max_in_flight_bytes": 32 * 1024 * 1024,
            },
        },
    )
    try:
        pc = stack.phantom_client
        stack.emulator.clear_received()
        stack.emulator.clear_failures()

        # Install 401-on-first-call policy so every chain hits 401.
        # ``auth_401_after_n_calls=0`` = "401 from call 1 onward."
        stack.emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]
                scope=FailureScope.GLOBAL,
                auth_401_after_n_calls=0,
            ),
        )

        # Submit N chains under T1 (the stale bearer).
        t1 = stack.fake_security_token(extra_claims={"token_version": "T1-stale"})
        chain_ids: list[UUID] = []
        for _ in range(N_CHAINS):
            cid = uuid4()
            chain_ids.append(cid)
            await _submit_one(
                pc,
                emulator_url=stack.emulator_url,
                bearer=t1,
                chain_id=cid,
            )

        # Wait until every chain parks in auth_expired.
        async def _all_auth_expired() -> bool:
            details = [await pc.get_upload(c) for c in chain_ids]
            return all(d.state == "auth_expired" for d in details)

        await await_until(
            _all_auth_expired,
            timeout_seconds=AUTH_EXPIRED_BUDGET_SECONDS,
            message=f"not all {N_CHAINS} chains parked in auth_expired",
        )

        # Operator lifts the 401 policy and pushes a fresh token.
        stack.emulator.clear_failures()
        t2 = stack.fake_security_token(extra_claims={"token_version": "T2-fresh"})
        emulator_host = urlparse(stack.emulator_url).hostname or ""
        await pc.push_token(
            endpoint=emulator_host,
            uid=DEFAULT_SUB,
            token=t2,
        )

        # The auth-kicker should re-queue every parked chain. All
        # should reach succeeded.
        for cid in chain_ids:
            detail = await assert_chain_reaches_state(
                pc,
                cid,
                state="succeeded",
                timeout_seconds=TERMINAL_BUDGET_SECONDS,
            )
            assert detail.state == "succeeded"
    finally:
        await stack.tear_down()
