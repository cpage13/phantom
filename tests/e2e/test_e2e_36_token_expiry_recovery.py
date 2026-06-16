"""E2E-36 — bearer expires mid-sequence; admin push recovers the parked chain.

Exercises a bearer-expiry-and-recovery lifecycle against only Phantom's
OWN auth machinery — the ``wait`` refresh strategy, the token cache, and
the auth-kicker's wake-on-cache-set — through the public ``PhantomDriver``
+ ``phantom-client``:

1. Several uploads succeed under a valid bearer (steady-state delivery).
2. The bearer starts being rejected upstream (a 401).
3. The next upload parks in ``auth_expired`` (wait strategy: the cache slot
   is left untouched, the row waits for an external bearer).
4. An external ``push_token`` admin call lands a fresh bearer; the cache-set
   fires the auth-kicker's wake handler, which re-queues the parked row.
5. The row reaches ``succeeded`` and the body lands at the emulator.

It is pure HTTP-workflow behaviour on fabricated data.
"""

from __future__ import annotations

import pytest
from phantom_client import PhantomClient
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from ._driver import PhantomDriver
from .helpers.assertions import assert_chain_reaches_state, assert_emulator_received
from .helpers.payloads import DEFAULT_BODY, build_create_file_request
from .helpers.stack import DEFAULT_FAKE_SUB, E2EStack, EmulatorControl
from .helpers.timing import await_until

# Uploads that succeed under the original bearer before it starts 401ing.
PRE_EXPIRY_UPLOAD_COUNT: int = 3

# Window for the post-expiry row to land in auth_expired after the 401.
AUTH_EXPIRED_BUDGET_SECONDS: float = 5.0

# Window for each pre-expiry upload to reach succeeded.
PRE_EXPIRY_TERMINAL_BUDGET_SECONDS: float = 10.0

# Window for the parked row to reach succeeded after the admin push.
POST_PUSH_BUDGET_SECONDS: float = 20.0

# ``auth_401_after_n_calls=0`` => the first upstream call after the policy is
# installed returns 401 (the middleware compares count > N after incrementing
# the per-scope counter, so 0 means "401 from call 1 onward").
FORCE_401_AFTER_N: int = 0


pytestmark = pytest.mark.e2e


async def test_e2e_36_token_expiry_mid_sequence_admin_recovery(
    stack: E2EStack,
    driver: PhantomDriver,
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """Bearer expires mid-sequence; an admin push recovers the parked chain.

    Validates Phantom's auth-expiry lifecycle end-to-end via the public
    driver: steady-state delivery, parking on 401 under the wait strategy,
    and recovery when a fresh bearer is pushed.
    """
    emulator.clear_received()
    emulator.clear_failures()

    # 1. Steady state — several uploads succeed under the valid bearer.
    for i in range(PRE_EXPIRY_UPLOAD_COUNT):
        ok = await driver.in_memory_upload(
            build_create_file_request(file_name=f"e2e36_pre_{i:02d}"), DEFAULT_BODY
        )
        await assert_chain_reaches_state(
            phantom_client,
            ok.id,
            state="succeeded",
            timeout_seconds=PRE_EXPIRY_TERMINAL_BUDGET_SECONDS,
        )

    # 2. The bearer starts being rejected upstream.
    emulator.inject_failure(
        FailurePolicy(  # type: ignore[call-arg]  # pydantic defaults; mypy lacks the plugin
            scope=FailureScope.GLOBAL,
            auth_401_after_n_calls=FORCE_401_AFTER_N,
        )
    )

    # 3. The next upload parks in auth_expired.
    parked = await driver.in_memory_upload(
        build_create_file_request(file_name="e2e36_parked"), DEFAULT_BODY
    )

    async def _row_auth_expired() -> bool:
        snapshot = await phantom_client.get_upload(parked.id)
        return snapshot.state == "auth_expired"

    await await_until(
        _row_auth_expired,
        timeout_seconds=AUTH_EXPIRED_BUDGET_SECONDS,
        message=(
            f"chain {parked.id} did not reach auth_expired within {AUTH_EXPIRED_BUDGET_SECONDS}s"
        ),
    )

    # Locate the parked row's cache-slot coordinates (endpoint, uid).
    rows, _ = await phantom_client.list_uploads(limit=50)
    matching = [row for row in rows if row.chain_id == parked.id]
    assert matching, f"row for chain {parked.id} not found"
    row = matching[0]
    assert row.uid == DEFAULT_FAKE_SUB, f"unexpected uid on parked row: {row.uid!r}"

    # 4. Clear the failure + push a fresh bearer; the cache-set wakes the
    #    auth-kicker, which re-queues the parked row. The pushed bearer must
    #    be a real JWT — the emulator's default auth mode verifies every
    #    inbound token, so a placeholder would 401 again on the retry.
    emulator.clear_failures()
    fresh_jwt = stack.fake_security_token()
    await phantom_client.push_token(endpoint=row.endpoint, uid=row.uid, token=fresh_jwt)

    # 5. The parked row reaches succeeded; the recovered body lands.
    chain_response = await assert_chain_reaches_state(
        phantom_client,
        parked.id,
        state="succeeded",
        timeout_seconds=POST_PUSH_BUDGET_SECONDS,
    )
    assert chain_response.state == "succeeded"
    assert chain_response.last_step_completed == "put_s3"

    received = await assert_emulator_received(
        emulator,
        phantom_local_uuid=str(parked.id),
        body_size=len(DEFAULT_BODY),
    )
    assert received.body_size == len(DEFAULT_BODY)
