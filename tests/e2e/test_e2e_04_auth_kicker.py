"""E2E-4 — 401 + auth-kicker.

Verifies Phantom's auth-failure handling and the auth-kicker wake-on-
cache-set semantics. Implemented sub-case:

- ``test_auth_kicker_with_wait_strategy_admin_push`` — Phantom parks
  the row in ``auth_expired`` and resumes only after an external
  ``push_token`` admin call lands a fresh bearer. End-to-end against
  the default ``wait`` refresh strategy from the suite's
  ``phantom-config.yml``.

The ``ad_client_credentials`` sub-case is not implemented here: the
strategy is built on ``azure.identity`` which talks the AAD-shaped
token endpoint (``{authority}/{tenant_id}/oauth2/v2.0/token``); the
emulator exposes the OAuth2-shaped ``/oauth/token`` endpoint plus
the ``.well-known`` discovery surface, but azure-identity's path
templating against a local emulator is not validated against this
stack and would require emulator-side surgery to be made reliable.
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

# Synthetic-return budget — the contract.
SYNTHETIC_RETURN_BUDGET_SECONDS: float = 0.1

# Window for the row to land in auth_expired after the initial 401.
# The first attempt is immediate per ``intervals_seconds: [0, ...]``;
# parking happens on the same tick. 2 seconds is generous.
AUTH_EXPIRED_BUDGET_SECONDS: float = 2.0

# After admin pushes a fresh token + clears the failure policy, how
# long the chain has to reach succeeded. Plan §3.4 calls for 10 s.
TERMINAL_AFTER_PUSH_BUDGET_SECONDS: float = 10.0

# ``auth_401_after_n_calls=0`` makes the first call (post-installation)
# return 401. The failure-injection middleware uses the comparison
# ``count > policy.auth_401_after_n_calls`` after incrementing the
# per-scope counter, so 0 = "401 from call 1 onward."
FORCE_401_AFTER_N: int = 0


pytestmark = pytest.mark.e2e


async def test_auth_kicker_with_wait_strategy_admin_push(
    stack: E2EStack,
    driver: PhantomDriver,
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """Wait-strategy 401 parks row; admin push_token wakes it (plan §3.4 sub-case B).

    Setup:
        - Default ``wait`` refresh strategy (from ``phantom-config.yml``).
        - Inject 401 globally so the upstream rejects every call.

    Scenario:
        - First submit: row attempts, gets 401, parks in
          ``auth_expired``. ``WaitStrategy.on_401`` is a no-op so the
          cache slot is untouched.
        - Test pushes a new token via ``phantom_client.push_token``;
          the cache-set fires the auth-kicker's wake handler. Test
          also clears the failure policy so the next attempt sees a
          healthy upstream.
        - Row transitions to ``queued``, then ``succeeded``.

    Expected outcome:
        - Within ``AUTH_EXPIRED_BUDGET_SECONDS``:
          ``state == "auth_expired"``.
        - After ``push_token`` + ``clear_failures``: ``state ==
          "succeeded"`` within ``TERMINAL_AFTER_PUSH_BUDGET_SECONDS``.
        - Emulator receives the body on the recovered attempt.
    """
    # Setup — clean state, install 401 policy under GLOBAL scope so
    # both ``/v2/files`` (POST step 1) and ``/v1/files/upload/...``
    # (PUT step 2) see it. Step 2 won't run while step 1 401s, so
    # the GLOBAL scope is the simplest correct setting.
    emulator.clear_received()
    emulator.clear_failures()
    emulator.inject_failure(
        FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
            scope=FailureScope.GLOBAL,
            auth_401_after_n_calls=FORCE_401_AFTER_N,
        )
    )

    request = build_create_file_request()
    contents = DEFAULT_BODY

    # Action — submit; the driver returns the minted chain id,
    # phantom parks the row when the upstream 401s.
    result = await driver.in_memory_upload(request, contents)

    async def _row_auth_expired() -> bool:
        snapshot = await phantom_client.get_upload(result.id)
        return snapshot.state == "auth_expired"

    await await_until(
        _row_auth_expired,
        timeout_seconds=AUTH_EXPIRED_BUDGET_SECONDS,
        message=(
            f"chain {result.id} did not reach auth_expired within {AUTH_EXPIRED_BUDGET_SECONDS}s"
        ),
    )

    # The driver's uid is derived from the fake JWT's sub claim.
    # The emulator's hostname is the endpoint key. We need both to
    # call push_token.
    parked = await phantom_client.get_upload(result.id)
    assert parked.state == "auth_expired"

    rows, _ = await phantom_client.list_uploads(limit=10)
    matching = [row for row in rows if row.chain_id == result.id]
    assert matching, f"row for chain {result.id} not found"
    row = matching[0]
    endpoint_for_slot = row.endpoint
    uid_for_slot = row.uid
    # uid should be the fake JWT's sub claim (DEFAULT_FAKE_SUB).
    assert uid_for_slot == DEFAULT_FAKE_SUB, f"unexpected uid on row: {uid_for_slot!r}"

    # Action — clear failures and push a fresh JWT. The push fires
    # the token-cache's wake handler; the auth-kicker re-queues the
    # parked row. The pushed bearer must be a real JWT because the
    # emulator's default ``oauth_client_credentials`` auth mode
    # verifies every inbound token; pushing a placeholder string
    # would 401 again on the retry. ``stack.fake_security_token()``
    # mints an HS256-signed JWT with the emulator's audience and
    # issuer, so the retry succeeds end-to-end.
    fresh_jwt = stack.fake_security_token()
    emulator.clear_failures()
    await phantom_client.push_token(
        endpoint=endpoint_for_slot,
        uid=uid_for_slot,
        token=fresh_jwt,
    )

    # Assertion — row reaches succeeded after wake + healthy upstream.
    chain_response = await assert_chain_reaches_state(
        phantom_client,
        result.id,
        state="succeeded",
        timeout_seconds=TERMINAL_AFTER_PUSH_BUDGET_SECONDS,
    )
    assert chain_response.state == "succeeded"
    assert chain_response.last_step_completed == "put_s3"

    # Emulator-side confirmation: the recovered attempt's body landed.
    received = await assert_emulator_received(
        emulator,
        phantom_local_uuid=str(result.id),
        body_size=len(contents),
    )
    assert received.body_size == len(contents)
