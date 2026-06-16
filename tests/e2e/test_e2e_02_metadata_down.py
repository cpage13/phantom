"""E2E-2 — metadata POST down, then recovery.

Verifies the retry-and-recover loop on the first step of the two-step
upload chain. The emulator is primed with a 5xx failure policy before
the upload starts; the driver returns immediately,
phantom-the-service queues and re-attempts step 1 several times
against the failing emulator, then - once the test lifts the failure
policy - the next attempt completes the chain.

Infrastructure note on scope choice: the natural choice would be
``scope="upstream.files.create"``. The emulator's failure-injection
middleware maps URL paths to scopes via prefix match
(:func:`phantom_emulator.failure.middleware._scope_for_path`), and
the driver posts to the aliased ``/v2/files`` path (the stack
fixture mounts an alias bridging the driver's metadata URL to
the emulator's stock ``/v1/files/create`` handler). Because
``/v2/files`` does not match the ``/v1/files/create`` prefix, the
middleware resolves its scope to ``FailureScope.GLOBAL``. We install
the 5xx policy under ``GLOBAL`` so the metadata POST fails as
intended; step 2 never runs while step 1 is failing, so a GLOBAL
policy here is semantically equivalent to the create-only scope. A
cleaner alternative is to add ``/v2/files`` as a permanent alias in
phantom-emulator's upstream router so scope mapping works cleanly.
"""

from __future__ import annotations

import time

import pytest
from phantom_client import PhantomClient
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from ._driver import PhantomDriver
from .helpers.assertions import assert_chain_reaches_state, assert_emulator_received
from .helpers.payloads import DEFAULT_BODY, build_create_file_request
from .helpers.stack import EmulatorControl
from .helpers.timing import await_until

# Synthetic-return budget mirrors the smoke test — the contract is
# "synthetic FileInformation immediately."
SYNTHETIC_RETURN_BUDGET_SECONDS: float = 0.1

# Window during which we expect Phantom to make at least two failed
# attempts before we clear the failure. Phantom's retry strategy in
# `phantom-config.yml` is `fixed_intervals` [0, 1, 2, 5, 10], so 2.5s
# gives time for the first attempt (immediate) plus the second (after
# 1s) to complete and be observable.
RETRY_OBSERVATION_WINDOW_SECONDS: float = 2.5

# Minimum number of attempts we expect to see during the failure
# window. The plan calls for ``attempts >= 2``; we hold the same line.
MIN_ATTEMPTS_DURING_FAILURE: int = 2

# After clearing failures, how long the chain has to reach ``succeeded``.
# Plan §3.2 calls for 15s; the helper default is 10s. We use 15s to
# match the plan and to give the next retry interval (post-failure
# attempt N is up to 10s away on the configured `fixed_intervals`) a
# small cushion.
TERMINAL_AFTER_CLEAR_BUDGET_SECONDS: float = 15.0

# 5xx error rate for the failure policy. 1.0 = every request fails.
FORCE_5XX_RATE: float = 1.0


pytestmark = pytest.mark.e2e


# perf-tier (whole test): asserts SYNTHETIC_RETURN_BUDGET_SECONDS,
# interwoven with the functional flow and sharing the stack boot — not
# cleanly splittable. Runs only via `-m perf` (see pyproject markers).
@pytest.mark.perf
async def test_e2e_02_metadata_down_then_recovery(
    driver: PhantomDriver,
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """Metadata POST 503s, chain retries, recovers after clear_failures.

    Satisfies plan §3.2: emulator returns 503 on every metadata POST;
    the driver returns in < 100 ms;
    Phantom retries step 1 until ``MIN_ATTEMPTS_DURING_FAILURE``
    attempts are observed; test calls ``clear_failures``; chain
    reaches ``succeeded`` within ``TERMINAL_AFTER_CLEAR_BUDGET_SECONDS``;
    emulator receives the body once recovery completes.
    """
    # Setup — fresh emulator state, then install the 5xx policy.
    emulator.clear_received()
    emulator.clear_failures()
    emulator.inject_failure(
        FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
            scope=FailureScope.GLOBAL,
            error_rate_5xx=FORCE_5XX_RATE,
        )
    )

    request = build_create_file_request()
    contents = DEFAULT_BODY

    # Action — the driver returns fast even though the upstream is
    # broken; the driver never waits on upstream reachability.
    start = time.perf_counter()
    result = await driver.in_memory_upload(request, contents)
    elapsed = time.perf_counter() - start

    assert elapsed < SYNTHETIC_RETURN_BUDGET_SECONDS, (
        f"in_memory_upload took {elapsed:.3f}s; expected < {SYNTHETIC_RETURN_BUDGET_SECONDS}s"
    )

    # Phantom should make at least MIN_ATTEMPTS_DURING_FAILURE attempts
    # while the failure is active. Poll the admin API until the row
    # reports enough attempts; the chain remains non-terminal the whole
    # time because every attempt fails.
    async def _enough_attempts() -> bool:
        snapshot = await phantom_client.get_upload(result.id)
        # Each step that fails contributes to the attempt count; we
        # only need to see the retry loop iterate, so we look at the
        # raw attempt-count field on the row.
        rows, _ = await phantom_client.list_uploads(limit=10)
        for row in rows:
            if row.chain_id == result.id:
                return row.attempts >= MIN_ATTEMPTS_DURING_FAILURE
        # If the row doesn't appear in the list (unlikely), keep
        # waiting — get_upload above already proved it exists.
        return snapshot.state in {"queued", "attempting"} and False

    await await_until(
        _enough_attempts,
        timeout_seconds=RETRY_OBSERVATION_WINDOW_SECONDS,
        message=(
            f"chain {result.id} did not reach "
            f"{MIN_ATTEMPTS_DURING_FAILURE} attempts within "
            f"{RETRY_OBSERVATION_WINDOW_SECONDS}s"
        ),
    )

    # Verify the chain is still non-terminal at this point — the row
    # should be queued or attempting, never succeeded.
    mid_snapshot = await phantom_client.get_upload(result.id)
    assert mid_snapshot.state in {"queued", "attempting"}, (
        f"chain unexpectedly terminal during failure window: state={mid_snapshot.state!r}"
    )

    # Stats sanity check — the aggregate counters should reflect the
    # row mid-failure. We sample DURING the failure window because
    # ``succeeded_recent`` rotates on the
    # ``succeeded_metadata_seconds`` retention window and a slow
    # runner could see the row already aged out by the time we get
    # past the recovery step.
    stats_mid = await phantom_client.get_stats()
    populated_mid = (
        stats_mid.by_state.attempting.count
        + stats_mid.by_state.queued.count
        + stats_mid.by_state.succeeded_recent.count
    )
    assert populated_mid >= 1, (
        f"expected at least one row reflected in by_state counters during "
        f"failure window; got attempting={stats_mid.by_state.attempting.count} "
        f"queued={stats_mid.by_state.queued.count} "
        f"succeeded_recent={stats_mid.by_state.succeeded_recent.count}"
    )

    # Action — lift the failure. The next retry attempt should land
    # against a healthy emulator and complete the chain end-to-end.
    emulator.clear_failures()

    # Assertion — chain reaches succeeded; emulator receives the body.
    chain_response = await assert_chain_reaches_state(
        phantom_client,
        result.id,
        state="succeeded",
        timeout_seconds=TERMINAL_AFTER_CLEAR_BUDGET_SECONDS,
    )
    assert chain_response.state == "succeeded"
    assert chain_response.last_step_completed == "put_s3", (
        f"expected last_step_completed='put_s3', got {chain_response.last_step_completed!r}"
    )

    received = await assert_emulator_received(
        emulator,
        phantom_local_uuid=str(result.id),
        body_size=len(contents),
    )
    assert received.body_size == len(contents)
