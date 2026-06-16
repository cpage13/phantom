"""E2E-3 — S3 (presigned PUT) down, then recovery.

Step 1 (metadata POST) succeeds; step 2 (S3 PUT) fails repeatedly
with 503; after the test clears the failure the chain reaches
succeeded. Two invariants make this test interesting beyond E2E-2:

1. ``last_step_completed`` must be ``create_file`` during the
   failure window — i.e., step 1 is recorded as complete and step 2
   is the one being retried, not the chain as a whole rolling back.
2. After recovery, the emulator received exactly ONE metadata-POST
   pair (Phantom does NOT re-execute step 1 just because step 2
   failed — the captured ``upload_url`` is still within its TTL).
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

# Synthetic-return budget mirrors the smoke test.
SYNTHETIC_RETURN_BUDGET_SECONDS: float = 0.1

# Window during which we expect Phantom to make at least one failed
# step-2 attempt before we clear the failure. Step 1 lands quickly
# (no failure scoped to it), then step 2 fails on the first try.
RETRY_OBSERVATION_WINDOW_SECONDS: float = 2.5

# Observed state during the failure window: ``state in {"attempting",
# "queued"}``; phantom's row-state machine parks the row in ``queued``
# between retry intervals.
NON_TERMINAL_STATES_DURING_FAILURE: frozenset[str] = frozenset({"queued", "attempting"})

# After clearing failures, how long the chain has to reach
# ``succeeded``. 15s budget.
TERMINAL_AFTER_CLEAR_BUDGET_SECONDS: float = 15.0

# 5xx error rate for the failure policy. 1.0 = every PUT fails.
FORCE_5XX_RATE: float = 1.0


pytestmark = pytest.mark.e2e


# perf-tier (whole test): asserts SYNTHETIC_RETURN_BUDGET_SECONDS,
# interwoven with the functional flow and sharing the stack boot — not
# cleanly splittable. Runs only via `-m perf` (see pyproject markers).
@pytest.mark.perf
async def test_e2e_03_s3_put_down_then_recovery(
    driver: PhantomDriver,
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """Step 1 succeeds; step 2 fails until cleared, then succeeds once.

    Distinguishes itself from E2E-2 by asserting
    that step 1 is captured exactly once even though the chain
    retries — Phantom must not re-execute a successful step on
    subsequent attempts of a later step (the captured ``upload_url``
    is still within its TTL).
    """
    # Setup — fresh emulator state, then install the PUT-only 5xx
    # policy. The metadata POST will land normally; the PUT will
    # 503 every time until cleared.
    emulator.clear_received()
    emulator.clear_failures()
    emulator.inject_failure(
        FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
            scope=FailureScope.UPSTREAM_FILES_UPLOAD,
            error_rate_5xx=FORCE_5XX_RATE,
        )
    )

    request = build_create_file_request()
    contents = DEFAULT_BODY

    # Action — synthetic return is unaffected by the upstream failure.
    start = time.perf_counter()
    result = await driver.in_memory_upload(request, contents)
    elapsed = time.perf_counter() - start

    assert elapsed < SYNTHETIC_RETURN_BUDGET_SECONDS, (
        f"in_memory_upload took {elapsed:.3f}s; expected < {SYNTHETIC_RETURN_BUDGET_SECONDS}s"
    )

    # During the failure window, Phantom should be making attempts on
    # step 2 with step 1 already recorded as completed. Poll the
    # admin row's ``last_step_completed`` until it transitions away
    # from ``None``; that confirms step 1 captured before step 2 ran.
    async def _step_one_landed() -> bool:
        snapshot = await phantom_client.get_upload(result.id)
        return snapshot.last_step_completed == "create_file"

    await await_until(
        _step_one_landed,
        timeout_seconds=RETRY_OBSERVATION_WINDOW_SECONDS,
        message=(
            f"chain {result.id} never recorded create_file as "
            f"last_step_completed within {RETRY_OBSERVATION_WINDOW_SECONDS}s"
        ),
    )

    # Capture the step-1 outputs while step 2 is still failing — they
    # should be present on the admin row even though the chain has
    # not yet reached a terminal state.
    rows, _ = await phantom_client.list_uploads(limit=10)
    matching = [row for row in rows if row.chain_id == result.id]
    assert matching, f"row for chain {result.id} not found in list_uploads"
    row = matching[0]
    assert row.state in NON_TERMINAL_STATES_DURING_FAILURE, (
        f"chain unexpectedly terminal during failure window: state={row.state!r}"
    )
    assert row.last_step_completed == "create_file", (
        f"expected last_step_completed='create_file' mid-failure; got {row.last_step_completed!r}"
    )

    # The captured-values map on the row should already have the step-1
    # captures (upload_url, file_information) even though the chain
    # isn't terminal yet.
    step_captures = row.captured_values.steps.get("create_file")
    assert step_captures is not None, (
        f"step 'create_file' captures missing on row; have steps={list(row.captured_values.steps)}"
    )
    assert "upload_url" in step_captures.values
    assert "file_information" in step_captures.values

    # Action — lift the failure. Phantom should re-attempt step 2
    # only (the captured upload_url is still within TTL), the next
    # attempt succeeds, chain transitions to succeeded.
    emulator.clear_failures()

    # Assertion — chain succeeds; emulator receives the body.
    chain_response = await assert_chain_reaches_state(
        phantom_client,
        result.id,
        state="succeeded",
        timeout_seconds=TERMINAL_AFTER_CLEAR_BUDGET_SECONDS,
    )
    assert chain_response.state == "succeeded"
    assert chain_response.last_step_completed == "put_s3", (
        f"expected last_step_completed='put_s3' after recovery; got "
        f"{chain_response.last_step_completed!r}"
    )

    received = await assert_emulator_received(
        emulator,
        phantom_local_uuid=str(result.id),
        body_size=len(contents),
    )
    assert received.body_size == len(contents)

    # Crucially: count entries with this phantom_local_uuid. There
    # must be exactly ONE — step 1 ran once, step 2 was retried, but
    # because step 1 produced the captured upload_url Phantom did NOT
    # re-execute it.
    matched_entries = [
        entry
        for entry in emulator.received()
        if entry.metadata_kvs.get("phantom_local_uuid") == str(result.id)
    ]
    assert len(matched_entries) == 1, (
        f"expected exactly one emulator-received entry for "
        f"phantom_local_uuid={result.id}; got {len(matched_entries)}"
    )
