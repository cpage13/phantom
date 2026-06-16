"""E2E-27 — concurrent gather with one upstream failure mid-flight.

Companion to E2E-26: three concurrent uploads, but the upstream
returns 401 starting at call 3 of ``upstream.files.upload``. The
first two PUTs succeed; the third gets 401 and never recovers
because the per-test retry budget is tightened to a single
zero-delay attempt. The other two chains must complete unaffected
— per-chain failure isolation is the load-bearing invariant.

The representative gather uses
``asyncio.gather(..., return_exceptions=True)`` and converts the
exception of any failed task into a UI message. With the driver's
return contract the client sees three ``FileInformation``
immediately; the per-chain failure surfaces later in Phantom's admin
API as ``state == "failed"``. The isolation invariant (chain B's
failure does not affect chains A and C) is the same, only the
observation surface differs.

Implementation note — fail-target selection:
The emulator's failure scopes are URL-prefix-bound, not
lane-name-bound. A per-lane target ("large_binary lane only") would
be ideal, but no per-lane scope exists yet. We approximate with
``auth_401_after_n_calls: 2`` on ``upstream.files.upload`` — calls
1-2 succeed, call 3 onward returns 401. Whichever lane's PUT lands
third at the emulator becomes the failing chain. The test asserts
"exactly one stuck and two succeeded" rather than naming the
failing lane up front, since the gather's submit ordering does not
strictly determine the upstream arrival order.

Implementation note — terminal state for the failing chain:
With the workspace default ``wait`` refresh strategy (per
phantom-config.yml), a 401 from the upstream parks the chain in
``auth_expired`` rather than transitioning to ``failed``. The
chain would only fail outright if Phantom were configured with
``ad_client_credentials`` and that minter also failed. So the
test waits for the failing chain to reach ``auth_expired`` while
the two surviving chains reach ``succeeded``. The isolation
invariant — chain B's 401 does not block chains A and C — is the
same.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from uuid import UUID

import pytest
import pytest_asyncio
from phantom_client import PhantomClient
from phantom_client.errors import PollDeadlineExceeded
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from ._driver import DriverUploadResult, PhantomDriver
from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads_routines import SequenceUpload, build_concurrent_gather_payloads
from .helpers.stack import E2EStack, EmulatorControl, boot_stack

# Per-chain terminal-state budget. The two succeeding chains complete
# within ~1 s; the failing chain transitions through one zero-delay
# attempt to ``failed`` within ~2 s. 10 s is the helper default and
# gives slow runners headroom.
TERMINAL_STATE_BUDGET_SECONDS: float = 10.0

# The representative gather fires exactly three uploads.
EXPECTED_GATHER_WIDTH: int = 3

# Expected outcome: two chains succeed, one parks in auth_expired
# (the wait-strategy reaction to 401). Asserted by count below.
EXPECTED_SUCCEEDED_COUNT: int = 2
EXPECTED_PARKED_COUNT: int = 1
# The terminal state the failing chain settles into. With the
# workspace ``wait`` refresh strategy, a 401 from upstream parks the
# row instead of failing it outright. See module docstring.
PARKED_STATE: str = "auth_expired"

# Number of upstream PUT calls that succeed before the 401 trips. Set
# to 2 so the 3rd PUT (and any subsequent retry attempt) returns 401.
PUT_SUCCESS_BUDGET: int = 2


pytestmark = pytest.mark.e2e


# Per-test config override — tighten Phantom's retry strategy so the
# failing chain transitions to ``failed`` within the test's polling
# window. The workspace default schedule (0,1,2,5,10) would keep the
# failing chain in ``queued`` for ~18 s, exceeding budget.
_RETRY_OVERRIDE: dict[str, object] = {
    "retry": {
        "default_strategy": {
            "type": "fixed_intervals",
            "intervals_seconds": [0],
        },
    },
}


@pytest_asyncio.fixture
async def stack() -> AsyncIterator[E2EStack]:
    """Boot the stack with the per-test tight-retry override.

    Shadows the conftest.py ``stack`` fixture so the downstream
    ``driver``, ``phantom_client``, and ``emulator`` fixtures —
    which depend on ``stack`` — pick this one up automatically.
    """
    s = await boot_stack(config_overrides=_RETRY_OVERRIDE)
    try:
        yield s
    finally:
        await s.tear_down()


@pytest.fixture
def driver(stack: E2EStack) -> PhantomDriver:
    """Public driver against the overridden (tight-retry) stack."""
    return PhantomDriver(
        stack.phantom_client,
        files_api=stack.emulator_url,
        get_security_token=stack.fake_security_token,
    )


async def test_e2e_27_concurrent_gather_one_fail(
    driver: PhantomDriver,
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """3 concurrent uploads — one parks on 401, two succeed.

    Gather returns 3 :class:`FileInformation` (the driver's return
    contract — exceptions are NOT surfaced at submit time); Phantom's
    admin API shows exactly 2 rows in ``succeeded`` and 1 in
    ``auth_expired`` within ``TERMINAL_STATE_BUDGET_SECONDS``. The
    isolation invariant: the parked chain does not block the other two.
    """
    # Setup — fresh emulator state and inject 401-after-N-calls so the
    # 3rd PUT (and beyond) returns 401. The default
    # ``oauth_client_credentials`` mode handles the 401's auth-token
    # validation; the failure middleware overlays the per-scope policy.
    emulator.clear_received()
    emulator.clear_failures()
    emulator.inject_failure(
        FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
            scope=FailureScope.UPSTREAM_FILES_UPLOAD,
            auth_401_after_n_calls=PUT_SUCCESS_BUDGET,
        )
    )

    payloads = build_concurrent_gather_payloads(ref_id="hist-cg-027")
    assert len(payloads) == EXPECTED_GATHER_WIDTH

    # Action — fire all three uploads in parallel. The driver's
    # return contract means every submit returns immediately
    # even if the underlying chain will eventually fail.
    async def _submit(entry: SequenceUpload) -> DriverUploadResult:
        return await driver.in_memory_upload(entry.request, entry.body)

    gather_results = await asyncio.gather(
        *(_submit(p) for p in payloads),
        return_exceptions=True,
    )

    # Assertion — no exceptions at submit time (buffering ack).
    file_infos: list[DriverUploadResult] = []
    for entry, result in zip(payloads, gather_results, strict=True):
        assert not isinstance(result, BaseException), (
            f"gather raised on {entry.label!r} at submit time (should ack fast): {result!r}"
        )
        assert isinstance(result.id, UUID)
        file_infos.append(result)

    # Assertion — exactly two chains succeed, one parks in
    # auth_expired. Race each chain against both observable states
    # (succeeded or auth_expired) and take the first one that arrives.
    async def _wait_terminal(fi: DriverUploadResult) -> str:
        succeeded_task = asyncio.create_task(
            assert_chain_reaches_state(
                phantom_client,
                fi.id,
                state="succeeded",
                timeout_seconds=TERMINAL_STATE_BUDGET_SECONDS,
            )
        )
        parked_task = asyncio.create_task(
            assert_chain_reaches_state(
                phantom_client,
                fi.id,
                state=PARKED_STATE,
                timeout_seconds=TERMINAL_STATE_BUDGET_SECONDS,
            )
        )
        try:
            done, pending = await asyncio.wait(
                {succeeded_task, parked_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            # Drain cancellations so we don't leave tasks dangling.
            for task in pending:
                with contextlib.suppress(
                    asyncio.CancelledError,
                    PollDeadlineExceeded,
                    AssertionError,
                ):
                    await task
            # Whichever task finished first tells us the observed state.
            first = done.pop()
            response = first.result()
            return response.state
        finally:
            # Belt-and-braces: ensure all tasks are awaited.
            for t in (succeeded_task, parked_task):
                if not t.done():
                    t.cancel()
                    with contextlib.suppress(
                        asyncio.CancelledError,
                        PollDeadlineExceeded,
                        AssertionError,
                    ):
                        await t

    states = await asyncio.gather(*(_wait_terminal(fi) for fi in file_infos))

    succeeded = sum(1 for s in states if s == "succeeded")
    parked = sum(1 for s in states if s == PARKED_STATE)
    assert succeeded == EXPECTED_SUCCEEDED_COUNT, (
        f"expected {EXPECTED_SUCCEEDED_COUNT} succeeded, got {succeeded}; states={states}"
    )
    assert parked == EXPECTED_PARKED_COUNT, (
        f"expected {EXPECTED_PARKED_COUNT} parked in {PARKED_STATE!r}, "
        f"got {parked}; states={states}"
    )

    # Isolation assertion — every chain reached a recognized state
    # independently. If chain B's 401 blocked chain A or C from
    # making progress, one of them would still be queued/attempting
    # past the per-chain budget. The succeeded+failed = N check above
    # already guarantees this.
    assert succeeded + parked == EXPECTED_GATHER_WIDTH, (
        f"chains did not all reach a recognized state: states={states}"
    )
