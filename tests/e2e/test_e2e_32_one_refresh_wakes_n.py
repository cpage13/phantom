"""E2E-32 — Sequential 401 across N chains; one refresh wakes them all.

When a v1 source's token expires mid-sequence, every subsequent upload
hits 401. The load-bearing invariant: **a single admin push (or
`ad_client_credentials` mint) should wake ALL parked rows on one
cache-write, not N separate refresh round-trips.**

This test parks 3 chains in ``auth_expired`` via
``auth_401_after_n_calls`` on GLOBAL, then verifies the count of
parked rows visible via Phantom's admin API before and after the
single-cache-write recovery.

The recovery half — a single ``push_token`` waking all N parked
rows — is exercised in
:func:`test_e2e_32_single_refresh_wakes_all_parked`. The kicker's
periodic rescan plus the cache-write wake event handle the parking
order regardless of whether the cache write lands before or after
the row commits to ``auth_expired``.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from phantom_client import PhantomClient
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from ._driver import CreateFileRequest, DriverUploadResult, FileMetadata, PhantomDriver
from .helpers.assertions import assert_chain_reaches_state
from .helpers.stack import E2EStack, EmulatorControl, boot_stack

# Number of pre-expiry successful uploads.
PRE_EXPIRY_UPLOAD_COUNT: int = 2

# Number of post-expiry uploads that should park.
POST_EXPIRY_UPLOAD_COUNT: int = 3

# Total chains.
TOTAL_CHAIN_COUNT: int = PRE_EXPIRY_UPLOAD_COUNT + POST_EXPIRY_UPLOAD_COUNT

# Per-upload synchronous-return budget.
SYNTHETIC_RETURN_BUDGET_SECONDS: float = 0.1

# Time to wait for all post-expiry chains to enter auth_expired.
AUTH_EXPIRED_BUDGET_SECONDS: float = 5.0

# Time to wait for all chains to succeed after recovery. 10 s base;
# we use 15 s to give slow runners headroom.
POST_RECOVERY_BUDGET_SECONDS: float = 15.0


pytestmark = pytest.mark.e2e


# Per-test config overlay — disable the reaper for the test's
# duration so chain rows survive the multi-step polling pass.
_REAPER_OVERRIDE: dict[str, object] = {
    "retention": {
        "reaper_interval_seconds": 3600,
    },
}


@pytest_asyncio.fixture
async def stack() -> AsyncIterator[E2EStack]:
    """Boot the stack with the reaper effectively disabled."""
    s = await boot_stack(config_overrides=_REAPER_OVERRIDE)
    try:
        yield s
    finally:
        await s.tear_down()


@pytest.fixture
def driver(stack: E2EStack) -> PhantomDriver:
    """Public driver against the overridden (reaper-disabled) stack."""
    return PhantomDriver(
        stack.phantom_client,
        files_api=stack.emulator_url,
        get_security_token=stack.fake_security_token,
    )


def _build_request(*, idx: int) -> CreateFileRequest:
    """Build a per-upload :class:`CreateFileRequest`."""
    return CreateFileRequest(
        domain="generic",
        lane_base_name="history_parquet_data",
        file_name=f"e2e32_{idx:02d}",
        metadata=FileMetadata(
            key_value_store={
                "uploader_id": "12345",
                "label": "alpha",
                "run_mode": "production",
                "sequence_name": "v1_sequence",
                "label_name": "v1_sequence",
            }
        ),
    )


# perf-tier (whole test): asserts SYNTHETIC_RETURN_BUDGET_SECONDS,
# interwoven with the parked-chain submission loop and sharing the
# stack boot — not cleanly splittable. Runs only via `-m perf` (see
# pyproject markers). The sibling test_e2e_32_single_refresh_wakes_all_parked
# has no budget assertion and stays in the default suite.
@pytest.mark.perf
async def test_e2e_32_one_refresh_wakes_n(
    driver: PhantomDriver,
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """N parked chains are visible after a single 401 burst.

    The primary invariant: when the upstream
    starts returning 401, every chain submitted under the bad
    bearer parks in ``auth_expired`` on the same
    ``(endpoint, uid)`` slot — exactly one cache slot, not N. The
    post-push recovery (a single cache-write waking all N rows) is
    documented but xfailed pending the recovery-path investigation
    flagged in E2E-31.
    """
    # Setup — fresh emulator state.
    emulator.clear_received()
    emulator.clear_failures()

    body = b"v1-history-row-bytes"

    # Action — first 2 uploads succeed under the original bearer.
    pre_results: list[DriverUploadResult] = []
    for i in range(PRE_EXPIRY_UPLOAD_COUNT):
        start = time.perf_counter()
        result = await driver.in_memory_upload(_build_request(idx=i), body)
        elapsed = time.perf_counter() - start
        assert elapsed < SYNTHETIC_RETURN_BUDGET_SECONDS
        pre_results.append(result)

    for r in pre_results:
        await assert_chain_reaches_state(
            phantom_client,
            r.id,
            state="succeeded",
            timeout_seconds=POST_RECOVERY_BUDGET_SECONDS,
        )

    # Action — install 401 policy. From now on every upstream call
    # returns 401.
    emulator.inject_failure(
        FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
            scope=FailureScope.GLOBAL,
            auth_401_after_n_calls=0,
        )
    )

    # Action — submit 3 more chains in rapid succession. Each chain
    # will hit 401 on the metadata POST and park in auth_expired.
    post_results: list[DriverUploadResult] = []
    for i in range(PRE_EXPIRY_UPLOAD_COUNT, TOTAL_CHAIN_COUNT):
        result = await driver.in_memory_upload(_build_request(idx=i), body)
        post_results.append(result)

    # Assertion — all 3 post-expiry chains reach auth_expired within budget.
    for r in post_results:
        await assert_chain_reaches_state(
            phantom_client,
            r.id,
            state="auth_expired",
            timeout_seconds=AUTH_EXPIRED_BUDGET_SECONDS,
        )

    # Assertion — exactly one (endpoint, uid) slot exists. All 3
    # parked chains share the same slot per ADR-002. This is the
    # load-bearing invariant: a future regression that creates a
    # slot-per-chain would multiply token churn and admin actions.
    tokens = await phantom_client.list_tokens()
    distinct_slots = {(t.endpoint, t.uid) for t in tokens}
    assert len(distinct_slots) == 1, (
        f"expected exactly 1 (endpoint, uid) slot; got {len(distinct_slots)}: {distinct_slots}"
    )

    # Assertion — admin API enumerates exactly 3 chains in auth_expired
    # (plus 2 succeeded from the pre-expiry batch). Counting via
    # list_uploads with state filter.
    rows_auth, _ = await phantom_client.list_uploads(state="auth_expired", limit=100)
    parked_ids = {row.chain_id for row in rows_auth}
    expected_parked_ids = {r.id for r in post_results}
    # parked_ids should contain at least our expected ids (other
    # tests in the same session don't share state because the stack
    # fixture is function-scoped, but be defensive).
    assert expected_parked_ids.issubset(parked_ids), (
        f"expected post-expiry chains in auth_expired set; missing: "
        f"{expected_parked_ids - parked_ids}"
    )
    assert len(parked_ids) == POST_EXPIRY_UPLOAD_COUNT, (
        f"expected exactly {POST_EXPIRY_UPLOAD_COUNT} parked chains; "
        f"got {len(parked_ids)}: {parked_ids}"
    )


async def test_e2e_32_single_refresh_wakes_all_parked(
    stack: E2EStack,
    driver: PhantomDriver,
    phantom_client: PhantomClient,
    emulator: EmulatorControl,
) -> None:
    """Single cache-write transitions N parked chains to succeeded."""
    emulator.clear_received()
    emulator.clear_failures()
    body = b"v1-history-row-bytes"

    # 2 pre-expiry uploads succeed.
    for i in range(PRE_EXPIRY_UPLOAD_COUNT):
        result = await driver.in_memory_upload(_build_request(idx=i), body)
        await assert_chain_reaches_state(
            phantom_client,
            result.id,
            state="succeeded",
            timeout_seconds=POST_RECOVERY_BUDGET_SECONDS,
        )

    # Install 401 failure.
    emulator.inject_failure(
        FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults
            scope=FailureScope.GLOBAL,
            auth_401_after_n_calls=0,
        )
    )

    # 3 post-expiry uploads park.
    post_results: list[DriverUploadResult] = []
    for i in range(PRE_EXPIRY_UPLOAD_COUNT, TOTAL_CHAIN_COUNT):
        result = await driver.in_memory_upload(_build_request(idx=i), body)
        post_results.append(result)

    for r in post_results:
        await assert_chain_reaches_state(
            phantom_client,
            r.id,
            state="auth_expired",
            timeout_seconds=AUTH_EXPIRED_BUDGET_SECONDS,
        )

    # Single cache-write: clear failures, push a fresh bearer. The
    # emulator validates the bearer as a JWT, so we mint a valid one
    # via the stack's signing-secret-aware helper rather than pushing
    # a literal string.
    emulator.clear_failures()
    tokens = await phantom_client.list_tokens()
    slot = tokens[0]
    fresh_bearer = stack.fake_security_token()
    await phantom_client.push_token(
        endpoint=slot.endpoint,
        uid=slot.uid,
        token=fresh_bearer,
    )

    # Assertion — all 3 parked rows succeed within budget.
    for r in post_results:
        await assert_chain_reaches_state(
            phantom_client,
            r.id,
            state="succeeded",
            timeout_seconds=POST_RECOVERY_BUDGET_SECONDS,
        )

    # Final sanity — no chains still in auth_expired.
    rows_auth, _ = await phantom_client.list_uploads(state="auth_expired", limit=100)
    leftover = {row.chain_id for row in rows_auth} & {r.id for r in post_results}
    assert not leftover, (
        f"chains still in auth_expired after single-cache-write recovery: {leftover}"
    )
