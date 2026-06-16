"""Bulk delete over the wire: rows + bodies removed, slots released (TEST-3).

``DELETE /v1/admin/chains`` is the destructive multi-row admin verb on
the never-lose-an-upload buffer. Its correctness and accounting are
pinned below the e2e tier (contract ``test_chains_endpoints``, unit
``test_admin_bulk_delete_c1``, the R8-4 cancel/delete saturation-release
pin, the R10-D1 late-body-vs-readmission race). What no test covers is
the FULL-STACK remove-plus-release angle: admit N rows over a real
boot, issue a filtered bulk delete on the wire, and prove ALL THREE
post-conditions hold together against the live process -

* the rows are gone (the admin GET 404s),
* their body files are gone from disk, and
* the saturation ledger drains back to zero (invariant #16) - the slots
  the in-flight rows held are released by the delete path, not leaked.

Only an e2e proves the destructive verb does all three atomically over a
real listener; the lower tiers prove each in isolation. The seeded rows
are held in flight against a dead (5xx) upstream so they keep their
saturation slots charged (the 401/``auth_expired`` park path releases
the slot on its own, so it would not exercise the delete-path release).
A retryable 5xx cycles each row between ``queued`` and ``attempting``,
so the filter matches on ``route`` (stable across that churn) rather
than a single state - and the bulk delete must still release every
charged slot, the worst case for a leak (the R8-4 cancel/delete-of-an-
in-flight-row release).
"""

from __future__ import annotations

import logging
from uuid import UUID, uuid4

import httpx
import pytest
from phantom_client import DeleteFilter, PhantomNotFoundError
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from tests.e2e._driver import build_in_memory_upload_envelope

from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.e2e

# Number of rows to seed and bulk-delete. Enough that "the gate idles at
# zero" is a meaningful drain, not a single-slot release.
SEEDED_ROW_COUNT: int = 4

# The single declared body_ref name (matches the driver's envelope name "body").
BODY_REF_NAME: str = "body"

# Body bytes per seeded upload; small keeps the lane quick.
SEED_BODY_BYTES: bytes = b"phantom-bulk-delete-e2e-seeded-body"

# Shared sub for the seeded uploads.
SHARED_SUB: str = "00000000-0000-0000-0000-000000000003"

# The route every seeded upload rides; the bulk-delete filter matches on
# it because the rows churn queued<->attempting under the 5xx hold-down.
SEED_ROUTE_NAME: str = "emulator"

# Budget for a seeded upload to record its first attempt (proof it is
# genuinely in flight and holding a slot before the delete races).
FIRST_ATTEMPT_BUDGET_SECONDS: float = 15.0

# Budget for the saturation ledger to idle at zero after the bulk delete.
IDLE_BUDGET_SECONDS: float = 10.0

# Every upstream call fails 5xx so the rows stay retryable (slot held).
FORCE_5XX_RATE: float = 1.0


async def _saturation_balance(stack: E2EStack) -> float:
    """Read the live ``saturation_balance`` no-label total off the wire."""
    async with httpx.AsyncClient() as http:
        response = await http.get(f"{stack.phantom_admin_url}/v1/admin/observability/gauges")
    response.raise_for_status()
    for entry in response.json()["gauges"]:
        if entry["name"] == "saturation_balance":
            return float(entry["values"][""])
    raise AssertionError("saturation_balance gauge missing from the gauges response")


async def _seed_in_flight_upload(stack: E2EStack, *, index: int) -> UUID:
    """Submit one real upload held in flight against the 5xx upstream.

    Waits until the row records its first attempt, which proves it is
    genuinely in flight (and therefore holding a saturation slot) before
    the bulk delete races it.
    """
    chain_id = uuid4()
    request = build_create_file_request(file_name=f"bulk-delete-{index}-{chain_id.hex[:8]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=stack.emulator_url,
        local_uuid=chain_id,
    )
    await stack.phantom_client.submit_chain(
        envelope,
        body_refs={BODY_REF_NAME: SEED_BODY_BYTES},
        uid=SHARED_SUB,
        auth_token=f"Bearer {stack.fake_security_token(sub=SHARED_SUB)}",
    )

    async def _attempted() -> bool:
        detail = await stack.phantom_client.get_upload(chain_id)
        return detail.attempts > 0

    await await_until(
        _attempted,
        timeout_seconds=FIRST_ATTEMPT_BUDGET_SECONDS,
        message=f"seeded row {chain_id} never recorded an attempt (not in flight)",
    )
    return chain_id


async def test_bulk_delete_removes_rows_bodies_and_releases_slots() -> None:
    """Filtered bulk delete removes rows + bodies and idles the gate at zero."""
    stack = await boot_stack(
        config_overrides={
            # all_disk so every seeded body lands on disk deterministically;
            # the bulk delete must remove those files, not just the rows.
            "storage": {"body_store": {"mode": "all_disk"}},
        },
    )
    try:
        instance = stack.get_instance("primary")
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()

        # Hold every upstream call at 5xx so the rows stay retryable and
        # keep their saturation slots charged (a retryable failure, unlike
        # a 401 park, does not release the slot).
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields default; mypy lacks the pydantic plugin
                scope=FailureScope.GLOBAL,
                error_rate_5xx=FORCE_5XX_RATE,
            ),
        )

        seeded: list[UUID] = []
        for index in range(SEEDED_ROW_COUNT):
            seeded.append(await _seed_in_flight_upload(stack, index=index))

        # Every seeded body is on disk under a live row before the delete.
        for chain_id in seeded:
            assert await instance.body_store.has_body_ref(chain_id, BODY_REF_NAME), (
                f"seeded body for chain_id={chain_id} should be on disk before the bulk delete"
            )

        # The in-flight rows hold saturation slots, so the ledger is
        # non-zero before the delete - the release has something to drain.
        balance_before = await _saturation_balance(stack)
        assert balance_before > 0.0, (
            f"expected a non-zero saturation balance with {SEEDED_ROW_COUNT} parked rows; "
            f"got {balance_before}"
        )

        # Issue the filtered bulk delete over the wire. The filter matches
        # the seeded rows by route - stable across the queued<->attempting
        # churn the 5xx hold-down causes.
        deleted = await stack.phantom_client.bulk_delete(DeleteFilter(route=SEED_ROUTE_NAME))
        assert deleted == SEEDED_ROW_COUNT, (
            f"bulk_delete reported {deleted} deletions; expected {SEEDED_ROW_COUNT}"
        )
        emulator.clear_failures()

        # Post-condition 1: every row is gone (the admin GET 404s).
        for chain_id in seeded:
            with pytest.raises(PhantomNotFoundError):
                await stack.phantom_client.get_upload(chain_id)

        # Post-condition 2: every body file is gone from disk.
        for chain_id in seeded:
            assert not await instance.body_store.has_body_ref(chain_id, BODY_REF_NAME), (
                f"body for deleted chain_id={chain_id} is still on disk after bulk delete"
            )

        # Post-condition 3: the saturation ledger drains to exactly zero -
        # the slots the deleted rows held were released, not leaked
        # (invariant #16).
        async def _idle() -> bool:
            return await _saturation_balance(stack) == 0.0

        await await_until(
            _idle,
            timeout_seconds=IDLE_BUDGET_SECONDS,
            message=(
                "saturation_balance never returned to zero after the bulk delete; "
                "the destructive verb leaked a slot (invariant #16)"
            ),
        )
    finally:
        await stack.tear_down()
