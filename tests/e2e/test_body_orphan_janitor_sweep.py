"""BodyOrphanJanitor sweep, end to end (TEST-1b).

The janitor's collection LOGIC - the R6-1 two-sweep confirmation, the
live-row re-read guard, the RAM no-op - is unit-pinned
(``test_body_orphan_janitor``, ``test_r6_1_janitor_fresh_entry_race``,
``test_f2_all_ram_orphan_sweep``). The full-stack detect-and-surface
path is not: no test plants a real orphan body in a booted instance,
waits for the live janitor coroutine to run its two confirming sweeps,
and asserts the file is gone AND ``orphan_body_count_total`` bumped on
the admin observability wire. This module is that proof, and it pins the
companion negative: a body WITH an owning ``uploads`` row is never
collected, however many sweeps run.

Determinism. The janitor requires the orphan to be a candidate on TWO
consecutive sweeps before it deletes (R6-1: a fresh admission that races
the known-set snapshot drops out on the next sweep, so a real crash
leftover is what survives both). The test boots ``all_disk`` mode (every
body lands on disk, the janitor's only meaningful mode alongside hybrid),
sets a tight sweep cadence, plants the orphan directly on the live body
store, and polls the wire counter until the two-sweep confirmation
completes - bounded, no naked waits.
"""

from __future__ import annotations

import logging
from uuid import UUID, uuid4

import pytest

from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.e2e

# Canonical counter name the janitor bumps per orphan removed.
COUNTER_ORPHAN_TOTAL: str = "orphan_body_count_total"

# Tight sweep cadence so the live janitor runs its two confirming sweeps
# inside the test window. The field validator requires >= 1.
FAST_SWEEP_SECONDS: int = 1

# The single declared body_ref name (matches the driver's envelope name "body").
BODY_REF_NAME: str = "body"

# Bytes for the planted orphan body. Their content is irrelevant - only
# the file's presence-then-absence matters.
ORPHAN_BODY_BYTES: bytes = b"phantom-orphan-janitor-e2e-planted-orphan-body"

# Body for the negative-case real upload (a row that DOES own its body).
OWNED_BODY_BYTES: bytes = b"phantom-orphan-janitor-e2e-owned-body-survives"

# Budget for the janitor's two-sweep confirmation to collect the orphan
# and surface the bump. Comfortably larger than two FAST_SWEEP periods.
COLLECT_BUDGET_SECONDS: float = 15.0

# Budget for a parked upload to reach auth_expired (body retained on disk).
PARK_BUDGET_SECONDS: float = 15.0


async def _orphan_total(stack: E2EStack) -> int:
    """Read the ``orphan_body_count_total`` no-label bucket off the wire."""
    response = await stack.phantom_client.get_observability_counters()
    for counter in response.counters:
        if counter.name == COUNTER_ORPHAN_TOTAL:
            return counter.values.get("", 0)
    raise AssertionError(
        f"{COUNTER_ORPHAN_TOTAL} counter missing from the observability surface; "
        f"present counters={[c.name for c in response.counters]}"
    )


async def _submit_parked_upload(stack: E2EStack, *, sub: str) -> UUID:
    """Drive one real upload to ``auth_expired`` (body retained on disk).

    Injects a 401 on the upstream so the sender parks the row in
    ``auth_expired`` - a non-terminal state whose body is retained per
    the suite's retention defaults - giving the negative case a body
    produced by the real admission pipeline (not a hand-planted file)
    that the janitor must leave alone because its row is live.
    """
    from phantom_emulator.failure.injection import FailurePolicy, FailureScope

    from tests.e2e._driver import build_in_memory_upload_envelope

    stack.emulator.inject_failure(
        FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields default; mypy lacks the pydantic plugin
            scope=FailureScope.GLOBAL,
            auth_401_after_n_calls=0,
        ),
    )
    chain_id = uuid4()
    request = build_create_file_request(file_name=f"orphan-neg-{chain_id.hex[:12]}")
    request.metadata.key_value_store["phantom_local_uuid"] = str(chain_id)
    envelope, _ = build_in_memory_upload_envelope(
        request=request,
        files_api_base=stack.emulator_url,
        local_uuid=chain_id,
    )
    await stack.phantom_client.submit_chain(
        envelope,
        body_refs={BODY_REF_NAME: OWNED_BODY_BYTES},
        uid=sub,
        auth_token=f"Bearer {stack.fake_security_token(sub=sub)}",
    )
    await assert_chain_reaches_state(
        stack.phantom_client,
        chain_id,
        state="auth_expired",
        timeout_seconds=PARK_BUDGET_SECONDS,
    )
    stack.emulator.clear_failures()
    return chain_id


async def test_janitor_collects_orphan_and_spares_owned_body() -> None:
    """A planted orphan body is swept; an owned body survives.

    Boots ``all_disk`` with a tight sweep cadence, seeds a real
    ``auth_expired`` upload whose body lives on disk under a live row,
    plants a second body file whose chain_id has NO ``uploads`` row, and
    asserts: (1) the live janitor collects the orphan within its
    two-sweep confirmation and ``orphan_body_count_total`` bumps on the
    admin wire, the orphan file is gone; (2) the owned body file is still
    present after a further sweep - the live-row guard spared it.
    """
    sub = "00000000-0000-0000-0000-00000000001b"
    stack = await boot_stack(
        config_overrides={
            "storage": {
                "body_store": {
                    "mode": "all_disk",
                    "body_orphan_sweep_seconds": FAST_SWEEP_SECONDS,
                },
            },
        },
    )
    try:
        instance = stack.get_instance("primary")

        # Negative-case fixture: a real upload parked in auth_expired with
        # its body retained on disk under a LIVE row.
        owned_chain_id = await _submit_parked_upload(stack, sub=sub)
        assert await instance.body_store.has_body_ref(owned_chain_id, BODY_REF_NAME), (
            "the owned upload's body should be on disk before the janitor runs"
        )

        # Baseline: no orphans collected yet on a healthy runtime.
        baseline = await _orphan_total(stack)
        assert baseline == 0, (
            f"orphan_body_count_total was already non-zero ({baseline}) before planting"
        )

        # Plant the orphan: a body file whose chain_id has no uploads row.
        orphan_chain_id = uuid4()
        await instance.body_store.put(orphan_chain_id, {BODY_REF_NAME: ORPHAN_BODY_BYTES})
        assert await instance.body_store.has_body_ref(orphan_chain_id, BODY_REF_NAME), (
            "planted orphan body should exist on disk before the sweep"
        )
        assert await instance.store.get(orphan_chain_id) is None, (
            "planted orphan must have NO uploads row, else it is not an orphan"
        )
        logger.info("planted orphan body chain_id=%s with no owning row", orphan_chain_id)

        # The live janitor's two-sweep confirmation collects the orphan
        # and bumps the wire counter.
        async def _orphan_collected() -> bool:
            return await _orphan_total(stack) > baseline

        await await_until(
            _orphan_collected,
            timeout_seconds=COLLECT_BUDGET_SECONDS,
            message=(
                "the live BodyOrphanJanitor never surfaced the planted orphan on the "
                "admin observability wire (orphan_body_count_total did not bump)"
            ),
        )

        # The orphan file is actually gone from disk.
        assert not await instance.body_store.has_body_ref(orphan_chain_id, BODY_REF_NAME), (
            "orphan_body_count_total bumped but the orphan body file is still on disk"
        )

        # Negative case, no extra waiting needed. The janitor walks every
        # on-disk body each sweep, so the owned body was offered to the
        # SAME two-plus sweeps that just collected the orphan. The orphan
        # bump is therefore proof the live-row guard has already had its
        # chance on the owned body - and it must still be on disk, and the
        # counter must read exactly one removal (the orphan, not the owned
        # row).
        assert await instance.body_store.has_body_ref(owned_chain_id, BODY_REF_NAME), (
            f"the janitor erroneously collected the owned body chain_id={owned_chain_id} "
            "whose uploads row is live (the live-row guard failed)"
        )
        assert await _orphan_total(stack) == baseline + 1, (
            "orphan_body_count_total should report exactly one removal (the planted orphan); "
            "a different count means the owned-row body was swept too"
        )
    finally:
        await stack.tear_down()
