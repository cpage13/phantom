"""The attempting-replay refusal over a live wire (R1-1 fix probe).

Round 2 adversary, judging the round 1 defender fix in substance. The
contract tier pins the envelope by inserting an ``attempting`` row
directly; this module reaches the same refusal the way an operator
does: a REAL sender drives a row against a stalled upstream (the
emulator sleeps the metadata-create handler), the admin replay arrives
while the row is genuinely in flight, and the SDK must raise the typed
``PhantomConflictError`` carrying ``replay_refused_attempting``. The
refusal must leave the in-flight attempt to win: after the stall
clears, the SAME attempt completes the delivery, the upstream sees
exactly one upload, and ``sent_at`` lands exactly once.
"""

from __future__ import annotations

import secrets
from uuid import UUID

import pytest
from phantom_client import ChainAdminDetail, PhantomClient, PhantomConflictError
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from ._driver import PhantomDriver
from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until

pytestmark = pytest.mark.e2e

# Pre-handler sleep injected into the upstream metadata-create handler.
# Long enough that the row is observably ``attempting`` while the admin
# replay lands (poll cadence is tens of ms), comfortably below the
# sender's 30 s per-request ceiling so the FIRST attempt still succeeds
# once the sleep elapses (the probe needs the in-flight attempt to win).
_CREATE_STALL_MS: int = 8000

# Body size for the probe upload; small keeps the lane quick.
_PROBE_BODY_BYTES: int = 1024

# Budget for the claim + stalled create to surface ``attempting`` on the
# admin detail (claim poll cadence is 100 ms in the e2e config).
_ATTEMPTING_BUDGET_SECONDS: float = 10.0

# Budget for the stalled attempt to finish delivering after the stall
# elapses (stall + delivery + admin visibility).
_DELIVERED_BUDGET_SECONDS: float = 30.0

# Reaper interval override that effectively disables the reaper so the
# succeeded row stays visible for the post-delivery asserts.
_REAPER_DISABLED_INTERVAL_SECONDS: int = 3600

# The error code the typed conflict must carry (ADR-017 row, R1-1).
_EXPECTED_CODE: str = "replay_refused_attempting"


async def _await_attempting(pc: PhantomClient, chain_id: UUID) -> ChainAdminDetail:
    """Poll the admin detail until the row is ``attempting``; return it."""
    snapshots: list[ChainAdminDetail] = []

    async def _is_attempting() -> bool:
        latest = await pc.get_upload(chain_id)
        snapshots.append(latest)
        return latest.state == "attempting"

    await await_until(
        _is_attempting,
        timeout_seconds=_ATTEMPTING_BUDGET_SECONDS,
        message="the stalled row never surfaced as attempting on the admin detail",
    )
    return snapshots[-1]


async def test_replay_of_attempting_row_is_typed_conflict_over_the_wire() -> None:
    """A live in-flight row refuses replay with the typed conflict (R1-1)."""
    stack: E2EStack = await boot_stack(
        config_overrides={
            "retention": {"reaper_interval_seconds": _REAPER_DISABLED_INTERVAL_SECONDS},
        },
    )
    try:
        pc: PhantomClient = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                scope=FailureScope.UPSTREAM_FILES_CREATE,
                latency_ms=_CREATE_STALL_MS,
            )
        )
        driver = PhantomDriver(
            pc,
            files_api=stack.emulator_url,
            get_security_token=stack.fake_security_token,
        )
        result = await driver.in_memory_upload(
            build_create_file_request(file_name="replay-attempting-probe.bin"),
            secrets.token_bytes(_PROBE_BODY_BYTES),
        )

        # The sender claims the row and parks inside the stalled create.
        await _await_attempting(pc, result.id)

        # The operator's replay against the in-flight row: the SDK must
        # raise the typed conflict with the canonical code, not the
        # envelope-parse failure the pre-fix raw body produced.
        with pytest.raises(PhantomConflictError) as excinfo:
            await pc.replay(result.id)
        assert excinfo.value.error_code == _EXPECTED_CODE
        assert str(result.id) in str(excinfo.value)

        # The refusal touched nothing: the row is still the sender's.
        mid_flight = await pc.get_upload(result.id)
        assert mid_flight.state == "attempting"
        assert mid_flight.sent_at is None

        # The in-flight attempt wins: once the stall elapses the SAME
        # attempt delivers; the upstream saw exactly one upload and the
        # stamp lands exactly once. A clobbering replay would have
        # re-queued mid-flight and doubled the create.
        emulator.clear_failures()
        await assert_chain_reaches_state(
            pc, result.id, state="succeeded", timeout_seconds=_DELIVERED_BUDGET_SECONDS
        )
        delivered = await pc.get_upload(result.id)
        assert delivered.sent_at is not None
        assert len(emulator.received()) == 1, (
            "the upstream must see exactly one completed upload; a second "
            "entry means the refused replay clobbered the in-flight attempt"
        )
    finally:
        await stack.tear_down()
