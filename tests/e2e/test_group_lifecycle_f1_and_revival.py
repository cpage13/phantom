"""Group lifecycle over the wire: the F1 corrupted-member case and token-push revival.

Cycle-7 plan 06_09 task 7.1(a). The SDK plumbing for the group surface
(submit with grouping options, rollup fields, the poller's mid-flight
flip, both identifier lookups) is pinned by
``test_sdk_group_and_lookups.py``; this module closes the two lifecycle
legs that file explicitly deferred to phase 7:

* The F1 regression END TO END: a group where one member lands
  ``corrupted`` while every other member ``succeeded`` MUST report
  ``all_finished=true`` (corrupted is finished; the rollup's
  finished rule is structural: no member still queued/attempting).
* Token-push revival: a member parked in ``auth_expired`` counts
  finished, so the group reads ``all_finished=true``; an admin token
  push wakes the member, the rollup honestly flips BACK to
  ``all_finished=false`` while the member moves again, then returns to
  ``all_finished=true`` when it delivers.

Both tests drive the real service + emulator stack purely over HTTP
(SDK admin surface), no store-level shortcuts except the F1 body
vandalism, which mirrors ``test_multipart_corrupted.py``'s established
recipe (delete the on-disk body so the sender's body-read integrity
check fires).
"""

from __future__ import annotations

import secrets
from pathlib import Path
from uuid import uuid4

import pytest
from phantom_client import PhantomClient, SubmitOptions
from phantom_emulator.failure.injection import FailurePolicy, FailureScope

from ._driver import DriverUploadResult, PhantomDriver
from .helpers.assertions import assert_chain_reaches_state
from .helpers.payloads import build_create_file_request
from .helpers.stack import E2EStack, boot_stack
from .helpers.timing import await_until

pytestmark = pytest.mark.e2e

# Members in the F1 group: one corrupted victim plus TWO succeeded
# siblings, the smallest group where "the rest succeeded" is plural.
F1_GROUP_SIZE: int = 3

# Per-member body size: large enough that the all_disk body write is a
# real file (so deleting it is a real vandalism), small enough that the
# burst stays quick in the default lane.
PER_MEMBER_BODY_BYTES: int = 4096

# 5xx error rate for hold-down policies. 1.0 = every upstream call fails.
FORCE_5XX_RATE: float = 1.0

# 401-after-N-calls threshold that makes EVERY call 401 (parks rows in
# auth_expired immediately).
AUTH_401_FROM_FIRST_CALL: int = 0

# Budget for a row to land its body on disk in all_disk mode (written
# at admission; this only bounds a pathological stall).
ON_DISK_BUDGET_SECONDS: float = 10.0

# Budget for one member to reach a terminal state once the upstream is
# healthy. The corrupted victim needs a retry beat or two (e2e retry
# cadence is fixed_intervals [0, 1, 2, 5, 10]).
TERMINAL_BUDGET_SECONDS: float = 30.0

# Budget for a member to park in auth_expired once every call 401s.
AUTH_EXPIRED_BUDGET_SECONDS: float = 10.0

# Budget for the rollup to flip after a token push (kicker wake +
# first re-attempt) or after the upstream heals (retry cadence above).
ROLLUP_FLIP_BUDGET_SECONDS: float = 20.0

# Poll interval for rollup observations; tight enough to catch the
# moving window, gentle enough to stay off the admin endpoint's back.
ROLLUP_POLL_INTERVAL_SECONDS: float = 0.1

# Reaper interval override that effectively disables the reaper for the
# test's lifetime, so succeeded rows survive the multi-step assertions
# (mirrors test_e2e_32's override).
REAPER_DISABLED_INTERVAL_SECONDS: int = 3600


def _driver_for(stack: E2EStack) -> PhantomDriver:
    """Build the public test driver bound to ``stack``."""
    return PhantomDriver(
        stack.phantom_client,
        files_api=stack.emulator_url,
        get_security_token=stack.fake_security_token,
    )


async def test_group_with_one_corrupted_member_reports_all_finished(tmp_path: Path) -> None:
    """F1 end to end: one corrupted member, the rest succeeded, all_finished true.

    Boot in all_disk mode; hold the upstream down; submit a three-member
    group; delete the first member's on-disk body (the established
    corruption recipe); heal the upstream. The victim must surface
    ``corrupted``, the siblings ``succeeded``, and the rollup must report
    the group FINISHED: corrupted is a settled outcome, not a moving one.
    """
    stack = await boot_stack(
        tmp_path=tmp_path,
        config_overrides={"storage": {"body_store": {"mode": "all_disk"}}},
    )
    try:
        pc: PhantomClient = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        # Hold the upstream down so no member can deliver before the
        # vandalism lands.
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]  # FailurePolicy fields have defaults; mypy lacks pydantic plugin
                scope=FailureScope.GLOBAL,
                error_rate_5xx=FORCE_5XX_RATE,
            )
        )

        driver = _driver_for(stack)
        group_id = uuid4()
        members: list[DriverUploadResult] = []
        for idx in range(F1_GROUP_SIZE):
            result = await driver.in_memory_upload(
                build_create_file_request(file_name=f"f1-member-{idx}.bin"),
                secrets.token_bytes(PER_MEMBER_BODY_BYTES),
                options=SubmitOptions(  # type: ignore[call-arg]  # defaults invisible without the pydantic mypy plugin
                    group_id=group_id, order=idx
                ),
            )
            members.append(result)
        victim = members[0]
        survivors = members[1:]

        # Wait for the victim's body to land on disk (all_disk writes at
        # admission), then vandalize it: the sender's body-read check
        # will mark the chain corrupted on its next attempt.
        instance = stack.get_instance("primary")

        async def _victim_body_on_disk() -> bool:
            row = await instance.store.get(victim.id)
            return row is not None and row.body_location == "file"

        await await_until(
            _victim_body_on_disk,
            timeout_seconds=ON_DISK_BUDGET_SECONDS,
            message="victim row never reached body_location='file' in all_disk mode",
        )
        await instance.body_store.delete(victim.id)

        # Heal the upstream: survivors deliver, the victim corrupts.
        emulator.clear_failures()
        await assert_chain_reaches_state(
            pc, victim.id, state="corrupted", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )
        for survivor in survivors:
            await assert_chain_reaches_state(
                pc, survivor.id, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
            )

        # THE F1 case, over the wire: corrupted counts finished.
        rollup = await pc.get_group_status(group_id)
        assert rollup.all_finished is True, (
            "a group with one corrupted member and the rest succeeded must report "
            f"all_finished=true; got counts={rollup.counts_by_state!r}"
        )
        assert rollup.total == F1_GROUP_SIZE
        assert rollup.counts_by_state["corrupted"] == 1
        assert rollup.counts_by_state["succeeded"] == F1_GROUP_SIZE - 1
        assert rollup.counts_by_state["queued"] == 0
        assert rollup.counts_by_state["attempting"] == 0

        # Member truth: the victim carries no sent_at stamp (it was never
        # delivered); every survivor does, and the rollup's last_sent_at
        # is the max over the survivors' stamps.
        by_id = {m.chain_id: m for m in rollup.members}
        assert by_id[victim.id].state == "corrupted"
        assert by_id[victim.id].sent_at is None, "a corrupted member was never delivered"
        survivor_stamps = []
        for survivor in survivors:
            stamp = by_id[survivor.id].sent_at
            assert stamp is not None, "a succeeded member must carry its sent_at stamp"
            survivor_stamps.append(stamp)
        assert rollup.last_sent_at == max(survivor_stamps)
        assert rollup.first_received_at is not None
    finally:
        await stack.tear_down()


async def test_token_push_revival_flips_all_finished_false_then_true() -> None:
    """A token push revives an auth_expired member; the rollup flips honestly.

    One member succeeds, a second parks in ``auth_expired`` behind a 401
    wall: the group reads FINISHED (auth_expired is settled). Swap the
    401 wall for a 5xx wall and push a fresh token: the revived member
    re-enters the moving states and the rollup flips BACK to
    ``all_finished=false``. Heal the upstream: the member delivers and
    the rollup returns to ``all_finished=true`` with both members
    succeeded and stamped.
    """
    stack = await boot_stack(
        config_overrides={
            "retention": {"reaper_interval_seconds": REAPER_DISABLED_INTERVAL_SECONDS},
        },
    )
    try:
        pc: PhantomClient = stack.phantom_client
        emulator = stack.emulator
        emulator.clear_received()
        emulator.clear_failures()
        driver = _driver_for(stack)
        group_id = uuid4()

        # Member 0 delivers under the healthy upstream.
        first = await driver.in_memory_upload(
            build_create_file_request(file_name="revival-member-0.bin"),
            secrets.token_bytes(PER_MEMBER_BODY_BYTES),
            options=SubmitOptions(group_id=group_id, order=0),  # type: ignore[call-arg]
        )
        await assert_chain_reaches_state(
            pc, first.id, state="succeeded", timeout_seconds=TERMINAL_BUDGET_SECONDS
        )

        # Member 1 parks in auth_expired behind the 401 wall.
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]
                scope=FailureScope.GLOBAL,
                auth_401_after_n_calls=AUTH_401_FROM_FIRST_CALL,
            )
        )
        second = await driver.in_memory_upload(
            build_create_file_request(file_name="revival-member-1.bin"),
            secrets.token_bytes(PER_MEMBER_BODY_BYTES),
            options=SubmitOptions(group_id=group_id, order=1),  # type: ignore[call-arg]
        )
        await assert_chain_reaches_state(
            pc, second.id, state="auth_expired", timeout_seconds=AUTH_EXPIRED_BUDGET_SECONDS
        )

        # auth_expired counts FINISHED: the group reads all_finished=true.
        parked = await pc.get_group_status(group_id)
        assert parked.all_finished is True, (
            "an auth_expired member is settled; the group must read finished, "
            f"got counts={parked.counts_by_state!r}"
        )
        assert parked.counts_by_state["succeeded"] == 1
        assert parked.counts_by_state["auth_expired"] == 1

        # Swap the 401 wall for a 5xx wall in ONE policy replacement
        # (policies key on scope, so this cannot leave a healthy gap),
        # then push a fresh bearer into the one (endpoint, uid) slot.
        # The revived member wakes into the 5xx wall and stays moving,
        # which makes the all_finished=false window deterministic.
        emulator.inject_failure(
            FailurePolicy(  # type: ignore[call-arg]
                scope=FailureScope.GLOBAL,
                error_rate_5xx=FORCE_5XX_RATE,
            )
        )
        tokens = await pc.list_tokens()
        assert tokens, "the parked member must have minted a token-cache slot"
        slot = tokens[0]
        await pc.push_token(
            endpoint=slot.endpoint,
            uid=slot.uid,
            token=stack.fake_security_token(),
        )

        # The rollup must flip back to unfinished while the member moves.
        async def _rollup_unfinished() -> bool:
            rollup = await pc.get_group_status(group_id)
            return rollup.all_finished is False

        await await_until(
            _rollup_unfinished,
            timeout_seconds=ROLLUP_FLIP_BUDGET_SECONDS,
            poll_interval_seconds=ROLLUP_POLL_INTERVAL_SECONDS,
            message="the token push never flipped the rollup back to all_finished=false",
        )
        mid = await pc.get_group_status(group_id)
        revived = {m.chain_id: m for m in mid.members}[second.id]
        assert revived.state in {"queued", "attempting"}, (
            f"the revived member must be moving again, got state={revived.state!r}"
        )

        # Heal the upstream: the revived member delivers and the group
        # finishes for real.
        emulator.clear_failures()

        async def _rollup_finished() -> bool:
            rollup = await pc.get_group_status(group_id)
            return rollup.all_finished is True

        await await_until(
            _rollup_finished,
            timeout_seconds=ROLLUP_FLIP_BUDGET_SECONDS,
            poll_interval_seconds=ROLLUP_POLL_INTERVAL_SECONDS,
            message="the revived member never delivered after the upstream healed",
        )
        final = await pc.get_group_status(group_id)
        assert final.counts_by_state["succeeded"] == final.total == 2
        assert final.last_sent_at is not None
        for member in final.members:
            assert member.sent_at is not None, "both delivered members carry sent_at"
    finally:
        await stack.tear_down()
